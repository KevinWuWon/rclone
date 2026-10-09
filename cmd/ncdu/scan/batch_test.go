package scan

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/dirtree"
	"github.com/rclone/rclone/fs/filter"
	"github.com/rclone/rclone/fstest/mockdir"
	"github.com/rclone/rclone/fstest/mockfs"
	"github.com/rclone/rclone/fstest/mockobject"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func batchFs(t *testing.T, listR fs.ListRFn) fs.Fs {
	t.Helper()
	f, err := mockfs.NewFs(context.Background(), "batch", "", nil)
	require.NoError(t, err)
	f.Features().ListR = listR
	return f
}

func batchResult(t *testing.T, ctx context.Context, f fs.Fs) (*Dir, error) {
	t.Helper()
	roots, errs, updates := ScanBatch(ctx, f)
	var err error
	select {
	case err = <-errs:
	case <-time.After(5 * time.Second):
		t.Fatal("batch scan did not finish")
	}
	select {
	case <-updates:
		t.Fatal("batch scan sent an incremental update")
	default:
	}
	if err != nil {
		select {
		case <-roots:
			t.Fatal("failed batch scan published a root")
		default:
		}
		return nil, err
	}
	select {
	case root := <-roots:
		return root, nil
	default:
		t.Fatal("successful batch scan did not publish a root")
		return nil, nil
	}
}

func TestScanBatch(t *testing.T) {
	unknown := mockobject.New("a/b/unknown").WithContent([]byte("unknown"), mockobject.SeekModeNone)
	unknown.SetUnknownSize(true)
	calls := 0
	f := batchFs(t, func(ctx context.Context, dir string, callback fs.ListRCallback) error {
		calls++
		assert.Empty(t, dir)
		// Descendants may precede their directories, across multiple batches.
		if err := callback(fs.DirEntries{unknown, fileEntry("a/b/file", "hello")}); err != nil {
			return err
		}
		return callback(fs.DirEntries{mockdir.New("empty"), mockdir.New("a/b"), mockdir.New("a"), fileEntry("rootfile", "hi")})
	})
	root, err := batchResult(t, context.Background(), f)
	require.NoError(t, err)
	assert.Equal(t, 1, calls)
	assert.Empty(t, root.Path())
	assert.Nil(t, root.Parent())
	size, count := root.Attr()
	assert.Equal(t, int64(7), size)
	assert.Equal(t, int64(3), count)
	assert.Equal(t, int64(1), root.countUnknownSize)
	a, isDir := root.GetDir(indexByName(root.Entries(), "a"))
	require.True(t, isDir)
	require.NotNil(t, a)
	assert.Same(t, root, a.Parent())
	b, isDir := a.GetDir(indexByName(a.Entries(), "a/b"))
	require.True(t, isDir)
	require.NotNil(t, b)
	assert.Same(t, a, b.Parent())
	attrs, err := a.AttrI(indexByName(a.Entries(), "a/b"))
	require.NoError(t, err)
	assert.Equal(t, int64(5), attrs.Size)
	assert.Equal(t, int64(2), attrs.Count)
	assert.Equal(t, int64(1), attrs.CountUnknownSize)
	assert.True(t, attrs.Readable)
	empty, _ := root.GetDir(indexByName(root.Entries(), "empty"))
	require.NotNil(t, empty)
	assert.Empty(t, empty.Entries())

	b.Remove(indexByName(b.Entries(), "a/b/unknown"))
	assert.Zero(t, root.countUnknownSize)
	a.Remove(indexByName(a.Entries(), "a/b"))
	size, count = root.Attr()
	assert.Equal(t, int64(2), size)
	assert.Equal(t, int64(1), count)
}

func TestScanBatchEmpty(t *testing.T) {
	for _, recursive := range []bool{false, true} {
		t.Run(map[bool]string{false: "List", true: "ListR"}[recursive], func(t *testing.T) {
			var listR fs.ListRFn
			if recursive {
				listR = func(ctx context.Context, dir string, callback fs.ListRCallback) error { return nil }
			}
			root, err := batchResult(t, context.Background(), batchFs(t, listR))
			require.NoError(t, err)
			require.NotNil(t, root)
			assert.Empty(t, root.Entries())
			size, count := root.Attr()
			assert.Zero(t, size)
			assert.Zero(t, count)
		})
	}
}

func TestScanBatchFiltersAndDepth(t *testing.T) {
	ctx, ci := fs.AddConfig(context.Background())
	ci.MaxDepth = 2
	ctx, fi := filter.AddConfig(ctx)
	require.NoError(t, fi.AddRule("- excluded/**"))
	require.NoError(t, fi.AddRule("- **/*.tmp"))
	f := batchFs(t, func(ctx context.Context, dir string, callback fs.ListRCallback) error {
		return callback(fs.DirEntries{
			fileEntry("root", "a"), fileEntry("a/keep", "bb"), fileEntry("a/drop.tmp", "ccc"),
			fileEntry("a/deep/drop", "dddd"), fileEntry("excluded/drop", "eeeee"),
			mockdir.New("a"), mockdir.New("a/deep"), mockdir.New("excluded"),
		})
	})
	root, err := batchResult(t, ctx, f)
	require.NoError(t, err)
	size, count := root.Attr()
	assert.Equal(t, int64(3), size)
	assert.Equal(t, int64(2), count)
	assert.Equal(t, -1, indexByName(root.Entries(), "excluded"))
	a, _ := root.GetDir(indexByName(root.Entries(), "a"))
	require.NotNil(t, a)
	assert.Equal(t, -1, indexByName(a.Entries(), "a/drop.tmp"))
	deep, isDir := a.GetDir(indexByName(a.Entries(), "a/deep"))
	assert.True(t, isDir)
	if deep != nil {
		assert.Empty(t, deep.Entries(), "files below the depth limit must be excluded")
	}
}

func TestScanBatchWaitsForCompletion(t *testing.T) {
	listed := make(chan struct{})
	finish := make(chan struct{})
	f := batchFs(t, func(ctx context.Context, dir string, callback fs.ListRCallback) error {
		if err := callback(fs.DirEntries{fileEntry("file", "abc")}); err != nil {
			return err
		}
		close(listed)
		<-finish
		return nil
	})
	roots, errs, updates := ScanBatch(context.Background(), f)
	<-listed
	select {
	case <-roots:
		t.Error("published root before listing completed")
	case <-updates:
		t.Error("published incremental update")
	default:
	}
	close(finish)
	require.NoError(t, <-errs)
	root := <-roots
	size, count := root.Attr()
	assert.Equal(t, int64(3), size)
	assert.Equal(t, int64(1), count)
}

func TestScanBatchErrors(t *testing.T) {
	listingError := errors.New("listing failed")
	for _, cancel := range []bool{false, true} {
		t.Run(map[bool]string{false: "listing", true: "cancellation"}[cancel], func(t *testing.T) {
			ctx, cancelScan := context.WithCancel(context.Background())
			defer cancelScan()
			f := batchFs(t, func(ctx context.Context, dir string, callback fs.ListRCallback) error {
				if err := callback(fs.DirEntries{fileEntry("partial", "abc")}); err != nil {
					return err
				}
				if cancel {
					cancelScan()
					return nil // Even a backend ignoring cancellation cannot publish a root.
				}
				return listingError
			})
			_, err := batchResult(t, ctx, f)
			if cancel {
				assert.ErrorIs(t, err, context.Canceled)
			} else {
				assert.ErrorIs(t, err, listingError)
			}
		})
	}
}

func TestScanBatchDuplicateDirectories(t *testing.T) {
	f := batchFs(t, func(ctx context.Context, dir string, callback fs.ListRCallback) error {
		return callback(fs.DirEntries{mockdir.New("duplicate"), fileEntry("duplicate/f", "a"), mockdir.New("duplicate")})
	})
	_, err := batchResult(t, context.Background(), f)
	require.ErrorContains(t, err, `duplicate directory path "duplicate"`)
}

func TestBatchTreeValidation(t *testing.T) {
	for _, tc := range []struct {
		name string
		tree dirtree.DirTree
		want string
	}{
		{"missing root", dirtree.DirTree{"a": nil}, "couldn't find root"},
		{"missing parent", dirtree.DirTree{"": nil, "a/b": nil}, "couldn't find parent"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root, err := batchTree(context.Background(), tc.tree)
			assert.Nil(t, root)
			assert.ErrorContains(t, err, tc.want)
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	root, err := batchTree(ctx, nil)
	assert.Nil(t, root)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestScanBatchListFallback(t *testing.T) {
	f, err := fs.NewFs(context.Background(), "../../tree/testfiles")
	require.NoError(t, err)
	root, err := batchResult(t, context.Background(), f)
	require.NoError(t, err)
	size, count := root.Attr()
	assert.Zero(t, size)
	assert.Equal(t, int64(5), count)
	subdir, isDir := root.GetDir(indexByName(root.Entries(), "subdir"))
	require.True(t, isDir)
	require.NotNil(t, subdir)
	assert.Same(t, root, subdir.Parent())

	ctx, ci := fs.AddConfig(context.Background())
	ci.MaxDepth = 1
	f.Features().ListR = func(ctx context.Context, dir string, callback fs.ListRCallback) error {
		return errors.New("ListR must not be called with max depth 1")
	}
	root, err = batchResult(t, ctx, f)
	require.NoError(t, err)
	_, count = root.Attr()
	assert.Equal(t, int64(3), count)
	subdir, isDir = root.GetDir(indexByName(root.Entries(), "subdir"))
	assert.True(t, isDir)
	assert.Nil(t, subdir)
}

type batchErrorFs struct {
	fs.Fs
}

func (f batchErrorFs) List(ctx context.Context, dir string) (fs.DirEntries, error) {
	return nil, fs.ErrorDirNotFound
}

func TestScanBatchListError(t *testing.T) {
	f := batchErrorFs{batchFs(t, nil)}
	_, err := batchResult(t, context.Background(), f)
	assert.ErrorIs(t, err, fs.ErrorDirNotFound)
}

func TestScanBatchFilesFrom(t *testing.T) {
	ctx, ci := fs.AddConfig(context.Background())
	ci.NoTraverse = true
	ctx, fi := filter.AddConfig(ctx)
	require.NoError(t, fi.AddFile("selected"))
	f := batchFs(t, func(ctx context.Context, dir string, callback fs.ListRCallback) error {
		return errors.New("ListR must not be called with files-from and no-traverse")
	})
	f.(*mockfs.Fs).AddObject(fileEntry("selected", "abc"))
	f.(*mockfs.Fs).AddObject(fileEntry("excluded", "12345"))
	root, err := batchResult(t, ctx, f)
	require.NoError(t, err)
	size, count := root.Attr()
	assert.Equal(t, int64(3), size)
	assert.Equal(t, int64(1), count)
	require.Len(t, root.Entries(), 1)
	assert.Equal(t, "selected", root.Entries()[0].Remote())
}
