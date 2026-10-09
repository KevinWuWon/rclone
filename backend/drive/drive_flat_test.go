package drive

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/filter"
	"github.com/rclone/rclone/lib/dircache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/drive/v3"
)

func flatTestFile(id, name, parent, mime string, size int64) *drive.File {
	item := &drive.File{Id: id, Name: name, Parents: []string{parent}, MimeType: mime, Size: size, ModifiedTime: "2026-01-01T00:00:00Z"}
	if mime == "text/plain" {
		item.Md5Checksum = "d41d8cd98f00b204e9800998ecf8427e"
	}
	return item
}

func flatTestFs(t *testing.T, handler http.Handler) *Fs {
	t.Helper()
	f := newPermissionTestFs(t, handler)
	f.ci = fs.GetConfig(context.Background())
	f.dirResourceKeys = new(sync.Map)
	f.grouping = listRGrouping
	f.rootFolderID = "root-id"
	f.dirCache = dircache.New("", f.rootFolderID, f)
	f.opt.ListRFlat = true
	return f
}

func flatTestEntries(t *testing.T, f *Fs, ctx context.Context, dir string) (fs.DirEntries, error) {
	t.Helper()
	var entries fs.DirEntries
	err := f.ListR(ctx, dir, func(batch fs.DirEntries) error {
		entries = append(entries, batch...)
		return nil
	})
	return entries, err
}

func TestListRFlatInventory(t *testing.T) {
	file := flatTestFile("file", "same", "nested", "text/plain", 3)
	second := flatTestFile("second", "same", "nested", "text/plain", 7)
	folder := flatTestFile("nested", "folder", "root-id", driveFolderType, 0)
	empty := flatTestFile("empty", "empty", "root-id", driveFolderType, 0)
	outside := flatTestFile("outside", "outside", "other-root", "text/plain", 99)
	pages := 0
	f := flatTestFs(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/files", r.URL.Path)
		assert.NotContains(t, r.URL.Query().Get("q"), "parents")
		assert.NotContains(t, r.URL.Query().Get("q"), "modifiedTime")
		assert.Equal(t, "trashed=false", r.URL.Query().Get("q"))
		assert.Contains(t, r.URL.Query().Get("fields"), "incompleteSearch")
		pages++
		result := drive.FileList{Files: []*drive.File{file, outside}, NextPageToken: "next"}
		if pages == 2 {
			assert.Equal(t, "next", r.URL.Query().Get("pageToken"))
			result = drive.FileList{Files: []*drive.File{folder, empty, second, file}}
		}
		require.NoError(t, json.NewEncoder(w).Encode(&result))
	}))
	ctx, fi := filter.AddConfig(context.Background())
	fi.ModTimeFrom = time.Now()
	ctx = filter.SetUseFilter(ctx, true)
	entries, err := flatTestEntries(t, f, ctx, "")
	require.NoError(t, err)
	assert.Equal(t, 2, pages)
	require.Len(t, entries, 4)
	var names []string
	var size int64
	for _, entry := range entries {
		names = append(names, entry.Remote())
		if _, ok := entry.(fs.Object); ok {
			size += entry.Size()
		}
	}
	assert.ElementsMatch(t, []string{"folder", "folder/same", "folder/same", "empty"}, names)
	assert.Equal(t, int64(10), size)
}

func TestListRFlatSubtreeAndShortcut(t *testing.T) {
	folder := flatTestFile("folder", "folder", "root-id", driveFolderType, 0)
	file := flatTestFile("file", "file", "folder", "text/plain", 5)
	target := flatTestFile("target", "target", "outside", driveFolderType, 0)
	child := flatTestFile("child", "child", "target", "text/plain", 8)
	shortcut := flatTestFile("shortcut", "alias", "folder", shortcutMimeType, 0)
	shortcut.ShortcutDetails = &drive.FileShortcutDetails{TargetId: "target", TargetMimeType: driveFolderType}
	calls := 0
	f := flatTestFs(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		assert.Equal(t, "/files", r.URL.Path, "shortcut resolution must not fetch targets")
		require.NoError(t, json.NewEncoder(w).Encode(&drive.FileList{Files: []*drive.File{child, shortcut, file, target, folder}}))
	}))
	require.NoError(t, f.dirCache.FindRoot(context.Background(), false))
	f.dirCache.Put("folder", "folder")
	entries, err := flatTestEntries(t, f, context.Background(), "folder")
	require.NoError(t, err)
	assert.Equal(t, 1, calls)
	var names []string
	for _, entry := range entries {
		names = append(names, entry.Remote())
	}
	assert.ElementsMatch(t, []string{"folder/file", "folder/alias", "folder/alias/child"}, names)
	assert.Equal(t, "target", target.Id, "shortcut resolution must not mutate inventory")
	assert.Equal(t, "target", target.Name)
}

func TestListRFlatFailures(t *testing.T) {
	for _, test := range []struct {
		name       string
		items      []*drive.File
		incomplete bool
		want       string
	}{
		{"incomplete", nil, true, "incomplete"},
		{"duplicate directories", []*drive.File{flatTestFile("a", "same", "root-id", driveFolderType, 0), flatTestFile("b", "same", "root-id", driveFolderType, 0)}, false, "duplicate directory"},
		{"missing shortcut details", []*drive.File{flatTestFile("s", "alias", "root-id", shortcutMimeType, 0)}, false, "no target details"},
		{"missing target", []*drive.File{{Id: "s", Name: "alias", Parents: []string{"root-id"}, MimeType: shortcutMimeType, ShortcutDetails: &drive.FileShortcutDetails{TargetId: "missing"}}}, false, "outside the inventory"},
		{"cycle", []*drive.File{flatTestFile("a", "a", "root-id", driveFolderType, 0), {Id: "s", Name: "loop", Parents: []string{"a"}, MimeType: shortcutMimeType, ShortcutDetails: &drive.FileShortcutDetails{TargetId: "a"}}}, false, "cycle"},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := flatTestFs(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.NoError(t, json.NewEncoder(w).Encode(&drive.FileList{Files: test.items, IncompleteSearch: test.incomplete}))
			}))
			entries, err := flatTestEntries(t, f, context.Background(), "")
			require.ErrorContains(t, err, test.want)
			assert.Empty(t, entries, "failed inventories must not invoke callbacks")
		})
	}
}

func TestListRFlatErrorsAndOptions(t *testing.T) {
	t.Run("http error", func(t *testing.T) {
		f := flatTestFs(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "denied", http.StatusForbidden) }))
		entries, err := flatTestEntries(t, f, context.Background(), "")
		require.Error(t, err)
		assert.Empty(t, entries)
	})
	t.Run("cancel", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		f := flatTestFs(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Error("unexpected request") }))
		_, err := flatTestEntries(t, f, ctx, "")
		require.ErrorIs(t, err, context.Canceled)
	})
	t.Run("callback", func(t *testing.T) {
		f := flatTestFs(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.NoError(t, json.NewEncoder(w).Encode(&drive.FileList{Files: []*drive.File{flatTestFile("a", "a", "root-id", "text/plain", 1)}}))
		}))
		want := errors.New("callback error")
		err := f.ListR(context.Background(), "", func(fs.DirEntries) error { return want })
		require.ErrorIs(t, err, want)
	})
	t.Run("skip shortcuts", func(t *testing.T) {
		f := flatTestFs(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.NoError(t, json.NewEncoder(w).Encode(&drive.FileList{Files: []*drive.File{flatTestFile("s", "alias", "root-id", shortcutMimeType, 0)}}))
		}))
		f.opt.SkipShortcuts = true
		entries, err := flatTestEntries(t, f, context.Background(), "")
		require.NoError(t, err)
		assert.Empty(t, entries)
	})
	for _, option := range []string{"shared", "starred", "trashed", "appdata"} {
		t.Run(option, func(t *testing.T) {
			f := flatTestFs(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Error("unexpected request") }))
			f.opt.SharedWithMe = option == "shared"
			f.opt.StarredOnly = option == "starred"
			f.opt.TrashedOnly = option == "trashed"
			if option == "appdata" {
				f.rootFolderID = "appDataFolder"
			}
			_, err := flatTestEntries(t, f, context.Background(), "")
			require.ErrorContains(t, err, "does not support")
		})
	}
}

func TestListRFlatParity(t *testing.T) {
	items := []*drive.File{flatTestFile("a", "a", "root-id", driveFolderType, 0), flatTestFile("b", "b", "a", "text/plain", 9)}
	f := flatTestFs(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		selected := items
		if q := r.URL.Query().Get("q"); strings.Contains(q, "parents") {
			selected = nil
			for _, item := range items {
				if strings.Contains(q, fmt.Sprintf("'%s' in parents", item.Parents[0])) {
					selected = append(selected, item)
				}
			}
		}
		require.NoError(t, json.NewEncoder(w).Encode(&drive.FileList{Files: selected}))
	}))
	flat, err := flatTestEntries(t, f, context.Background(), "")
	require.NoError(t, err)
	f.opt.ListRFlat = false
	normal, err := flatTestEntries(t, f, context.Background(), "")
	require.NoError(t, err)
	names := func(entries fs.DirEntries) []string {
		var out []string
		for _, entry := range entries {
			out = append(out, fmt.Sprintf("%s:%d", entry.Remote(), entry.Size()))
		}
		return out
	}
	assert.ElementsMatch(t, names(normal), names(flat))
}

func TestListRFlatSharedDrive(t *testing.T) {
	for _, accessible := range []bool{false, true} {
		t.Run(fmt.Sprint(accessible), func(t *testing.T) {
			checked := false
			f := flatTestFs(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/files":
					assert.Equal(t, "drive", r.URL.Query().Get("corpora"))
					assert.Equal(t, "team-id", r.URL.Query().Get("driveId"))
					assert.Equal(t, "true", r.URL.Query().Get("includeItemsFromAllDrives"))
					require.NoError(t, json.NewEncoder(w).Encode(&drive.FileList{}))
				case "/drives/team-id":
					checked = true
					if accessible {
						require.NoError(t, json.NewEncoder(w).Encode(&drive.Drive{Id: "team-id", Name: "Team"}))
					} else {
						http.Error(w, "missing", http.StatusNotFound)
					}
				default:
					t.Errorf("unexpected request %q", r.URL.Path)
				}
			}))
			f.isTeamDrive = true
			f.opt.TeamDriveID = "team-id"
			entries, err := flatTestEntries(t, f, context.Background(), "")
			if accessible {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, "Shared Drive info")
			}
			assert.Empty(t, entries)
			assert.True(t, checked)
		})
	}
}

func TestListRFlatRootAlias(t *testing.T) {
	f := flatTestFs(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/files/root":
			require.NoError(t, json.NewEncoder(w).Encode(&drive.File{Id: "root-id"}))
		case "/files":
			assert.NotContains(t, r.URL.Query().Get("q"), "parents")
			require.NoError(t, json.NewEncoder(w).Encode(&drive.FileList{Files: []*drive.File{flatTestFile("a", "a", "root-id", "text/plain", 2)}}))
		default:
			t.Errorf("unexpected request %q", r.URL.Path)
		}
	}))
	f.rootFolderID = "root"
	f.dirCache = dircache.New("", "root", f)
	entries, err := flatTestEntries(t, f, context.Background(), "")
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "a", entries[0].Remote())
}

func TestListRFlatLaterPageError(t *testing.T) {
	f := flatTestFs(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("pageToken") != "" {
			http.Error(w, "denied", http.StatusForbidden)
			return
		}
		require.NoError(t, json.NewEncoder(w).Encode(&drive.FileList{Files: []*drive.File{flatTestFile("a", "a", "root-id", "text/plain", 2)}, NextPageToken: "next"}))
	}))
	entries, err := flatTestEntries(t, f, context.Background(), "")
	require.Error(t, err)
	assert.Empty(t, entries)
}
