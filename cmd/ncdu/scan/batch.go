package scan

import (
	"context"
	"fmt"
	"path"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/dirtree"
	"github.com/rclone/rclone/fs/walk"
)

// ScanBatch scans f completely before publishing a root, using recursive listing
// when available. It applies filters and max depth, sends one completion error
// (nil on success), and sends no incremental updates. On failure no root is sent.
func ScanBatch(ctx context.Context, f fs.Fs) (chan *Dir, chan error, chan struct{}) {
	root := make(chan *Dir, 1)
	errChan := make(chan error, 1)
	updated := make(chan struct{})
	go func() {
		tree, err := walk.NewDirTree(ctx, f, "", false, fs.GetConfig(ctx).MaxDepth)
		var d *Dir
		if err == nil {
			d, err = batchTree(ctx, tree)
		}
		if err == nil {
			err = ctx.Err()
		}
		if err != nil {
			errChan <- fmt.Errorf("ncdu listing failed: %w", err)
			return
		}
		root <- d
		errChan <- nil
	}()
	return root, errChan, updated
}

// batchTree builds a private tree, then accumulates each child's totals once.
func batchTree(ctx context.Context, tree dirtree.DirTree) (*Dir, error) {
	if len(tree) == 0 {
		tree = dirtree.DirTree{"": nil}
	}
	names := tree.Dirs()
	dirs := make(map[string]*Dir, len(tree))
	for _, name := range names {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		d := &Dir{path: name, entries: tree[name], dirs: make(map[string]*Dir)}
		seen := make(map[string]bool)
		for _, entry := range d.entries {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			switch x := entry.(type) {
			case fs.Object:
				d.count++
				if size := x.Size(); size < 0 {
					d.countUnknownSize++
				} else {
					d.size += size
				}
			case fs.Directory:
				if seen[x.Remote()] {
					return nil, fmt.Errorf("duplicate directory path %q in batch listing", x.Remote())
				}
				seen[x.Remote()] = true
			}
		}
		dirs[name] = d
	}
	root := dirs[""]
	if root == nil {
		return nil, fmt.Errorf("couldn't find root in batch listing")
	}
	// Reverse lexical order places descendants before their parents.
	for i := len(names) - 1; i >= 0; i-- {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		d := dirs[names[i]]
		if d == root {
			continue
		}
		parentPath := path.Dir(d.path)
		if parentPath == "." {
			parentPath = ""
		}
		parent := dirs[parentPath]
		if parent == nil {
			return nil, fmt.Errorf("couldn't find parent for %q", d.path)
		}
		d.parent = parent
		parent.dirs[path.Base(d.path)] = d
		parent.size += d.size
		parent.count += d.count
		parent.countUnknownSize += d.countUnknownSize
	}
	return root, nil
}
