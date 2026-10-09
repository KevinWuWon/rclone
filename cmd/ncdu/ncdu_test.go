//go:build !plan9 && !js && !aix

package ncdu

import (
	"context"
	"testing"
	"time"

	_ "github.com/rclone/rclone/backend/local"
	"github.com/rclone/rclone/fs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBatchFlag(t *testing.T) {
	flag := commandDefinition.Flags().Lookup("batch")
	require.NotNil(t, flag)
	assert.Equal(t, "false", flag.DefValue)
}

func TestUIScanMode(t *testing.T) {
	original := batch
	t.Cleanup(func() { batch = original })
	for _, mode := range []bool{false, true} {
		t.Run(map[bool]string{false: "incremental", true: "batch"}[mode], func(t *testing.T) {
			batch = mode
			f, err := fs.NewFs(context.Background(), "../tree/testfiles")
			require.NoError(t, err)
			u := NewUI(f)
			roots, errs, updates := u.scan()
			defer u.cancel()
			select {
			case err := <-errs:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("scan did not finish")
			}
			root := <-roots
			_, count := root.Attr()
			assert.Equal(t, int64(5), count)
			select {
			case <-updates:
				assert.False(t, mode, "batch mode must not publish incremental updates")
			default:
				assert.True(t, mode, "default mode must retain incremental updates")
			}
		})
	}
}
