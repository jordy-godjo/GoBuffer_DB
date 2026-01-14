package disk

import (
	"os"
	"path/filepath"
	"testing"

	"malzahar-project/Projet_BDDA/config"
)

func TestDiskManagerLifecycle(t *testing.T) {
	dir := t.TempDir()
	cfg := config.NewDBConfigWithParams(dir, 1024, 4)
	dm := NewDiskManager(cfg)
	if err := dm.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	// allocate a page
	pid, err := dm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage: %v", err)
	}
	// write page
	data := []byte("hello")
	if err := dm.WritePage(pid, data); err != nil {
		t.Fatalf("WritePage: %v", err)
	}
	// read back
	got, err := dm.ReadPage(pid)
	if err != nil {
		t.Fatalf("ReadPage: %v", err)
	}
	if string(got[:5]) != "hello" {
		t.Fatalf("unexpected data: %q", got[:5])
	}
	// free
	if err := dm.FreePage(pid); err != nil {
		t.Fatalf("FreePage: %v", err)
	}
	// check bitmap file exists
	bmp := filepath.Join(dir, "BinData", "Data0.bitmap")
	if _, err := os.Stat(bmp); err != nil {
		t.Fatalf("bitmap missing: %v", err)
	}
}
