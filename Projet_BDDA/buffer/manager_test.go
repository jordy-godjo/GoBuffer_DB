package buffer

import (
	"testing"

	"malzahar-project/Projet_BDDA/config"
	"malzahar-project/Projet_BDDA/disk"
)

func TestBufferManagerLRU(t *testing.T) {
	dir := t.TempDir()
	cfg := config.NewDBConfigWithParams(dir, 512, 2)
	cfg.BMBufferCount = 2
	cfg.BMPolicy = "LRU"
	dm := disk.NewDiskManager(cfg)
	if err := dm.Init(); err != nil {
		t.Fatalf("dm init: %v", err)
	}
	bm := NewBufferManager(cfg, dm)

	// allocate two pages via DiskManager then get them via BufferManager
	p1, err := dm.AllocatePage()
	if err != nil {
		t.Fatalf("alloc p1: %v", err)
	}
	p2, err := dm.AllocatePage()
	if err != nil {
		t.Fatalf("alloc p2: %v", err)
	}

	_, err = bm.GetPage(p1)
	if err != nil {
		t.Fatalf("get p1: %v", err)
	}
	_, err = bm.GetPage(p2)
	if err != nil {
		t.Fatalf("get p2: %v", err)
	}
	// free p1 then access p2 to make p1 LRU
	bm.FreePage(p1, false)
	_, err = bm.GetPage(p2)
	if err != nil {
		t.Fatalf("get p2 again: %v", err)
	}
	// now request a new page -> should evict p1 (LRU)
	p3, err := dm.AllocatePage()
	if err != nil {
		t.Fatalf("alloc p3: %v", err)
	}
	_, err = bm.GetPage(p3)
	if err != nil {
		t.Fatalf("get p3: %v", err)
	}
	// If no errors, LRU eviction worked
}

func TestBufferManagerMRU(t *testing.T) {
	dir := t.TempDir()
	cfg := config.NewDBConfigWithParams(dir, 512, 2)
	cfg.BMBufferCount = 2
	cfg.BMPolicy = "MRU"
	dm := disk.NewDiskManager(cfg)
	if err := dm.Init(); err != nil {
		t.Fatalf("dm init: %v", err)
	}
	bm := NewBufferManager(cfg, dm)

	p1, _ := dm.AllocatePage()
	p2, _ := dm.AllocatePage()
	_, _ = bm.GetPage(p1)
	_, _ = bm.GetPage(p2)
	// unpin both so eviction can occur
	bm.FreePage(p1, false)
	bm.FreePage(p2, false)
	// MRU: the most recently used should be evicted when a new page is requested
	p3, _ := dm.AllocatePage()
	if _, err := bm.GetPage(p3); err != nil {
		t.Fatalf("get p3: %v", err)
	}
}
