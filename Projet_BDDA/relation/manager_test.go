package relation

import (
	"testing"

	"malzahar-project/Projet_BDDA/buffer"
	"malzahar-project/Projet_BDDA/config"
	"malzahar-project/Projet_BDDA/disk"
)

func setup(t *testing.T) (*RelationManager, func()) {
	dir := t.TempDir()
	cfg := config.NewDBConfigWithParams(dir, 512, 4)
	dm := disk.NewDiskManager(cfg)
	if err := dm.Init(); err != nil {
		t.Fatalf("dm init: %v", err)
	}
	bm := buffer.NewBufferManager(cfg, dm)
	cols := []ColumnInfo{{Name: "a", Kind: KindInt}, {Name: "b", Kind: KindChar, Size: 8}}
	rel := NewRelation("r_test", cols)
	rm, err := NewRelationManager(rel, dm, bm)
	if err != nil {
		t.Fatalf("new rm: %v", err)
	}
	return rm, func() {
		_ = bm.FlushBuffers()
		_ = dm.Finish()
		// temp dir will be removed by the testing framework
	}
}

func TestInsertAndReadMany(t *testing.T) {
	rm, cleanup := setup(t)
	defer cleanup()
	// insert several records to force multiple pages
	total := 20
	var ids []RecordId
	for i := 0; i < total; i++ {
		t.Logf("insert loop i=%d", i)
		rec := NewRecord("123", "hello")
		id, err := rm.InsertRecord(rec)
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
		ids = append(ids, id)
	}
	// read all
	recs, err := rm.GetAllRecords()
	if err != nil {
		t.Fatalf("get all: %v", err)
	}
	if len(recs) != total {
		t.Fatalf("expected %d records, got %d", total, len(recs))
	}
}

func TestDeleteAndReuse(t *testing.T) {
	rm, cleanup := setup(t)
	defer cleanup()
	// insert 10
	var ids []RecordId
	for i := 0; i < 10; i++ {
		rec := NewRecord("1", "x")
		id, err := rm.InsertRecord(rec)
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
		ids = append(ids, id)
	}
	// delete 5
	for i := 0; i < 5; i++ {
		if err := rm.DeleteRecord(ids[i]); err != nil {
			t.Fatalf("delete: %v", err)
		}
	}
	// insert 5 more - should reuse freed slots
	for i := 0; i < 5; i++ {
		if _, err := rm.InsertRecord(NewRecord("2", "y")); err != nil {
			t.Fatalf("insert reuse: %v", err)
		}
	}
	recs, err := rm.GetAllRecords()
	if err != nil {
		t.Fatalf("get all: %v", err)
	}
	if len(recs) != 10 {
		t.Fatalf("expected 10 records after reuse, got %d", len(recs))
	}
}

func TestPrependDoesNotSelfLoop(t *testing.T) {
	rm, cleanup := setup(t)
	defer cleanup()
	// create a data page
	pid, err := rm.addDataPage()
	if err != nil {
		t.Fatalf("addDataPage: %v", err)
	}
	// first prepend should set header.firstWithSpace = pid
	if err := rm.prependToWithSpace(pid); err != nil {
		t.Fatalf("first prepend: %v", err)
	}
	// second prepend should be a no-op and must not create a self-loop
	if err := rm.prependToWithSpace(pid); err != nil {
		t.Fatalf("second prepend: %v", err)
	}
	// read page's next pointer and ensure it's not pointing to itself
	next, err := rm.pageNext(pid)
	if err != nil {
		t.Fatalf("pageNext: %v", err)
	}
	if next == pid {
		t.Fatalf("page next points to itself (self-loop) %v", pid)
	}
}
