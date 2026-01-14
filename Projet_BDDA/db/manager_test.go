package db

import (
	"testing"

	"malzahar-project/Projet_BDDA/buffer"
	"malzahar-project/Projet_BDDA/config"
	"malzahar-project/Projet_BDDA/disk"
	"malzahar-project/Projet_BDDA/relation"
)

func TestDBManagerSaveLoad(t *testing.T) {
	dir := t.TempDir()
	cfg := config.NewDBConfig(dir)
	dm := disk.NewDiskManager(cfg)
	if err := dm.Init(); err != nil {
		t.Fatalf("dm.Init: %v", err)
	}
	bm := buffer.NewBufferManager(cfg, dm)
	m := NewDBManager(cfg, dm, bm)

	cols := []relation.ColumnInfo{{Name: "C1", Kind: relation.KindFloat}, {Name: "C2", Kind: relation.KindInt}}
	r := relation.NewRelation("Tab1", cols)
	if err := m.AddTable(r); err != nil {
		t.Fatalf("AddTable: %v", err)
	}
	s, err := m.DescribeTable("Tab1")
	if err != nil {
		t.Fatalf("DescribeTable: %v", err)
	}
	if s == "" {
		t.Fatalf("empty describe string")
	}
	if err := m.SaveState(); err != nil {
		t.Fatalf("SaveState: %v", err)
	}

	// create fresh managers and load
	dm2 := disk.NewDiskManager(cfg)
	if err := dm2.Init(); err != nil {
		t.Fatalf("dm2.Init: %v", err)
	}
	bm2 := buffer.NewBufferManager(cfg, dm2)
	m2 := NewDBManager(cfg, dm2, bm2)
	if err := m2.LoadState(); err != nil {
		t.Fatalf("LoadState: %v", err)
	}
	s2, err := m2.DescribeTable("Tab1")
	if err != nil {
		t.Fatalf("DescribeTable after LoadState: %v", err)
	}
	if s2 == "" {
		t.Fatalf("empty describe after load")
	}
}

func TestRemoveTables(t *testing.T) {
	dir := t.TempDir()
	cfg := config.NewDBConfig(dir)
	dm := disk.NewDiskManager(cfg)
	if err := dm.Init(); err != nil {
		t.Fatalf("dm.Init: %v", err)
	}
	bm := buffer.NewBufferManager(cfg, dm)
	m := NewDBManager(cfg, dm, bm)

	cols := []relation.ColumnInfo{{Name: "A", Kind: relation.KindInt}}
	r1 := relation.NewRelation("T1", cols)
	r2 := relation.NewRelation("T2", cols)
	if err := m.AddTable(r1); err != nil {
		t.Fatalf("AddTable r1: %v", err)
	}
	if err := m.AddTable(r2); err != nil {
		t.Fatalf("AddTable r2: %v", err)
	}
	if err := m.RemoveTable("T1"); err != nil {
		t.Fatalf("RemoveTable T1: %v", err)
	}
	if _, err := m.GetTable("T1"); err == nil {
		t.Fatalf("expected T1 to be removed")
	}
	if err := m.RemoveAllTables(); err != nil {
		t.Fatalf("RemoveAllTables: %v", err)
	}
}
