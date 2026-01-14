package config_test

import (
	"os"
	"path/filepath"
	"testing"

	"malzahar-project/Projet_BDDA/config"
)

func TestNewDBConfig(t *testing.T) {
	c := config.NewDBConfig("/tmp/DB")
	if c.DBPath != "/tmp/DB" {
		t.Fatalf("expected /tmp/DB got %s", c.DBPath)
	}
}

func TestLoadDBConfigSimpleFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cfg.txt")
	content := "dbpath = '../DB'\npagesize = 8192\ndm_maxfilecount = 16\nbm_buffercount = 4\nbm_policy = MRU\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	c, err := config.LoadDBConfig(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if c.DBPath != "../DB" {
		t.Fatalf("expected ../DB got %s", c.DBPath)
	}
	if c.PageSize != 8192 {
		t.Fatalf("expected pagesize 8192 got %d", c.PageSize)
	}
	if c.DMMaxFileCount != 16 {
		t.Fatalf("expected dm_maxfilecount 16 got %d", c.DMMaxFileCount)
	}
	if c.BMBufferCount != 4 {
		t.Fatalf("expected bm_buffercount 4 got %d", c.BMBufferCount)
	}
	if c.BMPolicy != "MRU" {
		t.Fatalf("expected bm_policy MRU got %s", c.BMPolicy)
	}
}

func TestLoadDBConfigJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cfg.json")
	content := "{\"dbpath\": \"./data\", \"pagesize\": 16384, \"dm_maxfilecount\": 4, \"bm_buffercount\": 3, \"bm_policy\": \"LRU\"}"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	c, err := config.LoadDBConfig(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if c.DBPath != "./data" {
		t.Fatalf("expected ./data got %s", c.DBPath)
	}
	if c.PageSize != 16384 {
		t.Fatalf("expected pagesize 16384 got %d", c.PageSize)
	}
	if c.DMMaxFileCount != 4 {
		t.Fatalf("expected dm_maxfilecount 4 got %d", c.DMMaxFileCount)
	}
	if c.BMBufferCount != 3 {
		t.Fatalf("expected bm_buffercount 3 got %d", c.BMBufferCount)
	}
	if c.BMPolicy != "LRU" {
		t.Fatalf("expected bm_policy LRU got %s", c.BMPolicy)
	}
}

func TestLoadDBConfigMissingFile(t *testing.T) {
	if _, err := config.LoadDBConfig("does-not-exist.cfg"); err == nil {
		t.Fatalf("expected error for missing file")
	}
}

func TestLoadDBConfigEmptyFile(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "empty.cfg")
	if err := os.WriteFile(p, []byte(""), 0o644); err != nil {
		t.Fatalf("write empty file: %v", err)
	}
	if _, err := config.LoadDBConfig(p); err == nil {
		t.Fatalf("expected error for empty config file")
	}
}

func TestLoadDBConfigNoDbPath(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "nodbp.cfg")
	if err := os.WriteFile(p, []byte("other=1\n"), 0o644); err != nil {
		t.Fatalf("write file without dbpath: %v", err)
	}
	if _, err := config.LoadDBConfig(p); err == nil {
		t.Fatalf("expected error when dbpath is missing")
	}
}
