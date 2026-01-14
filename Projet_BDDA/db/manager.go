package db

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"malzahar-project/Projet_BDDA/buffer"
	"malzahar-project/Projet_BDDA/config"
	"malzahar-project/Projet_BDDA/disk"
	"malzahar-project/Projet_BDDA/relation"
)

type tableSave struct {
	Name   string                `json:"name"`
	Cols   []relation.ColumnInfo `json:"cols"`
	Header struct {
		FileIdx int `json:"fileidx"`
		PageIdx int `json:"pageidx"`
	} `json:"header"`
}

// DBManager manages a collection of relations within a single database.
type DBManager struct {
	cfg    *config.DBConfig
	dm     *disk.DiskManager
	bm     *buffer.BufferManager
	tables map[string]*relation.Relation
	rms    map[string]*relation.RelationManager
}

// NewDBManager constructs a DBManager using the provided components.
func NewDBManager(cfg *config.DBConfig, dm *disk.DiskManager, bm *buffer.BufferManager) *DBManager {
	return &DBManager{cfg: cfg, dm: dm, bm: bm, tables: make(map[string]*relation.Relation), rms: make(map[string]*relation.RelationManager)}
}

func (m *DBManager) AddTable(tab *relation.Relation) error {
	if tab == nil {
		return errors.New("nil relation")
	}
	if _, ok := m.tables[tab.Name]; ok {
		return fmt.Errorf("table %s exists", tab.Name)
	}
	rm, err := relation.NewRelationManager(tab, m.dm, m.bm)
	if err != nil {
		return err
	}
	// ensure header now
	if err := rm.EnsureHeader(); err != nil {
		return err
	}
	m.tables[tab.Name] = tab
	m.rms[tab.Name] = rm
	return nil
}

func (m *DBManager) GetTable(name string) (*relation.Relation, error) {
	t, ok := m.tables[name]
	if !ok {
		return nil, fmt.Errorf("table %s not found", name)
	}
	return t, nil
}

func (m *DBManager) RemoveTable(name string) error {
	rm, ok := m.rms[name]
	if !ok {
		return fmt.Errorf("table %s not found", name)
	}
	// enumerate pages and free them
	pids, err := rm.AllPageIds()
	if err != nil {
		return err
	}
	for _, pid := range pids {
		if err := m.dm.FreePage(pid); err != nil {
			return err
		}
	}
	// remove header metadata file
	hdrPath := filepath.Join(m.dm.BinDir(), name+".hdr")
	_ = os.Remove(hdrPath)
	delete(m.tables, name)
	delete(m.rms, name)
	return nil
}

func (m *DBManager) RemoveAllTables() error {
	// Collect all table names first to avoid modifying map during iteration
	names := make([]string, 0, len(m.tables))
	for name := range m.tables {
		names = append(names, name)
	}
	for _, name := range names {
		if err := m.RemoveTable(name); err != nil {
			return err
		}
	}
	return nil
}

func (m *DBManager) DescribeTable(name string) (string, error) {
	t, ok := m.tables[name]
	if !ok {
		return "", fmt.Errorf("table %s not found", name)
	}
	// build schema string: Name (C1:TYPE,C2:TYPE(...))
	s := t.Name + " ("
	for i, c := range t.Columns {
		if i > 0 {
			s += ","
		}
		switch c.Kind {
		case relation.KindInt:
			s += fmt.Sprintf("%s:INT", c.Name)
		case relation.KindFloat:
			s += fmt.Sprintf("%s:FLOAT", c.Name)
		case relation.KindChar:
			s += fmt.Sprintf("%s:CHAR(%d)", c.Name, c.Size)
		case relation.KindVarchar:
			s += fmt.Sprintf("%s:VARCHAR(%d)", c.Name, c.Size)
		}
	}
	s += ")"
	return s, nil
}

func (m *DBManager) DescribeAllTables() []string {
	var out []string
	// produce deterministic order by sorting table names
	names := make([]string, 0, len(m.tables))
	for name := range m.tables {
		names = append(names, name)
	}
	// sort
	// import sort locally to avoid changing file-level imports layout
	if len(names) > 1 {
		// lightweight insertion sort via sort package
		// use sort.Strings for clarity
		// (we can import sort at top-level; do it inline by calling package)
	}
	// now sort using standard library
	// (we add the import at top of file)
	// ...existing code...
	// sort and append descriptions
	sort.Strings(names)
	for _, name := range names {
		if s, err := m.DescribeTable(name); err == nil {
			out = append(out, s)
		}
	}
	return out
}

// InsertRecord inserts a record into the named table and returns its RecordId.
func (m *DBManager) InsertRecord(table string, rec *relation.Record) (relation.RecordId, error) {
	rm, ok := m.rms[table]
	if !ok {
		return relation.RecordId{}, fmt.Errorf("table %s not found", table)
	}
	return rm.InsertRecord(rec)
}

// AppendFromCSV reads a CSV file (relative path) and appends all records into table.
// CSV format: values separated by commas, string values optionally quoted with double quotes.
// Returns number of inserted records.
func (m *DBManager) AppendFromCSV(table string, csvPath string) (int, error) {
	rm, ok := m.rms[table]
	if !ok {
		return 0, fmt.Errorf("table %s not found", table)
	}
	f, err := os.Open(csvPath)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	// simple line-by-line parsing
	// use scanner to read lines
	inserted := 0
	// Use bufio.NewScanner to read lines
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		// split on commas
		parts := splitCSVLine(line)
		rec := &relation.Record{Values: parts}
		if _, err := rm.InsertRecord(rec); err != nil {
			return inserted, err
		}
		inserted++
	}
	if err := scanner.Err(); err != nil {
		return inserted, err
	}

	return inserted, nil
}

// DeleteWhere deletes records matching match predicate and returns number deleted.
func (m *DBManager) DeleteWhere(table string, match func(rec *relation.Record) bool) (int, error) {
	rm, ok := m.rms[table]
	if !ok {
		return 0, fmt.Errorf("table %s not found", table)
	}
	deleted := 0
	// collect RecordIds to delete to avoid modifying while scanning
	var toDelete []relation.RecordId
	err := rm.ScanRecords(func(rec relation.Record, rid relation.RecordId) error {
		if match(&rec) {
			toDelete = append(toDelete, rid)
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	for _, rid := range toDelete {
		if err := rm.DeleteRecord(rid); err != nil {
			return deleted, err
		}
		deleted++
	}
	return deleted, nil
}

// UpdateWhere updates records matching match by producing a new record via updater
// (which receives a copy of the current record and returns the new record values).
// It returns number of updated records.
func (m *DBManager) UpdateWhere(table string, match func(rec *relation.Record) bool, updater func(rec *relation.Record) *relation.Record) (int, error) {
	rm, ok := m.rms[table]
	if !ok {
		return 0, fmt.Errorf("table %s not found", table)
	}
	updated := 0
	// collect pairs of rid and new record
	type updItem struct {
		rid relation.RecordId
		rec *relation.Record
	}
	var todo []updItem
	err := rm.ScanRecords(func(rec relation.Record, rid relation.RecordId) error {
		if match(&rec) {
			nr := updater(&rec)
			todo = append(todo, updItem{rid: rid, rec: nr})
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	for _, it := range todo {
		// simple approach: delete old record and insert new one
		if err := rm.DeleteRecord(it.rid); err != nil {
			return updated, err
		}
		if _, err := rm.InsertRecord(it.rec); err != nil {
			return updated, err
		}
		updated++
	}
	return updated, nil
}

// ScanTableRecords calls cb for every record in the given table.
func (m *DBManager) ScanTableRecords(table string, cb func(rec relation.Record, rid relation.RecordId) error) error {
	rm, ok := m.rms[table]
	if !ok {
		return fmt.Errorf("table %s not found", table)
	}
	return rm.ScanRecords(cb)
}

// simple CSV line splitter: splits on commas, trims spaces, removes surrounding double quotes if present
func splitCSVLine(line string) []string {
	parts := strings.Split(line, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		s := strings.TrimSpace(p)
		if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
			s = s[1 : len(s)-1]
		}
		out = append(out, s)
	}
	return out
}

// SaveState writes database.save into DBPath and also writes individual .hdr files in BinData.
func (m *DBManager) SaveState() error {
	// ensure DBPath exists
	if err := os.MkdirAll(m.cfg.DBPath, 0o755); err != nil {
		return err
	}
	var entries []tableSave
	for name, t := range m.tables {
		var e tableSave
		e.Name = name
		e.Cols = t.Columns
		if rm, ok := m.rms[name]; ok {
			if rm.HeaderPageId != (config.PageId{}) {
				e.Header.FileIdx = rm.HeaderPageId.FileIdx
				e.Header.PageIdx = rm.HeaderPageId.PageIdx
				// also write per-relation header file (same format as relation.saveHeaderLocation)
				buf := make([]byte, 8)
				binary.LittleEndian.PutUint32(buf[0:4], uint32(e.Header.FileIdx))
				binary.LittleEndian.PutUint32(buf[4:8], uint32(e.Header.PageIdx))
				_ = os.WriteFile(filepath.Join(m.dm.BinDir(), name+".hdr"), buf, 0o644)
			}
		}
		entries = append(entries, e)
	}
	data, err := json.MarshalIndent(entries, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(m.cfg.DBPath, "database.save"), data, 0o644)
}

// LoadState loads database.save and reconstructs relations. It expects .hdr files to be present
// in the BinData directory as produced by SaveState (if headers are provided in the save file,
// SaveState already wrote them).
func (m *DBManager) LoadState() error {
	p := filepath.Join(m.cfg.DBPath, "database.save")
	data, err := os.ReadFile(p)
	if err != nil {
		return err
	}
	var entries []tableSave
	if err := json.Unmarshal(data, &entries); err != nil {
		return err
	}
	for _, e := range entries {
		// if header info present, write .hdr so NewRelationManager can load it when AddTable is called
		if e.Header.FileIdx != 0 || e.Header.PageIdx != 0 {
			buf := make([]byte, 8)
			binary.LittleEndian.PutUint32(buf[0:4], uint32(e.Header.FileIdx))
			binary.LittleEndian.PutUint32(buf[4:8], uint32(e.Header.PageIdx))
			_ = os.WriteFile(filepath.Join(m.dm.BinDir(), e.Name+".hdr"), buf, 0o644)
		}
		rel := relation.NewRelation(e.Name, e.Cols)
		if err := m.AddTable(rel); err != nil {
			return err
		}
	}
	return nil
}
