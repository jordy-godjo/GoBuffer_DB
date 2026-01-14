package sgbd

import (
	"bytes"
	"strings"
	"testing"

	"malzahar-project/Projet_BDDA/config"
)

// TestScenario executes the README example scenario through ProcessCommand and Save.
func TestScenario(t *testing.T) {
	dir := t.TempDir()
	cfg := config.NewDBConfig(dir)
	s, err := NewSGBD(cfg)
	if err != nil {
		t.Fatalf("NewSGBD: %v", err)
	}

	var out bytes.Buffer

	cmds := []string{
		"CREATE TABLE Tab1 (C1:FLOAT,C2:INT)",
		"CREATE TABLE Tab2 (C7:CHAR(5),AA:VARCHAR(2))",
		"CREATE TABLE Tab3 (Toto:CHAR(120))",
		"DESCRIBE TABLE Tab1",
		"DESCRIBE TABLES",
		"DROP TABLE Tab1",
		"DESCRIBE TABLES",
	}

	// Execute commands sequentially
	for _, c := range cmds {
		out.Reset()
		if err := s.ProcessCommand(c, &out); err != nil {
			t.Fatalf("ProcessCommand(%q) failed: %v", c, err)
		}
		// for CREATE/DROP we expect "OK" line; for DESCRIBE we expect schema lines
		up := strings.ToUpper(c)
		if strings.HasPrefix(up, "CREATE TABLE") || strings.HasPrefix(up, "DROP TABLE") || strings.HasPrefix(up, "DROP TABLES") {
			got := strings.TrimSpace(out.String())
			if got != "OK" && got != "" { // some commands may not print anything for DESCRIBE TABLES
				t.Fatalf("expected OK for %s, got %q", c, got)
			}
		}
		if strings.HasPrefix(up, "DESCRIBE TABLE ") {
			got := strings.TrimSpace(out.String())
			if !strings.HasPrefix(got, "Tab1 (") {
				t.Fatalf("DESCRIBE TABLE Tab1 unexpected output: %q", got)
			}
		}
	}

	// final check: after DROP TABLE Tab1, DESCRIBE TABLES should not contain Tab1
	var allOut bytes.Buffer
	if err := s.ProcessCommand("DESCRIBE TABLES", &allOut); err != nil {
		t.Fatalf("ProcessCommand(DESCRIBE TABLES): %v", err)
	}
	txt := allOut.String()
	if strings.Contains(txt, "Tab1 (") {
		t.Fatalf("Tab1 still present after DROP TABLE: output=%q", txt)
	}

	// simulate EXIT (save state)
	if err := s.Save(); err != nil {
		t.Fatalf("Save() failed: %v", err)
	}
}

// TestDropTables tests the DROP TABLES command functionality.
func TestDropTables(t *testing.T) {
	dir := t.TempDir()
	cfg := config.NewDBConfig(dir)
	s, err := NewSGBD(cfg)
	if err != nil {
		t.Fatalf("NewSGBD: %v", err)
	}

	var out bytes.Buffer

	// Create some tables
	cmds := []string{
		"CREATE TABLE Tab1 (C1:FLOAT,C2:INT)",
		"CREATE TABLE Tab2 (C7:CHAR(5),AA:VARCHAR(2))",
		"CREATE TABLE Tab3 (Toto:CHAR(120))",
	}

	for _, c := range cmds {
		out.Reset()
		if err := s.ProcessCommand(c, &out); err != nil {
			t.Fatalf("ProcessCommand(%q) failed: %v", c, err)
		}
	}

	// Check that tables exist
	out.Reset()
	if err := s.ProcessCommand("DESCRIBE TABLES", &out); err != nil {
		t.Fatalf("ProcessCommand(DESCRIBE TABLES): %v", err)
	}
	txt := out.String()
	if !strings.Contains(txt, "Tab1 (") || !strings.Contains(txt, "Tab2 (") || !strings.Contains(txt, "Tab3 (") {
		t.Fatalf("Tables not created properly: output=%q", txt)
	}

	// Drop all tables
	out.Reset()
	if err := s.ProcessCommand("DROP TABLES", &out); err != nil {
		t.Fatalf("ProcessCommand(DROP TABLES) failed: %v", err)
	}
	got := strings.TrimSpace(out.String())
	if got != "OK" {
		t.Fatalf("expected OK for DROP TABLES, got %q", got)
	}

	// Check that no tables remain
	out.Reset()
	if err := s.ProcessCommand("DESCRIBE TABLES", &out); err != nil {
		t.Fatalf("ProcessCommand(DESCRIBE TABLES): %v", err)
	}
	txt = out.String()
	if strings.Contains(txt, "Tab1 (") || strings.Contains(txt, "Tab2 (") || strings.Contains(txt, "Tab3 (") {
		t.Fatalf("Tables still present after DROP TABLES: output=%q", txt)
	}

	// Save and reload to test persistence
	if err := s.Save(); err != nil {
		t.Fatalf("Save() failed: %v", err)
	}

	// Create new SGBD instance to test loading
	s2, err := NewSGBD(cfg)
	if err != nil {
		t.Fatalf("NewSGBD after save: %v", err)
	}

	out.Reset()
	if err := s2.ProcessCommand("DESCRIBE TABLES", &out); err != nil {
		t.Fatalf("ProcessCommand(DESCRIBE TABLES) after reload: %v", err)
	}
	txt = out.String()
	if strings.Contains(txt, "Tab1 (") || strings.Contains(txt, "Tab2 (") || strings.Contains(txt, "Tab3 (") {
		t.Fatalf("Tables still present after reload: output=%q", txt)
	}
}
