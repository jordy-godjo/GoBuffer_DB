package sgbd

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"malzahar-project/Projet_BDDA/buffer"
	"malzahar-project/Projet_BDDA/config"
	"malzahar-project/Projet_BDDA/db"
	"malzahar-project/Projet_BDDA/disk"
	"malzahar-project/Projet_BDDA/relation"
)

type SGBD struct {
	cfg *config.DBConfig
	dm  *disk.DiskManager
	bm  *buffer.BufferManager
	dbm *db.DBManager
}

func NewSGBD(cfg *config.DBConfig) (*SGBD, error) {
	dm := disk.NewDiskManager(cfg)
	if err := dm.Init(); err != nil {
		return nil, err
	}
	bm := buffer.NewBufferManager(cfg, dm)
	dbm := db.NewDBManager(cfg, dm, bm)
	// attempt to load previous DB state if present; ignore missing save file
	if err := dbm.LoadState(); err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
		// else no saved state found — continue with empty DB
	}
	return &SGBD{cfg: cfg, dm: dm, bm: bm, dbm: dbm}, nil
}

// Run listens on stdin for commands until EXIT. No prompt is printed.
func (s *SGBD) Run() error {
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if strings.EqualFold(line, "EXIT") {
			// save state and exit
			_ = s.dbm.SaveState()
			_ = s.bm.FlushBuffers()
			_ = s.dm.Finish()
			return nil
		}
		if err := s.ProcessCommand(line, os.Stdout); err != nil {
			// print error but continue
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
		}
	}
	return scanner.Err()
}

// ProcessCommand parses and executes a single command text, writing outputs to w.
func (s *SGBD) ProcessCommand(text string, w io.Writer) error {
	// normalize
	t := strings.TrimSpace(text)
	up := strings.ToUpper(t)
	switch {
	case strings.HasPrefix(up, "CREATE TABLE "):
		return s.ProcessCreateTableCommand(t, w)
	case strings.HasPrefix(up, "INSERT INTO "):
		return s.ProcessInsertCommand(t, w)
	case strings.HasPrefix(up, "APPEND INTO "):
		return s.ProcessAppendCommand(t, w)
	case strings.HasPrefix(up, "SELECT "):
		return s.ProcessSelectCommand(t, w)
	case strings.HasPrefix(up, "DELETE "):
		return s.ProcessDeleteCommand(t, w)
	case strings.HasPrefix(up, "UPDATE "):
		return s.ProcessUpdateCommand(t, w)
	case strings.HasPrefix(up, "DROP TABLES"):
		return s.ProcessDropTablesCommand(w)
	case strings.HasPrefix(up, "DROP TABLE "):
		return s.ProcessDropTableCommand(t, w)
	case strings.HasPrefix(up, "DESCRIBE TABLES"):
		return s.ProcessDescribeTablesCommand(w)
	case strings.HasPrefix(up, "DESCRIBE TABLE "):
		return s.ProcessDescribeTableCommand(t, w)
	default:
		return fmt.Errorf("unsupported command: %s", text)
	}
}

// helper parse column type like INT, FLOAT, CHAR(n), VARCHAR(n)
func parseColType(s string) (relation.ColumnKind, int, error) {
	s = strings.TrimSpace(s)
	sUp := strings.ToUpper(s)
	if sUp == "INT" {
		return relation.KindInt, 0, nil
	}
	if sUp == "FLOAT" {
		return relation.KindFloat, 0, nil
	}
	// accept REAL as an alias for FLOAT (README uses REAL)
	if sUp == "REAL" {
		return relation.KindFloat, 0, nil
	}
	if strings.HasPrefix(sUp, "CHAR(") && strings.HasSuffix(sUp, ")") {
		inner := sUp[len("CHAR(") : len(sUp)-1]
		n, err := strconv.Atoi(inner)
		if err != nil {
			return 0, 0, err
		}
		return relation.KindChar, n, nil
	}
	if strings.HasPrefix(sUp, "VARCHAR(") && strings.HasSuffix(sUp, ")") {
		inner := sUp[len("VARCHAR(") : len(sUp)-1]
		n, err := strconv.Atoi(inner)
		if err != nil {
			return 0, 0, err
		}
		return relation.KindVarchar, n, nil
	}
	return 0, 0, fmt.Errorf("unknown column type: %s", s)
}

// ProcessCreateTableCommand expects: CREATE TABLE Name (col:TYPE, ...)
func (s *SGBD) ProcessCreateTableCommand(text string, w io.Writer) error {
	// find opening paren
	idx := strings.Index(text, "(")
	if idx < 0 {
		return fmt.Errorf("invalid CREATE TABLE syntax")
	}
	pre := strings.TrimSpace(text[:idx])
	// pre is like "CREATE TABLE Name"
	parts := strings.Fields(pre)
	if len(parts) < 3 {
		return fmt.Errorf("invalid CREATE TABLE syntax")
	}
	name := parts[2]
	body := strings.TrimSpace(text[idx+1:])
	if strings.HasSuffix(body, ")") {
		body = body[:len(body)-1]
	}
	cols := strings.Split(body, ",")
	var cis []relation.ColumnInfo
	for _, c := range cols {
		c = strings.TrimSpace(c)
		if c == "" {
			continue
		}
		// split name:type
		sp := strings.SplitN(c, ":", 2)
		if len(sp) != 2 {
			return fmt.Errorf("invalid column definition: %s", c)
		}
		cname := strings.TrimSpace(sp[0])
		ctype := strings.TrimSpace(sp[1])
		kind, size, err := parseColType(ctype)
		if err != nil {
			return err
		}
		cis = append(cis, relation.ColumnInfo{Name: cname, Kind: kind, Size: size})
	}
	rel := relation.NewRelation(name, cis)
	if err := s.dbm.AddTable(rel); err != nil {
		return err
	}
	fmt.Fprintln(w, "OK")
	return nil
}

// INSERT INTO Name VALUES (v1,v2,...)
func (s *SGBD) ProcessInsertCommand(text string, w io.Writer) error {
	// find " VALUES ("
	up := strings.ToUpper(text)
	idx := strings.Index(up, " VALUES (")
	if idx < 0 {
		return fmt.Errorf("invalid INSERT syntax")
	}
	pre := strings.TrimSpace(text[:idx])
	parts := strings.Fields(pre)
	if len(parts) < 3 {
		return fmt.Errorf("invalid INSERT syntax")
	}
	name := parts[2]
	// extract values inside parentheses
	vstart := idx + len(" VALUES (")
	if !strings.HasSuffix(text, ")") {
		return fmt.Errorf("invalid INSERT syntax: missing )")
	}
	body := text[vstart : len(text)-1]
	vals := splitCSVLine(body)
	// strip quotes from string literals if present
	for i := range vals {
		v := strings.TrimSpace(vals[i])
		if len(v) >= 2 && v[0] == '"' && v[len(v)-1] == '"' {
			v = v[1 : len(v)-1]
		}
		vals[i] = v
	}
	rec := &relation.Record{Values: vals}
	if _, err := s.dbm.InsertRecord(name, rec); err != nil {
		return err
	}
	// Force flush to disk after each insert for data persistence
	if err := s.bm.FlushBuffers(); err != nil {
		return err
	}
	fmt.Fprintln(w, "OK")
	return nil
}

// APPEND INTO Name ALLRECORDS (file.csv)
func (s *SGBD) ProcessAppendCommand(text string, w io.Writer) error {
	// expected format: APPEND INTO name ALLRECORDS(filename)
	// split by spaces
	parts := strings.Fields(text)
	if len(parts) < 4 {
		return fmt.Errorf("invalid APPEND syntax")
	}
	name := parts[2]
	// find '(' and ')'
	idx := strings.Index(text, "(")
	jdx := strings.LastIndex(text, ")")
	if idx < 0 || jdx < 0 || jdx <= idx {
		return fmt.Errorf("invalid APPEND syntax: missing parentheses")
	}
	fname := strings.TrimSpace(text[idx+1 : jdx])
	// file path relative to project root
	cnt, err := s.dbm.AppendFromCSV(name, fname)
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "OK (%d inserted)\n", cnt)
	return nil
}

// Condition represents a simple comparison between terms (col or constant)
type Condition struct {
	LeftIsCol   bool
	LeftColIdx  int
	LeftConst   string
	RightIsCol  bool
	RightColIdx int
	RightConst  string
	Op          string
}

// helper to split CSV-style comma list used for INSERT parsing
func splitCSVLine(line string) []string {
	parts := strings.Split(line, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		out = append(out, strings.TrimSpace(p))
	}
	return out
}

// parse simple WHERE clause into conditions (conjunction of comparisons using AND)
func parseWhereClause(where string, rel *relation.Relation, alias string) ([]Condition, error) {
	var res []Condition
	where = strings.TrimSpace(where)
	if where == "" {
		return res, nil
	}
	parts := strings.Split(where, " AND ")
	for _, p := range parts {
		p = strings.TrimSpace(p)
		// find operator
		ops := []string{"<=", ">=", "<>", "=", "<", ">"}
		var found string
		var left, right string
		for _, op := range ops {
			if idx := strings.Index(p, op); idx >= 0 {
				found = op
				left = strings.TrimSpace(p[:idx])
				right = strings.TrimSpace(p[idx+len(op):])
				break
			}
		}
		if found == "" {
			return nil, fmt.Errorf("unsupported condition: %s", p)
		}
		cond := Condition{Op: found}
		// left can be alias.col or constant
		if strings.HasPrefix(left, alias+".") {
			col := left[len(alias)+1:]
			idx := -1
			for i, c := range rel.Columns {
				if c.Name == col {
					idx = i
					break
				}
			}
			if idx < 0 {
				return nil, fmt.Errorf("unknown column: %s", col)
			}
			cond.LeftIsCol = true
			cond.LeftColIdx = idx
		} else {
			// constant: strip quotes if present
			lv := left
			if len(lv) >= 2 && lv[0] == '"' && lv[len(lv)-1] == '"' {
				lv = lv[1 : len(lv)-1]
			}
			cond.LeftConst = lv
		}
		// right can be alias.col or constant
		if strings.HasPrefix(right, alias+".") {
			col := right[len(alias)+1:]
			idx := -1
			for i, c := range rel.Columns {
				if c.Name == col {
					idx = i
					break
				}
			}
			if idx < 0 {
				return nil, fmt.Errorf("unknown column: %s", col)
			}
			cond.RightIsCol = true
			cond.RightColIdx = idx
		} else {
			// constant: strip quotes if present
			rv := right
			if len(rv) >= 2 && rv[0] == '"' && rv[len(rv)-1] == '"' {
				rv = rv[1 : len(rv)-1]
			}
			cond.RightConst = rv
		}
		res = append(res, cond)
	}
	return res, nil
}

// evaluate conditions on a record
func evalConditions(rec *relation.Record, rel *relation.Relation, conds []Condition) (bool, error) {
	for _, c := range conds {
		var leftVal string
		if c.LeftIsCol {
			leftVal = rec.Values[c.LeftColIdx]
		} else {
			leftVal = c.LeftConst
		}
		var rightVal string
		if c.RightIsCol {
			rightVal = rec.Values[c.RightColIdx]
		} else {
			rightVal = c.RightConst
		}
		// determine column kind: prefer left if it's a column, else right
		var kind relation.ColumnKind
		if c.LeftIsCol {
			kind = rel.Columns[c.LeftColIdx].Kind
		} else if c.RightIsCol {
			kind = rel.Columns[c.RightColIdx].Kind
		} else {
			// both constants? not supported, but assume string
			kind = relation.KindVarchar
		}
		switch kind {
		case relation.KindInt:
			li, err := strconv.Atoi(leftVal)
			if err != nil {
				return false, err
			}
			ri, err := strconv.Atoi(rightVal)
			if c.RightIsCol && err != nil {
				return false, err
			}
			if !c.RightIsCol {
				ri, _ = strconv.Atoi(rightVal)
			}
			switch c.Op {
			case "=":
				if !(li == ri) {
					return false, nil
				}
			case "<>":
				if !(li != ri) {
					return false, nil
				}
			case "<":
				if !(li < ri) {
					return false, nil
				}
			case ">":
				if !(li > ri) {
					return false, nil
				}
			case "<=":
				if !(li <= ri) {
					return false, nil
				}
			case ">=":
				if !(li >= ri) {
					return false, nil
				}
			}
		case relation.KindFloat:
			lf, err := strconv.ParseFloat(leftVal, 64)
			if err != nil {
				return false, err
			}
			rf, err := strconv.ParseFloat(rightVal, 64)
			if c.RightIsCol && err != nil {
				return false, err
			}
			switch c.Op {
			case "=":
				if !(lf == rf) {
					return false, nil
				}
			case "<>":
				if !(lf != rf) {
					return false, nil
				}
			case "<":
				if !(lf < rf) {
					return false, nil
				}
			case ">":
				if !(lf > rf) {
					return false, nil
				}
			case "<=":
				if !(lf <= rf) {
					return false, nil
				}
			case ">=":
				if !(lf >= rf) {
					return false, nil
				}
			}
		case relation.KindChar, relation.KindVarchar:
			// lexical comparison
			switch c.Op {
			case "=":
				if !(leftVal == rightVal) {
					return false, nil
				}
			case "<>":
				if !(leftVal != rightVal) {
					return false, nil
				}
			case "<":
				if !(leftVal < rightVal) {
					return false, nil
				}
			case ">":
				if !(leftVal > rightVal) {
					return false, nil
				}
			case "<=":
				if !(leftVal <= rightVal) {
					return false, nil
				}
			case ">=":
				if !(leftVal >= rightVal) {
					return false, nil
				}
			}
		}
	}
	return true, nil
}

// SELECT ... FROM name alias [WHERE ...]
func (s *SGBD) ProcessSelectCommand(text string, w io.Writer) error {
	// split SELECT and FROM
	up := strings.ToUpper(text)
	idx := strings.Index(up, " FROM ")
	if idx < 0 {
		return fmt.Errorf("invalid SELECT syntax")
	}
	selPart := strings.TrimSpace(text[len("SELECT "):idx])
	rest := strings.TrimSpace(text[idx+len(" FROM "):])
	// rest -> "name alias [WHERE ...]"
	// find WHERE
	whereIdx := strings.Index(strings.ToUpper(rest), " WHERE ")
	var wherePart string
	fromPart := rest
	if whereIdx >= 0 {
		fromPart = strings.TrimSpace(rest[:whereIdx])
		wherePart = strings.TrimSpace(rest[whereIdx+len(" WHERE "):])
	}
	parts := strings.Fields(fromPart)
	if len(parts) < 2 {
		return fmt.Errorf("invalid SELECT FROM syntax")
	}
	name := parts[0]
	alias := parts[1]
	rel, err := s.dbm.GetTable(name)
	if err != nil {
		return err
	}
	// parse selection columns
	var projIdxs []int
	if strings.TrimSpace(selPart) == "*" {
		for i := range rel.Columns {
			projIdxs = append(projIdxs, i)
		}
	} else {
		cols := strings.Split(selPart, ",")
		for _, c := range cols {
			c = strings.TrimSpace(c)
			if strings.HasPrefix(c, alias+".") {
				col := c[len(alias)+1:]
				found := -1
				for i, cc := range rel.Columns {
					if cc.Name == col {
						found = i
						break
					}
				}
				if found < 0 {
					return fmt.Errorf("unknown column in projection: %s", col)
				}
				projIdxs = append(projIdxs, found)
			} else {
				return fmt.Errorf("projection must use alias: %s", c)
			}
		}
	}
	// parse where
	conds, err := parseWhereClause(wherePart, rel, alias)
	if err != nil {
		return err
	}
	// ensure all pending writes are flushed
	if err := s.bm.FlushBuffers(); err != nil {
		return err
	}
	// scan records and print matches
	total := 0
	err = s.dbm.ScanTableRecords(name, func(rec relation.Record, rid relation.RecordId) error {
		ok, err := evalConditions(&rec, rel, conds)
		if err != nil {
			return err
		}
		if ok {
			// print projection
			if len(projIdxs) == 0 {
				// nothing to print
				fmt.Fprintln(w, "")
			} else {
				out := ""
				for i, pi := range projIdxs {
					if i > 0 {
						out += " ; "
					}
					out += rec.Values[pi]
				}
				fmt.Fprintln(w, out)
			}
			total++
		}
		return nil
	})
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "Total selected records = %d\n", total)
	return nil
}

// DELETE name alias [WHERE ...]
func (s *SGBD) ProcessDeleteCommand(text string, w io.Writer) error {
	// split "DELETE " then rest
	rest := strings.TrimSpace(text[len("DELETE "):])
	// find WHERE
	whereIdx := strings.Index(strings.ToUpper(rest), " WHERE ")
	var wherePart string
	fromPart := rest
	if whereIdx >= 0 {
		fromPart = strings.TrimSpace(rest[:whereIdx])
		wherePart = strings.TrimSpace(rest[whereIdx+len(" WHERE "):])
	}
	parts := strings.Fields(fromPart)
	if len(parts) < 2 {
		return fmt.Errorf("invalid DELETE syntax")
	}
	name := parts[0]
	alias := parts[1]
	rel, err := s.dbm.GetTable(name)
	if err != nil {
		return err
	}
	conds, err := parseWhereClause(wherePart, rel, alias)
	if err != nil {
		return err
	}
	// define predicate
	match := func(rec *relation.Record) bool {
		ok, _ := evalConditions(rec, rel, conds)
		return ok
	}
	cnt, err := s.dbm.DeleteWhere(name, match)
	if err != nil {
		return err
	}
	// Force flush to disk after delete for data persistence
	if err := s.bm.FlushBuffers(); err != nil {
		return err
	}
	fmt.Fprintf(w, "Total deleted records = %d\n", cnt)
	return nil
}

// UPDATE name alias SET alias.col=val,... [WHERE ...]
func (s *SGBD) ProcessUpdateCommand(text string, w io.Writer) error {
	// strip leading UPDATE
	rest := strings.TrimSpace(text[len("UPDATE "):])
	// find SET
	upRest := strings.ToUpper(rest)
	setIdx := strings.Index(upRest, " SET ")
	if setIdx < 0 {
		return fmt.Errorf("invalid UPDATE syntax: missing SET")
	}
	before := strings.TrimSpace(rest[:setIdx]) // "name alias"
	after := strings.TrimSpace(rest[setIdx+len(" SET "):])
	// check for WHERE
	whereIdx := strings.Index(strings.ToUpper(after), " WHERE ")
	setPart := after
	wherePart := ""
	if whereIdx >= 0 {
		setPart = strings.TrimSpace(after[:whereIdx])
		wherePart = strings.TrimSpace(after[whereIdx+len(" WHERE "):])
	}
	parts := strings.Fields(before)
	if len(parts) < 2 {
		return fmt.Errorf("invalid UPDATE syntax")
	}
	name := parts[0]
	alias := parts[1]
	rel, err := s.dbm.GetTable(name)
	if err != nil {
		return err
	}
	// parse assignments
	assigns := strings.Split(setPart, ",")
	changes := make(map[int]string)
	for _, a := range assigns {
		a = strings.TrimSpace(a)
		spIdx := strings.Index(a, "=")
		if spIdx < 0 {
			return fmt.Errorf("invalid SET assignment: %s", a)
		}
		lhs := strings.TrimSpace(a[:spIdx])
		rhs := strings.TrimSpace(a[spIdx+1:])
		if !strings.HasPrefix(lhs, alias+".") {
			return fmt.Errorf("left side must be alias.column: %s", lhs)
		}
		col := lhs[len(alias)+1:]
		idx := -1
		for i, c := range rel.Columns {
			if c.Name == col {
				idx = i
				break
			}
		}
		if idx < 0 {
			return fmt.Errorf("unknown column: %s", col)
		}
		if len(rhs) >= 2 && rhs[0] == '"' && rhs[len(rhs)-1] == '"' {
			rhs = rhs[1 : len(rhs)-1]
		}
		changes[idx] = rhs
	}
	conds, err := parseWhereClause(wherePart, rel, alias)
	if err != nil {
		return err
	}
	// updater builds new record by copying and applying changes
	updater := func(rec *relation.Record) *relation.Record {
		nr := &relation.Record{Values: append([]string{}, rec.Values...)}
		for idx, val := range changes {
			nr.Values[idx] = val
		}
		return nr
	}
	match := func(rec *relation.Record) bool {
		ok, _ := evalConditions(rec, rel, conds)
		return ok
	}
	cnt, err := s.dbm.UpdateWhere(name, match, updater)
	if err != nil {
		return err
	}
	// Force flush to disk after update for data persistence
	if err := s.bm.FlushBuffers(); err != nil {
		return err
	}
	fmt.Fprintf(w, "Total updated records = %d\n", cnt)
	return nil
}

func (s *SGBD) ProcessDropTableCommand(text string, w io.Writer) error {
	parts := strings.Fields(text)
	if len(parts) < 3 {
		return fmt.Errorf("invalid DROP TABLE syntax")
	}
	name := parts[2]
	if err := s.dbm.RemoveTable(name); err != nil {
		return err
	}
	fmt.Fprintln(w, "OK")
	return nil
}

func (s *SGBD) ProcessDropTablesCommand(w io.Writer) error {
	if err := s.dbm.RemoveAllTables(); err != nil {
		return err
	}
	fmt.Fprintln(w, "OK")
	return nil
}

func (s *SGBD) ProcessDescribeTableCommand(text string, w io.Writer) error {
	parts := strings.Fields(text)
	if len(parts) < 3 {
		return fmt.Errorf("invalid DESCRIBE TABLE syntax")
	}
	name := parts[2]
	if sStr, err := s.dbm.DescribeTable(name); err == nil {
		fmt.Fprintln(w, sStr)
		return nil
	} else {
		return err
	}
}

func (s *SGBD) ProcessDescribeTablesCommand(w io.Writer) error {
	lines := s.dbm.DescribeAllTables()
	for _, l := range lines {
		fmt.Fprintln(w, l)
	}
	return nil
}

// Utility: Save DB state to disk (calls DBManager.SaveState)
func (s *SGBD) Save() error {
	return s.dbm.SaveState()
}
