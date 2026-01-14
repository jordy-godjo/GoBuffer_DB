package relation

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strconv"
)

type ColumnKind int

const (
	KindInt ColumnKind = iota
	KindFloat
	KindChar
	KindVarchar
)

type ColumnInfo struct {
	Name string
	Kind ColumnKind
	Size int // for CHAR/VARCHAR: length; for INT/FLOAT ignored
}

type Relation struct {
	Name       string
	Columns    []ColumnInfo
	RecordSize int
}

func NewRelation(name string, cols []ColumnInfo) *Relation {
	r := &Relation{Name: name, Columns: cols}
	sz := 0
	for _, c := range cols {
		switch c.Kind {
		case KindInt:
			sz += 4
		case KindFloat:
			sz += 4
		case KindChar, KindVarchar:
			sz += c.Size
		}
	}
	r.RecordSize = sz
	return r
}

// writeRecordToBuffer writes the record into buff starting at pos. buff must be large enough.
func (r *Relation) WriteRecordToBuffer(rec *Record, buff []byte, pos int) error {
	if len(rec.Values) != len(r.Columns) {
		return errors.New("record arity mismatch")
	}
	if pos < 0 || pos+r.RecordSize > len(buff) {
		return errors.New("buffer too small or pos out of range")
	}
	off := pos
	for i, col := range r.Columns {
		val := rec.Values[i]
		switch col.Kind {
		case KindInt:
			v, err := strconv.Atoi(val)
			if err != nil {
				return fmt.Errorf("col %s: invalid int: %v", col.Name, err)
			}
			binary.LittleEndian.PutUint32(buff[off:off+4], uint32(int32(v)))
			off += 4
		case KindFloat:
			f, err := strconv.ParseFloat(val, 32)
			if err != nil {
				return fmt.Errorf("col %s: invalid float: %v", col.Name, err)
			}
			bits := math.Float32bits(float32(f))
			binary.LittleEndian.PutUint32(buff[off:off+4], bits)
			off += 4
		case KindChar, KindVarchar:
			// write up to col.Size bytes, pad with zeros
			b := []byte(val)
			if len(b) > col.Size {
				b = b[:col.Size]
			}
			copy(buff[off:off+col.Size], b)
			// pad remainder
			for j := len(b); j < col.Size; j++ {
				buff[off+j] = 0
			}
			off += col.Size
		}
	}
	return nil
}

// ReadFromBuffer reads a record from buff at pos and fills rec.Values (must be empty slice).
func (r *Relation) ReadFromBuffer(rec *Record, buff []byte, pos int) error {
	if pos < 0 || pos+r.RecordSize > len(buff) {
		return errors.New("buffer too small or pos out of range")
	}
	rec.Values = make([]string, 0, len(r.Columns))
	off := pos
	for _, col := range r.Columns {
		switch col.Kind {
		case KindInt:
			v := int32(binary.LittleEndian.Uint32(buff[off : off+4]))
			rec.Values = append(rec.Values, strconv.FormatInt(int64(v), 10))
			off += 4
		case KindFloat:
			bits := binary.LittleEndian.Uint32(buff[off : off+4])
			f := math.Float32frombits(bits)
			rec.Values = append(rec.Values, fmt.Sprintf("%g", f))
			off += 4
		case KindChar, KindVarchar:
			b := buff[off : off+col.Size]
			// trim trailing zeros
			end := col.Size
			for k := 0; k < col.Size; k++ {
				if b[k] == 0 {
					end = k
					break
				}
			}
			rec.Values = append(rec.Values, string(b[:end]))
			off += col.Size
		}
	}
	return nil
}
