package relation

import (
	"testing"
)

func TestWriteReadRecord(t *testing.T) {
	cols := []ColumnInfo{
		{Name: "id", Kind: KindInt},
		{Name: "score", Kind: KindFloat},
		{Name: "code", Kind: KindChar, Size: 3},
		{Name: "note", Kind: KindVarchar, Size: 10},
	}
	rel := NewRelation("students", cols)
	rec := NewRecord("123", "12.5", "ABC", "hello")
	buf := make([]byte, rel.RecordSize)
	if err := rel.WriteRecordToBuffer(rec, buf, 0); err != nil {
		t.Fatalf("write failed: %v", err)
	}
	// read back
	r2 := &Record{}
	if err := rel.ReadFromBuffer(r2, buf, 0); err != nil {
		t.Fatalf("read failed: %v", err)
	}
	if len(r2.Values) != len(rec.Values) {
		t.Fatalf("arity mismatch: got %d want %d", len(r2.Values), len(rec.Values))
	}
	for i := range rec.Values {
		if r2.Values[i] != rec.Values[i] {
			t.Fatalf("mismatch at %d: got %q want %q", i, r2.Values[i], rec.Values[i])
		}
	}
}
