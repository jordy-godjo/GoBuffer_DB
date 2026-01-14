package relation

// Record represents a tuple as a slice of string values.
type Record struct {
	Values []string
}

func NewRecord(values ...string) *Record {
	return &Record{Values: append([]string{}, values...)}
}
