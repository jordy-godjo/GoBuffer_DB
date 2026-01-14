package disk

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"malzahar-project/Projet_BDDA/config"
)

// DiskManager handles page-level allocation and I/O on Datax.bin files under BinData.
type DiskManager struct {
	cfg    *config.DBConfig
	binDir string
	mu     sync.Mutex
	// bitmaps[fileIdx] = []byte (0 free, 1 used)
	bitmaps map[int][]byte
}

// NewDiskManager creates a manager but does not initialize on disk.
func NewDiskManager(cfg *config.DBConfig) *DiskManager {
	return &DiskManager{
		cfg:     cfg,
		binDir:  filepath.Join(cfg.DBPath, "BinData"),
		bitmaps: make(map[int][]byte),
	}
}

// Init creates the BinData directory and ensures at least Data0.bin exists.
func (m *DiskManager) Init() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := os.MkdirAll(m.binDir, 0o755); err != nil {
		return err
	}
	// ensure Data0.bin exists
	path := filepath.Join(m.binDir, fmt.Sprintf("Data%d.bin", 0))
	if _, err := os.Stat(path); os.IsNotExist(err) {
		f, err := os.Create(path)
		if err != nil {
			return err
		}
		f.Close()
	}
	// load bitmap if present, otherwise create empty
	if err := m.loadBitmap(0); err != nil {
		return err
	}
	return nil
}

func (m *DiskManager) bitmapPath(idx int) string {
	return filepath.Join(m.binDir, fmt.Sprintf("Data%d.bitmap", idx))
}

func (m *DiskManager) dataPath(idx int) string {
	return filepath.Join(m.binDir, fmt.Sprintf("Data%d.bin", idx))
}

func (m *DiskManager) loadBitmap(idx int) error {
	p := m.bitmapPath(idx)
	if _, err := os.Stat(p); os.IsNotExist(err) {
		m.bitmaps[idx] = []byte{}
		// create empty bitmap file
		if f, err := os.Create(p); err == nil {
			f.Close()
		}
		return nil
	}
	data, err := os.ReadFile(p)
	if err != nil {
		return err
	}
	m.bitmaps[idx] = data
	return nil
}

func (m *DiskManager) persistBitmap(idx int) error {
	p := m.bitmapPath(idx)
	return os.WriteFile(p, m.bitmaps[idx], 0o644)
}

// AllocatePage finds a free page or grows Data files and returns its PageId.
func (m *DiskManager) AllocatePage() (config.PageId, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ps := m.cfg.PageSize
	if ps <= 0 {
		return config.PageId{}, errors.New("invalid pagesize")
	}
	// search existing files
	for idx := 0; idx < m.cfg.DMMaxFileCount; idx++ {
		// ensure bitmap loaded
		if _, ok := m.bitmaps[idx]; !ok {
			if err := m.loadBitmap(idx); err != nil {
				return config.PageId{}, err
			}
		}
		bmp := m.bitmaps[idx]
		for i, b := range bmp {
			if b == 0 {
				m.bitmaps[idx][i] = 1
				if err := m.persistBitmap(idx); err != nil {
					return config.PageId{}, err
				}
				return config.PageId{FileIdx: idx, PageIdx: i}, nil
			}
		}
		// no free page, try to append one by extending file
		// open file and append one page sized zero bytes
		dataPath := m.dataPath(idx)
		f, err := os.OpenFile(dataPath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o644)
		if err != nil {
			return config.PageId{}, err
		}
		zero := make([]byte, ps)
		if _, err := f.Write(zero); err != nil {
			f.Close()
			return config.PageId{}, err
		}
		f.Close()
		// extend bitmap
		m.bitmaps[idx] = append(m.bitmaps[idx], 1)
		if err := m.persistBitmap(idx); err != nil {
			return config.PageId{}, err
		}
		return config.PageId{FileIdx: idx, PageIdx: len(m.bitmaps[idx]) - 1}, nil
	}
	return config.PageId{}, errors.New("no space: reached dm_maxfilecount")
}

// FreePage marks a page free.
func (m *DiskManager) FreePage(pid config.PageId) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if pid.FileIdx < 0 || pid.FileIdx >= m.cfg.DMMaxFileCount {
		return errors.New("invalid file idx")
	}
	if _, ok := m.bitmaps[pid.FileIdx]; !ok {
		if err := m.loadBitmap(pid.FileIdx); err != nil {
			return err
		}
	}
	if pid.PageIdx < 0 || pid.PageIdx >= len(m.bitmaps[pid.FileIdx]) {
		return errors.New("invalid page idx")
	}
	m.bitmaps[pid.FileIdx][pid.PageIdx] = 0
	return m.persistBitmap(pid.FileIdx)
}

// WritePage writes exactly one page worth of data to the page's offset.
func (m *DiskManager) WritePage(pid config.PageId, data []byte) error {
	if len(data) > m.cfg.PageSize {
		return errors.New("data too large")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if pid.FileIdx < 0 || pid.FileIdx >= m.cfg.DMMaxFileCount {
		return errors.New("invalid file idx")
	}
	if _, ok := m.bitmaps[pid.FileIdx]; !ok {
		if err := m.loadBitmap(pid.FileIdx); err != nil {
			return err
		}
	}
	if pid.PageIdx < 0 || pid.PageIdx >= len(m.bitmaps[pid.FileIdx]) {
		return errors.New("invalid page idx")
	}
	path := m.dataPath(pid.FileIdx)
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	off := int64(pid.PageIdx) * int64(m.cfg.PageSize)
	// ensure file large enough
	if stat, err := f.Stat(); err == nil {
		if stat.Size() < off+int64(m.cfg.PageSize) {
			// extend file with zeros
			if _, err := f.Seek(0, io.SeekEnd); err != nil {
				return err
			}
			if _, err := f.Write(make([]byte, off+int64(m.cfg.PageSize)-stat.Size())); err != nil {
				return err
			}
		}
	}
	// write at offset
	if _, err := f.WriteAt(padToPage(data, m.cfg.PageSize), off); err != nil {
		return err
	}
	// ensure data is written to disk
	if err := f.Sync(); err != nil {
		return err
	}
	return nil
}

// ReadPage reads exactly one page.
func (m *DiskManager) ReadPage(pid config.PageId) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if pid.FileIdx < 0 || pid.FileIdx >= m.cfg.DMMaxFileCount {
		return nil, errors.New("invalid file idx")
	}
	if _, ok := m.bitmaps[pid.FileIdx]; !ok {
		if err := m.loadBitmap(pid.FileIdx); err != nil {
			return nil, err
		}
	}
	if pid.PageIdx < 0 || pid.PageIdx >= len(m.bitmaps[pid.FileIdx]) {
		return nil, errors.New("invalid page idx")
	}
	path := m.dataPath(pid.FileIdx)
	f, err := os.OpenFile(path, os.O_RDONLY, 0o644)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	off := int64(pid.PageIdx) * int64(m.cfg.PageSize)
	buf := make([]byte, m.cfg.PageSize)
	if _, err := f.ReadAt(buf, off); err != nil && err != io.EOF {
		return nil, err
	}
	return buf, nil
}

func (m *DiskManager) Finish() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for idx := range m.bitmaps {
		if err := m.persistBitmap(idx); err != nil {
			return err
		}
	}
	return nil
}

// PageSize returns the configured page size.
func (m *DiskManager) PageSize() int {
	return m.cfg.PageSize
}

// BinDir returns the directory path used to store Data*.bin and metadata files.
func (m *DiskManager) BinDir() string {
	return m.binDir
}

func padToPage(data []byte, size int) []byte {
	if len(data) == size {
		return data
	}
	out := make([]byte, size)
	copy(out, data)
	return out
}
