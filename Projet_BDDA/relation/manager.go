package relation

import (
	"encoding/binary"
	"errors"
	"math"
	"os"
	"path/filepath"

	"malzahar-project/Projet_BDDA/buffer"
	"malzahar-project/Projet_BDDA/config"
	"malzahar-project/Projet_BDDA/disk"
)

// RecordId identifies a record by page and slot index.
type RecordId struct {
	PageId  config.PageId
	SlotIdx int
}

// RelationManager manages a relation's heap file: header page, data pages, and provides
// higher-level insertion/enumeration APIs.
type RelationManager struct {
	Rel          *Relation
	HeaderPageId config.PageId
	SlotsPerPage int
	dm           *disk.DiskManager
	bm           *buffer.BufferManager
}

// sentinel for invalid PageId
var invalidPage = config.PageId{FileIdx: -1, PageIdx: -1}

// NewRelationManager creates a RelationManager and allocates a header page persisted on disk.
func NewRelationManager(rel *Relation, dm *disk.DiskManager, bm *buffer.BufferManager) (*RelationManager, error) {
	rm := &RelationManager{Rel: rel, dm: dm, bm: bm, HeaderPageId: invalidPage}
	// try load header location from metadata file
	if err := rm.loadHeaderLocation(); err != nil {
		// if file does not exist, it's fine; other errors bubble up
		if !os.IsNotExist(err) {
			return nil, err
		}
	}
	// if header exists, compute slots per page
	if rm.HeaderPageId != invalidPage {
		rm.SlotsPerPage = computeSlotsPerPage(rm.dm.PageSize(), rm.Rel.RecordSize)
	}
	return rm, nil
}

// header metadata file: stores 8 bytes (int32 fileIdx, int32 pageIdx)
func (rm *RelationManager) headerFilePath() string {
	return filepath.Join(rm.dm.BinDir(), rm.Rel.Name+".hdr")
}

func (rm *RelationManager) saveHeaderLocation(pid config.PageId) error {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint32(buf[0:4], uint32(pid.FileIdx))
	binary.LittleEndian.PutUint32(buf[4:8], uint32(pid.PageIdx))
	return os.WriteFile(rm.headerFilePath(), buf, 0o644)
}

func (rm *RelationManager) loadHeaderLocation() error {
	data, err := os.ReadFile(rm.headerFilePath())
	if err != nil {
		return err
	}
	if len(data) < 8 {
		return errors.New("invalid header metadata")
	}
	fi := int32(binary.LittleEndian.Uint32(data[0:4]))
	pi := int32(binary.LittleEndian.Uint32(data[4:8]))
	rm.HeaderPageId = config.PageId{FileIdx: int(fi), PageIdx: int(pi)}
	return nil
}

// helpers to interpret page headers
func (rm *RelationManager) pageNext(pid config.PageId) (config.PageId, error) {
	bf, err := rm.bm.GetPage(pid)
	if err != nil {
		return config.PageId{}, err
	}
	fx := int32(binary.LittleEndian.Uint32(bf.Data[8:12]))
	fy := int32(binary.LittleEndian.Uint32(bf.Data[12:16]))
	if err := rm.bm.FreePage(pid, false); err != nil {
		return config.PageId{}, err
	}
	if fx == -1 && fy == -1 {
		return invalidPage, nil
	}
	return config.PageId{FileIdx: int(fx), PageIdx: int(fy)}, nil
}

func (rm *RelationManager) pageSetNext(pid config.PageId, next config.PageId) error {
	bf, err := rm.bm.GetPage(pid)
	if err != nil {
		return err
	}
	if next == invalidPage {
		writeInt32(bf.Data, 8, int32(-1))
		writeInt32(bf.Data, 12, int32(-1))
	} else {
		binary.LittleEndian.PutUint32(bf.Data[8:12], uint32(next.FileIdx))
		binary.LittleEndian.PutUint32(bf.Data[12:16], uint32(next.PageIdx))
	}
	bf.Dirty = true
	return rm.bm.FreePage(pid, true)
}

func (rm *RelationManager) pageNumSlots(pid config.PageId) (int, error) {
	bf, err := rm.bm.GetPage(pid)
	if err != nil {
		return 0, err
	}
	n := int(binary.LittleEndian.Uint32(bf.Data[16:20]))
	if err := rm.bm.FreePage(pid, false); err != nil {
		return 0, err
	}
	return n, nil
}

// header accessors
func (rm *RelationManager) headerFirstWithSpace() (config.PageId, error) {
	if rm.HeaderPageId == invalidPage {
		return invalidPage, nil
	}
	hbf, err := rm.bm.GetPage(rm.HeaderPageId)
	if err != nil {
		return config.PageId{}, err
	}
	fx := int32(binary.LittleEndian.Uint32(hbf.Data[8:12]))
	fy := int32(binary.LittleEndian.Uint32(hbf.Data[12:16]))
	if err := rm.bm.FreePage(rm.HeaderPageId, false); err != nil {
		return config.PageId{}, err
	}
	if fx == -1 && fy == -1 {
		return invalidPage, nil
	}
	return config.PageId{FileIdx: int(fx), PageIdx: int(fy)}, nil
}

func (rm *RelationManager) headerSetFirstWithSpace(pid config.PageId) error {
	if rm.HeaderPageId == invalidPage {
		return errors.New("header not initialized")
	}
	hbf, err := rm.bm.GetPage(rm.HeaderPageId)
	if err != nil {
		return err
	}
	if pid == invalidPage {
		writeInt32(hbf.Data, 8, int32(-1))
		writeInt32(hbf.Data, 12, int32(-1))
	} else {
		binary.LittleEndian.PutUint32(hbf.Data[8:12], uint32(pid.FileIdx))
		binary.LittleEndian.PutUint32(hbf.Data[12:16], uint32(pid.PageIdx))
	}
	hbf.Dirty = true
	return rm.bm.FreePage(rm.HeaderPageId, true)
}

// helper to check free slot in page and return first free slot idx or -1
func (rm *RelationManager) firstFreeSlotInPage(pid config.PageId) (int, error) {
	bf, err := rm.bm.GetPage(pid)
	if err != nil {
		return -1, err
	}
	slots := int(binary.LittleEndian.Uint32(bf.Data[16:20]))
	base := 20
	for i := 0; i < slots; i++ {
		if bf.Data[base+i] == 0 {
			if err := rm.bm.FreePage(pid, false); err != nil {
				return -1, err
			}
			return i, nil
		}
	}
	if err := rm.bm.FreePage(pid, false); err != nil {
		return -1, err
	}
	return -1, nil
}

// InsertRecord inserts rec into a page and returns its RecordId
func (rm *RelationManager) InsertRecord(rec *Record) (RecordId, error) {
	// ensure slots per page computed
	if rm.SlotsPerPage == 0 {
		rm.SlotsPerPage = computeSlotsPerPage(rm.dm.PageSize(), rm.Rel.RecordSize)
	}
	// ensure header exists
	if rm.HeaderPageId == invalidPage {
		if _, err := rm.addDataPage(); err != nil {
			return RecordId{}, err
		}
	}
	// find a page with space
	cur, err := rm.headerFirstWithSpace()
	if err != nil {
		return RecordId{}, err
	}
	if cur == invalidPage {
		// no page with space -> create one
		npid, err := rm.addDataPage()
		if err != nil {
			return RecordId{}, err
		}
		cur = npid
	}
	// traverse pages starting at cur until find free slot
	visited := make(map[config.PageId]bool)
	for pid := cur; pid != invalidPage; {
		if visited[pid] {
			// cycle detected, create new page
			npid, err := rm.addDataPage()
			if err != nil {
				return RecordId{}, err
			}
			pid = npid
			// continue with new page (don't mark as visited yet)
			continue
		}
		visited[pid] = true
		slot, err := rm.firstFreeSlotInPage(pid)
		if err != nil {
			return RecordId{}, err
		}
		if slot >= 0 {
			// write record into page
			bf, err := rm.bm.GetPage(pid)
			if err != nil {
				return RecordId{}, err
			}
			slots := int(binary.LittleEndian.Uint32(bf.Data[16:20]))
			dataStart := 20 + slots
			pos := dataStart + slot*rm.Rel.RecordSize
			if err := rm.Rel.WriteRecordToBuffer(rec, bf.Data, pos); err != nil {
				_ = rm.bm.FreePage(pid, false)
				return RecordId{}, err
			}
			// mark bytemap
			bf.Data[20+slot] = 1
			// check if page now full
			full := true
			for i := 0; i < slots; i++ {
				if bf.Data[20+i] == 0 {
					full = false
					break
				}
			}
			bf.Dirty = true
			if err := rm.bm.FreePage(pid, true); err != nil {
				return RecordId{}, err
			}
			if full {
				// if page became full, unlink from with-space list
				if err := rm.unlinkFromWithSpace(pid); err != nil {
					return RecordId{}, err
				}
				// add to full list (prepend)
				if err := rm.prependToFullList(pid); err != nil {
					return RecordId{}, err
				}
			}
			return RecordId{PageId: pid, SlotIdx: slot}, nil
		}
		// move to next
		next, err := rm.pageNext(pid)
		if err != nil {
			return RecordId{}, err
		}
		pid = next
		if pid == invalidPage {
			// exhausted list, create a new data page and try again
			npid, err := rm.addDataPage()
			if err != nil {
				return RecordId{}, err
			}
			pid = npid
		}
	}
	return RecordId{}, errors.New("could not insert record")
}

// helper: unlink a page from the with-space list; header->firstWithSpace may change
func (rm *RelationManager) unlinkFromWithSpace(target config.PageId) error {
	head, err := rm.headerFirstWithSpace()
	if err != nil {
		return err
	}
	if head == invalidPage {
		return nil
	}
	// if head is target
	if head == target {
		// set header firstWithSpace = head.next
		nx, err := rm.pageNext(head)
		if err != nil {
			return err
		}
		return rm.headerSetFirstWithSpace(nx)
	}
	// otherwise traverse to find predecessor
	prev := head
	for prev != invalidPage {
		curNext, err := rm.pageNext(prev)
		if err != nil {
			return err
		}
		if curNext == target {
			// get target next
			tnext, err := rm.pageNext(target)
			if err != nil {
				return err
			}
			// set prev.next = tnext
			return rm.pageSetNext(prev, tnext)
		}
		prev = curNext
	}
	return nil
}

func (rm *RelationManager) prependToFullList(pid config.PageId) error {
	if rm.HeaderPageId == invalidPage {
		return errors.New("header not initialized")
	}
	hbf, err := rm.bm.GetPage(rm.HeaderPageId)
	if err != nil {
		return err
	}
	// current firstFull at 0..3 and 4..7
	fx := int32(binary.LittleEndian.Uint32(hbf.Data[0:4]))
	fy := int32(binary.LittleEndian.Uint32(hbf.Data[4:8]))
	_ = rm.bm.FreePage(rm.HeaderPageId, false)
	var old config.PageId
	if fx == -1 && fy == -1 {
		old = invalidPage
	} else {
		old = config.PageId{FileIdx: int(fx), PageIdx: int(fy)}
	}
	// if already the head, nothing to do (avoid creating self-loop)
	if old == pid {
		return nil
	}
	// set target.next = old firstFull
	if err := rm.pageSetNext(pid, old); err != nil {
		return err
	}
	// set header.firstFull = pid
	hbf2, err := rm.bm.GetPage(rm.HeaderPageId)
	if err != nil {
		return err
	}
	binary.LittleEndian.PutUint32(hbf2.Data[0:4], uint32(pid.FileIdx))
	binary.LittleEndian.PutUint32(hbf2.Data[4:8], uint32(pid.PageIdx))
	hbf2.Dirty = true
	return rm.bm.FreePage(rm.HeaderPageId, true)
}

// GetAllRecords returns all records present in the relation by scanning both lists
func (rm *RelationManager) GetAllRecords() ([]Record, error) {
	var out []Record
	// ensure header exists
	if rm.HeaderPageId == invalidPage {
		return out, nil
	}
	// scan with-space list
	whead, err := rm.headerFirstWithSpace()
	if err != nil {
		return nil, err
	}
	for pid := whead; pid != invalidPage; {
		recs, nxt, err := rm.recordsInDataPage(pid)
		if err != nil {
			return nil, err
		}
		out = append(out, recs...)
		pid = nxt
	}
	// scan full list
	// read header firstFull
	if rm.HeaderPageId != invalidPage {
		hbf, err := rm.bm.GetPage(rm.HeaderPageId)
		if err != nil {
			return nil, err
		}
		fx := int32(binary.LittleEndian.Uint32(hbf.Data[0:4]))
		fy := int32(binary.LittleEndian.Uint32(hbf.Data[4:8]))
		_ = rm.bm.FreePage(rm.HeaderPageId, false)
		for pid := func() config.PageId {
			if fx == -1 && fy == -1 {
				return invalidPage
			}
			return config.PageId{FileIdx: int(fx), PageIdx: int(fy)}
		}(); pid != invalidPage; {
			recs, nxt, err := rm.recordsInDataPage(pid)
			if err != nil {
				return nil, err
			}
			out = append(out, recs...)
			pid = nxt
		}
	}
	return out, nil
}

// recordsInDataPage returns records in the given page and the next page id
func (rm *RelationManager) recordsInDataPage(pid config.PageId) ([]Record, config.PageId, error) {
	bf, err := rm.bm.GetPage(pid)
	if err != nil {
		return nil, invalidPage, err
	}
	slots := int(binary.LittleEndian.Uint32(bf.Data[16:20]))
	dataStart := 20 + slots
	var out []Record
	for i := 0; i < slots; i++ {
		if bf.Data[20+i] == 1 {
			rec := &Record{}
			if err := rm.Rel.ReadFromBuffer(rec, bf.Data, dataStart+i*rm.Rel.RecordSize); err != nil {
				_ = rm.bm.FreePage(pid, false)
				return nil, invalidPage, err
			}
			out = append(out, *rec)
		}
	}
	nx := int32(binary.LittleEndian.Uint32(bf.Data[8:12]))
	ny := int32(binary.LittleEndian.Uint32(bf.Data[12:16]))
	if err := rm.bm.FreePage(pid, false); err != nil {
		return nil, invalidPage, err
	}
	if nx == -1 && ny == -1 {
		return out, invalidPage, nil
	}
	return out, config.PageId{FileIdx: int(nx), PageIdx: int(ny)}, nil
}

// DeleteRecord frees a slot; updates header lists if needed
func (rm *RelationManager) DeleteRecord(rid RecordId) error {
	pid := rid.PageId
	bf, err := rm.bm.GetPage(pid)
	if err != nil {
		return err
	}
	slots := int(binary.LittleEndian.Uint32(bf.Data[16:20]))
	if rid.SlotIdx < 0 || rid.SlotIdx >= slots {
		_ = rm.bm.FreePage(pid, false)
		return errors.New("invalid slot index")
	}
	if bf.Data[20+rid.SlotIdx] == 0 {
		_ = rm.bm.FreePage(pid, false)
		return errors.New("slot already free")
	}
	bf.Data[20+rid.SlotIdx] = 0
	// optionally zero record bytes
	dataStart := 20 + slots
	for i := 0; i < rm.Rel.RecordSize; i++ {
		bf.Data[dataStart+rid.SlotIdx*rm.Rel.RecordSize+i] = 0
	}
	bf.Dirty = true
	if err := rm.bm.FreePage(pid, true); err != nil {
		return err
	}
	// if page was in full list, move it to with-space list
	// naive approach: ensure it's present in with-space list
	// check if any free slots remain
	slot, err := rm.firstFreeSlotInPage(pid)
	if err != nil {
		return err
	}
	if slot >= 0 {
		// ensure page is in with-space list
		// attempt to unlink from full list if present
		if err := rm.unlinkFromFull(pid); err != nil {
			return err
		}
		// prepend to with-space
		if err := rm.prependToWithSpace(pid); err != nil {
			return err
		}
	}
	return nil
}

func (rm *RelationManager) unlinkFromFull(target config.PageId) error {
	if rm.HeaderPageId == invalidPage {
		return nil
	}
	hbf, err := rm.bm.GetPage(rm.HeaderPageId)
	if err != nil {
		return err
	}
	fx := int32(binary.LittleEndian.Uint32(hbf.Data[0:4]))
	fy := int32(binary.LittleEndian.Uint32(hbf.Data[4:8]))
	_ = rm.bm.FreePage(rm.HeaderPageId, false)
	head := func() config.PageId {
		if fx == -1 && fy == -1 {
			return invalidPage
		}
		return config.PageId{FileIdx: int(fx), PageIdx: int(fy)}
	}()
	if head == invalidPage {
		return nil
	}
	if head == target {
		nx, err := rm.pageNext(head)
		if err != nil {
			return err
		}
		// set header.firstFull = nx
		hbf2, err := rm.bm.GetPage(rm.HeaderPageId)
		if err != nil {
			return err
		}
		if nx == invalidPage {
			writeInt32(hbf2.Data, 0, int32(-1))
			writeInt32(hbf2.Data, 4, int32(-1))
		} else {
			binary.LittleEndian.PutUint32(hbf2.Data[0:4], uint32(nx.FileIdx))
			binary.LittleEndian.PutUint32(hbf2.Data[4:8], uint32(nx.PageIdx))
		}
		hbf2.Dirty = true
		return rm.bm.FreePage(rm.HeaderPageId, true)
	}
	// traverse
	prev := head
	for prev != invalidPage {
		curNext, err := rm.pageNext(prev)
		if err != nil {
			return err
		}
		if curNext == target {
			// set prev.next = target.next
			tnext, err := rm.pageNext(target)
			if err != nil {
				return err
			}
			return rm.pageSetNext(prev, tnext)
		}
		prev = curNext
	}
	return nil
}

func (rm *RelationManager) prependToWithSpace(pid config.PageId) error {
	if rm.HeaderPageId == invalidPage {
		return errors.New("header not initialized")
	}
	hbf, err := rm.bm.GetPage(rm.HeaderPageId)
	if err != nil {
		return err
	}
	fx := int32(binary.LittleEndian.Uint32(hbf.Data[8:12]))
	fy := int32(binary.LittleEndian.Uint32(hbf.Data[12:16]))
	_ = rm.bm.FreePage(rm.HeaderPageId, false)
	var old config.PageId
	if fx == -1 && fy == -1 {
		old = invalidPage
	} else {
		old = config.PageId{FileIdx: int(fx), PageIdx: int(fy)}
	}
	// if already the head, nothing to do (avoid creating self-loop)
	if old == pid {
		return nil
	}
	if err := rm.pageSetNext(pid, old); err != nil {
		return err
	}
	return rm.headerSetFirstWithSpace(pid)
}

// helper: write ints little-endian into buffer
func writeInt32(b []byte, off int, v int32) {
	binary.LittleEndian.PutUint32(b[off:off+4], uint32(v))
}

func readInt32(b []byte, off int) int32 {
	return int32(binary.LittleEndian.Uint32(b[off : off+4]))
}

// computeSlotsPerPage calculates how many slots fit in a page, given pageSize and recordSize.
// headerFixed = prev(8) + next(8) + numSlots(4) = 20 bytes
func computeSlotsPerPage(pageSize int, recordSize int) int {
	headerFixed := 20
	// each slot requires 1 byte in bytemap and recordSize bytes in data
	return int(math.Floor(float64(pageSize-headerFixed) / float64(1+recordSize)))
}

// addDataPage allocates a new data page, initializes its header (prev/next = invalid) and
// an empty bytemap. It inserts the new page into the 'with space' list via the header page.
func (rm *RelationManager) addDataPage() (config.PageId, error) {
	// allocate a new page via DiskManager
	pid, err := rm.dm.AllocatePage()
	if err != nil {
		return config.PageId{}, err
	}

	// compute slotsPerPage now that we have PageSize from DiskManager
	pageSize := rm.dm.PageSize()
	slots := computeSlotsPerPage(pageSize, rm.Rel.RecordSize)
	if slots <= 0 {
		return config.PageId{}, errors.New("page too small for records")
	}

	// load page into buffer
	bf, err := rm.bm.GetPage(pid)
	if err != nil {
		return config.PageId{}, err
	}
	// initialize header: prev(FileIdx,PageIdx), next(FileIdx,PageIdx), numSlots
	// prev = invalid, next = invalid
	writeInt32(bf.Data, 0, int32(-1))
	writeInt32(bf.Data, 4, int32(-1))
	writeInt32(bf.Data, 8, int32(-1))
	writeInt32(bf.Data, 12, int32(-1))
	writeInt32(bf.Data, 16, int32(slots))
	// zero bytemap
	for i := 0; i < slots; i++ {
		bf.Data[20+i] = 0
	}
	bf.Dirty = true
	// free page (mark dirty)
	if err := rm.bm.FreePage(pid, true); err != nil {
		return config.PageId{}, err
	}

	// update header page: if none, create it
	if rm.HeaderPageId == invalidPage {
		hpid, err := rm.dm.AllocatePage()
		if err != nil {
			return config.PageId{}, err
		}
		// write header: firstFull=(-1,-1), firstWithSpace = pid
		hbf, err := rm.bm.GetPage(hpid)
		if err != nil {
			return config.PageId{}, err
		}
		// firstFull
		writeInt32(hbf.Data, 0, int32(-1))
		writeInt32(hbf.Data, 4, int32(-1))
		// firstWithSpace -> pid
		writeInt32(hbf.Data, 8, int32(pid.FileIdx))
		writeInt32(hbf.Data, 12, int32(pid.PageIdx))
		hbf.Dirty = true
		if err := rm.bm.FreePage(hpid, true); err != nil {
			return config.PageId{}, err
		}
		rm.HeaderPageId = hpid
		// persist header location
		if err := rm.saveHeaderLocation(hpid); err != nil {
			return config.PageId{}, err
		}
	} else {
		// load header and set the new page as head of with-space list
		hbf, err := rm.bm.GetPage(rm.HeaderPageId)
		if err != nil {
			return config.PageId{}, err
		}
		// existing firstWithSpace at offset 8..11,12..15
		oldFx := readInt32(hbf.Data, 8)
		oldFy := readInt32(hbf.Data, 12)
		// set new page's next to old head
		// set new page prev = -1 (already), next = old
		// and update header firstWithSpace to pid
		writeInt32(hbf.Data, 8, int32(pid.FileIdx))
		writeInt32(hbf.Data, 12, int32(pid.PageIdx))
		hbf.Dirty = true
		if err := rm.bm.FreePage(rm.HeaderPageId, true); err != nil {
			return config.PageId{}, err
		}
		// also set new page's next pointers to old head
		npbf, err := rm.bm.GetPage(pid)
		if err != nil {
			return config.PageId{}, err
		}
		writeInt32(npbf.Data, 8, oldFx)
		writeInt32(npbf.Data, 12, oldFy)
		npbf.Dirty = true
		if err := rm.bm.FreePage(pid, true); err != nil {
			return config.PageId{}, err
		}
	}

	rm.SlotsPerPage = slots
	return pid, nil
}

// EnsureHeader ensures the relation's header page exists by creating one if absent.
// This is exported for callers that want the header initialized at table creation time.
func (rm *RelationManager) EnsureHeader() error {
	if rm.HeaderPageId != invalidPage {
		return nil
	}
	_, err := rm.addDataPage()
	return err
}

// AllPageIds returns all data page ids (both with-space and full lists) belonging to the relation.
func (rm *RelationManager) AllPageIds() ([]config.PageId, error) {
	var out []config.PageId
	if rm.HeaderPageId == invalidPage {
		return out, nil
	}
	visited := make(map[config.PageId]bool)

	// with-space list
	whead, err := rm.headerFirstWithSpace()
	if err != nil {
		return nil, err
	}
	for pid := whead; pid != invalidPage; {
		if visited[pid] {
			// cycle detected, break
			break
		}
		visited[pid] = true
		out = append(out, pid)
		nx, err := rm.pageNext(pid)
		if err != nil {
			return nil, err
		}
		pid = nx
	}
	// full list
	hbf, err := rm.bm.GetPage(rm.HeaderPageId)
	if err != nil {
		return nil, err
	}
	fx := int32(binary.LittleEndian.Uint32(hbf.Data[0:4]))
	fy := int32(binary.LittleEndian.Uint32(hbf.Data[4:8]))
	_ = rm.bm.FreePage(rm.HeaderPageId, false)
	for pid := func() config.PageId {
		if fx == -1 && fy == -1 {
			return invalidPage
		}
		return config.PageId{FileIdx: int(fx), PageIdx: int(fy)}
	}(); pid != invalidPage; {
		if visited[pid] {
			// cycle detected, break
			break
		}
		visited[pid] = true
		out = append(out, pid)
		nx, err := rm.pageNext(pid)
		if err != nil {
			return nil, err
		}
		pid = nx
	}
	return out, nil
}

// ScanRecords iterates all records in the relation and calls cb for each record with its RecordId.
// If cb returns an error, scanning stops and the error is returned.
func (rm *RelationManager) ScanRecords(cb func(rec Record, rid RecordId) error) error {
	if rm.HeaderPageId == invalidPage {
		return nil
	}
	// helper to scan a single page
	scanPage := func(pid config.PageId) (config.PageId, error) {
		bf, err := rm.bm.GetPage(pid)
		if err != nil {
			return invalidPage, err
		}
		slots := int(binary.LittleEndian.Uint32(bf.Data[16:20]))
		dataStart := 20 + slots
		for i := 0; i < slots; i++ {
			if bf.Data[20+i] == 1 {
				rec := &Record{}
				if err := rm.Rel.ReadFromBuffer(rec, bf.Data, dataStart+i*rm.Rel.RecordSize); err != nil {
					_ = rm.bm.FreePage(pid, false)
					return invalidPage, err
				}
				rid := RecordId{PageId: pid, SlotIdx: i}
				if err := cb(*rec, rid); err != nil {
					_ = rm.bm.FreePage(pid, false)
					return invalidPage, err
				}
			}
		}
		nx := int32(binary.LittleEndian.Uint32(bf.Data[8:12]))
		ny := int32(binary.LittleEndian.Uint32(bf.Data[12:16]))
		if err := rm.bm.FreePage(pid, false); err != nil {
			return invalidPage, err
		}
		if nx == -1 && ny == -1 {
			return invalidPage, nil
		}
		return config.PageId{FileIdx: int(nx), PageIdx: int(ny)}, nil
	}

	// scan with-space list
	whead, err := rm.headerFirstWithSpace()
	if err != nil {
		return err
	}
	visited := make(map[config.PageId]bool)
	for pid := whead; pid != invalidPage; {
		if visited[pid] {
			break // cycle detected
		}
		visited[pid] = true
		nxt, err := scanPage(pid)
		if err != nil {
			return err
		}
		pid = nxt
	}

	// scan full list
	if rm.HeaderPageId != invalidPage {
		hbf, err := rm.bm.GetPage(rm.HeaderPageId)
		if err != nil {
			return err
		}
		fx := int32(binary.LittleEndian.Uint32(hbf.Data[0:4]))
		fy := int32(binary.LittleEndian.Uint32(hbf.Data[4:8]))
		_ = rm.bm.FreePage(rm.HeaderPageId, false)
		for pid := func() config.PageId {
			if fx == -1 && fy == -1 {
				return invalidPage
			}
			return config.PageId{FileIdx: int(fx), PageIdx: int(fy)}
		}(); pid != invalidPage; {
			if visited[pid] {
				break // cycle detected
			}
			visited[pid] = true
			nxt, err := scanPage(pid)
			if err != nil {
				return err
			}
			pid = nxt
		}
	}
	return nil
}
