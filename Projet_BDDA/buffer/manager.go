package buffer

import (
	"container/list"
	"errors"
	"strconv"
	"sync"

	"malzahar-project/Projet_BDDA/config"
	"malzahar-project/Projet_BDDA/disk"
)

type ReplacementPolicy string

const (
	PolicyLRU ReplacementPolicy = "LRU"
	PolicyMRU ReplacementPolicy = "MRU"
)

type BufferFrame struct {
	PageId   config.PageId
	Data     []byte
	PinCount int
	Dirty    bool
}

type BufferManager struct {
	cfg    *config.DBConfig
	dm     *disk.DiskManager
	frames []*BufferFrame
	mu     sync.Mutex
	policy ReplacementPolicy
	// replacement list: front = oldest for LRU
	repl *list.List
	// map from page key to list element
	lookup map[string]*list.Element
}

func pageKey(pid config.PageId) string {
	return strconv.Itoa(pid.FileIdx) + ":" + strconv.Itoa(pid.PageIdx)
}

func NewBufferManager(cfg *config.DBConfig, dm *disk.DiskManager) *BufferManager {
	bm := &BufferManager{
		cfg:    cfg,
		dm:     dm,
		frames: make([]*BufferFrame, cfg.BMBufferCount),
		policy: PolicyLRU,
		repl:   list.New(),
		lookup: make(map[string]*list.Element),
	}
	if cfg.BMPolicy != "" {
		bm.policy = ReplacementPolicy(cfg.BMPolicy)
	}
	// use an explicit invalid PageId sentinel for unused frames (avoid zero-value collision with FileIdx=0,PageIdx=0)
	empty := config.PageId{FileIdx: -1, PageIdx: -1}
	for i := range bm.frames {
		bm.frames[i] = &BufferFrame{PageId: empty, Data: make([]byte, cfg.PageSize)}
	}
	return bm
}

// GetPage returns a buffer frame containing the page; applies replacement if needed.
func (bm *BufferManager) GetPage(pid config.PageId) (*BufferFrame, error) {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	key := pageKey(pid)
	if el, ok := bm.lookup[key]; ok {
		// move in repl list according to policy
		if bm.policy == PolicyLRU {
			bm.repl.MoveToBack(el)
		} else {
			bm.repl.MoveToFront(el)
		}
		fr := el.Value.(*BufferFrame)
		fr.PinCount++
		return fr, nil
	}
	// find free frame
	for _, f := range bm.frames {
		if f.PinCount == 0 && (f.PageId.FileIdx == -1 && f.PageId.PageIdx == -1) {
			// use this
			data, err := bm.dm.ReadPage(pid)
			if err != nil {
				return nil, err
			}
			copy(f.Data, data)
			f.PageId = pid
			f.PinCount = 1
			f.Dirty = false
			el := bm.repl.PushBack(f)
			bm.lookup[key] = el
			return f, nil
		}
	}
	// need to evict according to policy
	var victimEl *list.Element
	if bm.policy == PolicyLRU {
		victimEl = bm.repl.Front()
	} else {
		victimEl = bm.repl.Back()
	}
	if victimEl == nil {
		return nil, errors.New("no available frame to evict")
	}
	victim := victimEl.Value.(*BufferFrame)
	if victim.PinCount != 0 {
		return nil, errors.New("all frames pinned")
	}
	// write back if dirty
	if victim.Dirty {
		if err := bm.dm.WritePage(victim.PageId, victim.Data); err != nil {
			return nil, err
		}
	}
	delete(bm.lookup, pageKey(victim.PageId))
	// load requested page into victim
	data, err := bm.dm.ReadPage(pid)
	if err != nil {
		return nil, err
	}
	copy(victim.Data, data)
	victim.PageId = pid
	victim.PinCount = 1
	victim.Dirty = false
	if bm.policy == PolicyLRU {
		bm.repl.MoveToBack(victimEl)
	} else {
		bm.repl.MoveToFront(victimEl)
	}
	bm.lookup[key] = victimEl
	return victim, nil
}

func (bm *BufferManager) FreePage(pid config.PageId, valdirty bool) error {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	key := pageKey(pid)
	el, ok := bm.lookup[key]
	if !ok {
		return errors.New("page not found in buffers")
	}
	f := el.Value.(*BufferFrame)
	if f.PinCount > 0 {
		f.PinCount--
	}
	if valdirty {
		f.Dirty = true
	}
	return nil
}

func (bm *BufferManager) SetCurrentReplacementPolicy(policy string) {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	bm.policy = ReplacementPolicy(policy)
}

func (bm *BufferManager) FlushBuffers() error {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	for _, f := range bm.frames {
		if f.Dirty && f.PageId != (config.PageId{}) {
			if err := bm.dm.WritePage(f.PageId, f.Data); err != nil {
				return err
			}
			f.Dirty = false
		}
		// reset frame
		f.PageId = config.PageId{FileIdx: -1, PageIdx: -1}
		f.PinCount = 0
		for i := range f.Data {
			f.Data[i] = 0
		}
	}
	bm.repl.Init()
	bm.lookup = make(map[string]*list.Element)
	return nil
}
