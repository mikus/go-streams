package flow

import (
	"sync"

	"github.com/reugn/go-streams"
)

type IndentifierExtractor[T comparable] func(interface{}) T

type void struct{}

var member void

type mapEntry struct {
	data    []any
	indexes map[int]void
}

func newMapEntry(size int) mapEntry {
	return mapEntry{
		data:    make([]any, size),
		indexes: make(map[int]void, size),
	}
}

func (entry *mapEntry) set(idx int, value any) {
	entry.indexes[idx] = member
	entry.data[idx] = value
}

func (entry *mapEntry) size() int {
	return len(entry.indexes)
}

type joinElementsMap[T comparable] struct {
	data      map[T]mapEntry
	entrySize int
	mtx       sync.Mutex
}

func newJoinElementMap[T comparable](entrySize int) joinElementsMap[T] {
	return joinElementsMap[T]{
		data:      make(map[T]mapEntry),
		entrySize: entrySize,
	}
}

func (m *joinElementsMap[T]) store(key T, idx int, value any) mapEntry {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	entry, ok := m.data[key]
	if !ok {
		entry = newMapEntry(m.entrySize)
	}
	entry.set(idx, value)
	m.data[key] = entry

	return entry
}

func (m *joinElementsMap[T]) delete(key T) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	delete(m.data, key)
}

func Join[T comparable](outlets []streams.Flow, identifiers ...IndentifierExtractor[T]) streams.Flow {
	if len(outlets) != len(identifiers) {
		panic("Number of joined flows has to be equal the number of identifier extractors")
	}
	size := len(outlets)
	data := newJoinElementMap[T](size)
	merged := NewPassThrough()
	var wg sync.WaitGroup
	wg.Add(size)

	for idx, out := range outlets {
		go func(idx int, outlet streams.Outlet) {
			for element := range outlet.Out() {
				key := identifiers[idx](element)
				entry := data.store(key, idx, element)
				if entry.size() == size {
					merged.In() <- entry.data
					data.delete(key)
				}
			}
			wg.Done()
		}(idx, out)
	}

	// close the in channel on the last outlet close.
	go func(wg *sync.WaitGroup) {
		wg.Wait()
		close(merged.In())
	}(&wg)

	return merged
}
