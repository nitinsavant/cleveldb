package main

import "bytes"

// Memtable - In-Memory Database (backed by a Skip List)
type Memtable struct {
	header   *SkipListNode
	topLevel int
	size     int
}

func newMemtable() *Memtable {
	newMdb := &Memtable{}
	newMdb.header = &SkipListNode{}
	newMdb.topLevel = 1
	return newMdb
}

// Get : searches memtable (i.e. skip list) for key.
// If key isn't found, 'current' points to the closest key greater than that key (for multi-table RangeScan support)
// 'current' will be nil if we reached the end of the list while searching
// skipListNode.val will be nil if key was deleted (i.e. tombstone)
func (mem *Memtable) Get(key []byte) (*SkipListNode, error) {

	// Start with pointers from the list's header node
	current := mem.header
	searchKey := string(key)

	// Use pointers to look ahead until you find one equal to or larger and then stop
	for level := mem.topLevel; level > 0; level-- {
		for current.ptrs[level-1] != nil && string(current.ptrs[level-1].key) < searchKey {
			current = current.ptrs[level-1]
		}
	}

	// Get the next pointer from the bottom level
	current = current.ptrs[0]

	// If key is found (including tombstone), return it.
	if current != nil && string(current.key) == searchKey {
		return current, nil
	}

	return current, notFoundInTableErr
}

func (mem *Memtable) RangeScan(start, limit []byte) (Iterator, error) {
	currentNode, err := mem.Get(start)
	if err != nil && err != notFoundInTableErr {
		return nil, err
	}

	return &MemtableIterator{
			currentNode: currentNode,
			limit:       limit,
		},
		nil
}

type MemtableIterator struct {
	currentNode *SkipListNode
	limit       []byte
}

func (i *MemtableIterator) Next() bool {
	if bytes.Compare(i.currentNode.key, i.limit) >= 0 {
		return false
	}

	i.currentNode = i.currentNode.ptrs[0]

	// Reached end-of-table
	if i.currentNode == nil {
		return false
	}

	return true
}

func (i *MemtableIterator) Error() error {
	return nil
}

func (i *MemtableIterator) Key() []byte {
	return i.currentNode.key
}

func (i *MemtableIterator) Value() []byte {
	return i.currentNode.val
}
