package main

import (
	"bytes"
	"math/rand"
	"os"
	"time"
)

const (
	p               float32 = 0.5 // from Skip List paper; Redis/LevelDB use 0.25
	maxLevel        int     = 24  // arbitrary / don't remember why
	journalFilename         = "memtable.log"
)

type ClevelDB struct {
	mdb         *MemDB
	journal     bool
	journalFile *os.File
}

// MemDB - In-Memory Database (implemented by a Skip List)
type MemDB struct {
	header   *node
	topLevel int
	size     int
}

func init() {
	rand.Seed(time.Now().Unix())
}

type node struct {
	key  []byte
	val  []byte
	ptrs [maxLevel]*node
}

func GetClevelDB() (*ClevelDB, error) {
	journalFile, _ := os.OpenFile(journalFilename, os.O_APPEND|os.O_RDWR|os.O_CREATE, os.ModePerm)

	recoverJournal(journalFile)

	return newClevelDB(true, journalFile), nil
}

func newClevelDB(journal bool, journalFile *os.File) *ClevelDB {
	newDB := &ClevelDB{}

	newMdb := &MemDB{}
	newMdb.header = &node{}
	newMdb.topLevel = 1

	newDB.mdb = newMdb
	newDB.journal = journal
	newDB.journalFile = journalFile
	return newDB
}

func (db *ClevelDB) get(key []byte) (*node, error) {
	mdb := db.mdb

	// Start with pointers from the list's header node
	current := mdb.header
	searchKey := string(key)

	// Use pointers to look ahead until you find one equal to or larger and then stop
	for level := mdb.topLevel; level > 0; level-- {
		for current.ptrs[level-1] != nil && string(current.ptrs[level-1].key) < searchKey {
			current = current.ptrs[level-1]
		}
	}

	// Get the next pointer from the bottom level
	current = current.ptrs[0]

	// If the value matches, return it. Else, the matching key doesn't exist.
	if current != nil && string(current.key) == searchKey {
		return current, nil
	} else {
		return &node{}, nil
	}
}

func (db *ClevelDB) Get(key []byte) ([]byte, error) {
	node, err := db.get(key)
	if err != nil {
		return nil, err
	}

	return node.val, nil
}

func (db *ClevelDB) Put(key, val []byte) error {
	if db.journal {
		_, err := writeInsertToJournal(db.journalFile, key, val)
		if err != nil {
			return err
		}
	}

	mdb := db.mdb

	// Track nodes who have a forward pointer that will need to be updated if a new node is inserted
	update := make([]*node, maxLevel)
	current := mdb.header
	searchKey := string(key)

	for level := mdb.topLevel; level > 0; level-- {
		for current.ptrs[level-1] != nil && string(current.ptrs[level-1].key) < searchKey {
			current = current.ptrs[level-1]
		}
		// Prior to descending, store the rightmost node that was reached on the current level
		update[level-1] = current
	}

	current = current.ptrs[0]

	// If there is an existing node with the matching key, just update its value.
	// Otherwise, insert new node below.
	if current != nil && searchKey == string(current.val) {
		current.val = val
		return nil
	}

	// Delete new node (at random level)
	newLevel := randomLevel()

	// If the new level is higher than the current max level, we'll need to also
	// update the header node at the new higher levels, so we add the header's new
	// levels to the update vector
	if newLevel > mdb.topLevel {
		for level := mdb.topLevel; level < newLevel; level++ {
			update[level] = mdb.header
		}
		mdb.topLevel = newLevel
	}

	// Create new node with empty forward pointers
	newNode := &node{key: key, val: val}

	for i := 0; i < newLevel; i++ {
		// Use update vector to fill new node's forward pointers
		newNode.ptrs[i] = update[i].ptrs[i]
		// Re-direct the update vector's pointers to point at the new node
		update[i].ptrs[i] = newNode
	}

	mdb.size++
	return nil
}

func randomLevel() int {
	level := 1
	for rand.Float32() < p && level < maxLevel {
		level++
	}
	return level
}

func (db *ClevelDB) Delete(key []byte) error {
	if db.journal {
		_, err := writeDeleteToJournal(db.journalFile, key)
		if err != nil {
			return err
		}
	}

	mdb := db.mdb

	update := make([]*node, maxLevel)
	current := mdb.header
	searchKey := string(key)

	for level := mdb.topLevel; level > 0; level-- {
		for current.ptrs[level-1] != nil && string(current.ptrs[level-1].key) < searchKey {
			current = current.ptrs[level-1]
		}
		update[level-1] = current
	}

	current = current.ptrs[0]

	if searchKey != string(current.key) {
		return nil // key not found; nothing to delete
	} else {
		// Set to nil (i.e. tombstone), so that when a key is deleted and Get(key) is called, we'll stop looking once
		// we see the tombstone, and we won't keep looking and return the original value
		current.val = nil
	}

	return nil
}

func (db *ClevelDB) Size() int {
	return db.mdb.size
}

func (db *ClevelDB) RangeScan(start, limit []byte) (Iterator, error) {
	startNode, err := db.get(start)
	if err != nil {
		return nil, err
	}

	return &ClevelIterator{currentNode: startNode, limit: limit}, nil
}

type ClevelIterator struct {
	currentNode *node
	limit       []byte
}

func (i *ClevelIterator) Next() bool {
	if bytes.Compare(i.currentNode.key, i.limit) == 0 {
		return false
	}
	i.currentNode = i.currentNode.ptrs[0]
	return true
}

func (i *ClevelIterator) Error() error {
	return nil
}

func (i *ClevelIterator) Key() []byte {
	return i.currentNode.key
}

func (i *ClevelIterator) Value() []byte {
	return i.currentNode.val
}
