package main

import (
	"bytes"
	"math/rand"
	"os"
	"time"
)

const (
	p           float32 = 0.5
	maxLevel    int     = 24
	logFileName         = "memtable.log"
)

var logFile *os.File

func init() {
	rand.Seed(time.Now().Unix())

	logFile, _ = os.OpenFile(logFileName, os.O_APPEND|os.O_RDWR|os.O_CREATE, os.ModePerm)
}

type node struct {
	key  []byte
	val  []byte
	ptrs [maxLevel]*node
}

type ClevelDB struct {
	header   *node
	topLevel int
	size     int
}

func GetClevelDB(loadFromFile bool) (*ClevelDB, error) {
	if loadFromFile {
		return loadMemtable(), nil
	}

	return newClevelDB(), nil
}

func newClevelDB() *ClevelDB {
	newDB := &ClevelDB{}
	newDB.header = &node{}
	newDB.topLevel = 1
	return newDB
}

func (db *ClevelDB) get(key []byte) (*node, error) {
	// Start with pointers from the list's header node
	current := db.header
	searchKey := string(key)

	// Use pointers to look ahead until you find one equal to or larger and then stop
	for level := db.topLevel; level > 0; level-- {
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
	err := logInsert(key, val)
	if err != nil {
		return err
	}
	return db.put(key, val)
}

func (db *ClevelDB) put(key, val []byte) error {
	// Track nodes who have a forward pointer that will need to be updated if a new node is inserted
	update := make([]*node, maxLevel)
	current := db.header
	searchKey := string(key)

	for level := db.topLevel; level > 0; level-- {
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

	// Insert new node (at random level)
	newLevel := randomLevel()

	// If the new level is higher than the current max level, we'll need to also
	// update the header node at the new higher levels, so we add the header's new
	// levels to the update vector
	if newLevel > db.topLevel {
		for level := db.topLevel; level < newLevel; level++ {
			update[level] = db.header
		}
		db.topLevel = newLevel
	}

	// Create new node with empty forward pointers
	newNode := &node{key: key, val: val}

	for i := 0; i < newLevel; i++ {
		// Use update vector to fill new node's forward pointers
		newNode.ptrs[i] = update[i].ptrs[i]
		// Re-direct the update vector's pointers to point at the new node
		update[i].ptrs[i] = newNode
	}

	db.size++

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
	err := logDelete(key)
	if err != nil {
		return err
	}
	return db.delete(key)
}

func (db *ClevelDB) delete(key []byte) error {
	update := make([]*node, maxLevel)
	current := db.header
	searchKey := string(key)

	for level := db.topLevel; level > 0; level-- {
		for current.ptrs[level-1] != nil && string(current.ptrs[level-1].key) < searchKey {
			current = current.ptrs[level-1]
		}
		update[level-1] = current
	}

	current = current.ptrs[0]

	if searchKey != string(current.key) {
		return nil // key not found; nothing to delete
	}

	for i := 0; i < db.topLevel; i++ {
		if update[i].ptrs[i] != current {
			break
		}
		update[i].ptrs[i] = current.ptrs[i]
	}

	for db.topLevel > 1 && db.header.ptrs[db.topLevel] == nil {
		db.topLevel--
	}

	db.size--
	return nil
}

func (db *ClevelDB) Size() int {
	return db.size
}

func (db *ClevelDB) RangeScan(start, limit []byte) (Iterator, error) {
	startNode, err := db.get(start)
	if err != nil {
		return nil, err
	}

	return &ClevelIterator{currentNode: startNode, limitVal: limit}, nil
}

type ClevelIterator struct {
	currentNode *node
	limitVal    []byte
}

func (i *ClevelIterator) Next() bool {
	if bytes.Compare(i.currentNode.val, i.limitVal) == 0 {
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
