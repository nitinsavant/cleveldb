package main

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"time"
)

const (
	p                      float32 = 0.5 // from Skip List paper; Redis/LevelDB use 0.25
	maxLevel               int     = 24  // arbitrary (i.e. don't remember)
	journalFilename                = "journal.log"
	maxMemtableSizeInBytes int     = 4000 // kept small for testing
)

var notFoundInTableErr = errors.New("key not found in table")
var notFoundInDBErr = errors.New("key not found in db")
var deletedErr = errors.New("key is deleted")

type ClevelDB struct {
	memtable         *Memtable
	flushingMemtable *Memtable
	tables           []*SSTable
	journal          bool
	journalFile      *os.File
}

func init() {
	rand.Seed(time.Now().Unix())
}

type SkipListNode struct {
	key  []byte
	val  []byte
	ptrs [maxLevel]*SkipListNode
}

func loadClevelDB() (*ClevelDB, error) {
	journalFile, _ := os.OpenFile(journalFilename, os.O_APPEND|os.O_RDWR|os.O_CREATE, os.ModePerm)

	db := recoverMemtable(journalFile)
	if db.Size() == 0 {
		db = newClevelDB(true, journalFile)
	}

	tables := loadSSTables(ssTablesDir)
	if len(tables) > 0 {
		db.tables = tables
	}

	return db, nil
}

func newClevelDB(journal bool, journalFile *os.File) *ClevelDB {
	newDB := &ClevelDB{}
	newMemtable := newMemtable()

	newDB.memtable = newMemtable
	newDB.journal = journal
	newDB.journalFile = journalFile
	return newDB
}

// Get : searches memtable first, and if key isn't found, searches all SSTables
func (db *ClevelDB) Get(key []byte) ([]byte, error) {
	node, err := db.memtable.Get(key)
	if err != nil && err != notFoundInTableErr {
		return nil, err
	} else if err == nil {
		return node.val, nil
	}

	// Code reaches here if key not found in memtable
	// Search most recently flushed tables first
	// Return immediately if key is found
	for i := len(db.tables) - 1; i > 0; i-- {
		_, val, _, err := db.tables[i].Get(key)
		if err == notFoundInTableErr {
			continue // Continue searching in other tables
		} else if err != nil {
			return nil, err
		} else if val == nil {
			return nil, notFoundInDBErr
		}

		return val, nil
	}

	return nil, notFoundInDBErr
}

// Put : Inserts value into memtable (i.e. skip list)
func (db *ClevelDB) Put(key, val []byte) error {
	if db.journal {
		_, err := writeKeyValPairToFile(db.journalFile, key, val, true)
		if err != nil {
			return err
		}
	}

	mdb := db.memtable

	// Track nodes who have a forward pointer that will need to be updated if a new node is inserted
	update := make([]*SkipListNode, maxLevel)
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
		mdb.size -= len(current.val) + len(val)

		return db.checkAndHandleFlush()
	}

	// Delete new node (at random level)
	newLevel := randomLevel()

	// If the new level is higher than the current max level, we'll need to also
	// update the header node at the new higher levels, so we Add the header's new
	// levels to the update vector
	if newLevel > mdb.topLevel {
		for level := mdb.topLevel; level < newLevel; level++ {
			update[level] = mdb.header
		}
		mdb.topLevel = newLevel
	}

	// Create new node with empty forward pointers
	newNode := &SkipListNode{key: key, val: val}

	for i := 0; i < newLevel; i++ {
		// Use update vector to fill new node's forward pointers
		newNode.ptrs[i] = update[i].ptrs[i]
		// Re-direct the update vector's pointers to point at the new node
		update[i].ptrs[i] = newNode
	}

	mdb.size += len(key) + len(val)

	return db.checkAndHandleFlush()
}

func (db *ClevelDB) checkAndHandleFlush() error {
	if db.memtable.size <= maxMemtableSizeInBytes {
		return nil
	}

	numTables := len(db.tables)
	filename := fmt.Sprintf(ssTableFilename, numTables+1)
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, os.ModePerm)
	if err != nil {
		return err
	}

	// TODO: Need to block reads until flush is complete (or allow flushingMemtable to also be searched)
	go func(db *ClevelDB) {
		ssTable, err := flushMemtable(db, file)
		if err != nil {
			fmt.Printf("error flushing memtable: %v", err)
		}

		// Prepends new table to slice, so tables are in descending order
		db.tables = append([]*SSTable{ssTable}, db.tables...)
	}(db)

	return nil
}

func randomLevel() int {
	level := 1
	for rand.Float32() < p && level < maxLevel {
		level++
	}
	return level
}

// Delete : Marks key as deleted in memtable
func (db *ClevelDB) Delete(key []byte) error {
	// Replace key's value with "tombstone" (i.e. nil)
	return db.Put(key, nil)
}

// Size - Returns the size in bytes
func (db *ClevelDB) Size() int {
	return db.memtable.size
}

// RangeScan : Scans for values across memtable and all SStables
func (db *ClevelDB) RangeScan(start, limit []byte) (Iterator, error) {
	var activeIterators []Iterator

	// Add memtable iterator
	memtableIterator, err := db.memtable.RangeScan(start, limit)
	if err != nil && err != notFoundInTableErr {
		return nil, err
	}

	activeIterators = append(activeIterators, memtableIterator)

	// Add sstable iterators
	for _, table := range db.tables {
		ssTableIterator, err := table.RangeScan(start, limit)
		if err != nil && err != notFoundInTableErr {
			return nil, err
		}

		activeIterators = append(activeIterators, ssTableIterator)
	}

	// After this call, all iterators point to either the 'start' key or the closest key greater than 'start'
	var minKeyIterator Iterator

	// Continue grabbing the min key until you find one that isn't a tombstone (i.e. non-nil value)
	for {
		activeIterators, minKeyIterator = getMinKeyIterator(activeIterators)
		if minKeyIterator.Value() != nil {
			break
		}
	}

	return &ClevelIterator{
		iterators:      activeIterators,
		minKeyIterator: minKeyIterator,
		limit:          limit,
	}, nil
}

func getMinKeyIterator(activeIterators []Iterator) ([]Iterator, Iterator) {
	// Find iterator with the smallest key
	// In the event of a tie, we want the first iterator (i.e. most recently flushed)
	var minIterIndex int
	minKey := activeIterators[0].Key()
	for i, iterator := range activeIterators {
		if string(iterator.Key()) < string(minKey) {
			minIterIndex = i
		}
	}

	// We also need to consume the key of the min iterator AND any other "older" iterators that contain the
	// same outdated key (by calling `Next()`)
	// As an optimization, we can also check if the return value is false" and remove the iterator from active iterators
	// because "false" indicates there are no more keys in that iterator within our target range
	for i, iterator := range activeIterators {
		if i == minIterIndex {
			continue
		}

		if string(iterator.Key()) == string(minKey) {
			if !iterator.Next() {
				activeIterators = remove(activeIterators, i)
			}
		}
	}

	return activeIterators, activeIterators[minIterIndex]
}

type ClevelIterator struct {
	iterators      []Iterator
	minKeyIterator Iterator
	limit          []byte
}

func (i *ClevelIterator) Next() bool {
	// Since iterators are removed when they return "false", our "combined" iterator returns "false" when none left
	if len(i.iterators) == 0 || bytes.Compare(i.minKeyIterator.Key(), i.limit) >= 0 {
		return false
	}

	i.minKeyIterator.Next()

	// This retrieves the active iterator with the smallest key
	activeIterators, minIterator := getMinKeyIterator(i.iterators)

	i.iterators = activeIterators
	i.minKeyIterator = minIterator

	return true
}

func (i *ClevelIterator) Error() error {
	return nil
}

func (i *ClevelIterator) Key() []byte {
	return i.minKeyIterator.Key()
}

func (i *ClevelIterator) Value() []byte {
	return i.minKeyIterator.Value()
}

func remove(slice []Iterator, s int) []Iterator {
	return append(slice[:s], slice[s+1:]...)
}
