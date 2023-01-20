package cleveldb

import (
	"errors"
	"sort"
)

type NaiveDB struct {
	storage map[string]string
}

func NewNaiveDB() (*NaiveDB, error) {
	storage := make(map[string]string)
	return &NaiveDB{storage: storage}, nil
}

func (db *NaiveDB) Get(key []byte) ([]byte, error) {
	val, ok := db.storage[string(key)]
	if !ok {
		return nil, errors.New("key not found")
	}
	return []byte(val), nil
}

func (db *NaiveDB) Put(key, value []byte) error {
	db.storage[string(key)] = string(value)
	return nil
}

func (db *NaiveDB) Delete(key []byte) error {
	delete(db.storage, string(key))
	return nil
}

func (db *NaiveDB) Size() int {
	return len(db.storage)
}

func (db *NaiveDB) RangeScan(start, limit []byte) (Iterator, error) {
	var keys []string
	for k := range db.storage {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	inRange := false
	var keysInRange []string
	for _, key := range keys {
		if !inRange && key == string(start) {
			inRange = true
		}

		if inRange {
			keysInRange = append(keysInRange, key)
		}

		if inRange && key == string(limit) {
			break
		}
	}

	return &NaiveIterator{keys: keysInRange, db: db}, nil
}

type NaiveIterator struct {
	keys []string
	db   *NaiveDB
	idx  int
	err  error
}

func (i *NaiveIterator) Next() bool {
	i.idx++
	return i.idx < len(i.keys)
}

func (i *NaiveIterator) Error() error {
	return i.err
}

func (i *NaiveIterator) Key() []byte {
	return []byte(i.keys[i.idx])
}

func (i *NaiveIterator) Value() []byte {
	key := i.keys[i.idx]
	return []byte(i.db.storage[key])
}

func (i *NaiveIterator) Size() int {
	return len(i.keys)
}
