package main

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type LevelDbWrapper struct {
	db *leveldb.DB
}

func GetLevelDB(file string) (*LevelDbWrapper, error) {
	db, err := leveldb.OpenFile(file, nil)
	if err != nil {
		return &LevelDbWrapper{}, err
	}

	return &LevelDbWrapper{db: db}, nil
}

func (levelDbWrapper *LevelDbWrapper) Get(key []byte) ([]byte, error) {
	return levelDbWrapper.db.Get(key, nil)
}

func (levelDbWrapper *LevelDbWrapper) Put(key, value []byte) error {
	return levelDbWrapper.db.Put(key, value, nil)
}

func (levelDbWrapper *LevelDbWrapper) Delete(key []byte) error {
	return levelDbWrapper.db.Delete(key, nil)
}

func (levelDbWrapper *LevelDbWrapper) RangeScan(start, limit []byte) (Iterator, error) {
	return levelDbWrapper.db.NewIterator(&util.Range{Start: start, Limit: limit}, nil), nil
}
