package main

type DB interface {
	Get(key []byte) ([]byte, error)
	Put(key, value []byte) error
	Delete(key []byte) error
	RangeScan(start, limit []byte) (Iterator, error)
}

//type ImmutableDB interface {
//	Get(key []byte) (value []byte, err error)
//	Has(key []byte) (ret bool, err error)
//	RangeScan(start, limit []byte) (Iterator, error)
//}

type Iterator interface {
	Next() bool
	Error() error
	Key() []byte
	Value() []byte
}
