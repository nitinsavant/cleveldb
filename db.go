package cleveldb

type DB interface {
	Get(key []byte) ([]byte, error)
	Put(key, value []byte) error
	Delete(key []byte) error
	RangeScan(start, limit []byte) (Iterator, error)
	Size() int
}

type Iterator interface {
	Next() bool
	Error() error
	Key() []byte
	Value() []byte
}
