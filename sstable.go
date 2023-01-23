package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

const ssTableFilename = "table.ss"
const indexBlockLength = 10

type SSTableDB struct {
	file  *os.File
	index []indexPair
}

type indexPair struct {
	key    []byte
	offset uint32
}

func newSSTableDB() *SSTableDB {
	ssTableFile, err := os.OpenFile(ssTableFilename, os.O_APPEND|os.O_RDWR|os.O_CREATE, os.ModePerm)
	if err != nil {
		return nil
	}

	index, err := loadIndexFromSSTable(ssTableFile)
	if err != nil {
		return nil
	}

	return &SSTableDB{file: ssTableFile, index: index}
}

func loadIndexFromSSTable(file *os.File) ([]indexPair, error) {
	// Read index offset from beginning of file
	indexOffsetBytes := make([]byte, 4)
	_, err := file.Read(indexOffsetBytes)
	if err != nil {
		return nil, err
	}
	indexOffset := binary.BigEndian.Uint32(indexOffsetBytes)

	// Seek to the beginning of the index
	_, err = file.Seek(int64(indexOffset), 0)
	if err != nil {
		return nil, err
	}

	keyLengthBytes := make([]byte, 2)
	offsetBytes := make([]byte, 4)
	var indexPairs []indexPair

	// Read every index block into memory
	for {
		_, err := file.Read(keyLengthBytes)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}

		keyLength := binary.BigEndian.Uint16(keyLengthBytes)

		key := make([]byte, keyLength)

		_, err = file.Read(key)
		if err != nil {
			return nil, err
		}

		_, err = file.Read(offsetBytes)
		if err != nil {
			return nil, err
		}

		offset := binary.BigEndian.Uint32(offsetBytes)

		indexPairs = append(indexPairs, indexPair{key: key, offset: offset})
	}

	return indexPairs, nil
}

func (db *SSTableDB) Get(key []byte) ([]byte, error) {
	val, _, err := db.get(key)
	if err != nil {
		return nil, err
	}

	return val, err
}

func (db *SSTableDB) get(key []byte) ([]byte, int64, error) {
	file := db.file
	searchKey := key

	var targetIdx int

	// Traverse through sorted index blocks until you find the first block with key that exceeds our search key
	for i, block := range db.index {
		if string(block.key) > string(searchKey) {
			targetIdx = i - 1
		}
	}

	// Find the offset for the index block we need to search and seek to it in the file
	targetBlock := db.index[targetIdx]
	targetOffset := targetBlock.offset

	_, err := file.Seek(int64(targetOffset), 0)
	if err != nil {
		return nil, 0, err
	}

	// Sequentially read each key-value pair within the selected index block
	for i := 0; i < indexBlockLength; i++ {
		op := make([]byte, 1)
		_, err := file.Read(op)
		if err == io.EOF {
			break
		} else if err != nil {
			fmt.Printf("Error reading op: %v\n", err)
			return nil, 0, err
		}

		keyLen := make([]byte, 2)
		_, err = file.Read(keyLen)
		if err != nil {
			fmt.Printf("Error reading key length: %v\n", err)
			return nil, 0, err
		}

		key := make([]byte, binary.BigEndian.Uint16(keyLen))
		_, err = file.Read(key)
		if err != nil {
			fmt.Printf("Error reading key: %v\n", err)
			return nil, 0, err
		}

		valLen := make([]byte, 2)
		_, err = file.Read(valLen)
		if err != nil {
			fmt.Printf("Error reading value length: %v\n", err)
			return nil, 0, err
		}

		val := make([]byte, binary.BigEndian.Uint16(valLen))
		_, err = file.Read(val)
		if err != nil {
			fmt.Printf("Error reading value: %v\n", err)
			return nil, 0, err
		}

		offset, err := file.Seek(0, io.SeekCurrent)
		if err != nil {
			fmt.Printf("Error retrieving current offset: %v\n", err)
			return nil, 0, err
		}

		// If we find an Insert op with a matching key, return the value
		// If we find a Delete op with a matching key, return nil (i.e. key doesn't exist)
		if string(key) == string(searchKey) {
			if op[0] == Insert {
				return val, offset, nil
			} else if op[0] == Delete {
				return nil, 0, nil
			}
		}
	}

	// If we reach the end of the index block without finding a match, return nil (i.e. key doesn't exist)
	return nil, 0, nil
}

func (db *SSTableDB) Has(key []byte) (ret bool, err error) {
	val, err := db.Get(key)
	if err != nil {
		return false, err
	}

	return val != nil, nil
}

func (db *SSTableDB) RangeScan(start, limit []byte) (Iterator, error) {
	val, offset, err := db.get(start)
	if err != nil {
		return nil, err
	}

	return &SSIterator{file: db.file, currentKey: start, currentVal: val, nextOffset: offset, limit: limit}, nil
}

type SSIterator struct {
	file       *os.File
	currentKey []byte
	currentVal []byte
	nextOffset int64
	limit      []byte
}

func (i *SSIterator) Next() bool {
	if bytes.Compare(i.currentKey, i.limit) == 0 {
		return false
	}

	file := i.file
	for {
		op := make([]byte, 1)
		_, err := file.Read(op)
		if err == io.EOF {
			return false
		} else if err != nil {
			fmt.Printf("Error reading op: %v\n", err)
			return false
		}

		if op[0] == Delete {
			continue
		}

		keyLen := make([]byte, 2)
		_, err = file.Read(keyLen)
		if err != nil {
			fmt.Printf("Error reading key length: %v\n", err)
			return false
		}

		key := make([]byte, binary.BigEndian.Uint16(keyLen))
		_, err = file.Read(key)
		if err != nil {
			fmt.Printf("Error reading key: %v\n", err)
			return false
		}

		valLen := make([]byte, 2)
		_, err = file.Read(valLen)
		if err != nil {
			fmt.Printf("Error reading value length: %v\n", err)
			return false
		}

		val := make([]byte, binary.BigEndian.Uint16(valLen))
		_, err = file.Read(val)
		if err != nil {
			fmt.Printf("Error reading value: %v\n", err)
			return false
		}

		nextOffset, err := file.Seek(0, io.SeekCurrent)
		if err != nil {
			fmt.Printf("Error retrieving current offset: %v\n", err)
			return false
		}

		i.nextOffset = nextOffset
		i.currentKey = key
		i.currentVal = val
		return true
	}
}

func (i *SSIterator) Error() error {
	return nil
}

func (i *SSIterator) Key() []byte {
	return i.currentKey
}

func (i *SSIterator) Value() []byte {
	return i.currentVal
}

func (db *SSTableDB) Put(key, value []byte) error {
	return errors.New("read-only")
}

func (db *SSTableDB) Delete(key []byte) error {
	return errors.New("read-only")
}

func (db *ClevelDB) flushSSTable() error {
	ssTableFile, _ := os.OpenFile(ssTableFilename, os.O_APPEND|os.O_RDWR|os.O_CREATE, os.ModePerm)

	current := db.mdb.header.ptrs[0]
	var op uint8

	var keyIdx uint16
	var indexOffset uint32
	var indexBlocks []indexPair

	// Reserve space for the index offset
	_, err := ssTableFile.Seek(4, 0)
	if err != nil {
		return err
	}

	// Write key-value pairs to file
	for current != nil {
		if current.val == nil {
			op = Delete
		} else {
			op = Insert
		}

		n, err := writeOpToFile(ssTableFile, op, current.key, current.val, false)
		if err != nil {
			return err
		}
		indexOffset += uint32(n)

		current = current.ptrs[0]

		if keyIdx%indexBlockLength == 0 {
			indexBlocks = append(indexBlocks, indexPair{key: current.key, offset: indexOffset})
		}
		keyIdx++
	}

	// Write index offset to the start of the file
	var indexOffsetBytes []byte
	indexOffsetBytes = binary.BigEndian.AppendUint32(indexOffsetBytes, indexOffset)
	_, err = ssTableFile.WriteAt(indexOffsetBytes, 0)
	if err != nil {
		return err
	}

	// Write index offset pairs to the end of the file
	for _, block := range indexBlocks {
		var toAppend []byte

		toAppend = binary.BigEndian.AppendUint16(toAppend, uint16(len(block.key)))
		toAppend = append(toAppend, block.key...)
		toAppend = binary.BigEndian.AppendUint32(toAppend, block.offset)

		_, err := ssTableFile.Write(toAppend)
		if err != nil {
			return errors.New("error writing index to file")
		}
	}

	err = ssTableFile.Close()
	if err != nil {
		return err
	}

	return nil
}
