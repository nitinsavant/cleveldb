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
const indexOffsetSizeInBytes = 4

type SSTableDB struct {
	file        *os.File
	index       []indexPair
	indexOffset uint32
}

type indexPair struct {
	key    []byte
	offset uint32
}

func loadIndexFromSSTable(file *os.File) (uint32, []indexPair, error) {
	// Read index offset from beginning of the file
	indexOffsetBytes := make([]byte, 4)
	_, err := file.ReadAt(indexOffsetBytes, 0)
	if err != nil {
		return 0, nil, err
	}
	indexOffset := binary.BigEndian.Uint32(indexOffsetBytes)

	// Seek to the beginning of the index
	_, err = file.Seek(int64(indexOffset), 0)
	if err != nil {
		return 0, nil, err
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
			return 0, nil, err
		}

		keyLength := binary.BigEndian.Uint16(keyLengthBytes)

		key := make([]byte, keyLength)

		_, err = file.Read(key)
		if err != nil {
			return 0, nil, err
		}

		_, err = file.Read(offsetBytes)
		if err != nil {
			return 0, nil, err
		}

		offset := binary.BigEndian.Uint32(offsetBytes)

		indexPairs = append(indexPairs, indexPair{key: key, offset: offset})
	}

	return indexOffset, indexPairs, nil
}

func (db *SSTableDB) Get(key []byte) ([]byte, error) {
	val, _, err := db.get(key)
	if err != nil {
		return nil, err
	}

	return val, err
}

func (db *SSTableDB) Put(key, value []byte) error {
	return errors.New("read-only")
}

func (db *SSTableDB) Delete(key []byte) error {
	return errors.New("read-only")
}

func (db *SSTableDB) get(searchKey []byte) ([]byte, int64, error) {
	file := db.file

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

	var offset int64

	// Sequentially read each key-value pair within the selected index block
	for i := 0; i < indexBlockLength && uint32(offset) < db.indexOffset; i++ {
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

		offset, err = file.Seek(0, io.SeekCurrent)
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

func (db *ClevelDB) flushSSTable(filename string) (*os.File, error) {
	ssTableFile, _ := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, os.ModePerm)

	err := ssTableFile.Truncate(0)
	if err != nil {
		return nil, err
	}

	current := db.mdb.header.ptrs[0]
	var op uint8

	var keyIdx uint16
	var indexOffset int64
	var indexBlocks []indexPair

	// Reserve space for the index offset
	_, err = ssTableFile.Seek(indexOffsetSizeInBytes, 0)
	if err != nil {
		return nil, err
	}

	// Write key-value pairs to file
	for current != nil {
		if current.val == nil {
			op = Delete
		} else {
			op = Insert
		}

		// TODO: Remove first return value. Not needed if Seek works below.
		_, err := writeOpToFile(ssTableFile, op, current.key, current.val, false)
		if err != nil {
			return nil, err
		}

		indexOffset, err = ssTableFile.Seek(0, io.SeekCurrent)
		if err != nil {
			fmt.Printf("Error retrieving current offset: %v\n", err)
			return nil, err
		}

		current = current.ptrs[0]

		if keyIdx%indexBlockLength == 0 {
			indexBlocks = append(indexBlocks, indexPair{key: current.key, offset: uint32(indexOffset)})
		}
		keyIdx++
	}

	// Write index offset to the start of the file
	var indexOffsetBytes []byte
	indexOffsetBytes = binary.BigEndian.AppendUint32(indexOffsetBytes, uint32(indexOffset))
	_, err = ssTableFile.WriteAt(indexOffsetBytes, 0)
	if err != nil {
		return nil, err
	}

	// Seek back to end-of-file
	_, err = ssTableFile.Seek(indexOffset, 0)
	if err != nil {
		return nil, err
	}

	// Write index offset pairs to the end of the file
	for _, block := range indexBlocks {
		var toAppend []byte

		toAppend = binary.BigEndian.AppendUint16(toAppend, uint16(len(block.key)))
		toAppend = append(toAppend, block.key...)
		toAppend = binary.BigEndian.AppendUint32(toAppend, block.offset)

		_, err := ssTableFile.Write(toAppend)
		if err != nil {
			return nil, errors.New("error writing index to file")
		}
	}

	return ssTableFile, nil
}
