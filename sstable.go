package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

const ssTableFilename = "sstables/segment_%d.ss"
const indexOffsetSizeInBytes = 4

// Purposely kept small for testing; should ideally be a multiple of disk block size (e.g. 4KB)
const indexBlockSizeInBytes = 20

type SSTable struct {
	file  *os.File
	index *Index
}

type Index struct {
	blocks []indexBlock
	offset int64
}

type indexBlock struct {
	key    []byte
	offset int64
	size   int64
}

func (db *ClevelDB) flushMemtable(file *os.File) (*SSTable, error) {
	// Clear contents of file
	err := file.Truncate(0)
	if err != nil {
		return nil, err
	}

	// Start at first node on lowest level of skip list
	current := db.flushMdb.header.ptrs[0]

	var currentOffset int64
	var currentBlockSize int
	var indexBlocks []indexBlock

	// Reserve space to later store the index offset
	_, err = file.Seek(indexOffsetSizeInBytes, io.SeekStart)
	if err != nil {
		return nil, err
	}

	// Initialize first block with block offset immediately after index offset value
	activeBlock := indexBlock{
		key:    current.key,
		offset: indexOffsetSizeInBytes,
	}

	// Write key-value pairs to file
	for current != nil {
		bytesWritten, err := writeOpToFile(file, current.key, current.val, false)
		if err != nil {
			return nil, err
		}

		currentBlockSize += bytesWritten

		currentOffset, err = file.Seek(0, io.SeekCurrent)
		if err != nil {
			fmt.Printf("Error retrieving current offset: %v\n", err)
			return nil, err
		}

		current = current.ptrs[0]

		// Close/append active block (and initialize next block)
		if currentBlockSize >= indexBlockSizeInBytes {
			// Before appending, set block size (and reset to 0 for next block)
			activeBlock.size = int64(currentBlockSize)
			currentBlockSize = 0
			indexBlocks = append(indexBlocks, activeBlock)

			if current == nil {
				continue // could also just be "break"
			}

			nextBlockOffset := uint32(currentOffset)
			activeBlock = indexBlock{
				key:    current.key,
				offset: int64(nextBlockOffset),
			}

		}
	}

	// The current offset is where we stopped writing key-value data and where the index will be now written
	indexOffset := currentOffset

	// We store the offset in the bytes we had reserved earlier
	var indexOffsetBytes []byte
	indexOffsetBytes = binary.BigEndian.AppendUint32(indexOffsetBytes, uint32(indexOffset))
	_, err = file.WriteAt(indexOffsetBytes, 0)
	if err != nil {
		return nil, err
	}

	// Seek back to the index offset
	_, err = file.Seek(indexOffset, io.SeekStart)
	if err != nil {
		return nil, err
	}

	// Write index blocks
	for _, block := range indexBlocks {
		var toAppend []byte

		toAppend = binary.BigEndian.AppendUint16(toAppend, uint16(len(block.key)))
		toAppend = append(toAppend, block.key...)
		toAppend = binary.BigEndian.AppendUint32(toAppend, uint32(block.offset))
		toAppend = binary.BigEndian.AppendUint32(toAppend, uint32(block.size))

		_, err := file.Write(toAppend)
		if err != nil {
			return nil, errors.New("error writing index block to file")
		}
	}

	err = db.truncateJournal()
	if err != nil {
		return nil, err
	}

	return &SSTable{
		file:  file,
		index: &Index{blocks: indexBlocks, offset: indexOffset},
	}, nil
}

func loadIndexFromSSTable(file *os.File) (int64, []indexBlock, error) {
	// Read index offset from start of file
	indexOffsetBytes := make([]byte, 4)
	_, err := file.ReadAt(indexOffsetBytes, 0)
	if err != nil {
		return 0, nil, err
	}
	indexOffset := int64(binary.BigEndian.Uint32(indexOffsetBytes))

	// Seek to index
	_, err = file.Seek(indexOffset, io.SeekStart)
	if err != nil {
		return 0, nil, err
	}

	keyLengthBytes := make([]byte, 2)
	offsetBytes := make([]byte, 4)
	sizeBytes := make([]byte, 4)

	var indexBlocks []indexBlock

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
		offset := int64(binary.BigEndian.Uint32(offsetBytes))

		_, err = file.Read(sizeBytes)
		if err != nil {
			return 0, nil, err
		}
		size := int64(binary.BigEndian.Uint32(sizeBytes))

		indexBlocks = append(indexBlocks, indexBlock{key: key, offset: offset, size: size})
	}

	return indexOffset, indexBlocks, nil
}

func (db *SSTable) Get(key []byte) ([]byte, error) {
	val, _, err := db.getValueAndOffset(key)
	if err != nil {
		return nil, err
	}

	return val, nil
}

func (db *SSTable) Put(key, value []byte) error {
	return errors.New("read-only")
}

func (db *SSTable) Delete(key []byte) error {
	return errors.New("read-only")
}

func (db *SSTable) getValueAndOffset(searchKey []byte) ([]byte, int64, error) {
	file := db.file

	// Find the offset for the index block we need to search and seek to it in the file
	targetBlock := search(db.index, searchKey)

	offset, err := file.Seek(targetBlock.offset, io.SeekStart)
	if err != nil {
		return nil, 0, err
	}

	// Sequentially read each key-value pair within the selected index block
	// Stop when you reach the end of the index block
	for offset < (targetBlock.offset + targetBlock.size) {
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
		// If we find a Delete op with a matching key, return nil
		if string(key) == string(searchKey) {
			if op[0] == Insert {
				return val, offset, nil
			} else if op[0] == Delete {
				return nil, offset, nil
			}
		}
	}

	// If we reach the end of the block without finding a match, return nil (i.e. key doesn't exist)
	return nil, 0, keyNotFoundErr
}

func (db *SSTable) Has(key []byte) (ret bool, err error) {
	val, err := db.Get(key)
	if err != nil {
		return false, err
	}

	return val != nil, nil
}

func (db *SSTable) RangeScan(start, limit []byte) (Iterator, error) {
	val, offset, err := db.getValueAndOffset(start)
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

func search(index *Index, key []byte) indexBlock {
	blocks := index.blocks

	targetKey := string(key)

	left := 0
	right := len(blocks) - 1
	var mid int

	for left < right-1 {
		mid = (right - left) / 2
		blockKey := string(blocks[mid].key)

		if targetKey == blockKey {
			return blocks[mid]
		}

		if targetKey < blockKey {
			right = mid - 1
		} else {
			left = mid + 1
		}
	}

	// If an exact match not found, then we need the block to the left because it could contain the key

	if targetKey >= string(blocks[right].key) {
		return blocks[right]
	} else {
		return blocks[left]
	}
}
