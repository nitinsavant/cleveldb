package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
)

const ssTablesDir = "sstables/"
const ssTableFilename = ssTablesDir + "segment_%d.ss"
const indexOffsetSizeInBytes = 4

// Purposely kept small for testing; should ideally be a multiple of disk block size (e.g. 4KB)
const indexBlockSizeInBytes = 20

type SSTable struct {
	file        *os.File
	index       *Index
	bloomFilter *BloomFilter
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

func flushMemtable(db *ClevelDB, file *os.File) (*SSTable, error) {
	// Clear contents of file
	err := file.Truncate(0)
	if err != nil {
		return nil, err
	}

	db.flushingMemtable = db.memtable
	db.memtable = newMemtable()

	// Begin reading from first node of skip list (at the node's lowest level)
	current := db.flushingMemtable.header.ptrs[0]

	var currentOffset int64
	var currentBlockSize int
	var indexBlocks []indexBlock

	// Reserve some space to store the index offset (once we know where the index will begin)
	_, err = file.Seek(indexOffsetSizeInBytes, io.SeekStart)
	if err != nil {
		return nil, err
	}

	// Initialize the first block (which starts immediately after the index offset)
	activeBlock := indexBlock{
		key:    current.key,
		offset: indexOffsetSizeInBytes,
	}

	// Write "sorted" key-value pairs to file while also accumulating  "sorted" index blocks
	for current != nil {
		numBytes, err := writeKeyValPairToFile(file, current.key, current.val, false)
		if err != nil {
			return nil, err
		}

		currentBlockSize += numBytes

		currentOffset, err = file.Seek(0, io.SeekCurrent)
		if err != nil {
			fmt.Printf("Error retrieving current offset: %v\n", err)
			return nil, err
		}

		current = current.ptrs[0]

		// Once we reach end of skip list or size of index block crosses threshold, append to blocks slice
		if currentBlockSize >= indexBlockSizeInBytes || current == nil {
			activeBlock.size = int64(currentBlockSize)
			currentBlockSize = 0
			indexBlocks = append(indexBlocks, activeBlock)

			// Initialize next block (if we aren't at end of skip list)
			if current != nil {
				nextBlockOffset := uint32(currentOffset)
				activeBlock = indexBlock{
					key:    current.key,
					offset: int64(nextBlockOffset),
				}
			}

		}
	}

	indexOffset := currentOffset

	// Start writing index blocks (immediately after the key-value data on disk)
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

	// We can store the index offset value in the space we set aside at the beginning of the file
	var indexOffsetBytes []byte
	indexOffsetBytes = binary.BigEndian.AppendUint32(indexOffsetBytes, uint32(indexOffset))
	_, err = file.WriteAt(indexOffsetBytes, 0)
	if err != nil {
		return nil, err
	}

	err = file.Sync()
	if err != nil {
		return nil, err
	}

	err = db.clearJournal()
	if err != nil {
		return nil, err
	}

	return &SSTable{
		file:  file,
		index: &Index{blocks: indexBlocks, offset: indexOffset},
	}, nil
}

func loadIndexFromSSTable(file *os.File) (int64, []indexBlock, error) {
	var indexBlocks []indexBlock
	keyLengthBytes := make([]byte, 2)
	offsetBytes := make([]byte, 4)
	sizeBytes := make([]byte, 4)

	// Read index offset
	indexOffsetBytes := make([]byte, 4)
	_, err := file.ReadAt(indexOffsetBytes, 0)
	if err != nil {
		return 0, nil, err
	}
	indexOffset := int64(binary.BigEndian.Uint32(indexOffsetBytes))

	// Seek to index offset
	_, err = file.Seek(indexOffset, io.SeekStart)
	if err != nil {
		return 0, nil, err
	}

	// Read index blocks into memory
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

func loadSSTable(file *os.File) *SSTable {
	offset, indexBlocks, err := loadIndexFromSSTable(file)
	if err != nil {
		return nil
	}

	index := &Index{blocks: indexBlocks, offset: offset}

	return &SSTable{file: file, index: index}
}

func loadSSTables(path string) []*SSTable {
	dirEntries, err := os.ReadDir(path)
	if err != nil {
		log.Fatal(err)
	}

	// Iterates over directories (in reverse order), so that tables slice starts with most recently flushed table
	var tables []*SSTable
	for i := len(dirEntries) - 1; i >= 0; i-- {
		dir := dirEntries[i]
		file, err := os.Open(ssTablesDir + dir.Name())
		if err != nil {
			log.Fatal(err)
		}

		tables = append(tables, loadSSTable(file))
	}

	return tables
}

// Get : Searches sstable for a given key
// If key isn't found, error is returned, and the current offset points at the next key greater than that key (for multi-table RangeScan support)
func (ss *SSTable) Get(searchKey []byte) ([]byte, []byte, int64, error) {
	file := ss.file

	//if !ss.bloomFilter.MaybeContains(searchKey) {
	//	return searchKey, nil, 0, notFoundInTableErr
	//}

	// Find the index block which encompasses the range where the key can be found
	targetBlock := ss.index.search(searchKey)

	// Seek to the selected index block's offset
	currentOffset, err := file.Seek(targetBlock.offset, io.SeekStart)
	if err != nil {
		return nil, nil, 0, err
	}

	var key, val []byte

	endOfBlockOffset := targetBlock.offset + targetBlock.size

	// Sequentially read each key-value pair within the target block
	for currentOffset < endOfBlockOffset {
		op := make([]byte, 1)
		_, err := file.Read(op)
		if err == io.EOF {
			break
		} else if err != nil {
			fmt.Printf("Error reading op: %v\n", err)
			return nil, nil, 0, err
		}

		key, val, currentOffset, err = readKeyVal(file)
		if err != nil {
			return nil, nil, 0, err
		}

		// If we find Insert with a matching key, return the value
		// If we find Delete with a matching key, return nil
		if string(searchKey) == string(key) {
			if op[0] == Insert {
				return key, val, currentOffset, nil
			} else if op[0] == Delete {
				return key, nil, currentOffset, nil
			}
		} else if string(key) > string(searchKey) {
			// Since keys are sorted, reaching here means the searchKey wasn't found in the block
			// Even though 'key' doesn't match the target key, we return it (because it's the closest key greater
			// than the target key) in order to support multi-table RangeScan
			return key, val, currentOffset, notFoundInTableErr
		}
	}

	return key, val, currentOffset, notFoundInTableErr
}

func readKeyVal(file *os.File) ([]byte, []byte, int64, error) {
	keyLen := make([]byte, 2)
	_, err := file.Read(keyLen)
	if err != nil {
		fmt.Printf("Error reading key length: %v\n", err)
		return nil, nil, 0, err
	}

	key := make([]byte, binary.BigEndian.Uint16(keyLen))
	_, err = file.Read(key)
	if err != nil {
		fmt.Printf("Error reading key: %v\n", err)
		return nil, nil, 0, err
	}

	valLen := make([]byte, 2)
	_, err = file.Read(valLen)
	if err != nil {
		fmt.Printf("Error reading value length: %v\n", err)
		return nil, nil, 0, err
	}

	val := make([]byte, binary.BigEndian.Uint16(valLen))
	_, err = file.Read(val)
	if err != nil {
		fmt.Printf("Error reading value: %v\n", err)
		return nil, nil, 0, err
	}

	// After the reading of a key-value pair, update the current offset
	currentOffset, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		fmt.Printf("Error retrieving current offset: %v\n", err)
		return nil, nil, 0, err
	}
	return key, val, currentOffset, nil
}

func (ss *SSTable) Delete(key []byte) error {
	panic("read-only")
}

func (ss *SSTable) Put(key []byte) error {
	panic("read-only")
}

func (ss *SSTable) RangeScan(start, limit []byte) (Iterator, error) {
	key, val, currentOffset, err := ss.Get(start)
	if err != nil && err != notFoundInTableErr {
		return nil, err
	}

	return &SSIterator{
		file:          ss.file,
		currentKey:    key,
		currentVal:    val,
		nextKeyOffset: currentOffset,
		limit:         limit,
	}, nil
}

type SSIterator struct {
	file          *os.File
	currentKey    []byte
	currentVal    []byte
	nextKeyOffset int64
	limit         []byte
}

func (i *SSIterator) Next() bool {
	if bytes.Compare(i.currentKey, i.limit) >= 0 {
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

		key, val, currentOffset, err := readKeyVal(file)
		if err != nil {
			return false
		}

		i.nextKeyOffset = currentOffset
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

// Performs a binary search and return the index block whose range matches the key
func (index *Index) search(key []byte) indexBlock {
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

	// If an exact match not found, we select the block to the left because it contains the range where the key could be found
	if targetKey >= string(blocks[right].key) {
		return blocks[right]
	} else {
		return blocks[left]
	}
}
