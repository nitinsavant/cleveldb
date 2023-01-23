package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

const (
	Delete uint8 = iota
	Insert
)

func writeOpToFile(file *os.File, op uint8, key, val []byte, sync bool) (int, error) {
	var toAppend []byte

	toAppend = append(toAppend, op)

	toAppend = binary.BigEndian.AppendUint16(toAppend, uint16(len(key)))
	toAppend = append(toAppend, key...)

	if op == Insert {
		toAppend = binary.BigEndian.AppendUint16(toAppend, uint16(len(val)))
		toAppend = append(toAppend, val...)
	}

	n, err := file.Write(toAppend)
	if err != nil {
		return 0, errors.New("error writing to file")
	}

	if sync {
		err = file.Sync()
		if err != nil {
			return 0, errors.New("error syncing file")
		}
	}

	return n, nil
}

func writeInsertToJournal(file *os.File, key, val []byte) (int, error) {
	return writeOpToFile(file, Insert, key, val, true)
}

func writeDeleteToJournal(file *os.File, key []byte) (int, error) {
	return writeOpToFile(file, Delete, key, nil, true)
}

func recoverJournal(journalFile *os.File) *ClevelDB {
	db := newClevelDB(false, journalFile)

	op := make([]byte, 1)
	keyLen := make([]byte, 2)
	valLen := make([]byte, 2)

	for {
		_, err := journalFile.Read(op)
		if err == io.EOF {
			break
		} else if err != nil {
			fmt.Printf("Error reading op: %v\n", err)
			return nil
		}

		_, err = journalFile.Read(keyLen)
		if err != nil {
			fmt.Printf("Error reading key length: %v\n", err)
			return nil
		}

		key := make([]byte, binary.BigEndian.Uint16(keyLen))
		_, err = journalFile.Read(key)
		if err != nil {
			fmt.Printf("Error reading key: %v\n", err)
			return nil
		}

		_, err = journalFile.Read(valLen)
		if err != nil {
			fmt.Printf("Error reading value length: %v\n", err)
			return nil
		}

		val := make([]byte, binary.BigEndian.Uint16(valLen))
		_, err = journalFile.Read(val)
		if err != nil {
			fmt.Printf("Error reading value: %v\n", err)
			return nil
		}

		if op[0] == Insert {
			err := db.Put(key, val)
			if err != nil {
				fmt.Printf("Error inserting key: %v\n", err)
				return nil
			}
		} else if op[0] == Delete {
			err := db.Delete(key)
			if err != nil {
				fmt.Printf("Error deleting key: %v\n", err)
				return nil
			}
		}
	}
	db.journal = true
	return db
}
