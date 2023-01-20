package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const (
	Insert uint16 = iota
	Delete
)

func writeToLog(key, val []byte, op uint16) error {
	var toAppend []byte

	toAppend = binary.BigEndian.AppendUint16(toAppend, op)

	toAppend = binary.BigEndian.AppendUint16(toAppend, uint16(len(key)))
	toAppend = append(toAppend, key...)

	toAppend = binary.BigEndian.AppendUint16(toAppend, uint16(len(val)))
	toAppend = append(toAppend, val...)

	_, err := logFile.Write(toAppend)
	if err != nil {
		return errors.New("error writing to log file")
	}

	err = logFile.Sync()
	if err != nil {
		return errors.New("error flushing/syncing log file")
	}

	return nil
}

func logInsert(key, val []byte) error {
	return writeToLog(key, val, Insert)
}

func logDelete(key []byte) error {
	return writeToLog(key, []byte{}, Delete)
}

func loadMemtable() *ClevelDB {
	db := newClevelDB(false)

	opBytes := make([]byte, 2)
	keyLen := make([]byte, 2)
	valLen := make([]byte, 2)

	var op uint16

	for {
		_, err := logFile.Read(opBytes)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil
		}

		op = binary.BigEndian.Uint16(opBytes)

		_, err = logFile.Read(keyLen)
		if err != nil {
			fmt.Printf("Error reading key length: %v\n", err)
			return nil
		}

		key := make([]byte, binary.BigEndian.Uint16(keyLen))
		_, err = logFile.Read(key)
		if err != nil {
			fmt.Printf("Error reading key: %v\n", err)
			return nil
		}

		_, err = logFile.Read(valLen)
		if err != nil {
			fmt.Printf("Error reading value length: %v\n", err)
			return nil
		}

		val := make([]byte, binary.BigEndian.Uint16(valLen))
		_, err = logFile.Read(val)
		if err != nil {
			fmt.Printf("Error reading value: %v\n", err)
			return nil
		}

		if op == Insert {
			err := db.Put(key, val)
			if err != nil {
				fmt.Printf("Error inserting key: %v\n", err)
				return nil
			}
		} else if op == Delete {
			err := db.Delete(key)
			if err != nil {
				fmt.Printf("Error deleting key: %v\n", err)
				return nil
			}
		}
	}
	db.log = true
	return db
}
