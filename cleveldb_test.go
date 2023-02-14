package main

import (
	"fmt"
	"os"
	"testing"
)

func Test_ClevelDBGetReturnsCorrectValue(t *testing.T) {
	testGetReturnsCorrectValue(t, newClevelDB(false, nil))
}

func Test_ClevelDBGetReturnsCorrectValueFromSSTable(t *testing.T) {
	db := newClevelDB(false, nil)

	_ = db.Put([]byte("firstName"), []byte("neha"))
	_ = db.Put([]byte("lastName"), []byte("munoz"))
	_ = db.Put([]byte("maidenName"), []byte("savant"))
	_ = db.Put([]byte("middleName"), []byte("gajendra"))

	file, err := os.OpenFile("test/segment_1.ss", os.O_RDWR|os.O_CREATE, os.ModePerm)
	if err != nil {
		fmt.Printf("error opening file: %v", err)
		return
	}

	ssTable, err := flushMemtable(db, file)
	if err != nil {
		fmt.Printf("error flushing memtable: %v", err)
	}

	db.tables = append(db.tables, ssTable)

	db.memtable = newMemtable()

	_ = db.Put([]byte("firstName"), []byte("nitin"))
	_ = db.Put([]byte("lastName"), []byte("savant"))
	_ = db.Put([]byte("maidenName"), []byte(""))
	_ = db.Delete([]byte("middleName"))

	file, err = os.OpenFile("test/segment_2.ss", os.O_RDWR|os.O_CREATE, os.ModePerm)
	if err != nil {
		fmt.Printf("error opening file: %v", err)
		return
	}

	ssTable, err = flushMemtable(db, file)
	if err != nil {
		fmt.Printf("error flushing memtable: %v", err)
	}

	db.tables = append(db.tables, ssTable)

	var tests = []struct {
		key   string
		value string
		err   error
	}{
		{"lastName", "savant", nil},
		{"firstName", "nitin", nil},
		{"maidenName", "", nil},
		{"middleName", "", notFoundInDBErr},
	}

	for _, test := range tests {
		actualValue, actualErr := db.Get([]byte(test.key))
		expectedValue := []byte(test.value)

		if string(actualValue) != string(expectedValue) {
			t.Errorf(`storage.Get("%s") returns unexpected value: "%s"`, test.key, actualValue)
		}

		if test.err != actualErr {
			t.Errorf(`storage.Get("%s") returns unexpected err: "%s"`, test.key, actualErr)
		}
	}
}

func Test_ClevelDBDeleteRemovesValue(t *testing.T) {
	testDeleteSetsValueToNil(t, newClevelDB(false, nil))
}

func Test_ClevelDBRangeScanAndNextReturnCorrectOrderedValues(t *testing.T) {
	testRangeScanAndNextReturnCorrectOrderedValues(t, newClevelDB(false, nil))
}

func Benchmark_ClevelDBFillSeq(b *testing.B) {
	benchmarkFillSeq(b, newClevelDB(false, nil))
}

func Benchmark_ClevelDBFillRand(b *testing.B) {
	benchmarkFillRand(b, newClevelDB(false, nil))
}

func Benchmark_ClevelDBDeleteSeq(b *testing.B) {
	benchmarkDeleteSeq(b, newClevelDB(false, nil))
}

func Benchmark_ClevelDBReadSeq(b *testing.B) {
	benchmarkReadSeq(b, newClevelDB(false, nil))
}

func Benchmark_ClevelDBRangeScan(b *testing.B) {
	benchmarkRangeScan(b, newClevelDB(false, nil))
}

func Benchmark_ClevelDBFLogFillSeq(b *testing.B) {
	benchmarkFillSeq(b, newClevelDB(true, nil))
}

func Benchmark_ClevelDBLogFillRand(b *testing.B) {
	benchmarkFillRand(b, newClevelDB(true, nil))
}

func Benchmark_ClevelDBLogDeleteSeq(b *testing.B) {
	benchmarkDeleteSeq(b, newClevelDB(true, nil))
}

func Benchmark_ClevelDBLogReadSeq(b *testing.B) {
	benchmarkReadSeq(b, newClevelDB(true, nil))
}
