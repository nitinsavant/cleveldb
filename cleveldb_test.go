package main

import (
	"testing"
)

func Test_ClevelDBGetReturnsCorrectValue(t *testing.T) {
	testGetReturnsCorrectValue(t, newClevelDB(false, nil))
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

//func Benchmark_ClevelDBFLogFillSeq(b *testing.B) {
//	benchmarkFillSeq(b, newClevelDB(true))
//}
//
//func Benchmark_ClevelDBLogFillRand(b *testing.B) {
//	benchmarkFillRand(b, newClevelDB(true))
//}
//
//func Benchmark_ClevelDBLogDeleteSeq(b *testing.B) {
//	benchmarkDeleteSeq(b, newClevelDB(true))
//}
//
//func Benchmark_ClevelDBLogReadSeq(b *testing.B) {
//	benchmarkReadSeq(b, newClevelDB(true))
//}
