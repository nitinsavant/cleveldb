package main

import "testing"

func Test_SSTableDBGetReturnsCorrectValue(t *testing.T) {
	testGetReturnsCorrectValue(t, newSSTableDB())
}

func Test_SSTableDBDeleteSetsValueToNil(t *testing.T) {
	testDeleteSetsValueToNil(t, newSSTableDB())
}

func Test_SSTableDBRangeScanAndNextReturnCorrectOrderedValues(t *testing.T) {
	testRangeScanAndNextReturnCorrectOrderedValues(t, newSSTableDB())
}

//func Benchmark_SSTableDBFillSeq(b *testing.B) {
//	benchmarkFillSeq(b, newSSTableDB())
//}
//
//func Benchmark_SSTableDBFillRand(b *testing.B) {
//	benchmarkFillRand(b, newSSTableDB())
//}
//
//func Benchmark_SSTableDBDeleteSeq(b *testing.B) {
//	benchmarkDeleteSeq(b, newSSTableDB())
//}
//
//func Benchmark_SSTableDBReadSeq(b *testing.B) {
//	benchmarkReadSeq(b, newSSTableDB())
//}
//
//func Benchmark_SSTableDBRangeScan(b *testing.B) {
//	benchmarkRangeScan(b, newSSTableDB())
//}
