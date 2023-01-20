package main

import (
	"testing"
)

func Test_NaiveDBGetReturnsCorrectValue(t *testing.T) {
	testGetReturnsCorrectValue(t, newNaiveDB())
}

func Test_NaiveDBDeleteRemovesValue(t *testing.T) {
	testDeleteRemovesValue(t, newNaiveDB())
}

func Test_NaiveDBRangeScanAndNextReturnCorrectOrderedValues(t *testing.T) {
	testRangeScanAndNextReturnCorrectOrderedValues(t, newNaiveDB())
}

func Benchmark_NaiveDBFillSeq(b *testing.B) {
	benchmarkFillSeq(b, newNaiveDB())
}

func Benchmark_NaiveDBFillRand(b *testing.B) {
	benchmarkFillRand(b, newNaiveDB())
}

func Benchmark_NaiveDBDeleteSeq(b *testing.B) {
	benchmarkDeleteSeq(b, newNaiveDB())
}

func Benchmark_NaiveDBReadSeq(b *testing.B) {
	benchmarkReadSeq(b, newNaiveDB())
}

func Benchmark_NaiveDBRangeScan(b *testing.B) {
	benchmarkRangeScan(b, newNaiveDB())
}
