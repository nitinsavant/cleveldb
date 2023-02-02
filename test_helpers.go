package main

import (
	"math/rand"
	"strconv"
	"testing"
	"time"
)

func init() {
	rand.Seed(time.Now().Unix())
}

func testGetReturnsCorrectValue(t *testing.T, db DB) {
	_ = db.Put([]byte("firstName"), []byte("nitin"))
	_ = db.Put([]byte("lastName"), []byte("savant"))
	_ = db.Put([]byte("maidenName"), []byte(""))

	var tests = []struct {
		key   string
		value string
		err   error
	}{
		{"lastName", "savant", nil},
		{"firstName", "nitin", nil},
		{"maidenName", "", nil},
		{"middleName", "", keyNotFoundErr},
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

func testDeleteSetsValueToNil(t *testing.T, db DB) {
	key := []byte("name")
	val := []byte("nitin")

	_ = db.Put(key, val)
	actual, _ := db.Get(key)
	if string(actual) != string(val) {
		t.Errorf(`storage.Get("%s") returns unexpected value: "%s"`, key, actual)
	}

	_ = db.Delete(key)
	actual, _ = db.Get(key)
	if actual != nil {
		t.Errorf(`storage.Get("%s") returns unexpected value: "%s"`, key, actual)
	}
}

func testRangeScanAndNextReturnCorrectOrderedValues(t *testing.T, db DB) {
	keys := [][]byte{[]byte("b"), []byte("c"), []byte("a"), []byte("f"), []byte("d")}
	vals := [][]byte{[]byte("nitin"), []byte("neha"), []byte("cassie"), []byte("karli"), []byte("david")}

	_ = db.Put(keys[0], vals[0])
	_ = db.Put(keys[1], vals[1])
	_ = db.Put(keys[2], vals[2])
	_ = db.Put(keys[3], vals[3])
	_ = db.Put(keys[4], vals[4])

	iter, _ := db.RangeScan([]byte("b"), []byte("d"))

	expectedKeys := [][]byte{[]byte("b"), []byte("c"), []byte("d")}
	expectedVals := [][]byte{[]byte("nitin"), []byte("neha"), []byte("david")}
	expectedNexts := []bool{true, true, false}

	for i := 0; i < len(expectedKeys); i++ {
		expectedVal := string(expectedVals[i])
		expectedKey := string(expectedKeys[i])
		expectedNext := expectedNexts[i]
		actualVal := string(iter.Value())
		actualKey := string(iter.Key())
		actualNext := iter.Next()

		if expectedKey != actualKey || expectedVal != actualVal {
			t.Errorf(`storage.RangeScan returns unexpected key/value: "%s: %s" at index: %v`, actualKey, actualVal, i)
		}

		if expectedNext != actualNext {
			t.Errorf(`storage.RangeScan returns unexpected next value-- Expected: "%v, Actual: %v" at index: %v`, expectedNext, actualNext, i)
		}
	}
}

func benchmarkFillSeq(b *testing.B, db DB) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Put([]byte(strconv.Itoa(i)), []byte("v"))
	}
}

func benchmarkFillRand(b *testing.B, db DB) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Put([]byte(strconv.Itoa(rand.Int())), []byte("v"))
	}
}

func benchmarkDeleteSeq(b *testing.B, db DB) {
	for i := 0; i < b.N; i++ {
		db.Put([]byte(strconv.Itoa(i)), []byte("v"))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Delete([]byte(strconv.Itoa(i)))
	}
}

func benchmarkReadSeq(b *testing.B, db DB) {
	for i := 0; i < b.N; i++ {
		db.Put([]byte(strconv.Itoa(i)), []byte("v"))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Get([]byte(strconv.Itoa(i)))
	}
}

func benchmarkRangeScan(b *testing.B, db DB) {
	for i := 0; i < b.N; i++ {
		db.Put([]byte(strconv.Itoa(i)), []byte("v"))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = db.RangeScan([]byte("l"), []byte("p"))
	}
}
