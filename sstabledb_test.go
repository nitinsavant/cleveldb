package main

var ssTableDB *SSTable

//func getTestSSTableDB() DB {
//	if ssTableDB != nil {
//		return ssTableDB
//	}
//
//	db, err := loadClevelDB()
//	if err != nil {
//		return nil
//	}
//
//	// For Get/Delete tests
//	_ = db.Put([]byte("firstName"), []byte("nitin"))
//	_ = db.Put([]byte("lastName"), []byte("savant"))
//	_ = db.Put([]byte("maidenName"), []byte(""))
//
//	// For RangeScan test
//	keys := [][]byte{[]byte("b"), []byte("c"), []byte("a"), []byte("f"), []byte("d")}
//	vals := [][]byte{[]byte("nitin"), []byte("neha"), []byte("cassie"), []byte("karli"), []byte("david")}
//	_ = db.Put(keys[0], vals[0])
//	_ = db.Put(keys[1], vals[1])
//	_ = db.Put(keys[2], vals[2])
//	_ = db.Put(keys[3], vals[3])
//	_ = db.Put(keys[4], vals[4])
//
//	file, err := os.OpenFile("test/segment_1.ss", os.O_RDWR|os.O_CREATE, os.ModePerm)
//	if err != nil {
//		fmt.Printf("error opening file: %v", err)
//		return nil
//	}
//
//	ssTable, err := db.flushMemtable(file)
//	if err != nil {
//		return nil
//	}
//
//	return loadSSTable(ssTable.file)
//}
//
//func Test_SSTableDBGetReturnsCorrectValue(t *testing.T) {
//	db := getTestSSTableDB()
//
//	var tests = []struct {
//		key   string
//		value string
//		err   error
//	}{
//		{"lastName", "savant", nil},
//		{"firstName", "nitin", nil},
//		{"maidenName", "", nil},
//		{"middleName", "", notFoundInTableErr},
//	}
//
//	for _, test := range tests {
//		actualValue, actualErr := db.Get([]byte(test.key))
//		expectedValue := []byte(test.value)
//
//		if string(actualValue) != string(expectedValue) {
//			t.Errorf(`storage.Get("%s") returns unexpected value: "%s"`, test.key, actualValue)
//		}
//
//		if test.err != actualErr {
//			t.Errorf(`storage.Get("%s") returns unexpected err: "%s"`, test.key, actualErr)
//		}
//	}
//}
//
//func Test_SSTableDBRangeScanAndNextReturnCorrectOrderedValues(t *testing.T) {
//	db := getTestSSTableDB()
//
//	iter, _ := db.RangeScan([]byte("b"), []byte("d"))
//
//	expectedKeys := [][]byte{[]byte("b"), []byte("c"), []byte("d")}
//	expectedVals := [][]byte{[]byte("nitin"), []byte("neha"), []byte("david")}
//	expectedNexts := []bool{true, true, false}
//
//	for i := 0; i < len(expectedKeys); i++ {
//		expectedVal := string(expectedVals[i])
//		expectedKey := string(expectedKeys[i])
//		expectedNext := expectedNexts[i]
//		actualVal := string(iter.Value())
//		actualKey := string(iter.Key())
//		actualNext := iter.Next()
//
//		if expectedKey != actualKey || expectedVal != actualVal {
//			t.Errorf(`storage.RangeScan returns unexpected key/value: "%s: %s" at index: %v`, actualKey, actualVal, i)
//		}
//
//		if expectedNext != actualNext {
//			t.Errorf(`storage.RangeScan returns unexpected next value-- Expected: "%v, Actual: %v" at index: %v`, expectedNext, actualNext, i)
//		}
//	}
//}
