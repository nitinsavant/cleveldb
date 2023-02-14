package main

import "fmt"

func main() {
	db, err := loadClevelDB()
	if err != nil {
		return
	}

	fmt.Println(db.memtable.header)

	//db.Put([]byte("firstName"), []byte("nitin"))
	//db.Put([]byte("lastName"), []byte("savant"))
	//db.Put([]byte("middleName"), []byte("gajendra"))
	//db.Insert([]byte("middleName"))

}
