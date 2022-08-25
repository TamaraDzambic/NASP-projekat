package main

import (
	"fmt"
	"github.com/TamaraDzambic/nasp/Memtable"
	"github.com/TamaraDzambic/nasp/WriteAheadLog"
)

func main() {

	m := Memtable.NewMemtable(20)

	m.SkipList.PrintList()

	a := m.Set("111",[]byte("1"), false)
	fmt.Println(a)
	a = m.Set("555",[]byte("65"), false)
	fmt.Println(a)

	a = m.Set("333",[]byte("3"), false)
	fmt.Println(a)
	a = m.Set("444",[]byte("4"), false)
	fmt.Println(a)
	a = m.Set("222",[]byte("2"), false)
	fmt.Println(a)
	a = m.Set("666",[]byte("2"), false)
	fmt.Println(a)
	a = m.Set("888",[]byte("2"), false)
	fmt.Println(a)
	a = m.Set("777",[]byte("2"), false)
	fmt.Println(a)
	a = m.Set("999",[]byte("2"), false)
	fmt.Println(a)
	a = m.Set("999",[]byte("2"), true)
	fmt.Println(a)

	m.SkipList.PrintList()


		w, err := WriteAheadLog.CreateWAL("WriteAheadLog/WAL/", 3, 2)
		if err != nil{
			fmt.Println("error")
			return
		}

		fmt.Println(w.CurrentSegment, " Current segment")
		fmt.Println(w.NumberOfSegments, " Number of segments")
		fmt.Println(w.NumberOfEntries, " Number of entries in current segment")


		fmt.Println(w.CurrentSegment, " Current segment")
		fmt.Println(w.NumberOfSegments, " Number of segments")
		fmt.Println(w.NumberOfEntries, " Number of entries in current segment")

		//w.RemoveSegments()
		w.RemoveAllSegments()

		fmt.Println(w.CurrentSegment, " Current segment")
		fmt.Println(w.NumberOfSegments, " Number of segments")
		fmt.Println(w.NumberOfEntries, " Number of entries in current segment")
}