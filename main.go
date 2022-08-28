package main

import (
	"fmt"
	"github.com/TamaraDzambic/NASP-projekat/SSTable"
	"github.com/TamaraDzambic/NASP-projekat/WriteAheadLog"
)

func main() {
	e1 := WriteAheadLog.CreateEntry("5", []byte("0001"), 0)
	e2 := WriteAheadLog.CreateEntry("2", []byte("0002"), 0)
	e3 := WriteAheadLog.CreateEntry("1", []byte("000853"), 0)
	e4 := WriteAheadLog.CreateEntry("4", []byte("0004"), 0)
	e5 := WriteAheadLog.CreateEntry("3", []byte("005"), 0)
	e6 := WriteAheadLog.CreateEntry("6", []byte("0006"), 0)

	data := []WriteAheadLog.Entry{e1, e2, e3, e4, e5, e6}
	table := SSTable.NewSST(6, "SSTable\\files\\data", "SSTable\\files\\index", "SSTable\\files\\summary")
	table.WriteData(data)

	e7 := WriteAheadLog.CreateEntry("0", []byte("0007"), 0)

	data2 := []WriteAheadLog.Entry{e7}
	table.WriteData(data2)

	entry, _ := table.Find("6")
	fmt.Println(entry.Key, entry.Value, entry.Tombstone)
	entry, _ = table.Find("4")
	fmt.Println(entry.Key, entry.Value, entry.Tombstone)
	entry, _ = table.Find("2")
	fmt.Println(entry.Key, entry.Value, entry.Tombstone)
	entry, _ = table.Find("3")
	fmt.Println(entry.Key, entry.Value, entry.Tombstone)
	entry, _ = table.Find("0")
	fmt.Println(entry.Key, entry.Value, entry.Tombstone)
	entry, _ = table.Find("5")
	fmt.Println(entry.Key, entry.Value, entry.Tombstone)



	//m := Memtable.NewMemtable(20)
	//
	//m.SkipList.PrintList()
	//
	//a := m.Set("111",[]byte("1"), false)
	//fmt.Println(a)
	//a = m.Set("555",[]byte("65"), false)
	//fmt.Println(a)
	//
	//a = m.Set("333",[]byte("3"), false)
	//fmt.Println(a)
	//a = m.Set("444",[]byte("4"), false)
	//fmt.Println(a)
	//a = m.Set("222",[]byte("2"), false)
	//fmt.Println(a)
	//a = m.Set("666",[]byte("2"), false)
	//fmt.Println(a)
	//a = m.Set("888",[]byte("2"), false)
	//fmt.Println(a)
	//a = m.Set("777",[]byte("2"), false)
	//fmt.Println(a)
	//a = m.Set("999",[]byte("2"), false)
	//fmt.Println(a)
	//a = m.Set("999",[]byte("2"), true)
	//fmt.Println(a)
	//
	//m.SkipList.PrintList()
	//
	//
	//	w, err := WriteAheadLog.CreateWAL("WriteAheadLog/WAL/", 6, 2)
	//	if err != nil{
	//		fmt.Println("error")
	//		return
	//	}
	//
	//	fmt.Println(w.CurrentSegment, " Current segment")
	//	fmt.Println(w.NumberOfSegments, " Number of segments")
	//	fmt.Println(w.NumberOfEntries, " Number of entries in current segment")
	//	w.AddEntry("333",[]byte("3"), 0)
	//	w.AddEntry("44",[]byte("3"), 0)
	//	w.AddEntry("222",[]byte("3"), 0)
	//
	//
	//	fmt.Println(w.CurrentSegment, " Current segment")
	//	fmt.Println(w.NumberOfSegments, " Number of segments")
	//	fmt.Println(w.NumberOfEntries, " Number of entries in current segment")

	//	w.RemoveSegments()
	//	w.RemoveAllSegments()
	//
	//	fmt.Println(w.CurrentSegment, " Current segment")
	//	fmt.Println(w.NumberOfSegments, " Number of segments")
	//	fmt.Println(w.NumberOfEntries, " Number of entries in current segment")
}