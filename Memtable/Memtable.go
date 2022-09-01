package Memtable

import (
	"fmt"
	"github.com/TamaraDzambic/NASP-projekat/SSTable"
)

type Memtable struct {
	SkipList *SkipList
	capacity int
	SSTable *SSTable.SSTable
}


func (memtable *Memtable) Set(key string, value []byte, tombstone bool) bool{
	if memtable.SkipList.size <= memtable.capacity {
		fmt.Println("put: ", key)
		memtable.SkipList.Set(key, value, tombstone)
		return false //no flush
	} else {
		memtable.Flush()
		memtable.SkipList.Set(key, value, tombstone)
		return true //flush
	}
}

func (memtable *Memtable) Get(key string) *Element {
	element := memtable.SkipList.Get(key)
	if element != nil{
		return element
	}
	return nil
}

func (memtable *Memtable) Remove(key string){
	memtable.SkipList.Remove(key)
}

func NewMemtable(maxCapacity int, table *SSTable.SSTable) *Memtable {
	return &Memtable{
		SkipList: CreateSkipList(maxCapacity),
		capacity: maxCapacity,
		SSTable: table,
	}
}


func (memtable *Memtable) Flush(){
	elements := memtable.SkipList.GetElements()
	memtable.SSTable.WriteData(elements)
	memtable.SkipList.PrintList()
	memCap := memtable.capacity
	*memtable = *NewMemtable(memCap, memtable.SSTable)
}