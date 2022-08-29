package memtable

import (
	"github.com/TamaraDzambic/NASP-projekat/SSTable"
)

type Memtable struct {
	SkipList *SkipList
	capacity int
	SSTable SSTable.SSTable
}


func (memtable *Memtable) Set(key string, value []byte, tombstone bool) {
	if memtable.SkipList.size <= memtable.capacity {
		memtable.SkipList.Set(key, value, tombstone)
	} else {
		memtable.Flush()
		memtable.SkipList.Set(key, value, tombstone)
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

func NewMemtable(maxCapacity int) *Memtable {
	return &Memtable{
		SkipList: CreateSkipList(maxCapacity),
		capacity: maxCapacity,
		SSTable: *SSTable.NewSST(uint(maxCapacity), "SSTable\\files\\data", "SSTable\\files\\index", "SSTable\\files\\summary"),
	}
}


func (memtable *Memtable) Flush(){
	elements := memtable.SkipList.GetElements()
	memtable.SSTable.WriteData(elements)
	memtable.SkipList.PrintList()
	memCap := memtable.capacity
	*memtable = *NewMemtable(memCap)
}