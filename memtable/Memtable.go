package memtable
type Memtable struct {
	SkipList SkipList
	capacity int64
	currentCapacity int64
}


func (memtable *Memtable) Set(key string, value []byte, tombstone bool) bool {
	if memtable.currentCapacity < memtable.capacity {
		return memtable.SkipList.Set(key, value, tombstone)
	} else {
		//flush
		return false
	}
}

func (memtable *Memtable) Get(key string) *Element {
	var element *Element
	element = memtable.SkipList.Get(key)
	if element != nil{
		return element
	}
	return nil
}

func (memtable *Memtable) Remove(key string, value []byte) bool {
	var element *Element
	element = memtable.SkipList.Get(key)
	if element != nil{
		element.tombstone = true
		// videti da li izbrisati skroz ili upisati sa novim flegom
		memtable.SkipList.Remove(key)
		return true
	}
	return false

}

func NewMemtable(maxCapacity int64) *Memtable {
	return &Memtable{
		SkipList: *createSkipList(int(maxCapacity)),
		capacity: maxCapacity,
		currentCapacity: 0,
	}
}

func (memtable *Memtable) Flush(){
	// funkcija flush
}