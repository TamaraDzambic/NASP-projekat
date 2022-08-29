package LRU

import (
	"container/list"

	"github.com/TamaraDzambic/NASP-projekat/WriteAheadLog"
)

type Cache struct {
	capacity int
	list     *list.List
	elements map[string]*list.Element
}


func New(capacity int) *Cache {
	return &Cache{
		capacity: capacity,
		list:     new(list.List),
		elements: make(map[string]*list.Element, capacity),
	}
}

func (cache *Cache) Get(key string) []byte {
	if node, ok := cache.elements[key]; ok {
		value := node.Value.(*list.Element).Value.(WriteAheadLog.Entry).Value
		cache.list.MoveToFront(node)
		return value
	}
	return nil
}

func (cache *Cache) Put(entry WriteAheadLog.Entry) {
	if node, ok := cache.elements[entry.Key]; ok {
		cache.list.MoveToFront(node)
		node.Value.(*list.Element).Value = WriteAheadLog.CreateEntry(entry.Key, entry.Value, entry.Tombstone)
	} else {
		if cache.list.Len() == cache.capacity {
			idx := cache.list.Back().Value.(*list.Element).Value.(WriteAheadLog.Entry).Key
			delete(cache.elements, idx)
			cache.list.Remove(cache.list.Back())
		}
	}

	node := &list.Element{
		Value: WriteAheadLog.CreateEntry(entry.Key, entry.Value, entry.Tombstone),
	}

	pointer := cache.list.PushFront(node)
	cache.elements[entry.Key] = pointer
}

//func (cache *Cache) Print() {
//	for key, value := range cache.elements {
//		fmt.Printf("Key:%d,Value:%+v\n", key, string(value.Value.(*list.Element).Value.(WriteAheadLog.Entry).Value))
//	}
//}

//func (cache *Cache) Keys() []interface{} {
//	var keys []interface{}
//	for k := range cache.elements {
//		keys = append(keys, k)
//	}
//	return keys
//}

//func (cache *Cache) RecentlyUsed() interface{} {
//	return string(cache.list.Front().Value.(*list.Element).Value.(WriteAheadLog.Entry).Value)
//}

//func (cache *Cache) Remove(key string) {
//	if node, ok := cache.elements[key]; ok {
//		delete(cache.elements, key)
//		cache.list.Remove(node)
//	}
//}
//
//func (cache *Cache) Purge() {
//	cache.capacity = 0
//	cache.elements = nil
//	cache.list = nil
//}
//

