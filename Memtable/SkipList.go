package Memtable

import (
	"fmt"
	"github.com/TamaraDzambic/NASP-projekat/WriteAheadLog"
	"math/rand"
)

type Element struct {
	key       string
	value     []byte
	tombstone bool
	next      []*Element
}
func createElement(key string, value []byte, tombstone bool, height int)*Element{
	return &Element{
		key: key,
		value: value,
		tombstone: tombstone,
		next: make([]*Element, height),
	}
}


type SkipList struct {
	maxHeight int
	height    int
	size      int
	head      *Element
}


func CreateSkipList(maxHeight int) *SkipList {
	root := createElement("", []byte("none"), false, maxHeight)
	return &SkipList{maxHeight, 1, 1, root}
}

func (skipL *SkipList) roll() int {
	height := 0
	for ; rand.Int31n(2) == 1; height++ {
		if height > skipL.height {
			skipL.height = height
			return height
		}
	}
	return height
}

func (skipL *SkipList) Set(key string, value []byte, tombstone bool) *Element {
	newElement := skipL.Get(key)
	if newElement==nil{
		height := skipL.roll()
		node := createElement(key, value, tombstone, height+1)
		for i := skipL.height - 1; i >= 0; i-- {
			currentNode := skipL.head
			next := currentNode.next[i]
			for next != nil {
				if next == nil || next.key > key {
					break
				}
				currentNode = next
				next = currentNode.next[i]

			}
			if i <= height {
				node.next[i] = next
				currentNode.next[i] = node
			}
		}
		skipL.size++
		return node
	} else {
		skipL.Update(key, value, tombstone)
		return newElement
	}

}

func (skipL *SkipList) Get(key string) *Element {
	currentNode := skipL.head
	for i := skipL.height - 1; i >= 0; i-- {
		next := currentNode.next[i]
		for next != nil {
			currentNode = next
			next = currentNode.next[i]
			if currentNode.key == key {
				return currentNode
			}
			if next == nil || currentNode.key > key {
				break
			}
		}
	}

	return nil
}

func (skipL *SkipList) Remove(key string) *Element {
	currentNode := skipL.head
	for i := skipL.height - 1; i >= 0; i-- {
		next := currentNode.next[i]
		for next != nil {
			currentNode = next
			next = currentNode.next[i]
			if next == nil || currentNode.key > key {
				break
			}
			if currentNode.key == key {
				currentNode.tombstone = true
				tmp := currentNode
				currentNode = currentNode.next[i]
				return tmp
			}
		}
	}

	return nil

}

func (skipL *SkipList) Update(key string, value []byte, tombstone bool) *Element {

	currentNode := skipL.head
	for i := skipL.height - 1; i >= 0; i-- {
		next := currentNode.next[i]
		for next != nil {
			currentNode = next
			next = currentNode.next[i]
			if next == nil || currentNode.key > key {
				break
			}
			if currentNode.key == key {
				currentNode.value = value
				currentNode.tombstone = tombstone
				tmp := currentNode
				currentNode = currentNode.next[i]
				return tmp
			}
		}
	}

	return nil

}

func (skipL *SkipList) GetElements () []WriteAheadLog.Entry{
	var lista []WriteAheadLog.Entry
	curr := skipL.head
	for curr.next[0] != nil {
		entry := WriteAheadLog.CreateEntry(curr.next[0].key, curr.next[0].value, BoolToByte(curr.next[0].tombstone))
		lista = append(lista, entry)
		curr = curr.next[0]
	}
	return lista
}

func BoolToByte(flag bool) byte{
	if flag==true{
		return 1
	} else {
		return 0
	}
}


func (skipL *SkipList) PrintList () {
	for i := skipL.height; i >= 0; i-- {
		curr := skipL.head
		fmt.Print("[")
		for curr.next[i] != nil {
			if curr.next[i].tombstone == false {
				fmt.Print(curr.next[i].key + " ")
			}
			curr = curr.next[i]
		}
		fmt.Print("]\n")
	}
}