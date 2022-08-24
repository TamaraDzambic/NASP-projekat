package memtable
import (
	"fmt"
	"math/rand"
)

type Element struct {
	key       string
	value     []byte
	tombstone bool
	prev 	  *Element
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

func createSkipList(maxHeight int) *SkipList{
	newEl := createElement("", []byte("none"), false, maxHeight)
	return &SkipList{maxHeight: maxHeight, height: 0, size: 0, head: newEl}
}


func (skipL *SkipList) roll() int {
	height := 0
	for ; rand.Int31n(2) == 1 && height < skipL.maxHeight; height++ {
		if height > skipL.height {
			skipL.height = height
			return height
		}
	}
	return height
}



func (skipL *SkipList) Set (key string, value []byte, tombstone bool) bool{
	newElement := skipL.Get(key)
	if newElement == nil {
		height := skipL.roll()
		fmt.Println(height, ": heigt, ", key, ": key")
		newElement = createElement(key, value, tombstone, height+1)

		current := skipL.head
		for i := skipL.height - 1; i >= 0; i-- {

			next := current.next[i]
			for next != nil {
				if next == nil || next.key > key {
					break
				}
				current = next
				next = current.next[i]
			}
			if i <= height {
				skipL.size++
				newElement.next[i] = next
				current.next[i] = newElement
			}
		}
		return true
	}else{
		for i := skipL.height - 1; i >= 0; i-- {
			current := skipL.head
			next := current.next[i]
			for next != nil {
				if next == nil || next.key > key {
					break
				}
				current = next
				next = current.next[i]
			}
			if current.key == key {
				newElement.value = value
				newElement.tombstone = tombstone
			}
		}
		return true
	}
}

func (skipL *SkipList) Get (key string) *Element{
	current := skipL.head
	for i:= skipL.height; i>=0; i--{
		for ; current.next[i] != nil; current = current.next[i] {
			next := current.next[i]
			if next.key > key {
				break
			}
		}
		if current.key == key {
			return current
		}
	}
	return nil
}

func (skipL *SkipList) Remove (key string) bool{

	return true
}

func (skipL *SkipList) PrintList () {
	for i := skipL.height; i >= 0; i-- {
		curr := skipL.head
		fmt.Print("[")
		for curr.next[i] != nil {
			if curr.next[i].tombstone == false {
				fmt.Print(curr.next[i].key + ", ")
			}
			curr = curr.next[i]
		}
		fmt.Print("]\n")
	}
}