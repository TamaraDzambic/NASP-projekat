package MerkleTree

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"log"
	"os"
)

type MerkleRoot struct {
	Root *Node
}

func (mr *MerkleRoot) String() string {
	return mr.Root.String()
}

type Node struct {
	data  [20]byte
	left  *Node
	right *Node
}

func (n *Node) String() string {
	return hex.EncodeToString(n.data[:])
}

func Hash(data []byte) [20]byte {
	return sha1.Sum(data)
}


func BuildTree(entries [][]byte, path string) *MerkleRoot {
	data := entries
	var leaves  []*Node
	for i := 0; i < len(data); i++ {
		node := Node{Hash(data[i]), nil, nil}
		leaves = append(leaves, &node)
	}

	root := MerkleRoot{Nodes(leaves)}
	WriteInFile(root.Root, path, 0)
	return &root
}


func Nodes(leaves []*Node) *Node {
	var level []*Node
	nodes := leaves

	if len(nodes) > 1 {
		for i := 0; i < len(nodes); i += 2 {
			if (i + 1) < len(nodes) {
				newNodeBytes := append(nodes[i].data[:], nodes[i+1].data[:]...)
				newNode := Node{Hash(newNodeBytes), nodes[i], nodes[i+1]}
				level = append(level, &newNode)
			} else {
				node2 := Node{data: [20]byte{}, left: nil, right: nil}
				newNodeBytes := append(nodes[i].data[:], node2.data[:]...)
				newNode := Node{Hash(newNodeBytes), nodes[i], &node2}
				level = append(level, &newNode)
			}
		}
		nodes = level

		if len(nodes) == 1 {
			return nodes[0]
		}
	}
	return Nodes(level)
}

func WriteInFile(root *Node, path string, flag int) {
	newFile, err := os.Create(path)
	err = newFile.Close()
	if err != nil {
		return
	}
	file, err := os.OpenFile(path, os.O_WRONLY, 0444)
	if err != nil {
		log.Fatal(err)
	}

	queue := make([]*Node, 0)
	queue = append(queue, root)


	for len(queue) != 0 {
		e := queue[0]
		queue = queue[1:]
		if flag==1 {
			fmt.Println(e.String())
		}
		_, _ = file.WriteString(e.String() + "\n")

		if e.left != nil {
			queue = append(queue, e.left)
		}
		if e.right != nil {
			queue = append(queue, e.right)
		}
	}
	err = file.Close()
	if err != nil {
		fmt.Println(err)
	}
}