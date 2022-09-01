package SSTable

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/TamaraDzambic/NASP-projekat/MerkleTree"
	"github.com/TamaraDzambic/NASP-projekat/WriteAheadLog"
)

type SSTable struct {
	indexMap 	map[string]uint64
	data         *os.File
	index        *os.File
	summary 	 *os.File
	bf  BloomFilter
	MerkleTree   MerkleTree.MerkleRoot
}

func  NewSST(datasize uint, dataFN string, indexFN string, summaryFN string) *SSTable{
	sstable :=	SSTable{indexMap: make(map[string]uint64), summary: CreateFile(summaryFN), data: CreateFile(dataFN), index: CreateFile(indexFN), bf: *NewBF(datasize, 0.01)}
	defer CloseFiles(sstable)
	return &sstable
}

func CreateFile(fn string) *os.File {
	err := os.MkdirAll(filepath.Dir(fn), 0777)
	fp, err := os.Create(fn)
	if err != nil {
		panic(err)
	}
	return fp
}

func (table *SSTable) createIndex(){
	//sortiraj index mapu i upisi u index i summary
	keys := make([]string, 0, len(table.indexMap))
	for k := range table.indexMap{
		keys = append(keys, k)
	}
	sort.Strings(keys)

	table.index = CreateFile(table.index.Name())
	table.summary = CreateFile(table.summary.Name())

	WriteKey(table.summary, keys[0])
	WriteKey(table.summary, keys[len(keys)-1])

	for _, k := range keys {
		indexOffset := FileSize(table.index)
		WriteKey(table.index, k)
		WriteOffset(table.index, table.indexMap[k])
		WriteKey(table.summary, k)
		WriteOffset(table.summary, indexOffset)
	}

	CloseFiles(*table)
}

func (table *SSTable) WriteData(data []WriteAheadLog.Entry) {
	var dataInBytesForMerkle [][]byte
	table.data, _ = os.OpenFile(table.data.Name(), os.O_APPEND, 0777)

	for _, entry := range data {
		offset := FileSize(table.data)
		table.data.Write(entry.Encode())
		table.indexMap[entry.Key] = offset

		table.bf.Add(entry)
		dataInBytesForMerkle = append(dataInBytesForMerkle, entry.Encode())
	}
	table.createIndex()
	table.MerkleTree = *MerkleTree.BuildTree(dataInBytesForMerkle, "C:\\Users\\ANJA\\Documents\\GitHub\\NASP-projekat\\MerkleTree\\Files\\metadata.txt")
	writeBloomFilter("bloomFile", &table.bf)
}


func (table *SSTable) Find( key string) (entry WriteAheadLog.Entry, found bool) {
	table.data, _ = os.OpenFile(table.data.Name(), os.O_RDONLY, 0777)
	table.index, _ =os.OpenFile(table.index.Name(), os.O_RDONLY, 0777)
	table.summary, _ =os.OpenFile(table.summary.Name(), os.O_RDONLY, 0777)
	defer CloseFiles(*table)

	if !table.bf.Find(key) {
		found = false
		return
	}

	readerS := bufio.NewReader(table.summary)
	readerI := bufio.NewReader(table.index)

	minKey := ReadKey(readerS)
	maxKey := ReadKey(readerS)

	if minKey > key || maxKey < key {
		found = false
		return
	}
	for {
		keyS := ReadKey(readerS)
		if keyS == "" {
			found = false
			break
		}
		offset, _ := ReadOffset(readerS)
		if key < keyS {
			found = false
			return
		}
		if key == keyS {
			table.index.Seek(int64(offset), 0)
			ReadKey(readerI)
			dataOffset, _ := ReadOffset(readerI)

			table.data.Seek(int64(dataOffset), 0)

			readerD := bufio.NewReader(table.data)
			err := os.ErrExist
			err, entry = WriteAheadLog.Decode(readerD)
			if err == nil {
				found = true
				return
			}

		}
	}
	found = false
	return
}


func WriteKey(fp *os.File, key string) {
	keyBytes := []byte(key)
	keyLen := uint64((len(keyBytes)))
	lenBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(lenBytes, keyLen)

	bytes := make([]byte, 0, 8+keyLen)
	bytes = append(bytes, lenBytes...)
	bytes = append(bytes, keyBytes...)
	fp.Write(bytes)
}


func ReadKey(reader *bufio.Reader) string {
	keyLenBytes := make([]byte, 8)
	_, err := reader.Read(keyLenBytes)
	if err != nil {
		panic(err)
	}
	keyLen := binary.LittleEndian.Uint64(keyLenBytes)

	keyBytes := make([]byte, keyLen)
	_, err = reader.Read(keyBytes)
	if err != nil {
		panic(err)
	}

	return string(keyBytes)
}

func WriteOffset(fp *os.File, value uint64) {
	bytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytes, value)
	fp.Write(bytes)
}

func ReadOffset(reader *bufio.Reader) (uint64, error) {
	bytes := make([]byte, 8)
	_, err := reader.Read(bytes)
	if err == io.EOF {
		return 0, err
	} else {
		if err != nil {
			panic(err)
		}
	}
	return binary.LittleEndian.Uint64(bytes), nil
}


func CloseFiles(sst SSTable) {
	sst.data.Close()
	sst.index.Close()
	sst.summary.Close()
}

func FileSize(fp *os.File) uint64 {
	fi, err := fp.Stat()
	if err != nil {
		panic(err)
	}
	return uint64(fi.Size())
}




