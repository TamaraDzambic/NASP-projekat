package main

import (
	"bufio"
	"encoding/binary"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"time"
)

/*
   +---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
   |    CRC (4B)   | Timestamp (16B) | Tombstone(1B) | Key Size (8B) | Value Size (8B) | Key | Value |
   +---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
   CRC = 32bit hash computed over the payload using CRC
   Key Size = Length of the Key data
   Tombstone = If this record was deleted and has a value,
   Value Size = Length of the Value data
   Key = Key data
   Value = Value data
   Timestamp = Timestamp of the operation in seconds
*/

type Entry struct {
	CRC uint32
	Timestamp uint64
	Tombstone byte
	KeySize uint64
	ValueSize uint64
	Key string
	Value []byte
}

func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

func CreateEntry(key string, value []byte, tombstone byte) Entry{
	crc32 := CRC32(value)
	timestamp := time.Now().Unix()
	keySize := uint64(len([]byte(key)))
	valueSize := uint64(len(value))
	return Entry{crc32, uint64(timestamp), tombstone, keySize, valueSize, key, value}
}

func (entry* Entry) Encode() []byte{
	crc32 := make([]byte, 4)
	binary.LittleEndian.PutUint32(crc32, entry.CRC)

	timestamp := make([]byte, 8)
	binary.LittleEndian.PutUint64(timestamp, entry.Timestamp)

	tombstone := []byte{0}
	if entry.Tombstone == 1{
		tombstone = []byte{1}
	}

	keySize := make([]byte, 8)
	binary.LittleEndian.PutUint64(keySize, entry.KeySize)

	valueSize:= make([]byte, 8)
	binary.LittleEndian.PutUint64(valueSize, entry.ValueSize)

	ret := make([]byte, 0, 29+entry.KeySize+entry.ValueSize)
	ret = append(ret, crc32...)
	ret = append(ret, timestamp...)
	ret = append(ret, tombstone...)
	ret = append(ret, keySize...)
	ret = append(ret, valueSize...)
	ret = append(ret, []byte(entry.Key)...)
	ret = append(ret, entry.Value...)
	return ret
}

func (entry* Entry) Decode(reader *bufio.Reader) error{

	err := binary.Read(reader, binary.LittleEndian, &entry.CRC)
	if err != nil {
		if err == io.EOF {
			return err
		}
		return err
	}
	binary.Read(reader, binary.LittleEndian, &entry.Timestamp)
	binary.Read(reader, binary.LittleEndian, &entry.Tombstone)
	binary.Read(reader, binary.LittleEndian, &entry.KeySize)
	binary.Read(reader, binary.LittleEndian, &entry.ValueSize)
	key := make([]byte, entry.KeySize)
	binary.Read(reader, binary.LittleEndian, &key)
	entry.Key = string(key)
	value := make([]byte, entry.ValueSize)
	err = binary.Read(reader, binary.LittleEndian, &value)
	if err != nil {
		return err
	}
	entry.Value = value

	return nil
}

type WAL struct{
	walPath string
	currentSegment string
	numberOfSegments int
	lwm uint32
	segmentCapacity uint32
	numberOfEntries uint32
}

func CreateWAL(walPath string, segmentCapacity uint32, lwm uint32) (*WAL, error){
	w := &WAL{walPath: walPath, segmentCapacity: segmentCapacity, lwm: lwm}
	segments, err := ioutil.ReadDir(walPath)
	w.numberOfSegments = len(segments)
	if err != nil{
		panic(err)
	}
	if len(segments) == 0{
		w.createNewSegment()
	}else {
		w.currentSegment = walPath+segments[len(segments)-1].Name()
		w.numberOfEntries = w.getNumberOfEntries()
		if w.numberOfEntries >= w.segmentCapacity{
			w.createNewSegment()
		}
	}
	return w, nil
}
func (w* WAL) getNumberOfEntries() uint32{
	file, err := os.OpenFile(w.currentSegment, os.O_RDONLY, 0777)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	entry := Entry{}
	var num uint32 = 0
	for {
		if entry.Decode(reader) == nil {
			num++
		}else {
			break
		}
	}
	file.Close()
	return num
}


func (w* WAL) addEntry(key string, value []byte, tombstone byte) bool{
	newEntry := CreateEntry(key, value, tombstone)
	encodedEntry := newEntry.Encode()
	if w.numberOfEntries >= w.segmentCapacity{
		w.createNewSegment()
	}


	file, err := os.OpenFile(w.currentSegment, os.O_APPEND, 0777)
	if err != nil{
		return false
	}

	err = binary.Write(file, binary.LittleEndian, encodedEntry)
	if err != nil{
		return false
	}
	file.Close()
	w.numberOfEntries++
	return true
}

func (w* WAL) createNewSegment(){
	newFile, err := os.Create(w.walPath+"log_"+ strconv.Itoa(w.numberOfSegments+1) +".bin")
	if err != nil{
		panic(err)
	}
	w.currentSegment = newFile.Name()
	w.numberOfEntries = 0
	w.numberOfSegments++
	newFile.Close()
}

func (w* WAL) removeSegments() {
	segments, err := ioutil.ReadDir(w.walPath)
	if err != nil{
		panic(err)
	}
	for i := 0; i < len(segments); i++ {
		if uint32(i) < w.lwm{
			err := os.Remove(w.walPath + segments[i].Name())
			if err != nil {
				panic(err)
			}
			w.numberOfSegments--
		}else{
			newName :="log_"+ strconv.Itoa(i+1-int(w.lwm))+ ".bin"
			err := os.Rename(w.walPath + segments[i].Name(), w.walPath+newName)
			if err != nil {
				panic(err)
			}
			w.currentSegment = w.walPath+newName
		}

	}

	w.numberOfEntries = w.getNumberOfEntries()
}
func (w* WAL) removeAllSegments(){

	segments, err := ioutil.ReadDir(w.walPath)
	if err != nil{
		panic(err)
	}
	for i := 0; i < len(segments); i++ {
		err := os.Remove(w.walPath + segments[i].Name())
		if err != nil {
			panic(err)
		}
	}
	w.numberOfSegments = 0
	w.createNewSegment()
}


func main() {
	//w, err := CreateWAL("WriteAheadLog/WAL/", 15, 3)
	//if err != nil{
	//	fmt.Println("error")
	//	return
	//}
	//fmt.Println(w.currentSegment, " Current segment")
	//fmt.Println(w.numberOfSegments, " Number of segments")
	//fmt.Println(w.numberOfEntries, " Number of entries in current segment")
	//
	//_ = w.addEntry("aaaa", []byte("1"), 1)
	//_ = w.addEntry("bbbb", []byte("2"), 0)
	//_ =w.addEntry("cccc", []byte("3"), 1)
	//
	//_ =w.addEntry("dddd", []byte("4"), 0)
	//
	//fmt.Println(w.currentSegment, " Current segment")
	//fmt.Println(w.numberOfSegments, " Number of segments")
	//fmt.Println(w.numberOfEntries, " Number of entries in current segment")

	//w.removeSegments()
	//w.removeAllSegments()

	//fmt.Println(w.currentSegment, " Current segment")
	//fmt.Println(w.numberOfSegments, " Number of segments")
	//fmt.Println(w.numberOfEntries, " Number of entries in current segment")
}
