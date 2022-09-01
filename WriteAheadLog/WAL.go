package WriteAheadLog

import (
	"bufio"
	"encoding/binary"
	"io/ioutil"
	"os"
	"strconv"
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



type WAL struct{
	WalPath        string
	CurrentSegment string
	NumberOfSegments int
	lwm uint32
	segmentCapacity uint32
	NumberOfEntries uint32
}

func CreateWAL(walPath string, segmentCapacity uint32, lwm uint32) *WAL {
	w := &WAL{WalPath: walPath, segmentCapacity: segmentCapacity, lwm: lwm}
	segments, err := ioutil.ReadDir(walPath)
	w.NumberOfSegments = len(segments)
	if err != nil{
		panic(err)
	}
	if len(segments) == 0{
		w.createNewSegment()
	}else {
		w.CurrentSegment = walPath+segments[len(segments)-1].Name()
		w.NumberOfEntries = w.getNumberOfEntries()
		if w.NumberOfEntries >= w.segmentCapacity{
			w.createNewSegment()
		}
	}
	return w
}
func (w* WAL) getNumberOfEntries() uint32{
	file, err := os.OpenFile(w.CurrentSegment, os.O_RDONLY, 0777)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	var num uint32 = 0
	for {
		err, _ := Decode(reader)
		if err == nil {
			num++
		}else {
			break
		}
	}
	return num
}


func (w* WAL) AddEntry(key string, value []byte, tombstone byte) bool{
	newEntry := CreateEntry(key, value, tombstone)
	encodedEntry := newEntry.Encode()
	if w.NumberOfEntries >= w.segmentCapacity{
		w.createNewSegment()
	}


	file, err := os.OpenFile(w.CurrentSegment, os.O_APPEND, 0777)
	if err != nil{
		return false
	}

	err = binary.Write(file, binary.LittleEndian, encodedEntry)
	if err != nil{
		return false
	}
	file.Close()
	w.NumberOfEntries++
	return true
}

func (w* WAL) createNewSegment(){
	newFile, err := os.Create(w.WalPath +"log_"+ strconv.Itoa(w.NumberOfSegments+1) +".bin")
	if err != nil{
		panic(err)
	}
	w.CurrentSegment = newFile.Name()
	w.NumberOfEntries = 0
	w.NumberOfSegments++
	newFile.Close()
}

func (w* WAL) RemoveSegments() {
	segments, err := ioutil.ReadDir(w.WalPath)
	if err != nil{
		panic(err)
	}
	for i := 0; i < len(segments); i++ {
		if uint32(i) < w.lwm{
			err := os.Remove(w.WalPath + segments[i].Name())
			if err != nil {
				panic(err)
			}
			w.NumberOfSegments--
		}else{
			newName :="log_"+ strconv.Itoa(i+1-int(w.lwm))+ ".bin"
			err := os.Rename(w.WalPath+ segments[i].Name(), w.WalPath+newName)
			if err != nil {
				panic(err)
			}
			w.CurrentSegment = w.WalPath +newName
		}

	}

	w.NumberOfEntries = w.getNumberOfEntries()
}
func (w* WAL) RemoveAllSegments(){

	segments, err := ioutil.ReadDir(w.WalPath)
	if err != nil{
		panic(err)
	}
	for i := 0; i < len(segments); i++ {
		err := os.Remove(w.WalPath + segments[i].Name())
		if err != nil {
			panic(err)
		}
	}
	w.NumberOfSegments = 0
	w.createNewSegment()
}

