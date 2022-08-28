package WriteAheadLog

import (
	"bufio"
	"encoding/binary"
	"hash/crc32"
	"io"
	"time"
)

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

func Decode(reader *bufio.Reader) (error, Entry) {
	entry := Entry{}

	err := binary.Read(reader, binary.LittleEndian, &entry.CRC)
	if err != nil {
		if err == io.EOF {
			return err, entry
		}
		return err, entry
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
		return err, entry
	}
	entry.Value = value
	return nil, entry
}