package SSTable

import (
	"encoding/gob"
	"github.com/TamaraDzambic/NASP-projekat/WriteAheadLog"
	"hash"
	"math"
	"os"
	"time"

	"github.com/spaolacci/murmur3"
)


type BloomFilter struct {
	n     uint          // broj elemenata
	p 	  float64       // verovatnoća greške
	m     uint          // veličina bitseta
	k     uint          // broj hash funkcija
	set   []byte        // set sa bitovima
	hashs []hash.Hash32 // hash funkcije
	TimeConst uint
}

func NewBF(n uint, p float64) *BloomFilter {
	m := M(int(n), p)
	k := K(int(n), m)
	hashs, tc := Hash(k)
	return &BloomFilter{n, p, m, k, make([]byte, m), hashs, tc}
}

func (bf *BloomFilter) Add(elem WriteAheadLog.Entry) {
	for _, hashF := range bf.hashs {
		i := HashElement(hashF, elem.Key, bf.m)
		bf.set[i] = 1
	}
}

func (bf *BloomFilter) Find(elem string) bool {
	for _, hashF := range bf.hashs {
		i := HashElement(hashF, elem, bf.m)
		if bf.set[i] != 1 {
			return false
		}
	}
	return true
}

func HashElement(hashF hash.Hash32, elem string, m uint) uint32 {
	_, err := hashF.Write([]byte(elem))
	if err != nil {
		panic(err)
	}
	i := hashF.Sum32() % uint32(m)
	hashF.Reset()
	return i
}

func M(n int, p float64) uint {
	return uint(math.Ceil(float64(n) * math.Abs(math.Log(p)) / math.Pow(math.Log(2), float64(2))))
}

func K(n int, m uint) uint {
	return uint(math.Ceil((float64(m) / float64(n)) * math.Log(2)))
}

func Hash(k uint) ([]hash.Hash32, uint) {
	var h []hash.Hash32
	ts := uint(time.Now().Unix())
	for i := uint(0); i < k; i++ {
		h = append(h, murmur3.New32WithSeed(uint32(ts+1)))
	}
	return h, ts
}

func CopyHashFunctions(k uint, tc uint) []hash.Hash32 {
	var h []hash.Hash32
	for i := uint(0); i < k; i++ {
		h = append(h, murmur3.New32WithSeed(uint32(tc+1)))
	}
	return h
}

func writeBloomFilter(filename string, bf *BloomFilter) {
	file, err := os.Create(filename)
	if err != nil {
		panic(err)
	}

	defer file.Close()

	encoder := gob.NewEncoder(file)
	err = encoder.Encode(bf)
	if err != nil {
		panic(err)
	}
}

func readBloomFilter(filename string) (bf *BloomFilter) {
	file, err := os.Open(filename)
	if err != nil {
		file, err = os.Create(filename)
		if err!=nil{
			panic(err)
		}
	}
	defer file.Close()



	decoder := gob.NewDecoder(file)
	bf = new(BloomFilter)
	_, err = file.Seek(0, 0)
	if err != nil {
		return nil
	}

	for {
		err = decoder.Decode(bf)
		if err != nil {
			break
		}
	}
	bf.hashs = CopyHashFunctions(bf.k, bf.TimeConst)
	return
}

