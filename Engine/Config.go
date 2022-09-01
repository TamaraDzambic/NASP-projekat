package Engine

import (
	"bufio"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	WALSegmentCapacity uint32
	WALlwm uint32
	TokenBucketRate int64
	TokenBucketMaxTokens int64
	SSTableDataSize uint
	MemtableMaxCapacity int
	LRUcapacity int
}

func CreateConfig(mapa map[string]int) *Config{
	return &Config{
		WALSegmentCapacity : uint32(mapa["WALSegmentCapacity"]),
		WALlwm : uint32(mapa["WALlwm"]),
		TokenBucketRate : int64(mapa["TokenBucketRate"]),
		TokenBucketMaxTokens : int64(mapa["TokenBucketMaxTokens"]),
		SSTableDataSize : uint(mapa["SSTableDataSize"]),
		MemtableMaxCapacity : mapa["MemtableMaxCapacity"],
		LRUcapacity : mapa["LRUcapacity"],
	}
}

func NewConfig() *Config{
	return CreateConfig(createConfigMap())
}

func createConfigMap() (configMap map[string]int){
	mapa := map[string]int{}
	f, _ := os.Open("Engine\\config.txt")
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		v := strings.Split(line, ":")
		i, _ := strconv.Atoi(v[1])
		mapa[v[0]]=i
	}
	return mapa
}
