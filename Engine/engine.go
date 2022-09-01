package Engine

import (
	"bufio"
	"fmt"
	"github.com/TamaraDzambic/NASP-projekat/LRU"
	"github.com/TamaraDzambic/NASP-projekat/Memtable"
	"github.com/TamaraDzambic/NASP-projekat/SSTable"
	"github.com/TamaraDzambic/NASP-projekat/TokenBucket"
	"github.com/TamaraDzambic/NASP-projekat/WriteAheadLog"
	"io/ioutil"
	"os"
)

type Engine struct {
	wal *WriteAheadLog.WAL
	memtable *Memtable.Memtable
	sstable *SSTable.SSTable
	cache *LRU.Cache
	tokenBucket *TokenBucket.TokenBucket
}

const capacity = 6

func CreateEngine() *Engine{
	engine := Engine{}
	engine.wal = WriteAheadLog.CreateWAL("WriteAheadLog\\WAL\\", capacity/2, 2)
	engine.sstable = SSTable.NewSST(capacity, "SSTable\\files\\data", "SSTable\\files\\index", "SSTable\\files\\summary")
	engine.memtable = Memtable.NewMemtable(capacity, engine.sstable)
	engine.cache = LRU.New(15)
	engine.tokenBucket = TokenBucket.NewTokenBucket(5,10)
	engine.walToMem()
	return &engine
}

func (engine *Engine)walToMem(){
	//proveri da li ima nesto u walu, i dodaj to u mem
	if engine.wal.NumberOfEntries > 0{
		segments, err := ioutil.ReadDir(engine.wal.WalPath)
		if err != nil{
			panic(err)
		}
		for i := range segments {

			file, err := os.OpenFile(engine.wal.WalPath + segments[i].Name(), os.O_RDONLY, 0777)
			if err != nil {
				panic(err)
			}
			defer file.Close()

			reader := bufio.NewReader(file)
			for {
				err, el := WriteAheadLog.Decode(reader)
				if err == nil {
					engine.put(el.Key, el.Value, el.Tombstone)
				}else {
					break
				}
			}
		}
	}
}

func Menu(engine *Engine){
	for{
		var option string
		fmt.Println("_____________MENU_____________")
		fmt.Println("1 Put (key, value)")
		fmt.Println("2 Delete (key)")
		fmt.Println("3 Get (key)")
		fmt.Println("0 Exit")
		fmt.Println("Option: ")
		_, err := fmt.Scanln(&option)
		if err != nil {
			fmt.Println(err)
		}
		if engine.tokenBucket.IsRequestAllowed()==true{

		}
		if option == "0"{
			os.Exit(0)
		}else if option == "1"{

			var key string
			var value string
			fmt.Println("Key: ")
			_, err := fmt.Scanln(&key)
			if err != nil {
				fmt.Println(err)
			}
			fmt.Println("Value: ")
			_, err = fmt.Scanln(&value)
			if err != nil {
				fmt.Println(err)
			}
			if engine.tokenBucket.IsRequestAllowed(){
				if engine.wal.AddEntry(key, []byte(value), 0) {
					engine.put(key, []byte(value), 0)
				}
			}


		}else if option == "2"{

			var key string
			fmt.Println("Key: ")
			_, err := fmt.Scanln(&key)
			if err != nil {
				fmt.Println(err)
			}
			if engine.tokenBucket.IsRequestAllowed(){
				engine.delete(key, 0)
			}


		}else if option == "3"{

			var key string
			fmt.Println("Key: ")
			_, err := fmt.Scanln(&key)
			if err != nil {
				fmt.Println(err)
			}
			if engine.tokenBucket.IsRequestAllowed(){
				engine.get(key)
			}

		}else{
			fmt.Println("Invalid option. Try again.")
		}
	}
}


func (engine *Engine) put(key string, value []byte, tombstone byte) bool{
	t := true
	if tombstone == 0{
		t = false
	}
	if engine.memtable.Set(key, value, t){
		engine.wal.RemoveSegments()    //ako je pozvana flush funkcija izbrisi segmente sa tim elementima iz wala
	}
	engine.memtable.SkipList.PrintList()


	return false
}

func (engine *Engine) delete(key string, tombstone byte) bool{
	entry, f := engine.get(key)
	if f == true {
		if engine.wal.AddEntry(entry.Key, entry.Value, 1){
			return engine.put(key, entry.Value, 1)
		}
	}
	return false
}

func (engine *Engine) get(key string) (*WriteAheadLog.Entry, bool){
	//mem cache bloom summary index data
	var element = engine.memtable.Get(key)
	if element!=nil{
		x := WriteAheadLog.CreateEntry(element.Key, element.Value, Memtable.BoolToByte(element.Tombstone))
		engine.cache.Put(x)
		fmt.Println("u memtable/skiplist")
		return &x, true
	} else {
		var value = engine.cache.Get(key)
		if value!=nil{
			fmt.Println("u cache-u")
			x := WriteAheadLog.CreateEntry(key, value, 0)
			engine.cache.Put(x)
			return &x, true
		} else{
			var entry, f = engine.sstable.Find(key)
			if f!=false{
				fmt.Println("u sstable")
				engine.cache.Put(entry)
				return &entry, f
			}
		}
	}

	return nil, false
}