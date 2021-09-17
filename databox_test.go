package databox

import (
	"encoding/gob"
	"log"
	"os"
	"strconv"
	"testing"

	"github.com/474420502/structure/search/treelist"
)

func TestCase(t *testing.T) {
	tree := treelist.New()
	for i := 0; i < 100; i += 2 {
		v := strconv.Itoa(i)
		tree.Put([]byte(v), v)
	}

	var result []string
	tree.Traverse(func(s *treelist.Slice) bool {
		result = append(result, string(s.Key))
		return true
	})

	log.Println(result)

	iter := tree.Iterator()
	iter.SeekForNext([]byte(strconv.Itoa(35)))
	log.Println(string(iter.Key()))
}

var testdict map[int]int = func() map[int]int {
	d := make(map[int]int)
	f, err := os.Open("testdict.gob")
	if err != nil {
		return d
	}
	err = gob.NewDecoder(f).Decode(&d)
	if err != nil {
		panic(err)
	}
	return d
}()

// func TestPut2(t *testing.T) {
// 	// os.Remove("wal.data/data.wal.00000000")
// 	// os.Remove("wal.data/blockinfo.wal.00000000")

// 	db, err := Open("wal.data")
// 	if err != nil {
// 		panic(err)
// 	}

// 	// var blocksize int64 = 0
// 	// db.blockList.Traverse(func(s *treelist.Slice) bool {
// 	// 	b := s.Value.(*Block)
// 	// 	b.Load()
// 	// 	blocksize += b.tlist.Size()
// 	// 	return true
// 	// })

// 	// //197539116549934702
// 	// db.codeInfo.tlist.Traverse(func(s *treelist.Slice) bool {
// 	// 	bi := &BlockInfo{}
// 	// 	bi.Decode(NewBuffer(s.Value.([]byte)))
// 	// 	log.Println(string(bi.Key))
// 	// 	return true
// 	// })

// 	// log.Println(blocksize, len(testdict))

// 	r := random.New()
// 	now := time.Now()
// 	for i := 0; i < 100000; i++ {
// 		v := r.Intn(1000000000000000000) + 100
// 		k := []byte(strconv.Itoa(v))
// 		db.Cover(k, k)
// 		testdict[v] = v
// 	}
// 	log.Println(time.Since(now).Milliseconds())

// 	f, err := os.OpenFile("testdict.gob", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
// 	if err != nil {
// 		panic(err)
// 	}
// 	err = gob.NewEncoder(f).Encode(testdict)
// 	if err != nil {
// 		panic(nil)
// 	}
// }

func testdictsave() {
	f, err := os.OpenFile("testdict.gob", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		panic(err)
	}

	err = gob.NewEncoder(f).Encode(testdict)
	if err != nil {
		panic(nil)
	}
}
