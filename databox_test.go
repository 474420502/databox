package databox

import (
	"log"
	"strconv"
	"testing"
	"time"

	"github.com/474420502/random"
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

func TestPut(t *testing.T) {
	db, err := Open("wal.data")
	if err != nil {
		panic(err)
	}

	r := random.New()
	now := time.Now()
	for i := 0; i < 100000; i++ {
		v := r.Intn(1000000000000000000) + 100
		k := []byte(strconv.Itoa(v))
		db.Put(k, k)
	}
	log.Println(time.Since(now).Milliseconds())
}
