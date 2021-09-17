package databox

import (
	"log"
	"strconv"
	"testing"

	"github.com/474420502/random"
)

var testdir = "wal.data"
var testname = "test00001"

func TestBlock(t *testing.T) {
	r := random.New()

	block := NewBlock(testdir, testname)
	log.Println(block.Size(), len(testdict))
	for i := 0; i < 100000; i++ {
		v := r.Intn(100000000000) + 1000
		sv := []byte(strconv.Itoa(v))
		block.Cover(sv, sv)
		testdict[v] = v
	}

	testdictsave()
}
