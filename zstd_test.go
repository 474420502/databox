package databox

import (
	"encoding/binary"
	"log"
	"os"
	"testing"

	"github.com/klauspost/compress/zstd"
)

func TestZstdWrite(t *testing.T) {
	// 	r := random.New()
	// 	fpath := "wal.data/test.wal.dat"

	// 	f, err := os.OpenFile(fpath, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	// 	if err != nil {
	// 		panic(err)
	// 	}

	// 	w := bufio.NewWriter(f)

	// 	for i := 0; i < 10; i++ {
	// 		v := strconv.Itoa(r.Intn(1000000000) + 100)
	// 		var l OperateLog = OperateLog{
	// 			OperateHead: OperateHead{
	// 				HeadSign: '\x02',
	// 				EndSign:  '\x01',
	// 			},
	// 			Slice: Slice{
	// 				Key:   []byte(v),
	// 				Value: []byte(v),
	// 			},
	// 		}
	// 		var buf = NewBuffer(nil)
	// 		buf.BinaryMustWrite(binary.BigEndian, &l)
	// 		w.Write(buf.Bytes())
	// 		w.Flush()
	// 	}

	// 	w.Close()
}

func TestZstdRead(t *testing.T) {

	fpath := "wal.data/test.wal.dat"
	f, err := os.OpenFile(fpath, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		panic(err)
	}

	reader, err := zstd.NewReader(f)
	if err != nil {
		panic(err)
	}

	for {

		var buf = NewBufferFromReader(reader)
		var l OperateLog
		buf.BinaryMustRead(binary.BigEndian, l)
		log.Println(l)
	}

}
