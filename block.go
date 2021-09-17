package databox

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"

	"github.com/474420502/structure/compare"
	"github.com/474420502/structure/search/treelist"
)

type BlockWriter interface {
	io.Writer
	Flush() error
}

type Block struct {
	file   *os.File
	writer BlockWriter

	name string
	dir  string

	key []byte

	isSync bool
	isLoad bool

	data *treelist.Tree
}

func NewBlock(dir, name string) *Block {
	if dir[len(dir)-1] != '/' {
		dir += "/"
	}
	b := &Block{dir: dir, name: name, data: treelist.New()}

	walfile := fmt.Sprintf("%sbox.wal.%s", b.dir, b.name)
	f, err := os.OpenFile(walfile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
	b.file = f
	b.writer = bufio.NewWriter(f)

	runtime.SetFinalizer(b, func(block *Block) {
		var err error
		err = block.writer.Flush()
		if err != nil {
			panic(err)
		}
		err = block.file.Close()
		if err != nil {
			panic(err)
		}
	})

	return b
}

func NewBlockSync(dir, name string) *Block {
	if dir[len(dir)-1] != '/' {
		dir += "/"
	}
	b := &Block{dir: dir, name: name, isSync: true, data: treelist.New()}

	walfile := fmt.Sprintf("%sbox.wal.%s", b.dir, b.name)
	f, err := os.OpenFile(walfile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}

	b.file = f

	runtime.SetFinalizer(b, func(block *Block) {
		var err error
		err = block.writer.Flush()
		if err != nil {
			panic(err)
		}
		err = block.file.Close()
		if err != nil {
			panic(err)
		}
	})

	return b
}

func (block *Block) Size() int64 {
	if !block.isLoad {
		block.load()
	}
	return block.data.Size()
}

func (block *Block) Put(key []byte, value []byte) bool {
	if !block.isLoad {
		block.load()
	}

	result := block.data.Put(key, value)
	if result {
		_, err := block.file.Write(NewOperate(OT_Cover, key, value).EncodeBytes())
		if err != nil {
			panic(err)
		}
		if compare.Bytes(block.key, key) < 0 {
			block.key = key
		}
	}
	return result
}

func (block *Block) Cover(key []byte, value []byte) {
	_, err := block.file.Write(NewOperate(OT_Cover, key, value).EncodeBytes())
	if err != nil {
		panic(err)
	}
	if compare.Bytes(block.key, key) < 0 {
		block.key = key
	}
	if block.isLoad {
		block.data.Cover(key, value)
	}
}

func (block *Block) load() {
	var err error

	basefile := fmt.Sprintf("%sbox.bas.%s", block.dir, block.name)
	_, err = os.Stat(basefile)
	if err == nil {
		bf, err := os.Open(basefile)
		if err != nil {
			panic(err)
		}
		log.Println(bf)
	}

	walfile := fmt.Sprintf("%sbox.wal.%s", block.dir, block.name)
	_, err = os.Stat(walfile)
	if os.IsNotExist(err) {
		panic(err)
	}

	wf, err := os.Open(walfile)
	if err != nil {
		panic(err)
	}

	block.isLoad = true

	var op Operate
	var count int
	for {

		if err = op.Decode(wf); err != nil {
			if err == io.EOF {
				break
			} else {
				panic(err)
			}
		}
		count++
		switch op.Type {
		case OT_Cover:
			block.data.Cover(op.Key(), op.Value())
		case OT_REMOVE:
			block.data.Remove(op.Key())
		case OT_REMOVE_RANGE:
			block.data.RemoveRange(op.Key(), op.Value())
		default:
			panic(fmt.Errorf("no this type %d", op.Type))
		}
	}

}
