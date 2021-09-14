package databox

import (
	"bufio"
	"fmt"
	"io"
	"os"

	"github.com/474420502/structure/search/treelist"
	"github.com/klauspost/compress/zstd"
)

type Wal struct {
	code uint32 // 文件编号

	dir   string // 路径
	label string // 文件标签 唯一

	isLoaded bool // 判断是否被加载
	isSync   bool

	logfile   *os.File
	logWriter WalWriter
}

type WalWriter interface {
	io.Writer
	Flush() error
}

func (wal *Wal) Wrtie(data []byte) error {
	_, err := wal.logWriter.Write(data)
	if err != nil {
		return err
	}
	return nil
}

type Slice struct {
	Key   []byte
	Value []byte
}

type Block struct {
	tlist *treelist.Tree
	wal   *Wal
}

func NewBlock(code uint32, label, dir string) *Block {
	b := &Block{}

	wfilename := fmt.Sprintf("%s%s.wal.%08d", dir, label, code)
	f, err := os.OpenFile(wfilename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)

	if err != nil {
		panic(fmt.Sprintf("%s\n. 创建 %s 失败", err, wfilename))
	}

	w := bufio.NewWriter(f)
	b.tlist = treelist.New()
	b.wal = &Wal{
		code:      code,
		dir:       dir,
		label:     label,
		logfile:   f,
		logWriter: w,
	}

	return b
}

func NewBlockSync(code uint32, label, dir string) *Block {
	block := NewBlock(code, label, dir)
	block.SetSync(true)
	return block
}

func (db *Block) PutCover(key, value []byte) bool {
	result := db.tlist.PutDuplicate(key, value, func(exists *treelist.Slice) {
		exists.Value = value
	})

	err := db.wal.Wrtie(newOperateLog(OT_PUT, key, value).Encode())
	if err != nil {
		panic(err)
	}
	if db.wal.isSync {
		err := db.wal.logWriter.Flush()
		if err != nil {
			panic(err)
		}
	}
	return result
}

func (db *Block) Put(key, value []byte) bool {
	result := db.tlist.Put(key, value)
	if result {
		err := db.wal.Wrtie(newOperateLog(OT_PUT, key, value).Encode())
		if err != nil {
			panic(err)
		}
		if db.wal.isSync {
			err := db.wal.logWriter.Flush()
			if err != nil {
				panic(err)
			}
		}
	}
	return result
}

func (db *Block) Remove(key []byte) bool {
	result := db.tlist.Remove(key) != nil
	if result {
		err := db.wal.Wrtie(newOperateLog(OT_REMOVE, key, nil).Encode())
		if err != nil {
			panic(err)
		}
	}
	return result
}

func (db *Block) RemoveRange(low, high []byte) bool {
	result := db.tlist.RemoveRange(low, high)
	if result {
		err := db.wal.Wrtie(newOperateLog(OT_REMOVE_RANGE, low, high).Encode())
		if err != nil {
			panic(err)
		}
	}
	return result
}

func (db *Block) Code() uint32 {
	return db.wal.code
}

func (db *Block) SetSync(is bool) {
	db.wal.isSync = is
}

func (db *Block) Load() {
	basefile := fmt.Sprintf("%s%s.bas.%08d", db.wal.dir, db.wal.label, db.wal.code)
	_, err := os.Stat(basefile)
	if err == nil {
		bf, err := os.OpenFile(basefile, os.O_RDWR, 0644)
		if err != nil {
			panic(err)
		}

		dec, err := zstd.NewReader(bf)
		if err != nil {
			panic(err)
		}
		var oplog *OperateLog = &OperateLog{}
		for {
			err = oplog.Decode(dec)
			if err != nil {
				if err == io.EOF {
					break
				} else {
					panic(err)
				}
			}
			db.tlist.PutDuplicate(oplog.Key, oplog.Value, func(exists *treelist.Slice) {
				exists.Value = oplog.Value
			})
		}
	}

	walfile := fmt.Sprintf("%s%s.wal.%08d", db.wal.dir, db.wal.label, db.wal.code)
	_, err = os.Stat(walfile)
	if os.IsNotExist(err) {
		panic(err)
	}

	wf, err := os.OpenFile(walfile, os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		panic(err)
	}

	dec := bufio.NewReader(wf)
	if err != nil {
		panic(err)
	}
	var oplog *OperateLog = &OperateLog{}
	for {

		err = oplog.Decode(dec)
		if err != nil {
			if err == io.EOF {
				break
			} else {
				panic(err)
			}
		}

		switch oplog.Type() {
		case OT_PUT:
			db.tlist.PutDuplicate(oplog.Key, oplog.Value, func(exists *treelist.Slice) {
				exists.Value = oplog.Value
			})
		case OT_REMOVE:
			db.tlist.Remove(oplog.Key)
		case OT_REMOVE_RANGE:
			db.tlist.RemoveRange(oplog.Key, oplog.Value)
		}
	}

	db.wal.isLoaded = true
}

func (db *Block) IsLoaded() bool {
	return db.wal.isLoaded
}
