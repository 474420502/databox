package databox

import (
	"fmt"
	"io"
	"os"

	"github.com/474420502/structure/search/treelist"
	"github.com/klauspost/compress/zstd"
)

type Wal struct {
	code     uint32 // 文件编号
	isLoaded bool   // 判断是否被加载

	logfile   *os.File
	logWriter WalWriter
}

type WalWriter interface {
	io.WriteCloser
	Flush() error
}

func (wal *Wal) Wrtie(data []byte) error {
	_, err := wal.logWriter.Write(data)
	if err != nil {
		return err
	}
	// err = wal.logWriter.Flush()
	// if err != nil {
	// 	return err
	// }
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

	w, err := zstd.NewWriter(f)
	if err != nil {
		panic(err)
	}

	b.tlist = treelist.New()
	b.wal = &Wal{
		code:      code,
		logfile:   f,
		logWriter: w,
	}

	return b
}

func (db *Block) Put(key, value []byte) bool {
	result := db.tlist.Put(key, value)
	if result {
		err := db.wal.Wrtie(newOperateLog(OT_PUT, key, value).Encode())
		if err != nil {
			panic(err)
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

func (db *Block) Load() {
	basefile := fmt.Sprintf("data.bas.%8d", db.wal.code)
	_, err := os.Stat(basefile)
	if os.IsExist(err) {
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
			db.tlist.Put(oplog.Key, oplog.Value)
		}
	}

	walfile := fmt.Sprintf("data.wal.%d", db.wal.code)
	_, err = os.Stat(walfile)
	if os.IsExist(err) {
		wf, err := os.OpenFile(basefile, os.O_APPEND, 0644)
		if err != nil {
			panic(err)
		}
		dec, err := zstd.NewReader(wf)
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
				db.tlist.Put(oplog.Key, oplog.Value)
			case OT_REMOVE:
				db.tlist.Remove(oplog.Key)
			case OT_REMOVE_RANGE:
				db.tlist.RemoveRange(oplog.Key, oplog.Value)
			}
		}
	} else {
		panic(err)
	}

	db.wal.isLoaded = true
}

func (db *Block) IsLoaded() bool {
	return db.wal.isLoaded
}
