package databox

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"

	"github.com/474420502/structure/search/treelist"
)

type DataBox struct {
	dir        string
	lockedfile *os.File

	code     uint32
	codeInfo *Block

	blockList *treelist.Tree
}

func Open(dir string) (*DataBox, error) {

	if dir[len(dir)-1] != '/' {
		dir += "/"
	}

	finfo, err := os.Stat(dir)
	if os.IsNotExist(err) {
		err = os.MkdirAll(dir, os.ModePerm)
		if err != nil {
			panic(err)
		}
	} else if err != nil {
		panic(err)
	} else if !finfo.IsDir() {
		panic("open is not directory")
	}

	lockedfile, err := os.Create(dir + "LOCK")
	if err != nil {
		panic(err)
	}

	err = syscall.Flock(int(lockedfile.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		panic(fmt.Errorf("%v LOCK file is locked", err))
	}

	db := &DataBox{blockList: treelist.New(), dir: dir, lockedfile: lockedfile}

	matches, err := filepath.Glob(dir + "blockinfo.wal.*")
	if err != nil {
		panic(err)
	}

	if len(matches) == 0 {
		db.codeInfo = NewBlockSync(0, "blockinfo", dir)
	}

	sort.Slice(matches, func(i, j int) bool {
		return matches[i] > matches[j]
	})

	for _, match := range matches {
		code, err := strconv.Atoi(match[strings.LastIndexByte(match, '.')+1:])
		if err != nil {
			log.Println("blockinfo list:", match)
			panic(err)
		}
		db.codeInfo = NewBlockSync(uint32(code), "blockinfo", dir)
		db.codeInfo.Load()

		if db.codeInfo.tlist.Size() != 0 {
			var maxcode uint32
			db.codeInfo.tlist.Traverse(func(s *treelist.Slice) bool {

				var binfo = &BlockInfo{}
				var buf = NewBuffer(s.Value.([]byte))
				err = binfo.Decode(buf)
				if err != nil {
					panic(err)
				}

				db.blockList.Put(binfo.Key, NewBlock(binfo.Code, "data", dir))
				if maxcode < binfo.Code {
					maxcode = binfo.Code
				}
				return true
			})
			db.code = maxcode + 1
		}

		break
	}

	return db, nil
}

type BlockInfo struct {
	Code uint32
	Key  []byte
}

func (bi *BlockInfo) Encode() []byte {
	var buf = NewBuffer(nil)
	buf.BinaryMustWrite(binary.BigEndian, byte('\x02'))
	buf.BinaryMustWrite(binary.BigEndian, bi.Code)
	buf.BinaryMustWrite(binary.BigEndian, uint32(len(bi.Key)))
	buf.BinaryMustWrite(binary.BigEndian, bi.Key)
	buf.BinaryMustWrite(binary.BigEndian, byte('\x01'))
	return buf.Bytes()
}

func (bi *BlockInfo) Decode(reader io.Reader) error {
	var err error
	var sign byte

	err = binary.Read(reader, binary.BigEndian, &sign)
	if err != nil {
		return err
	}

	if sign != '\x02' {
		return fmt.Errorf("数据格式错误")
	}

	err = binary.Read(reader, binary.BigEndian, &bi.Code)
	if err != nil {
		return err
	}
	var klen uint32
	err = binary.Read(reader, binary.BigEndian, &klen)
	if err != nil {
		return err
	}
	bi.Key = make([]byte, klen)
	err = binary.Read(reader, binary.BigEndian, &bi.Key)
	if err != nil {
		return err
	}
	err = binary.Read(reader, binary.BigEndian, &sign)
	if err != nil {
		return err
	}

	if sign != '\x01' {
		return fmt.Errorf("数据格式错误")
	}

	return nil
}

func updateBlockListInfo(codeInfo *Block, code uint32, key []byte) {
	var buf = NewBuffer(nil)
	var bi = &BlockInfo{Code: code, Key: key}
	buf.BinaryMustWrite(binary.BigEndian, bi.Encode())
	var codebuf []byte = make([]byte, 4)
	binary.BigEndian.PutUint32(codebuf, code)
	codeInfo.PutCover(codebuf, buf.Bytes())
}

func (db *DataBox) Put(key, value []byte) bool {
	if db.blockList.Size() == 0 {
		b := NewBlock(db.code, "data", db.dir)
		db.blockList.Put(key, NewBlock(db.code, "data", db.dir))
		result := b.Put(key, value)
		updateBlockListInfo(db.codeInfo, db.code, key)
		db.code++
		return result
	}

	iter := db.blockList.Iterator()
	iter.SeekForNext(key)
	if !iter.Valid() {
		s := db.blockList.Tail()
		b := s.Value.(*Block)
		s.Key = key
		result := b.Put(key, value)
		updateBlockListInfo(db.codeInfo, b.Code(), key)
		return result
	}

	b := iter.Value().(*Block)
	return b.Put(key, value)
}

func (db *DataBox) PutCover(key, value []byte) bool {

	if db.blockList.Size() == 0 {
		b := NewBlock(db.code, "data", db.dir)
		db.blockList.PutDuplicate(key, b, func(exists *treelist.Slice) {
			exists.Value = b
		})

		result := b.PutCover(key, value)
		updateBlockListInfo(db.codeInfo, db.code, key)
		db.code++
		return result
	}

	iter := db.blockList.Iterator()
	iter.SeekForNext(key)
	if !iter.Valid() {
		s := db.blockList.Tail()
		b := s.Value.(*Block)
		s.Key = key
		result := b.PutCover(key, value)
		updateBlockListInfo(db.codeInfo, b.Code(), key)
		return result
	}

	b := iter.Value().(*Block)
	return b.Put(key, value)
}
