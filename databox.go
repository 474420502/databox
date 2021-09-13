package databox

import (
	"encoding/binary"
	"fmt"
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
	directory  string
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

	db := &DataBox{blockList: treelist.New(), directory: dir, lockedfile: lockedfile}

	matches, err := filepath.Glob(dir + "blockinfo.wal.*")
	if err != nil {
		panic(err)
	}

	if len(matches) == 0 {
		db.codeInfo = NewBlock(0, "blockinfo", dir)
	}

	sort.Slice(matches, func(i, j int) bool {
		return matches[i] > matches[j]
	})

	for _, match := range matches {
		code, err := strconv.Atoi(match[strings.IndexByte(match, '.')+1:])
		if err != nil {
			log.Println("blockinfo list:", match)
			panic(err)
		}
		db.codeInfo = NewBlock(uint32(code), "blockinfo", dir)
		db.codeInfo.Load()
		var maxcode uint32
		db.codeInfo.tlist.Traverse(func(s *treelist.Slice) bool {
			binfo := s.Value.(*BlockInfo)
			if maxcode < binfo.Code {
				maxcode = binfo.Code
			}
			return true
		})
		db.code = maxcode + 1
		break
	}

	// runtime.SetFinalizer(db, func(db *DataBox) {

	// 	db.blockList.Traverse(func(s *treelist.Slice) bool {
	// 		b := s.Value.(*Block)

	// 		if b.wal.logWriter != nil {
	// 			err := b.wal.logWriter.Flush()
	// 			log.Printf("%d flush", b.Code())
	// 			if err != nil {
	// 				log.Println(err)
	// 			}

	// 			err = b.wal.logWriter.Close()
	// 			if err != nil {
	// 				log.Println(err)
	// 			}
	// 		}

	// 		if b.wal.logfile != nil {
	// 			err := b.wal.logfile.Close()
	// 			if err != nil {
	// 				log.Println(err)
	// 			}
	// 		}
	// 		return true
	// 	})

	// })

	return db, nil
}

type BlockInfo struct {
	Code uint32
}

func (db *DataBox) Put(key, value []byte) bool {

	if db.blockList.Size() == 0 {
		b := NewBlock(db.code, "data", db.directory)
		db.blockList.Put(key, b)

		binfo := BlockInfo{Code: db.code}
		var buf = NewBuffer()
		buf.BinaryMustWrite(binary.BigEndian, binfo)

		db.codeInfo.Put(key, buf.Bytes())
		db.code++
		return b.Put(key, value)
	}

	iter := db.blockList.Iterator()
	iter.SeekForNext(key)
	if !iter.Valid() {
		s := db.blockList.Tail()
		b := s.Value.(*Block)
		s.Key = key
		return b.Put(key, value)
	}

	b := iter.Value().(*Block)
	return b.Put(key, value)
}
