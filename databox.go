package databox

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"syscall"

	"github.com/474420502/structure/search/treelist"
)

type DataBox struct {
	dir        string
	lockedfile *os.File

	code uint32

	blockskey *Block

	blocks *treelist.Tree
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

	db := &DataBox{blocks: treelist.New(), dir: dir, lockedfile: lockedfile}

	matches, err := filepath.Glob(dir + "blockinfo.wal.*")
	if err != nil {
		panic(err)
	}

	if len(matches) == 0 {
		// db.codeInfo = NewBlockSync(0, "blockinfo", dir)
	}

	sort.Slice(matches, func(i, j int) bool {
		return matches[i] > matches[j]
	})

	// for _, match := range matches {
	// 	code, err := strconv.Atoi(match[strings.LastIndexByte(match, '.')+1:])
	// 	if err != nil {
	// 		log.Println("blockinfo list:", match)
	// 		panic(err)
	// 	}
	// 	db.codeInfo = NewBlockSync(uint32(code), "blockinfo", dir)
	// 	db.codeInfo.Load()

	// 	if db.codeInfo.tlist.Size() != 0 {
	// 		var maxcode uint32
	// 		db.codeInfo.tlist.Traverse(func(s *treelist.Slice) bool {

	// 			var binfo = &BlockInfo{}
	// 			var buf = NewBuffer(s.Value.([]byte))
	// 			err = binfo.Decode(buf)

	// 			switch err {
	// 			case io.ErrUnexpectedEOF:
	// 				return false
	// 			case nil:
	// 			default:
	// 				panic(err)
	// 			}

	// 			db.blockList.Cover(binfo.Key, NewBlock(binfo.Code, "data", dir))
	// 			if maxcode < binfo.Code {
	// 				maxcode = binfo.Code
	// 			}
	// 			return true
	// 		})
	// 		db.code = maxcode + 1
	// 	}

	// 	break
	// }

	return db, nil
}

// func updateBlockListInfo(codeInfo *Block, code uint32, key []byte) {
// 	var buf = NewBuffer(nil)
// 	var bi = &BlockInfo{Code: code, Key: key}
// 	buf.BinaryMustWrite(binary.BigEndian, bi.Encode())
// 	var codebuf []byte = make([]byte, 4)
// 	binary.BigEndian.PutUint32(codebuf, code)
// 	codeInfo.Cover(codebuf, buf.Bytes())
// }

// func (db *DataBox) Put(key, value []byte) bool {
// 	if db.blockList.Size() == 0 {
// 		b := NewBlock(db.code, "data", db.dir)
// 		if !db.blockList.Put(key, NewBlock(db.code, "data", db.dir)) {
// 			log.Panicf("label:data code:%d 文件存在", db.code)
// 		}

// 		if !b.Put(key, value) {
// 			log.Panicf("label:data code:%d 文件存在", db.code)
// 		}

// 		updateBlockListInfo(db.codeInfo, db.code, key)
// 		db.code++

// 		return true
// 	}

// 	iter := db.blockList.Iterator()
// 	iter.SeekForNext(key)
// 	if !iter.Valid() {
// 		s := db.blockList.Tail()
// 		b := s.Value.(*Block)
// 		s.Key = key
// 		result := b.Put(key, value)
// 		if result {
// 			updateBlockListInfo(db.codeInfo, b.Code(), key)
// 		}
// 		return result
// 	}
// 	b := iter.Value().(*Block)
// 	return b.Put(key, value)
// }

// func (db *DataBox) Cover(key, value []byte) bool {

// 	if db.blockList.Size() == 0 {
// 		b := NewBlock(db.code, "data", db.dir)
// 		if !db.blockList.Put(key, NewBlock(db.code, "data", db.dir)) {
// 			log.Panicf("label:data code:%d 文件存在", db.code)
// 		}

// 		if !b.Put(key, value) {
// 			log.Panicf("label:data code:%d 文件存在", db.code)
// 		}

// 		updateBlockListInfo(db.codeInfo, db.code, key)
// 		db.code++
// 		return true
// 	}

// 	iter := db.blockList.Iterator()
// 	iter.SeekForNext(key)
// 	if !iter.Valid() {
// 		s := db.blockList.Tail()
// 		b := s.Value.(*Block)
// 		s.Key = key
// 		result := b.Cover(key, value)
// 		updateBlockListInfo(db.codeInfo, b.Code(), key)
// 		return result
// 	}

// 	b := iter.Value().(*Block)
// 	return b.Cover(key, value)
// }
