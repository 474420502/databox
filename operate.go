package databox

import (
	"bytes"
	"encoding/binary"
	"io"
)

type OperateType int8

const (
	OT_PUT          OperateType = 10
	OT_REMOVE       OperateType = 20
	OT_REMOVE_RANGE OperateType = 21
)

type OperateHead struct {
	HeadSign  byte
	OpType    OperateType
	KeySize   uint32
	ValueSize uint32
	EndSign   byte
}

type OperateLog struct {
	OperateHead
	Slice
}

func newOperateLog(opType OperateType, key, value []byte) *OperateLog {
	olog := &OperateLog{}

	olog.HeadSign = '\x02'
	olog.OpType = opType
	olog.KeySize = uint32(len(key))
	olog.ValueSize = uint32(len(value))
	olog.EndSign = '\x01'
	olog.Key = key
	olog.Value = value

	return olog
}

func newOperateLogFromReader(reader io.Reader) (*OperateLog, error) {
	olog := &OperateLog{}

	err := olog.Decode(reader)
	if err != nil {
		return nil, err
	}

	return olog, nil
}

func (log *OperateLog) Decode(reader io.Reader) error {
	var err error
	err = binary.Read(reader, binary.BigEndian, &log.OperateHead)
	if err != nil {
		return err
	}

	if log.OperateHead.HeadSign == '\x02' && log.OperateHead.EndSign == '\x01' {
		kvsize := log.OperateHead.KeySize + log.OperateHead.ValueSize
		var kvbuf []byte = make([]byte, kvsize)
		err = binary.Read(reader, binary.BigEndian, kvbuf)
		if err != nil {
			panic(err)
		}
		log.Key = kvbuf[0:log.OperateHead.KeySize]
		log.Value = kvbuf[log.OperateHead.KeySize:]
	} else {
		panic("数据格式错误")
	}

	return nil
}

func (log *OperateLog) Encode() []byte {
	var buf bytes.Buffer
	var err error

	err = binary.Write(&buf, binary.BigEndian, log.OperateHead)
	if err != nil {
		panic(err)
	}
	err = binary.Write(&buf, binary.BigEndian, log.Key)
	if err != nil {
		panic(err)
	}
	err = binary.Write(&buf, binary.BigEndian, log.Value)
	if err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func (log *OperateLog) Type() OperateType {
	return log.OpType
}
