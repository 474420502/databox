package databox

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"time"
)

type OperateType uint8

const (
	OT_Cover OperateType = 10

	OT_REMOVE       OperateType = 20
	OT_REMOVE_RANGE OperateType = 21
)

type OperateHead struct {
	SignHead byte

	TimeStamp int64

	Type     OperateType
	KeyLen   uint32
	ValueLen uint32

	SignTail byte
}

type Operate struct {
	OperateHead
	key   []byte
	value []byte
}

func NewOperate(optype OperateType, Key, Value []byte) *Operate {
	op := Operate{}
	op.SignHead = '\x02'
	op.SignTail = '\x01'

	op.Type = optype
	op.KeyLen = uint32(len(Key))
	op.ValueLen = uint32(len(Value))
	op.SetData(Key, Value)
	return &op
}

func (op *Operate) Key() []byte {
	return op.key
}

func (op *Operate) Value() []byte {
	return op.value
}

func (op *Operate) SetKey(key []byte) {
	if cap(op.key) >= len(key) {
		op.key = op.key[0:len(key)]
	} else {
		op.key = make([]byte, len(key))
	}
	copy(op.key, key)
	op.KeyLen = uint32(len(op.key))
}

func (op *Operate) SetValue(value []byte) {
	if cap(op.value) >= len(value) {
		op.value = op.value[0:len(value)]
	} else {
		op.value = make([]byte, len(value))
	}
	copy(op.value, value)
	op.ValueLen = uint32(len(value))
}

func (op *Operate) SetData(key, value []byte) {
	op.SetKey(key)
	op.SetValue(value)
}

func (op *Operate) Encode(writer io.Writer) error {
	var buf bytes.Buffer

	op.OperateHead.TimeStamp = time.Now().UnixNano()

	err := binary.Write(&buf, binary.BigEndian, op.OperateHead)
	if err != nil {
		panic(err)
	}

	err = binary.Write(&buf, binary.BigEndian, op.key)
	if err != nil {
		panic(err)
	}

	err = binary.Write(&buf, binary.BigEndian, op.value)
	if err != nil {
		panic(err)
	}

	_, err = buf.WriteTo(writer)
	if err != nil {
		return err
	}
	return nil
}

func (op *Operate) EncodeBytes() []byte {
	var buf bytes.Buffer

	op.OperateHead.TimeStamp = time.Now().UnixNano()

	err := binary.Write(&buf, binary.BigEndian, op.OperateHead)
	if err != nil {
		panic(err)
	}

	err = binary.Write(&buf, binary.BigEndian, op.key)
	if err != nil {
		panic(err)
	}

	err = binary.Write(&buf, binary.BigEndian, op.value)
	if err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func (op *Operate) Decode(reader io.Reader) error {
	var err error

	err = binary.Read(reader, binary.BigEndian, &op.OperateHead)
	if err != nil {
		return err
	}

	if op.SignHead != '\x02' || op.SignTail != '\x01' {
		return fmt.Errorf("数据格式错误 %v", op.OperateHead)

	}

	if cap(op.key) >= int(op.KeyLen) {
		op.key = op.key[0:op.KeyLen]
	} else {
		op.key = make([]byte, op.KeyLen)
	}
	err = binary.Read(reader, binary.BigEndian, &op.key)
	if err != nil {
		return err
	}

	if cap(op.value) >= int(op.ValueLen) {
		op.value = op.value[0:op.ValueLen]
	} else {
		op.value = make([]byte, op.ValueLen)
	}
	err = binary.Read(reader, binary.BigEndian, &op.value)
	if err != nil {
		return err
	}

	return nil
}

func (op *Operate) DecodeBytes(data []byte) error {
	var err error

	reader := bytes.NewBuffer(data)

	err = binary.Read(reader, binary.BigEndian, &op.OperateHead)
	if err != nil {
		return err
	}

	if op.SignHead != '\x02' || op.SignTail != '\x01' {
		return fmt.Errorf("数据格式错误 %v", op.OperateHead)

	}

	if cap(op.key) >= int(op.KeyLen) {
		op.key = op.key[0:op.KeyLen]
	} else {
		op.key = make([]byte, op.KeyLen)
	}
	err = binary.Read(reader, binary.BigEndian, op.key)
	if err != nil {
		return err
	}

	if cap(op.value) >= int(op.ValueLen) {
		op.value = op.value[0:op.ValueLen]
	} else {
		op.value = make([]byte, op.ValueLen)
	}
	err = binary.Read(reader, binary.BigEndian, op.value)
	if err != nil {
		return err
	}

	return nil
}
