package databox

import (
	"bytes"
	"encoding/binary"
	"io"
)

type Buffer struct {
	*bytes.Buffer
}

func NewBuffer(buf []byte) *Buffer {
	return &Buffer{bytes.NewBuffer(buf)}
}

func NewBufferFromReader(reader io.Reader) *Buffer {
	buf := NewBuffer(nil)
	_, err := buf.ReadFrom(reader)
	if err != nil {
		panic(err)
	}
	return buf
}

func (buf *Buffer) BinaryWrite(order binary.ByteOrder, data interface{}) error {
	return binary.Write(buf, order, data)
}

func (buf *Buffer) BinaryMustWrite(order binary.ByteOrder, data interface{}) {
	err := binary.Write(buf, order, data)
	if err != nil {
		panic(err)
	}
}

func (buf *Buffer) BinaryRead(order binary.ByteOrder, data interface{}) error {
	return binary.Read(buf, order, data)
}

func (buf *Buffer) BinaryMustRead(order binary.ByteOrder, data interface{}) {
	err := binary.Read(buf, order, data)
	if err != nil {
		panic(err)
	}
}
