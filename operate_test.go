package databox

import (
	"bufio"
	"bytes"
	"log"
	"testing"
)

func TestOp1(t *testing.T) {
	op1 := newOperateLog(OT_PUT, []byte("abc"), []byte("123"))
	op2 := newOperateLog(OT_PUT, []byte("def"), []byte("456"))

	var buf bytes.Buffer
	w := bufio.NewWriter(&buf)
	w.Write(op1.Encode())
	w.Write(op2.Encode())
	w.Flush()
	log.Println(buf.Bytes())
	l, err := newOperateLogFromReader(&buf)
	log.Println(l, err)
	l2, err := newOperateLogFromReader(&buf)
	log.Println(l2, err)
}
