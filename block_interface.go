package databox

type BlockValue interface {
	Encode() []byte
	Decode([]byte)
}
