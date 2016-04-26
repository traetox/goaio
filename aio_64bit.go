//+build linux
//+build amd64 arm64

package goaio

import (
	"unsafe"
)

type aiocb struct {
	data   unsafe.Pointer
	key    uint64
	opcode uint16
	prio   int16
	fd     uint32
	buffer unsafe.Pointer
	nbytes uint64
	offset int64
	_res   uint64
	flags  uint32
	resfd  uint32
}

type event struct {
	data unsafe.Pointer
	cb   *aiocb
	res  int
	res2 int
}
