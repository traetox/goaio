//+build linux
//+build i386 arm mips

package goaio

import (
	"unsafe"
)

type aiocb struct {
	data   unsafe.Pointer
	pad    uint32
	key    uint64
	opcode uint16
	prio   int16
	fd     uint32
	buffer unsafe.Pointer
	pad2   uint32
	nbytes uint64
	offset int64
	_res   uint64
	flags  uint32
	resfd  uint32
}

type event struct {
	data unsafe.Pointer
	pad  uint32
	cb   *aiocb
	pad2 uint32
	res  int
	pad3 uint32
	res2 int
	pad4 uint32
}
