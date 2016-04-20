package goaio

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"
	"unsafe"
)

const (
	testFile     string = `/tmp/test.txt`
	testBuffSize        = 12345
)

func TestInfo(t *testing.T) {
	var cb aiocb
	var ev event
	if unsafe.Sizeof(cb) != 64 {
		t.Fatal(fmt.Sprintf("Invalid aio callback structure size: %d != %d", unsafe.Sizeof(cb), 64))
	}
	if unsafe.Sizeof(ev) != 32 {
		t.Fatal(fmt.Sprintf("Invalid event structure size", unsafe.Sizeof(ev), 32))
	}
}

func TestNew(t *testing.T) {
	a, err := NewAIO(testFile, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	clean(t)
}

func TestWrite(t *testing.T) {
	bb := make([]byte, testBuffSize)
	a, err := NewAIO(testFile, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		t.Fatal(err)
	}
	for i := range bb {
		bb[i] = 0xab
	}

	if _, err := a.Write(bb); err != nil {
		t.Fatal(err)
	}

	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	clean(t)
}

func TestWriteAt(t *testing.T) {
	bb := make([]byte, testBuffSize)
	a, err := NewAIO(testFile, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		t.Fatal(err)
	}
	for i := range bb {
		bb[i] = 0xab
	}

	if _, err := a.Write(bb); err != nil {
		t.Fatal(err)
	}

	if _, err := a.WriteAt(bb, 1000); err != nil {
		t.Fatal(err)
	}

	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	buff, err := ioutil.ReadFile(testFile)
	if err != nil {
		t.Fatal(err)
	}
	if len(buff) != (len(bb) + 1000) {
		t.Fatal("invalid file length")
	}

	clean(t)
}
func TestWriteMulti(t *testing.T) {
	bb := make([]byte, testBuffSize)
	a, err := NewAIO(testFile, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		t.Fatal(err)
	}
	for i := range bb {
		bb[i] = 0xab
	}

	if _, err := a.Write(bb); err != nil {
		t.Fatal(err)
	}

	for i := range bb {
		bb[i] = 0xde
	}

	if _, err := a.Write(bb); err != nil {
		t.Fatal(err)
	}

	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	buff, err := ioutil.ReadFile(testFile)
	if err != nil {
		t.Fatal(err)
	}
	if len(buff) != len(bb)*2 {
		t.Fatal("invalid file length")
	}
	for i := 0; i < len(bb); i++ {
		if buff[i] != 0xab || buff[len(bb)+i] != 0xde {
			t.Fatal("invalid file contents")
		}
	}
	clean(t)
}

func TestDone(t *testing.T) {
	bb := make([]byte, testBuffSize)
	a, err := NewAIO(testFile, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		t.Fatal(err)
	}
	for i := range bb {
		bb[i] = 0xab
	}
	checkID, err := a.Write(bb)
	if err != nil {
		t.Fatal(err)
	}
	_, err = a.Done(checkID)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	done, err := a.Done(checkID)
	if err != nil {
		t.Fatal(err)
	}
	if !done {
		t.Fatal("not done")
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	clean(t)
}

func TestRead(t *testing.T) {
	bb := make([]byte, testBuffSize)
	a, err := NewAIO(testFile, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		t.Fatal(err)
	}
	for i := range bb {
		bb[i] = 0xab
	}
	checkID, err := a.Write(bb)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.WaitFor(checkID); err != nil {
		t.Fatal(err)
	}
	if err := a.Ack(checkID); err != nil {
		t.Fatal(err)
	}
	for i := range bb {
		bb[i] = 0
	}
	checkID, err = a.ReadAt(bb, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.WaitFor(checkID); err != nil {
		t.Fatal(err)
	}
	if err := a.Ack(checkID); err != nil {
		t.Fatal(err)
	}
	for i := range bb {
		if bb[i] != 0xab {
			t.Fatal("Invalid value in read")
		}
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestClean(t *testing.T) {
	clean(t)
}

func clean(t *testing.T) {
	if err := os.RemoveAll(testFile); err != nil {
		t.Fatal(err)
	}
}