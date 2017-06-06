package goaio

import (
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"
	"unsafe"
)

const (
	testFile              string = `/dev/shm/test.bin`
	testBuffSize                 = 12345
	brutalTestWorkerCount        = 32
	brutalRequestCount           = 4096
	waitAnyWriteCount            = defaultQueueDepth
	workerBlockSize              = 2 * MB

	KB = 1024
	MB = 1024 * KB
	GB = 1024 * MB
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

func TestFlush(t *testing.T) {
	bb := make([]byte, testBuffSize)
	a, err := NewAIO(testFile, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		t.Fatal(err)
	}
	for i := range bb {
		bb[i] = 0xab
	}

	id, err := a.Write(bb)
	if err != nil {
		t.Fatal(err)
	}
	n, err := a.WaitFor(id)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(bb) {
		t.Fatal("Short byte count")
	}

	if err := a.Flush(); err != nil {
		t.Fatal(err)
	}

	if err := a.Close(); err != nil {
		t.Fatal(err)
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
	n, err := a.WaitFor(checkID)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(bb) {
		t.Fatal("Short byte count")
	}
	for i := range bb {
		bb[i] = 0
	}
	checkID, err = a.ReadAt(bb, 0)
	if err != nil {
		t.Fatal(err)
	}
	n, err = a.WaitFor(checkID)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(bb) {
		t.Fatal("Short byte count")
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

func TestShortRead(t *testing.T) {
	testData := []byte("HELLO MY FRIENDS")
	if err := ioutil.WriteFile(testFile, testData, 0600); err != nil {
		t.Fatal(err)
	}

	bb := make([]byte, testBuffSize)
	a, err := NewAIO(testFile, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		t.Fatal(err)
	}

	checkID, err := a.ReadAt(bb, 0)
	if err != nil {
		t.Fatal(err)
	}
	n, err := a.WaitFor(checkID)
	if err != nil {
		t.Fatal(err)
	}

	if n != len(testData) {
		t.Fatal("Failed on short read")
	}

	if err := a.Close(); err != nil {
		t.Fatal(err)
	}

}

func writer(a *AIO, errChan chan error, reqChan chan int64) {
	bb := make([]byte, workerBlockSize)
	for i := range bb {
		bb[i] = byte(i % 255)
	}
	for req := range reqChan {
		checkID, err := a.WriteAt(bb, req)
		if err != nil {
			errChan <- err
			return
		}
		n, err := a.WaitFor(checkID)
		if err != nil {
			errChan <- err
			return
		}
		if n != len(bb) {
			errChan <- errors.New("Short byte count")
			return
		}
	}
	errChan <- nil
}

func reader(a *AIO, errChan chan error, reqChan chan int64) {
	bb := make([]byte, workerBlockSize)
	for req := range reqChan {
		checkID, err := a.ReadAt(bb, req)
		if err != nil {
			errChan <- err
			return
		}
		n, err := a.WaitFor(checkID)
		if err != nil {
			errChan <- err
			return
		}
		if n != len(bb) {
			errChan <- errors.New("Short byte count")
			return
		}
	}
	errChan <- nil
}

func TestBrutal(t *testing.T) {
	bb := make([]byte, 32*MB)
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
	n, err := a.WaitFor(checkID)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(bb) {
		t.Fatal("Short byte count")
	}
	errChan := make(chan error, 8)
	reqChan := make(chan int64, brutalTestWorkerCount)
	for i := 0; i < brutalTestWorkerCount; i++ {
		if (i & 0x1) == 0 {
			go reader(a, errChan, reqChan)
		} else {
			go writer(a, errChan, reqChan)
		}
	}

	for i := 0; i < brutalRequestCount; i++ {
		//check on errors
		select {
		case err := <-errChan:
			t.Fatal(err)
		case reqChan <- rand.Int63n((32 * MB) - workerBlockSize):
		}
	}
	close(reqChan)
	for i := 0; i < brutalTestWorkerCount; i++ {
		if err := <-errChan; err != nil {
			t.Fatal(err)
		}
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	clean(t)
}

func removeIdFromList(id RequestId, ids []RequestId) []RequestId {
	for i := range ids {
		if ids[i] == id {
			ids[i] = ids[len(ids)-1]
			return ids[:len(ids)-1]
		}
	}
	return ids
}

func TestWaitAny(t *testing.T) {
	var ids []RequestId
	bb := make([]byte, MB)
	a, err := NewAIO(testFile, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		t.Fatal(err)
	}
	for i := range bb {
		bb[i] = byte(i & 0xff)
	}

	for i := 0; i < waitAnyWriteCount; i++ {
		checkID, err := a.Write(bb)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, checkID)
	}

	toWait := len(ids)
	waitIdList := make([]RequestId, len(ids))
	for toWait > 0 {
		n, err := a.WaitAny(waitIdList)
		if err != nil {
			t.Fatal(err)
		}
		if n > toWait {
			t.Fatal("More returned than possible on wait any")
		}
		toWait -= n
		for i := 0; i < n; i++ {
			ids = removeIdFromList(waitIdList[i], ids)
		}
	}
	if len(ids) > 0 {
		t.Fatal("not all ids were completed by WaitAny")
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	clean(t)
}

func TestWaitAnyShortList(t *testing.T) {
	var ids []RequestId
	bb := make([]byte, MB)
	a, err := NewAIO(testFile, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		t.Fatal(err)
	}
	for i := range bb {
		bb[i] = byte(i & 0xff)
	}

	for i := 0; i < waitAnyWriteCount; i++ {
		checkID, err := a.Write(bb)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, checkID)
	}

	toWait := len(ids)
	waitIdList := make([]RequestId, 4)
	for toWait > 0 {
		n, err := a.WaitAny(waitIdList)
		if err != nil {
			t.Fatal(err)
		}
		if n > toWait {
			t.Fatal("More returned than possible on wait any")
		}
		toWait -= n
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	clean(t)
}

func writeBigFile(t *testing.T, sz int) {
	bb := make([]byte, sz)
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
	n, err := a.WaitFor(checkID)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(bb) {
		t.Fatal("Short byte count")
	}

	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
}

func readBigFile(t *testing.T, sz int) {
	bb := make([]byte, sz)
	a, err := NewAIO(testFile, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		t.Fatal(err)
	}
	checkID, err := a.ReadAt(bb, 0)
	if err != nil {
		t.Fatal(err)
	}
	n, err := a.WaitFor(checkID)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(bb) {
		t.Fatal("Short byte count")
	}
	for i := range bb {
		if bb[i] != 0xab {
			t.Fatal(fmt.Errorf("invalid file content: %x != 0xab", bb[i]))
		}
	}

	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
}
func TestBig(t *testing.T) {
	if testing.Short() {
		return
	}
	writeBigFile(t, 8*MB)
	readBigFile(t, 8*MB)
	clean(t)
}

func TestBigger(t *testing.T) {
	if testing.Short() {
		return
	}
	writeBigFile(t, 128*MB)
	readBigFile(t, 128*MB)
	clean(t)
}

func TestBiggest(t *testing.T) {
	if testing.Short() {
		return
	}
	writeBigFile(t, 1*GB)
	readBigFile(t, 1*GB)
	clean(t)
}

func TestClean(t *testing.T) {
	clean(t)
}

func clean(t *testing.T) {
	if err := os.RemoveAll(testFile); err != nil {
		t.Fatal(err)
	}
}

func Example() {
	bb := make([]byte, testBuffSize)
	a, err := NewAIO(testFile, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		fmt.Println("Failed to create AIO", err)
	}
	for i := range bb {
		bb[i] = 0xab
	}

	var requests []RequestId
	//kick off 16 requests to write to the file
	//this should generate a file of testBuffSize*16
	for i := 0; i < 16; i++ {
		r, err := a.Write(bb)
		if err != nil {
			fmt.Println("Failure to issue write", err)
			return
		}
		requests = append(requests, r)
	}

	//wait for the first 8 requests to finish in order
	for i := 0; i < 8; i++ {
		if _, err := a.WaitFor(requests[i]); err != nil {
			fmt.Println("Failed waiting for write request", err)
			return
		}
	}

	//wait for the next 8 requests to finish in any order
	//this is a batch wait, several can come back
	completed := make([]RequestId, 8)
	toComplete := 8
	for toComplete > 0 {
		n, err := a.WaitAny(completed)
		if err != nil {
			fmt.Println("Failed to wait for any write requests", err)
			return
		}
		toComplete -= n
	}

	//flush the file handle the AIO for good measure
	if err := a.Flush(); err != nil {
		fmt.Println("Failed to sync AIO")
	}

	if err := a.Close(); err != nil {
		fmt.Println("Failed to close AIO")
	}
}
