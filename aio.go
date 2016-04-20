//+build linux

package goaio

import (
	"errors"
	"os"
	"sync"
	"syscall"
	"unsafe"
)

const (
	maxRequests     int   = 16
	defaultPriority int16 = 1

	iocb_cmd_pread  uint16 = 0
	iocb_cmd_pwrite uint16 = 1
	iocb_cmd_fsync  uint16 = 2
	iocb_cmd_fdsync uint16 = 3
	iocb_cmd_poll   uint16 = 5
)

var (
	ErrInitFail          = errors.New("Vailed to initailize AIO context")
	ErrNotInit           = errors.New("Not initialized")
	ErrDestroy           = errors.New("Failed to tear down context")
	ErrIoSubFail         = errors.New("Failed to submit new IO request")
	ErrInvalidBuffer     = errors.New("Invalid buffer")
	ErrWaitAllFailed     = errors.New("Failed to wait for all requests to complete")
	ErrNilEvent          = errors.New("The kernel returned a nil event result.  Fatal error")
	ErrNilCallback       = errors.New("The kernel returned a nil callback structure.  Fatal error")
	ErrUntrackedEventKey = errors.New("The kernel returned an event key we weren't tracking")
	ErrInvalidEventPtr   = errors.New("The kernel returned an invalid callback event pointer")
	ErrCompletion        = errors.New("The kernel failed to process all of the request")
	ErrWhatTheHell       = errors.New("A callback event occurred but no buffer was put into the pool")
	ErrNotFound          = errors.New("ID not found")
	ErrNotDone           = errors.New("Request not finished")

	zeroTime        timespec
	nonblockTimeout = timespec{
		sec:  0,
		nsec: 1,
	}
)

type RequestId uint
type aio_context uint

type activeEvent struct {
	data []byte
	cb   *aiocb
	id   RequestId
}

type timespec struct {
	sec  int
	nsec int
}

type requestState struct {
	cbKey *aiocb
	done  bool
}

type AIO struct {
	f   *os.File
	ctx aio_context
	cbp [](*aiocb)
	evt []event
	mtx *sync.Mutex
	end int64
	//tracker with keys and pointers to prevent GC taking our buffer
	active   map[*aiocb](activeEvent)
	avail    map[*aiocb]bool
	requests map[RequestId]*requestState
	reqId    RequestId
}

//NewAIO opens a file with the appropriate flags and permissions and positions the file index at the end of the file
func NewAIO(name string, flag int, perm os.FileMode) (*AIO, error) {
	var err error
	var ctx aio_context

	//try to open the file
	fio, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}

	//figure out what the end of the file is
	st, err := fio.Stat()
	if err != nil {
		fio.Close()
		return nil, err
	}
	end := st.Size()

	//get the context up and running
	_, _, errno := syscall.Syscall(syscall.SYS_IO_SETUP, uintptr(maxRequests), uintptr(unsafe.Pointer(&ctx)), 0)
	if errno != 0 {
		fio.Close()
		return nil, ErrInitFail
	}
	availPool := make(map[*aiocb]bool, maxRequests)
	evts := make([]event, maxRequests)
	cbp := make([](*aiocb), maxRequests)
	for i := range cbp {
		cbp[i] = &aiocb{
			fd:   uint32(fio.Fd()),
			prio: defaultPriority,
		}
		availPool[cbp[i]] = true
	}
	return &AIO{
		f:        fio,
		ctx:      ctx,
		cbp:      cbp,
		evt:      evts,
		mtx:      &sync.Mutex{},
		end:      end,
		active:   make(map[*aiocb](activeEvent), maxRequests),
		avail:    availPool,
		requests: make(map[RequestId]*requestState, 8),
	}, err
}

//Close up the aio object, waiting for all requests to finish first
func (a *AIO) Close() error {
	a.mtx.Lock()
	defer a.mtx.Unlock()
	if a.ctx == 0 || a.f == nil {
		return ErrNotInit
	}
	if err := a.waitAll(); err != nil {
		return err
	}
	_, _, errno := syscall.Syscall(syscall.SYS_IO_DESTROY, uintptr(a.ctx), 0, 0)
	a.ctx = 0
	if err := a.f.Close(); err != nil {
		return err
	}
	if errno == 0 {
		return nil
	}
	return ErrDestroy
}

//verifyResult checks that a retuned event is for a valid request
func (a *AIO) verifyResult(evnt event) error {
	if evnt.cb == nil {
		return ErrNilCallback
	}
	ae, ok := a.active[evnt.cb]
	if !ok {
		return ErrUntrackedEventKey
	}
	if ae.cb != evnt.cb {
		return ErrInvalidEventPtr
	}
	//ok, we have an active event returned and its one we are tracking
	//ensure it wrote our entire buffer
	if uint(len(ae.data)) != evnt.res {
		return ErrCompletion
	}
	//the result is all good, delete the item from the active list
	delete(a.active, evnt.cb)

	//put the pointer back into the available pool
	a.avail[evnt.cb] = true

	//update the state in the requests map
	r, ok := a.requests[ae.id]
	if !ok {
		return ErrNotFound
	}
	r.done = true

	return nil
}

//waitAll will block until all submitted requests are done
func (a *AIO) waitAll() error {
	for len(a.active) > 0 {
		if err := a.wait(zeroTime); err != nil {
			return err
		}
	}
	return nil
}

//wait until SOMETHING comes back
func (a *AIO) wait(to timespec) error {
	if len(a.active) == 0 {
		return nil
	}

	//wait for at least one active request to complete
	x, _, ret := syscall.Syscall6(syscall.SYS_IO_GETEVENTS, uintptr(a.ctx), uintptr(1), uintptr(len(a.active)), uintptr(unsafe.Pointer(&a.evt[0])), uintptr(unsafe.Pointer(&to)), uintptr(0))
	if ret != 0 {
		return errLookup(ret)
	}
	if x == uintptr(0) || x > uintptr(len(a.active)) {
		return ErrWaitAllFailed
	}
	var err error
	for i := uintptr(0); i < x; i++ {
		if e := a.verifyResult(a.evt[i]); e != nil {
			err = e
		}
	}
	return err
}

//submit sends a block of data out to be read or written
func (a *AIO) submit(cbp *aiocb) error {
	x, _, ret := syscall.Syscall(syscall.SYS_IO_SUBMIT, uintptr(a.ctx), 1, uintptr(unsafe.Pointer(&cbp)))
	if ret != 0 {
		errLookup(ret)
	}
	if x != 1 {
		return ErrIoSubFail
	}
	//add the request to our active set
	return nil
}

//Ready returns whether or not there is a callback buffer ready to go
//basically a check on whether or not we will block on a read/write attempt
func (a *AIO) Ready() bool {
	a.mtx.Lock()
	defer a.mtx.Unlock()
	if len(a.active) == len(a.cbp) {
		return false
	}
	return true
}

//Wait will block until there is an available request slot open
func (a *AIO) Wait() error {
	a.mtx.Lock()
	defer a.mtx.Unlock()
	if len(a.avail) > 0 {
		//if an available slot exists, return immediately
		return nil
	}
	return a.wait(zeroTime)
}

//WaitFor will block until the given RequestId is done
func (a *AIO) WaitFor(id RequestId) error {
	a.mtx.Lock()
	defer a.mtx.Unlock()
	for {
		r, ok := a.requests[id]
		if !ok {
			return ErrNotFound
		}
		if r.done {
			break
		}
		//wait for some completions
		if err := a.wait(zeroTime); err != nil {
			return err
		}
		//retry
	}
	return nil
}

//getNextReady will retrieve the next available callback pointer for use
//if no callback pointers are available, it blocks and waits for one
func (a *AIO) getNextReady() (*aiocb, error) {
	if len(a.avail) == 0 {
		if err := a.wait(zeroTime); err != nil {
			return nil, err
		}
	}
	for k, _ := range a.avail {
		return k, nil
	}
	return nil, ErrWhatTheHell
}

//Done asks if a request is done
func (a *AIO) Done(id RequestId) (bool, error) {
	a.mtx.Lock()
	defer a.mtx.Unlock()
	//service any ready events without blocking
	if err := a.wait(nonblockTimeout); err != nil {
		return false, err
	}

	//check if the value is in the available pool
	r, ok := a.requests[id]
	if !ok {
		return false, ErrNotFound
	}
	return r.done, nil
}

//Write will submit the bytes for writting at the end of the file,
//the buffer CANNOT change before the write completes, this is ASYNC!
func (a *AIO) Write(b []byte) (RequestId, error) {
	a.mtx.Lock()
	defer a.mtx.Unlock()
	id, err := a.writeAt(b, a.end)
	if err != nil {
		return 0, err
	}

	//calculate new offset for the end of the file
	a.end += int64(len(b))

	return id, nil
}

//WriteAt will write at a specific file offset
func (a *AIO) WriteAt(b []byte, offset int64) (RequestId, error) {
	a.mtx.Lock()
	defer a.mtx.Unlock()
	id, err := a.writeAt(b, offset)
	if err != nil {
		return 0, err
	}
	if (offset + int64(len(b))) > a.end {
		a.end = (offset + int64(len(b)))
	}
	return id, nil
}

func (a *AIO) writeAt(b []byte, offset int64) (RequestId, error) {
	if len(b) <= 0 {
		return 0, ErrInvalidBuffer
	}
	//go get the next available callback pointer
	cbp, err := a.getNextReady()
	if err != nil {
		return 0, err
	}
	cbp.offset = offset
	cbp.buffer = unsafe.Pointer(&b[0])
	cbp.nbytes = uint64(len(b))
	cbp.opcode = iocb_cmd_pwrite

	if err := a.submit(cbp); err != nil {
		return 0, err
	}
	a.reqId++
	//add the cb to the active event buffer
	a.active[cbp] = activeEvent{
		data: b, //this prevents the GC from collecting the buffer
		cb:   cbp,
		id:   a.reqId,
	}
	//remove the cb from the available pool
	delete(a.avail, cbp)

	a.requests[a.reqId] = &requestState{
		cbKey: cbp,
		done:  false,
	}

	return a.reqId, nil
}

//ReadAt reads data from the file at a specific offset
func (a *AIO) ReadAt(b []byte, offset int64) (RequestId, error) {
	a.mtx.Lock()
	defer a.mtx.Unlock()
	if len(b) <= 0 {
		return 0, ErrInvalidBuffer
	}
	//go get the next available callback pointer
	cbp, err := a.getNextReady()
	if err != nil {
		return 0, err
	}
	cbp.offset = offset
	cbp.buffer = unsafe.Pointer(&b[0])
	cbp.nbytes = uint64(len(b))
	cbp.opcode = iocb_cmd_pread

	if err := a.submit(cbp); err != nil {
		return 0, err
	}
	a.reqId++
	//add the cb to the active event buffer
	a.active[cbp] = activeEvent{
		data: b, //this prevents the GC from collecting the buffer
		cb:   cbp,
		id:   a.reqId,
	}
	//remove the cb from the available pool
	delete(a.avail, cbp)

	a.requests[a.reqId] = &requestState{
		cbKey: cbp,
		done:  false,
	}

	return a.reqId, nil
}

//Ack acknowledges that we have accepted a finished result ID
//if the request is not done, an error is returned
func (a *AIO) Ack(id RequestId) error {
	a.mtx.Lock()
	defer a.mtx.Unlock()
	st, ok := a.requests[id]
	if !ok {
		return ErrNotFound
	}
	if st.done {
		delete(a.requests, id)
		return nil
	}
	return ErrNotDone
}

func errLookup(errno syscall.Errno) error {
	return errors.New(errno.Error())
}
