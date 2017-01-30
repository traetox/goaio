//+build linux

package goaio

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"syscall"
	"unsafe"
)

const (
	maxQueueDepth     int   = 256
	defaultQueueDepth int   = 16
	defaultPriority   int16 = 1

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
	ErrInvalidQueueDepth = errors.New("Invalid queue depth")

	zeroTime        timespec
	nonblockTimeout = timespec{
		sec:  0,
		nsec: 1,
	}
)

type RequestId uint
type aio_context uint

type activeEvent struct {
	data    []byte
	written uint
	cb      *aiocb
	id      RequestId
}

type timespec struct {
	sec  int
	nsec int
}

type requestState struct {
	cbKey     *aiocb
	done      bool
	err       error
	byteCount int
}

type AIO struct {
	f    *os.File
	ctx  aio_context
	cbp  [](*aiocb)
	evt  []event
	dmtx *sync.Mutex //the mutex protecting data
	wmtx *sync.Mutex //the mutex protecting blocking syscalls, like wait
	end  int64
	//tracker with keys and pointers to prevent GC taking our buffer
	active   map[*aiocb](*activeEvent)
	avail    map[*aiocb]bool
	requests map[RequestId]*requestState
	reqId    RequestId
}

type AIOExtConfig struct {
	QueueDepth int
}

//Create is shorthand for NewAIO(name, O_RDWR|O_CREATE|O_TRUNC, 0660)
func Create(name string) (*AIO, error) {
	return NewAIO(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0660)
}

//Open is shorthand for NewAIO(name, O_RDONLY, 0)
func Open(name string) (*AIO, error) {
	return NewAIO(name, os.O_RDONLY, 0)
}

//NewAIO opens a file with the appropriate flags and permissions and positions the file index at the end of the file
func NewAIO(name string, flag int, perm os.FileMode) (*AIO, error) {
	var cfg AIOExtConfig
	if err := fixupConfig(&cfg); err != nil {
		return nil, err
	}
	return NewAIOExt(name, cfg, flag, perm)
}

//NewAIOExt opens a file with the appropriate flags and permissions and positions the file index at the end of the file with additional configuration options
func NewAIOExt(name string, cfg AIOExtConfig, flag int, perm os.FileMode) (*AIO, error) {
	var err error
	var ctx aio_context

	if err := fixupConfig(&cfg); err != nil {
		return nil, err
	}

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
	_, _, errno := syscall.Syscall(syscall.SYS_IO_SETUP, uintptr(cfg.QueueDepth), uintptr(unsafe.Pointer(&ctx)), 0)
	if errno != 0 {
		fio.Close()
		return nil, ErrInitFail
	}
	availPool := make(map[*aiocb]bool, cfg.QueueDepth)
	evts := make([]event, cfg.QueueDepth)
	cbp := make([](*aiocb), cfg.QueueDepth)
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
		dmtx:     &sync.Mutex{},
		wmtx:     &sync.Mutex{},
		end:      end,
		active:   make(map[*aiocb](*activeEvent), cfg.QueueDepth),
		avail:    availPool,
		requests: make(map[RequestId]*requestState, 8),
		reqId:    1, //ID should start counting at 1 so callers can use 0 as "not in use"
	}, err
}

//Close up the aio object, waiting for all requests to finish first
func (a *AIO) Close() error {
	a.dmtx.Lock()
	if a.ctx == 0 || a.f == nil {
		a.dmtx.Unlock()
		return ErrNotInit
	}
	a.dmtx.Unlock()
	a.wmtx.Lock()
	defer a.wmtx.Unlock()
	if err := a.waitAll(); err != nil {
		return err
	}
	_, _, errno := syscall.Syscall(syscall.SYS_IO_DESTROY, uintptr(a.ctx), 0, 0)
	a.ctx = 0
	if err := a.f.Close(); err != nil {
		return err
	}
	a.f = nil
	if errno == 0 {
		return nil
	}
	return ErrDestroy
}

//resubmit puts a request back into the kernel
//this is done when a partial read or write occurs
func (a *AIO) resubmit(ae *activeEvent) error {
	//double check we are not about to roll outside our buffer
	if ae.written >= uint(len(ae.data)) {
		return ErrCompletion
	}
	toProcess := uint(len(ae.data)) - ae.written
	ae.cb.offset = ae.cb.offset + int64(ae.written)
	ae.cb.buffer = unsafe.Pointer(&ae.data[ae.written])
	ae.cb.nbytes = uint64(toProcess)
	return a.submit(ae.cb)
}

//remove an active event and return its callback and io_event to the available pool
func (a *AIO) freeEvent(ae *activeEvent, cb *aiocb, errno int) error {
	//help out the GC  abit
	ae.data = nil
	delete(a.active, cb)

	//put the pointer back into the available pool
	a.avail[cb] = true

	//update the state in the requests map
	r, ok := a.requests[ae.id]
	if !ok {
		return ErrNotFound
	}
	r.done = true
	r.byteCount = int(ae.written)
	if errno < 0 {
		r.err = lookupErrNo(errno)
	}
	return nil
}

//verifyResult checks that a retuned event is for a valid request
func (a *AIO) verifyResult(evnt event, compLen *int, completed []RequestId) error {
	a.dmtx.Lock()
	defer a.dmtx.Unlock()
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
	if evnt.res < 0 {
		//an error occured with this event, remove the active event and set
		//the event error code
		return a.freeEvent(ae, evnt.cb, evnt.res)
	}
	//ok, we have an active event returned and its one we are tracking
	//ensure it wrote our entire buffer.  res is > 0 at this point
	if evnt.res > 0 && uint(len(ae.data)) != (uint(evnt.res)+ae.written) {
		ae.written += uint(evnt.res)
		if err := a.resubmit(ae); err != nil {
			return err
		}
		return nil //chunk went back in, so don't clear anything
	}
	ae.written += uint(evnt.res)

	//the result is all good, increment the compLen and delete the item from the active list
	if *compLen < len(completed) {
		completed[*compLen] = ae.id
	}
	(*compLen)++
	return a.freeEvent(ae, evnt.cb, 0)
}

//waitAll will block until all submitted requests are done
func (a *AIO) waitAll() error {
	for len(a.active) > 0 {
		if _, err := a.wait(zeroTime, nil); err != nil {
			return err
		}
	}
	return nil
}

//wait until SOMETHING comes back
func (a *AIO) wait(to timespec, completed []RequestId) (int, error) {
	var compLen int
	if len(a.active) == 0 {
		return 0, nil
	}

	//wait for at least one active request to complete
	x, _, ret := syscall.Syscall6(syscall.SYS_IO_GETEVENTS, uintptr(a.ctx), uintptr(1), uintptr(len(a.active)), uintptr(unsafe.Pointer(&a.evt[0])), uintptr(unsafe.Pointer(&to)), uintptr(0))
	if ret != 0 {
		return 0, errLookup(ret)
	}
	if x == uintptr(0) || x > uintptr(len(a.active)) {
		return 0, ErrWaitAllFailed
	}
	var err error
	for i := uintptr(0); i < x; i++ {
		//we pass in our completed slice and the length pointer to be populated
		if e := a.verifyResult(a.evt[i], &compLen, completed); e != nil {
			if err == nil {
				err = e
			}
		}
	}
	return compLen, err
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
	return nil
}

//Ready returns whether or not there is a callback buffer ready to go
//basically a check on whether or not we will block on a read/write attempt
func (a *AIO) Ready() bool {
	a.dmtx.Lock()
	defer a.dmtx.Unlock()
	if len(a.active) == len(a.cbp) {
		return false
	}
	return true
}

//Wait will block until there is an available request slot open
func (a *AIO) Wait() error {
	a.dmtx.Lock()
	l := len(a.avail)
	a.dmtx.Unlock()
	if l > 0 {
		//if an available slot exists, return immediately
		return nil
	}
	a.wmtx.Lock()
	_, err := a.wait(zeroTime, nil)
	a.wmtx.Unlock()
	return err
}

//WaitAny will block until a request finishes and will populate a
//buffer of request IDs with the items that finish, returning the completion
//count and a potential error.  If there are no outstanding requests it will
//return 0, nil
func (a *AIO) WaitAny(completed []RequestId) (int, error) {
	a.dmtx.Lock()
	l := len(a.active)
	a.dmtx.Unlock()
	if l == 0 {
		//no active requests, bail
		return 0, nil
	}
	a.wmtx.Lock()
	n, err := a.wait(zeroTime, completed)
	a.wmtx.Unlock()
	return n, err
}

func (a *AIO) idDone(id RequestId) (bool, error) {
	a.dmtx.Lock()
	defer a.dmtx.Unlock()
	r, ok := a.requests[id]
	if !ok {
		return false, ErrNotFound
	}
	return r.done, nil
}

//WaitFor will block until the given RequestId is done
func (a *AIO) WaitFor(id RequestId) (int, error) {
	for {
		//check if its ready
		done, err := a.idDone(id)
		if err != nil {
			return 0, err
		}
		if done {
			break
		}

		//wait for some completions
		a.wmtx.Lock()
		//once we grab the lock, we need to recheck
		done, err = a.idDone(id)
		if err != nil {
			a.wmtx.Unlock()
			return 0, err
		}
		if done {
			a.wmtx.Unlock()
			break
		}

		_, err = a.wait(zeroTime, nil)
		a.wmtx.Unlock()
		if err != nil {
			return 0, err
		}
		//retry
	}
	return a.ack(id)
}

//getNextReady will retrieve the next available callback pointer for use
//if no callback pointers are available, it blocks and waits for one
func (a *AIO) getNextReady() (*aiocb, error) {
	for {
		a.dmtx.Lock()
		for k, _ := range a.avail {
			//remove the cb from the available pool
			delete(a.avail, k)
			a.dmtx.Unlock()
			return k, nil
		}
		a.dmtx.Unlock()
		a.wmtx.Lock()
		_, err := a.wait(zeroTime, nil)
		a.wmtx.Unlock()
		if err != nil {
			return nil, err
		}
	}
	return nil, ErrWhatTheHell
}

//Write will submit the bytes for writting at the end of the file,
//the buffer CANNOT change before the write completes, this is ASYNC!
func (a *AIO) Write(b []byte) (RequestId, error) {
	id, err := a.writeAt(b, a.end)
	if err != nil {
		return 0, err
	}

	return id, nil
}

//WriteAt will write at a specific file offset
func (a *AIO) WriteAt(b []byte, offset int64) (RequestId, error) {
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

	a.dmtx.Lock()
	if err := a.submit(cbp); err != nil {
		a.avail[cbp] = true
		a.dmtx.Unlock()
		return 0, err
	}
	a.reqId++
	id := a.reqId

	//add the cb to the active event buffer
	a.active[cbp] = &activeEvent{
		data: b, //this prevents the GC from collecting the buffer
		cb:   cbp,
		id:   id,
	}

	a.requests[id] = &requestState{
		cbKey: cbp,
		done:  false,
	}

	if a.end < (offset + int64(len(b))) {
		//calculate new offset for the end of the file
		a.end += int64(len(b))
	}

	a.dmtx.Unlock()

	return id, nil
}

//ReadAt reads data from the file at a specific offset
func (a *AIO) ReadAt(b []byte, offset int64) (RequestId, error) {
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

	a.dmtx.Lock()
	if err := a.submit(cbp); err != nil {
		a.avail[cbp] = true
		//delete the request
		a.dmtx.Unlock()
		return 0, err
	}
	a.reqId++
	id := a.reqId

	//add the cb to the active event buffer
	a.active[cbp] = &activeEvent{
		data: b, //this prevents the GC from collecting the buffer
		cb:   cbp,
		id:   id,
	}

	a.requests[id] = &requestState{
		cbKey: cbp,
		done:  false,
	}

	a.dmtx.Unlock()

	return id, nil
}

//Ack acknowledges that we have accepted a finished result ID
//if the request is not done, an error is returned
func (a *AIO) ack(id RequestId) (int, error) {
	a.dmtx.Lock()
	defer a.dmtx.Unlock()
	st, ok := a.requests[id]
	if !ok {
		return 0, ErrNotFound
	}
	if st.done {
		err := st.err
		cnt := st.byteCount
		delete(a.requests, id)
		return cnt, err
	}
	return 0, ErrNotDone
}

//Flush will wait for all submitted jobs to finish and then flush
//the file descriptor.  Because the Linux kernel does not actually
//support Flush via the AIO interface we just issue a plain old flush
//via userland.  No async here.  Flush DOES NOT ack outstanding requests
func (a *AIO) Flush() error {
	//we want to hold the wait mutex throghout all of this
	//this ensures we have TOTAL exclusivity over the file IO
	a.wmtx.Lock()
	defer a.wmtx.Unlock()
	if err := a.waitAll(); err != nil {
		return err
	}
	return a.f.Sync()
}

//Truncate will wait for all submitted jobs to finish and then trunctate the
//file to the designated size.
func (a *AIO) Truncate(sz int64) error {
	//we want to hold the wait mutex throghout all of this
	//this ensures we have TOTAL exclusivity over the file IO
	a.wmtx.Lock()
	defer a.wmtx.Unlock()
	if err := a.waitAll(); err != nil {
		return err
	}
	return a.f.Truncate(sz)
}

//FD hands back the underlying *os.File pointer
//This is NOT A COPY, so do not do close or do anything
//crazy with it.  This is purely a convienence method, use
//at your own peril
func (a *AIO) FD() *os.File {
	return a.f
}

func errLookup(errno syscall.Errno) error {
	return errors.New(errno.Error())
}

//translate an error code to error
//TODO: actually populate this so the return is sane
func lookupErrNo(errno int) error {
	return fmt.Errorf("Error %d", errno)
}

func fixupConfig(cfg *AIOExtConfig) error {
	if cfg.QueueDepth <= 0 {
		cfg.QueueDepth = defaultQueueDepth
	}
	if cfg.QueueDepth >= maxQueueDepth {
		return ErrInvalidQueueDepth
	}
	return nil
}
