package replication

import (
	"sync"
	"time"

	"github.com/juju/errors"
)

var (
	ErrGetEventTimeout = errors.New("Get event timeout, try get later")
	ErrNeedSyncAgain   = errors.New("Last sync error or closed, try sync and get event again")
	ErrSyncClosed      = errors.New("Sync was closed")
)

type BinlogStreamer struct {
	ch  chan *BinlogEvent
	ech chan error
	sync.Mutex
	err error
}

func (s *BinlogStreamer) GetEventChannel() <-chan *BinlogEvent {
	return s.ch
}
func (s *BinlogStreamer) GetErrorChannel() <-chan error {
	return s.ech
}

func (s *BinlogStreamer) needSyncAgain() bool {
	s.Lock()
	defer s.Unlock()
	return s.err != nil
}
func (s *BinlogStreamer) GetEvent() (*BinlogEvent, error) {
	if s.needSyncAgain() {
		return nil, ErrNeedSyncAgain
	}

	select {
	case c := <-s.ch:
		return c, nil
	case err := <-s.ech:
		return nil, err
	}
}

// if timeout, ErrGetEventTimeout will returns
// timeout value won't be set too large, otherwise it may waste lots of memory
func (s *BinlogStreamer) GetEventTimeout(d time.Duration) (*BinlogEvent, error) {
	if s.needSyncAgain() {
		return nil, ErrNeedSyncAgain
	}

	select {
	case c := <-s.ch:
		return c, nil
	case err := <-s.ech:
		return nil, err
	case <-time.After(d):
		return nil, ErrGetEventTimeout
	}
}

func (s *BinlogStreamer) close() {
	s.closeWithError(ErrSyncClosed)
}

func (s *BinlogStreamer) closeWithError(err error) {
	if s.needSyncAgain() {
		err = ErrSyncClosed
	}
	s.Lock()
	s.err = err
	s.Unlock()
	select {
	case s.ech <- err:
	default:
	}
}

func newBinlogStreamer() *BinlogStreamer {
	s := new(BinlogStreamer)

	s.ch = make(chan *BinlogEvent)
	s.ech = make(chan error)

	return s
}
