package xdrive

import (
	"context"
	"os"
	"sync"
	"time"

	"github.com/xtls/xray-core/common/net"
)

type slotConn struct {
	ctx     context.Context
	cancel  context.CancelFunc
	writer  *slotWriter
	reader  *slotReader
	onClose func()

	readBuf []byte

	deadlineMu    sync.Mutex
	readDeadline  time.Time
	writeDeadline time.Time

	closeOnce sync.Once
	closeErr  error
}

func newSlotConn(ctx context.Context, store slotStore, table slotAnnounce, p params, onClose func()) *slotConn {
	ctx, cancel := context.WithCancel(ctx)
	reader := newSlotReader(ctx, store, table.Downlink, p)
	writer := newSlotWriter(ctx, store, table.Uplink, p)
	writer.setWake(reader.Wake)
	return &slotConn{
		ctx:     ctx,
		cancel:  cancel,
		writer:  writer,
		reader:  reader,
		onClose: onClose,
	}
}

func (c *slotConn) Read(b []byte) (int, error) {
	if len(c.readBuf) == 0 {
		deadline := c.getDeadline(true)
		var data []byte
		var ok bool
		if deadline.IsZero() {
			data, ok = <-c.reader.ch
		} else {
			if !time.Now().Before(deadline) {
				return 0, os.ErrDeadlineExceeded
			}
			timer := time.NewTimer(time.Until(deadline))
			select {
			case data, ok = <-c.reader.ch:
				timer.Stop()
			case <-timer.C:
				return 0, os.ErrDeadlineExceeded
			}
		}
		if !ok {
			return 0, c.reader.Err()
		}
		c.readBuf = data
	}
	n := copy(b, c.readBuf)
	c.readBuf = c.readBuf[n:]
	return n, nil
}

func (c *slotConn) Write(b []byte) (int, error) {
	if deadline := c.getDeadline(false); !deadline.IsZero() && !time.Now().Before(deadline) {
		return 0, os.ErrDeadlineExceeded
	}
	return c.writer.Write(b)
}

func (c *slotConn) Close() error {
	c.closeOnce.Do(func() {
		c.closeErr = c.writer.Close()
		c.cancel()
		if c.onClose != nil {
			c.onClose()
		}
	})
	return c.closeErr
}

func (c *slotConn) LocalAddr() net.Addr  { return placeholderAddr }
func (c *slotConn) RemoteAddr() net.Addr { return placeholderAddr }

func (c *slotConn) getDeadline(read bool) time.Time {
	c.deadlineMu.Lock()
	defer c.deadlineMu.Unlock()
	if read {
		return c.readDeadline
	}
	return c.writeDeadline
}

func (c *slotConn) SetDeadline(t time.Time) error {
	c.deadlineMu.Lock()
	defer c.deadlineMu.Unlock()
	c.readDeadline = t
	c.writeDeadline = t
	return nil
}

func (c *slotConn) SetReadDeadline(t time.Time) error {
	c.deadlineMu.Lock()
	defer c.deadlineMu.Unlock()
	c.readDeadline = t
	return nil
}

func (c *slotConn) SetWriteDeadline(t time.Time) error {
	c.deadlineMu.Lock()
	defer c.deadlineMu.Unlock()
	c.writeDeadline = t
	return nil
}
