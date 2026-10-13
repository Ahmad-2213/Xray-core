package xdrive

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/xtls/xray-core/common/errors"
)

const (
	maxCoalescedTicks = 8

	segSuffix = ".seg"
	endSuffix = ".end"
	errSuffix = ".err"

	maxAdaptiveSegmentBytes = 4 * 1024 * 1024

	probeSafetyNetInterval = time.Second
	probeHotWindow         = 2 * time.Second
	lookaheadMinGap        = probeSafetyNetInterval / 2
)

func objectName(prefix string, seq int64, suffix string) string {
	return fmt.Sprintf("%s/%09d%s", prefix, seq, suffix)
}

func parseEntry(name string) (int64, bool) {
	dot := strings.LastIndexByte(name, '.')
	if dot < 0 {
		return 0, false
	}
	switch name[dot:] {
	case segSuffix, endSuffix, errSuffix:
	default:
		return 0, false
	}
	seq, err := strconv.ParseInt(name[:dot], 10, 64)
	if err != nil || seq < 0 {
		return 0, false
	}
	return seq, true
}

type walWriter struct {
	ctx     context.Context
	storage Storage
	prefix  string
	params
	sem chan struct{}
	wg  sync.WaitGroup

	mu       sync.Mutex
	buf      []byte
	seq      int64
	segBytes int
	lastSize int
	held     int
	closed   bool
	err      error
}

func newWALWriter(ctx context.Context, storage Storage, prefix string, p params) *walWriter {
	w := &walWriter{
		ctx:      ctx,
		storage:  storage,
		prefix:   prefix,
		params:   p,
		segBytes: p.segmentBytes,
		sem:      make(chan struct{}, p.concurrency),
	}
	go w.flushLoop()
	return w
}

func (w *walWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.err != nil {
		return 0, w.err
	}
	if w.closed {
		return 0, io.ErrClosedPipe
	}

	w.buf = append(w.buf, p...)
	for len(w.buf) >= w.segBytes {
		if err := w.flushLocked(); err != nil {
			return 0, err
		}
	}
	return len(p), nil
}

func (w *walWriter) flushLoop() {
	ticker := time.NewTicker(w.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-w.ctx.Done():
			return
		case <-ticker.C:
			w.mu.Lock()
			if !w.closed && w.err == nil && len(w.buf) > 0 && w.readyToFlush() {
				w.flushLocked()
			}
			done := w.closed || w.err != nil
			w.mu.Unlock()
			if done {
				return
			}
		}
	}
}

func (w *walWriter) readyToFlush() bool {
	grew := len(w.buf) > w.lastSize
	fresh := w.lastSize == 0
	w.lastSize = len(w.buf)

	if grew && !fresh && w.held < maxCoalescedTicks {
		w.held++
		return false
	}
	w.held = 0
	return true
}

func (w *walWriter) flushLocked() error {
	if len(w.buf) == 0 {
		return nil
	}
	if w.err != nil {
		return w.err
	}

	n := len(w.buf)
	if n > w.segBytes {
		n = w.segBytes
	}

	chunk := make([]byte, n)
	copy(chunk, w.buf[:n])
	seq := w.seq
	w.seq++

	if n == len(w.buf) {
		w.buf = w.buf[:0]
	} else {
		w.buf = append(w.buf[:0], w.buf[n:]...)
	}
	w.lastSize = len(w.buf)
	w.held = 0

	limit := maxAdaptiveSegmentBytes
	if maxSegmentBytes < limit {
		limit = maxSegmentBytes
	}
	if n == w.segBytes && w.segBytes < limit {
		if g, ok := w.storage.(interface{ shouldGrow() bool }); ok && g.shouldGrow() {
			if m := int(float64(w.segBytes) * w.growth); m > w.segBytes {
				w.segBytes = m
				if w.segBytes > limit {
					w.segBytes = limit
				}
			}
		}
	}

	w.wg.Add(1)
	w.mu.Unlock()
	select {
	case w.sem <- struct{}{}:
	case <-w.ctx.Done():
		w.mu.Lock()
		w.wg.Done()
		return w.ctx.Err()
	}
	go w.upload(seq, chunk)
	w.mu.Lock()
	return nil
}

func (w *walWriter) upload(seq int64, chunk []byte) {
	defer w.wg.Done()
	defer func() { <-w.sem }()

	if err := w.storage.Put(w.ctx, objectName(w.prefix, seq, segSuffix), chunk); err != nil {
		w.mu.Lock()
		if w.err == nil {
			w.err = errors.New("failed to store segment").Base(err)
		}
		w.mu.Unlock()
		w.storage.Put(w.ctx, objectName(w.prefix, seq, errSuffix), nil)
	}
}

func (w *walWriter) Close() error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil
	}
	w.closed = true
	for len(w.buf) > 0 && w.err == nil {
		w.flushLocked()
	}
	w.mu.Unlock()

	w.wg.Wait()

	w.mu.Lock()
	err, seq := w.err, w.seq
	w.mu.Unlock()

	if err != nil {
		return err
	}
	return w.storage.Put(w.ctx, objectName(w.prefix, seq, endSuffix), nil)
}

type walReader struct {
	ctx     context.Context
	storage Storage
	prefix  string
	params
	seq int64

	ch        chan []byte
	discards  chan string
	wake      chan struct{}
	holeSince time.Time
	missSince time.Time
	lastHit   time.Time

	errMu sync.Mutex
	err   error
}

func newWALReader(ctx context.Context, storage Storage, prefix string, p params) *walReader {
	r := &walReader{
		ctx:      ctx,
		storage:  storage,
		prefix:   prefix,
		params:   p,
		ch:       make(chan []byte, p.concurrency),
		discards: make(chan string, 4*p.concurrency),
		wake:     make(chan struct{}, 1),
	}
	go r.run()
	go r.discardLoop()
	return r
}

func (r *walReader) Wake() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

func (r *walReader) run() {
	defer close(r.ch)

	delay := r.minPollInterval
	active := time.Now()
	for {
		polled := time.Now()
		advanced, eof, err := r.poll()
		if err != nil {
			r.setErr(err)
			return
		}
		if eof {
			return
		}

		switch {
		case advanced:
			active = time.Now()
			delay = r.minPollInterval
		case time.Since(active) < r.eagerWindow:
			delay = r.minPollInterval
		default:
			delay *= 2
			if delay > r.maxPollInterval {
				delay = r.maxPollInterval
			}
		}

		timer := time.NewTimer(delay)
		select {
		case <-r.ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		case <-r.wake:
			timer.Stop()
			active = time.Now()
			delay = r.minPollInterval
			if rest := r.minPollInterval - time.Since(polled); rest > 0 {
				select {
				case <-r.ctx.Done():
					return
				case <-time.After(rest):
				}
			}
		}
	}
}

func (r *walReader) poll() (advanced, eof bool, err error) {
	var listed []Entry
	probed := false
	if p, ok := r.storage.(interface {
		probe(context.Context, string, int64) ([]Entry, error)
	}); ok {
		probed = true
		var perr error
		listed, perr = p.probe(r.ctx, r.prefix, r.seq)
		if perr == nil {
			r.missSince = time.Time{}
			r.lastHit = time.Now()
		} else if perr != errNotFound {
			return false, false, perr
		} else if time.Since(r.lastHit) < probeHotWindow {
			return false, false, nil
		} else if r.missSince.IsZero() {
			r.missSince = time.Now()
		} else if time.Since(r.missSince) >= probeSafetyNetInterval {
			r.missSince = time.Now()
			listed, err = r.storage.List(r.ctx, r.prefix)
			if err != nil {
				return false, false, err
			}
		}
	}
	if !probed {
		listed, err = r.storage.List(r.ctx, r.prefix)
		if err != nil {
			return false, false, err
		}
	}
	if len(listed) == 0 {
		return false, false, nil
	}

	pending := make(map[int64]Entry, len(listed))
	ahead := false
	for _, entry := range listed {
		seq, ok := parseEntry(entry.Name)
		if !ok {
			continue
		}
		pending[seq] = entry
		if seq > r.seq {
			ahead = true
		}
	}

	if _, ok := pending[r.seq]; !ok && ahead {
		if r.holeSince.IsZero() {
			r.holeSince = time.Now()
		} else if time.Since(r.holeSince) >= r.holeTimeout {
			return false, false, errors.New("segment ", r.seq,
				" never arrived while later ones did, the peer lost it")
		}
	} else {
		r.holeSince = time.Time{}
	}

	outstanding := make(map[int64]chan struct {
		chunk []byte
		err   error
	})
	var look chan struct {
		listed []Entry
		err    error
	}
	var lookTimer <-chan time.Time
	for {
		if e, ok := pending[r.seq]; ok {
			if strings.HasSuffix(e.Name, errSuffix) {
				r.discard(r.prefix + "/" + e.Name)
				return advanced, false, errors.New("the peer could not store segment ", r.seq)
			}
			if strings.HasSuffix(e.Name, endSuffix) && len(outstanding) == 0 {
				r.discard(r.prefix + "/" + e.Name)
				return advanced, true, nil
			}
		}

		for i := 0; len(outstanding) < r.concurrency; i++ {
			seq := r.seq + int64(i)
			e, ok := pending[seq]
			if !ok || !strings.HasSuffix(e.Name, segSuffix) {
				break
			}
			if _, ok := outstanding[seq]; ok {
				continue
			}
			ch := make(chan struct {
				chunk []byte
				err   error
			}, 1)
			outstanding[seq] = ch
			if e.Inline != nil {
				ch <- struct {
					chunk []byte
					err   error
				}{e.Inline, nil}
				continue
			}
			go func(name string, ch chan struct {
				chunk []byte
				err   error
			}) {
				chunk, err := r.storage.Get(r.ctx, r.prefix+"/"+name)
				ch <- struct {
					chunk []byte
					err   error
				}{chunk, err}
			}(e.Name, ch)
		}

		if len(outstanding) == 0 && look == nil {
			return advanced, false, nil
		}
		if look == nil && lookTimer == nil {
			fetch := func() ([]Entry, error) {
				return r.storage.List(r.ctx, r.prefix)
			}
			if p, ok := r.storage.(interface {
				probe(context.Context, string, int64) ([]Entry, error)
			}); ok {
				seq := r.seq
				fetch = func() ([]Entry, error) {
					return p.probe(r.ctx, r.prefix, seq)
				}
			}
			ch := make(chan struct {
				listed []Entry
				err    error
			}, 1)
			go func() {
				listed, err := fetch()
				ch <- struct {
					listed []Entry
					err    error
				}{listed, err}
			}()
			look = ch
		}

		var head chan struct {
			chunk []byte
			err   error
		}
		if ch, ok := outstanding[r.seq]; ok {
			head = ch
		}
		select {
		case res := <-head:
			delete(outstanding, r.seq)
			if res.err != nil {
				if res.err == errNotFound {
					return advanced, false, nil
				}
				return advanced, false, res.err
			}
			name := pending[r.seq].Name
			select {
			case r.ch <- res.chunk:
			case <-r.ctx.Done():
				return true, true, nil
			}
			r.seq++
			advanced = true
			r.discard(r.prefix + "/" + name)
		case res := <-look:
			look = nil
			if res.err != nil && res.err != errNotFound {
				return false, false, res.err
			}
			fresh := false
			for _, entry := range res.listed {
				if seq, ok := parseEntry(entry.Name); ok && seq >= r.seq {
					if _, ok := pending[seq]; !ok {
						pending[seq] = entry
						fresh = true
					}
				}
			}
			for seq := range pending {
				if seq < r.seq {
					delete(pending, seq)
				}
			}
			if !fresh {
				gap := r.minPollInterval
				if gap < lookaheadMinGap {
					gap = lookaheadMinGap
				}
				lookTimer = time.After(gap)
			}
		case <-lookTimer:
			lookTimer = nil
		case <-r.ctx.Done():
			return advanced, false, r.ctx.Err()
		}
	}
}

func (r *walReader) discard(name string) {
	select {
	case r.discards <- name:
	default:
	}
}

func (r *walReader) discardLoop() {
	var wg sync.WaitGroup
	defer wg.Wait()

	sem := make(chan struct{}, r.concurrency)
	for {
		select {
		case <-r.ctx.Done():
			return
		case name := <-r.discards:
			sem <- struct{}{}
			wg.Add(1)
			go func(name string) {
				defer wg.Done()
				defer func() { <-sem }()
				r.storage.Delete(r.ctx, name)
			}(name)
		}
	}
}

func (r *walReader) setErr(err error) {
	r.errMu.Lock()
	defer r.errMu.Unlock()
	if r.err == nil {
		r.err = err
	}
}

func (r *walReader) Err() error {
	r.errMu.Lock()
	defer r.errMu.Unlock()
	if r.err != nil {
		return r.err
	}
	return io.EOF
}
