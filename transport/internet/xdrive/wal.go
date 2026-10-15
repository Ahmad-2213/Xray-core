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
	lookaheadMinGap        = 100 * time.Millisecond

	maxSubmitSpacing = 500 * time.Millisecond

	// inlineRampSegments is how many full segments the writer emits at the
	// current ramp size before doubling it. Eight 16 KiB segments is 128 KiB,
	// which covers a TLS handshake and a request with room to spare.
	inlineRampSegments = 8
)

var maxPartialInflight = 4

var errPeerStore = errors.New("the peer could not store the segment")
var errHoleTimeout = errors.New("a segment never arrived while later ones did")

var readerErrorGracePeriod = 30 * time.Second

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
	partial  int
	closed   bool
	err      error

	upAvg       time.Duration
	upN         int
	lastSubmit  time.Time
	submitEvery time.Duration
	wakeReader  func()
	ramped      int

	maxBufSize int // maximum buffer size before forced flush
}

func (w *walWriter) setReaderWake(fn func()) {
	w.mu.Lock()
	w.wakeReader = fn
	w.mu.Unlock()
}

func newWALWriter(ctx context.Context, storage Storage, prefix string, p params) *walWriter {
	w := &walWriter{
		ctx:        ctx,
		storage:    storage,
		prefix:     prefix,
		params:     p,
		segBytes:   p.segmentBytes,
		sem:        make(chan struct{}, p.concurrency),
		maxBufSize: p.concurrency * p.segmentBytes, // force flush after enough data
	}
	// A connection being set up is latency bound, not throughput bound, and
	// a Drive inline create is about a second cheaper than a multipart one.
	// So the first segments of a session are emitted at the inline size and
	// then doubled up to the configured size. Bulk traffic walks out of the
	// ramp within a second; a session that only carries small writes stays
	// small, which is the right size for it anyway.
	if start := rampStart(p.segmentBytes); start < p.segmentBytes {
		w.segBytes = start
	}
	go w.flushLoop()
	return w
}

// rampStart is the size the writer opens a session with, capped at what the
// storage can store without a media body.
func rampStart(segmentBytes int) int {
	if driveInlineLimit < segmentBytes {
		return driveInlineLimit
	}
	return segmentBytes
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
				if len(w.buf) >= w.segBytes || w.partial < maxPartialInflight {
					w.flushLocked()
				}
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
	// This function must be called with w.mu held.
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
	part := n < w.segBytes

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

	// Walk out of the latency ramp. Only full segments count, so a burst of
	// small interactive writes does not push the writer into the bulk size
	// before the session has actually started moving bulk data.
	if n == w.segBytes && w.segBytes < w.segmentBytes {
		w.ramped++
		if w.ramped >= inlineRampSegments {
			w.ramped = 0
			m := w.segBytes * 2
			if m > w.segmentBytes {
				m = w.segmentBytes
			}
			w.segBytes = m
		}
	}

	if part {
		w.partial++
	}

	w.wg.Add(1)

	// A full segment's upload takes one Drive create, roughly the same for
	// every segment. Releasing all the slots at once therefore makes them
	// finish as one cluster, and the reader then drains that cluster and
	// waits. Spacing submissions by upload/parallelism keeps the same
	// throughput but staggers the completions into a steady flow. Partials
	// carry interactive latency and are never paced.
	var pace time.Duration
	if !part && w.submitEvery > 0 && !w.lastSubmit.IsZero() {
		if d := w.submitEvery - time.Since(w.lastSubmit); d > 0 {
			pace = d
		}
	}

	w.mu.Unlock()

	if pace > 0 {
		timer := time.NewTimer(pace)
		select {
		case <-w.ctx.Done():
			timer.Stop()
			w.mu.Lock()
			if part {
				w.partial--
			}
			w.wg.Done()
			return w.ctx.Err()
		case <-timer.C:
		}
	}

	select {
	case w.sem <- struct{}{}:
	case <-w.ctx.Done():
		w.mu.Lock()
		if part {
			w.partial--
		}
		w.wg.Done()
		return w.ctx.Err()
	}

	w.mu.Lock()
	wake := w.wakeReader
	if !part {
		w.lastSubmit = time.Now()
		// A full segment means bulk traffic in this direction, so the
		// reader should stop backing off and probe at the hot rate.
		wake = w.wakeReader
	}
	w.mu.Unlock()

	if wake != nil {
		wake()
	}

	go w.upload(seq, chunk, part)
	w.mu.Lock()
	return nil
}

// noteUpload records how long a full segment took to store so that
// submissions can be spaced by that time over the available parallelism.
func (w *walWriter) noteUpload(d time.Duration) {
	if w.upN == 0 {
		w.upAvg = d
	} else {
		w.upAvg = (w.upAvg*3 + d) / 4
	}
	w.upN++

	every := w.upAvg / time.Duration(w.concurrency)
	if every > maxSubmitSpacing {
		every = maxSubmitSpacing
	}
	w.submitEvery = every
}

func (w *walWriter) upload(seq int64, chunk []byte, partial bool) {
	start := time.Now()
	defer w.wg.Done()
	defer func() { <-w.sem }()
	if partial {
		defer func() {
			w.mu.Lock()
			w.partial--
			w.mu.Unlock()
		}()
	}

	if err := w.storage.Put(w.ctx, objectName(w.prefix, seq, segSuffix), chunk); err != nil {
		w.mu.Lock()
		if w.err == nil {
			w.err = errors.New("failed to store segment").Base(err)
			errors.LogWarning(w.ctx, "xdrive: ", w.prefix, " failed: ", w.err)
		}
		w.mu.Unlock()
		w.storage.Put(w.ctx, objectName(w.prefix, seq, errSuffix), nil)
	}

	if !partial {
		w.mu.Lock()
		w.noteUpload(time.Since(start))
		w.mu.Unlock()
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

// lookGap paces look-ahead probes. It never drops below lookaheadMinGap, so
// a re-armed probe can never be served from the list cache.
func (r *walReader) lookGap() time.Duration {
	if r.minPollInterval < lookaheadMinGap {
		return lookaheadMinGap
	}
	return r.minPollInterval
}

func (r *walReader) run() {
	defer close(r.ch)

	delay := r.minPollInterval
	active := time.Now()
	var failSince time.Time
	for {
		polled := time.Now()
		advanced, eof, err := r.poll()
		if err != nil {
			if r.ctx.Err() != nil {
				r.setErr(err)
				return
			}
			switch errors.Cause(err) {
			case errPeerStore, errHoleTimeout:
				r.setErr(err)
				return
			}
			if failSince.IsZero() {
				failSince = time.Now()
				errors.LogWarning(r.ctx, "transient WAL read error, retrying: ", err)
			} else if time.Since(failSince) >= readerErrorGracePeriod {
				r.setErr(err)
				return
			}
			advanced = false
		} else {
			failSince = time.Time{}
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
		} else if time.Since(r.lastHit) < max(probeHotWindow, r.eagerWindow) {
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

	// The listing above already covered this sequence. Arm the timer now so
	// the first look-ahead is not an immediate duplicate of it, and so the
	// reader is never left without a probe in flight between poll cycles.
	var look chan struct {
		listed []Entry
		err    error
	}
	var lookTimer <-chan time.Time
	if probed {
		lookTimer = time.After(r.lookGap())
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
				" never arrived while later ones did, the peer lost it").Base(errHoleTimeout)
		}
	} else {
		r.holeSince = time.Time{}
	}

	outstanding := make(map[int64]chan struct {
		chunk []byte
		err   error
	})
	spinBound := r.eagerWindow
	if spinBound <= 0 {
		spinBound = r.holeTimeout
	}
	progressAt := time.Now()
	var missAt time.Time
	for {
		if e, ok := pending[r.seq]; ok {
			if strings.HasSuffix(e.Name, errSuffix) {
				r.discard(r.prefix + "/" + e.Name)
				return advanced, false, errors.New("the peer could not store segment ", r.seq).Base(errPeerStore)
			}
			if strings.HasSuffix(e.Name, endSuffix) && len(outstanding) == 0 {
				r.discard(r.prefix + "/" + e.Name)
				return advanced, true, nil
			}
		}

		if spinBound > 0 && !missAt.IsZero() && time.Since(missAt) >= spinBound {
			if _, ok := outstanding[r.seq]; !ok {
				return advanced, false, nil
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

		if spinBound > 0 && time.Since(progressAt) >= spinBound && len(outstanding) == 0 {
			return advanced, false, nil
		}
		if len(outstanding) == 0 && look == nil && lookTimer == nil {
			if !advanced || r.eagerWindow <= 0 {
				return advanced, false, nil
			}
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
				if res.err != errNotFound {
					return advanced, false, res.err
				}
				// The entry left the window, so the refill loop cannot
				// re-issue the same Get back to back. The look-ahead probe
				// re-discovers the segment if the miss was Drive's index
				// lagging behind its own upload.
				delete(pending, r.seq)
				if missAt.IsZero() {
					missAt = time.Now()
				}
			} else {
				name := pending[r.seq].Name
				select {
				case r.ch <- res.chunk:
				case <-r.ctx.Done():
					return true, true, nil
				}
				r.seq++
				advanced = true
				progressAt = time.Now()
				missAt = time.Time{}
				r.discard(r.prefix + "/" + name)
			}
		case res := <-look:
			look = nil
			if res.err != nil && res.err != errNotFound {
				return false, false, res.err
			}
			for _, entry := range res.listed {
				if seq, ok := parseEntry(entry.Name); ok && seq >= r.seq {
					if _, ok := pending[seq]; !ok {
						pending[seq] = entry
					}
				}
			}
			for seq := range pending {
				if seq < r.seq {
					delete(pending, seq)
				}
			}
			lookTimer = time.After(r.lookGap())
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
		errors.LogWarning(r.ctx, "xdrive: ", r.prefix, " failed: ", err)
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
