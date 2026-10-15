package xdrive

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"io"
	"strconv"
	"sync"
	"time"

	"github.com/xtls/xray-core/common/errors"
)

const (
	slotMagic       = "XDRVSL01"
	slotInlineLimit = 14 * 1024
	slotDepthLimit  = 8

	slotModeInline = 1
	slotModeMedia  = 2
	slotModeEnd    = 3

	slotHeaderLen = len(slotMagic) + 8 + 1 + 4
)

var (
	slotHuntCeiling  = 120 * time.Millisecond
	slotHuntFloor    = 50 * time.Millisecond
	slotTailMin      = 150 * time.Millisecond
	slotTailMax      = 800 * time.Millisecond
	slotFetchAhead   = 6
	slotAckStride    = 2
	slotAckRetry     = 150 * time.Millisecond
	slotWatermarkTTL = 700 * time.Millisecond
	slotStallTimeout = 60 * time.Second
	slotCloseGrace   = 2 * time.Second
)

type slotTiming struct {
	ackRetry     time.Duration
	ackStride    int
	ackTTL       time.Duration
	stallTimeout time.Duration
	closeGrace   time.Duration
	huntFloor    time.Duration
	huntCeiling  time.Duration
	tailMin      time.Duration
	tailMax      time.Duration
	fetchAhead   int
}

func takeSlotTiming() slotTiming {
	return slotTiming{
		ackRetry:     slotAckRetry,
		ackStride:    slotAckStride,
		ackTTL:       slotWatermarkTTL,
		stallTimeout: slotStallTimeout,
		closeGrace:   slotCloseGrace,
		huntFloor:    slotHuntFloor,
		huntCeiling:  slotHuntCeiling,
		tailMin:      slotTailMin,
		tailMax:      slotTailMax,
		fetchAhead:   slotFetchAhead,
	}
}

type slotFrame struct {
	Seq  int64
	Mode uint8
	Data []byte
}

func encodeSlot(f slotFrame) string {
	buf := make([]byte, slotHeaderLen+len(f.Data))
	copy(buf, slotMagic)
	pos := len(slotMagic)
	binary.BigEndian.PutUint64(buf[pos:], uint64(f.Seq))
	pos += 8
	buf[pos] = f.Mode
	pos++
	binary.BigEndian.PutUint32(buf[pos:], uint32(len(f.Data)))
	pos += 4
	copy(buf[pos:], f.Data)
	return base64.StdEncoding.EncodeToString(buf)
}

func decodeSlot(description string) (slotFrame, bool) {
	if description == "" {
		return slotFrame{}, false
	}
	raw, err := base64.StdEncoding.DecodeString(description)
	if err != nil || len(raw) < slotHeaderLen {
		return slotFrame{}, false
	}
	if string(raw[:len(slotMagic)]) != slotMagic {
		return slotFrame{}, false
	}
	pos := len(slotMagic)
	f := slotFrame{Seq: int64(binary.BigEndian.Uint64(raw[pos:]))}
	pos += 8
	f.Mode = raw[pos]
	pos++
	length := int(binary.BigEndian.Uint32(raw[pos:]))
	pos += 4
	if f.Mode == slotModeInline && length == len(raw)-pos {
		f.Data = raw[pos:]
	}
	return f, true
}

func encodeWatermark(seq int64) string {
	return base64.StdEncoding.EncodeToString([]byte(strconv.FormatInt(seq, 10)))
}

func decodeWatermark(description string) int64 {
	raw, err := base64.StdEncoding.DecodeString(description)
	if err != nil {
		return -1
	}
	seq, err := strconv.ParseInt(string(raw), 10, 64)
	if err != nil {
		return -1
	}
	return seq
}

type slotStore interface {
	makeSlot(ctx context.Context, name string) (string, error)
	writeSlot(ctx context.Context, id string, seq int64, mode uint8, data []byte) error
	readSlot(ctx context.Context, id string) (slotFrame, bool, error)
	readSlotBody(ctx context.Context, id string) ([]byte, error)
	writeWatermark(ctx context.Context, id string, seq int64) error
	readWatermark(ctx context.Context, id string) (int64, error)
	Close() error
}

type slotTable struct {
	Slots     []string `json:"slots"`
	Watermark string   `json:"watermark"`
}

type slotAnnounce struct {
	Uplink   slotTable `json:"u"`
	Downlink slotTable `json:"d"`
}

func encodeAnnounce(a slotAnnounce) string {
	raw, err := json.Marshal(a)
	if err != nil {
		return ""
	}
	return base64.StdEncoding.EncodeToString(raw)
}

func decodeAnnounce(description string) (slotAnnounce, bool) {
	raw, err := base64.StdEncoding.DecodeString(description)
	if err != nil {
		return slotAnnounce{}, false
	}
	var a slotAnnounce
	if json.Unmarshal(raw, &a) != nil {
		return slotAnnounce{}, false
	}
	if len(a.Uplink.Slots) == 0 || len(a.Downlink.Slots) == 0 ||
		a.Uplink.Watermark == "" || a.Downlink.Watermark == "" {
		return slotAnnounce{}, false
	}
	return a, true
}

type slotWriter struct {
	ctx   context.Context
	store slotStore
	table slotTable
	tune  slotTiming
	params

	mu       sync.Mutex
	buf      []byte
	seq      int64
	segBytes int
	ramped   int
	partial  int
	closed   bool
	err      error
	acked    int64
	ackAt    time.Time
	wake     func()

	slots chan struct{}
	wg    sync.WaitGroup
}

func newSlotWriter(ctx context.Context, store slotStore, table slotTable, p params) *slotWriter {
	w := &slotWriter{
		ctx:      ctx,
		store:    store,
		table:    table,
		tune:     takeSlotTiming(),
		params:   p,
		segBytes: p.segmentBytes,
		slots:    make(chan struct{}, len(table.Slots)),
		acked:    -1,
	}
	if start := slotRampStart(p.segmentBytes); start < p.segmentBytes {
		w.segBytes = start
	}
	go w.flushLoop()
	return w
}

func slotRampStart(segmentBytes int) int {
	if slotInlineLimit < segmentBytes {
		return slotInlineLimit
	}
	return segmentBytes
}

func (w *slotWriter) setWake(fn func()) {
	w.mu.Lock()
	w.wake = fn
	w.mu.Unlock()
}

func (w *slotWriter) Write(p []byte) (int, error) {
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

func (w *slotWriter) flushLoop() {
	ticker := time.NewTicker(w.flushInterval)
	defer ticker.Stop()
	held := 0
	var last int
	for {
		select {
		case <-w.ctx.Done():
			return
		case <-ticker.C:
			w.mu.Lock()
			done := w.closed || w.err != nil
			ready := false
			if !done && len(w.buf) > 0 && w.partial < maxPartialInflight {
				busy := len(w.slots) > 0
				grew := len(w.buf) > last
				last = len(w.buf)
				switch {
				case busy && held < maxCoalescedTicks && len(w.buf) < w.segBytes/2:
					held++
				case grew && held < maxCoalescedTicks:
					held++
				default:
					held = 0
					ready = true
				}
			}
			if ready {
				w.flushLocked()
			}
			w.mu.Unlock()
			if done {
				return
			}
		}
	}
}

func (w *slotWriter) flushLocked() error {
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
	if floor := 4 * slotRampStart(w.segmentBytes); len(w.buf) > floor && w.segmentBytes > n {
		n = w.segmentBytes
		if n > len(w.buf) {
			n = len(w.buf)
		}
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

	if !part && n == w.segBytes && w.segBytes < w.segmentBytes {
		w.ramped++
		if w.ramped >= inlineRampSegments {
			w.ramped = 0
			if m := w.segBytes * 2; m <= w.segmentBytes {
				w.segBytes = m
			}
		}
	}
	if part {
		w.partial++
	}

	id := w.slotID(seq)
	wake := w.wake
	w.wg.Add(1)
	w.mu.Unlock()

	if wake != nil {
		wake()
	}
	go w.upload(id, seq, chunk, part)

	w.mu.Lock()
	return nil
}

func (w *slotWriter) slotID(seq int64) string {
	return w.table.Slots[int(seq%int64(len(w.table.Slots)))]
}

func (w *slotWriter) drained(seq int64) (bool, error) {
	depth := int64(len(w.table.Slots))
	w.mu.Lock()
	known, refreshed := w.acked, time.Since(w.ackAt)
	w.mu.Unlock()
	if known >= seq-depth {
		return true, nil
	}
	if refreshed < w.tune.ackTTL {
		return false, nil
	}
	acked, err := w.store.readWatermark(w.ctx, w.table.Watermark)
	if err != nil {
		return false, err
	}
	w.mu.Lock()
	w.acked, w.ackAt = acked, time.Now()
	w.mu.Unlock()
	return acked >= seq-depth, nil
}

func (w *slotWriter) await(seq int64) error {
	start := time.Now()
	deadline := start.Add(w.tune.stallTimeout)
	wait := 100 * time.Millisecond
	for {
		ok, err := w.drained(seq)
		if err == nil && ok {
			return nil
		}
		w.mu.Lock()
		closing := w.closed
		w.mu.Unlock()
		if time.Now().After(deadline) || (closing && time.Since(start) > w.tune.closeGrace) {
			return errors.New("the reader stopped acknowledging slot ", w.slotID(seq)).Base(err)
		}
		if err != nil {
			wait *= 2
			if wait > 2*time.Second {
				wait = 2 * time.Second
			}
		} else {
			wait = 100 * time.Millisecond
		}
		select {
		case <-w.ctx.Done():
			return w.ctx.Err()
		case <-time.After(wait):
		}
	}
}

func (w *slotWriter) upload(id string, seq int64, chunk []byte, part bool) {
	defer w.wg.Done()
	defer w.unpark(part)

	select {
	case w.slots <- struct{}{}:
	case <-w.ctx.Done():
		return
	}
	if err := w.await(seq); err != nil {
		w.fail(errors.New("failed to store segment ", seq).Base(err))
		return
	}
	mode := uint8(slotModeMedia)
	if len(chunk) <= slotInlineLimit {
		mode = slotModeInline
	}
	if err := w.store.writeSlot(w.ctx, id, seq, mode, chunk); err != nil {
		w.fail(errors.New("failed to store segment ", seq).Base(err))
	}
}

func (w *slotWriter) unpark(part bool) {
	<-w.slots
	if !part {
		return
	}
	w.mu.Lock()
	w.partial--
	w.mu.Unlock()
}

func (w *slotWriter) fail(err error) {
	w.mu.Lock()
	if w.err == nil {
		w.err = err
		errors.LogWarning(w.ctx, "xdrive slot writer failed: ", err)
	}
	w.mu.Unlock()
}

func (w *slotWriter) Err() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.err
}

func (w *slotWriter) Close() error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil
	}
	w.closed = true
	for len(w.buf) > 0 && w.err == nil {
		w.flushLocked()
	}
	seq := w.seq
	w.seq++
	w.mu.Unlock()

	w.wg.Wait()

	w.mu.Lock()
	err := w.err
	w.mu.Unlock()
	if err != nil {
		return err
	}
	if err := w.await(seq); err != nil {
		return err
	}
	return w.store.writeSlot(w.ctx, w.slotID(seq), seq, slotModeEnd, nil)
}

type slotReader struct {
	ctx   context.Context
	store slotStore
	table slotTable
	tune  slotTiming
	params

	ch   chan []byte
	wake chan struct{}

	mu          sync.Mutex
	seq         int64
	acked       int64
	ackWanted   int64
	started     bool
	lastHit     time.Time
	intervalAvg float64
	stopped     string
}

func newSlotReader(ctx context.Context, store slotStore, table slotTable, p params) *slotReader {
	r := &slotReader{
		ctx:     ctx,
		store:   store,
		table:   table,
		tune:    takeSlotTiming(),
		params:  p,
		ch:      make(chan []byte, p.concurrency),
		wake:    make(chan struct{}, 1),
		acked:   -1,
		lastHit: time.Now(),
	}
	go r.run()
	go r.ackLoop()
	return r
}

func (r *slotReader) Wake() {
	r.mu.Lock()
	r.lastHit = time.Now()
	r.mu.Unlock()
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

func (r *slotReader) slotID(seq int64) string {
	return r.table.Slots[int(seq%int64(len(r.table.Slots)))]
}

type slotFetch struct {
	ready chan struct{}
	body  []byte
	end   bool
}

func (r *slotReader) run() {
	defer close(r.ch)
	pending := make(map[int64]*slotFetch, r.tune.fetchAhead+1)

	for {
		if r.ctx.Err() != nil {
			r.stop("the session ended: " + r.ctx.Err().Error())
			return
		}
		r.mu.Lock()
		head := r.seq
		started := r.started
		r.mu.Unlock()

		window := 1
		if started {
			window = r.tune.fetchAhead
		}
		for i := 0; i < window; i++ {
			next := head + int64(i)
			if _, ok := pending[next]; ok {
				continue
			}
			fetch := &slotFetch{ready: make(chan struct{})}
			pending[next] = fetch
			go r.collect(next, i, fetch)
		}

		fetch := pending[head]
		select {
		case <-fetch.ready:
		case <-r.ctx.Done():
			r.stop("the session ended while waiting for segment " + strconv.FormatInt(head, 10))
			return
		}
		if fetch.end {
			r.stop("the peer closed the stream")
			return
		}
		delete(pending, head)
		r.consumed(head)

		select {
		case r.ch <- fetch.body:
		case <-r.ctx.Done():
			r.stop("the session ended while delivering")
			return
		}
	}
}

func (r *slotReader) collect(seq int64, offset int, f *slotFetch) {
	if offset > 0 {
		if !r.sleep(time.Duration(float64(offset) * r.interval())) {
			return
		}
	}
	head := offset == 0
	backoff, ceiling := r.tune.huntFloor, r.tune.huntCeiling
	if offset > 0 {
		backoff = time.Duration(r.interval() * float64(time.Second))
		if backoff < r.tune.tailMin {
			backoff = r.tune.tailMin
		}
		if backoff > r.tune.tailMax {
			backoff = r.tune.tailMax
		}
		ceiling = backoff
	}
	for {
		if r.ctx.Err() != nil {
			return
		}
		frame, ok, err := r.store.readSlot(r.ctx, r.slotID(seq))
		if err == nil && ok && frame.Seq == seq {
			switch frame.Mode {
			case slotModeEnd:
				f.end = true
			case slotModeInline:
				f.body = frame.Data
			case slotModeMedia:
				body, berr := r.store.readSlotBody(r.ctx, r.slotID(seq))
				if berr != nil {
					if !r.hold(backoff, ceiling) {
						return
					}
					continue
				}
				f.body = body
			default:
				if !r.hold(backoff, ceiling) {
					return
				}
				continue
			}
			r.armed()
			close(f.ready)
			return
		}

		if head {
			backoff *= 2
			if backoff > ceiling {
				backoff = ceiling
			}
		}
		if !r.hold(backoff, ceiling) {
			return
		}
	}
}

func (r *slotReader) interval() float64 {
	r.mu.Lock()
	gap := r.intervalAvg
	r.mu.Unlock()
	if gap < 0.3 {
		return 0.3
	}
	if gap > 3 {
		return 3
	}
	return gap
}

func (r *slotReader) noteInterval() {
	r.mu.Lock()
	if !r.lastHit.IsZero() {
		gap := time.Since(r.lastHit).Seconds()
		if r.intervalAvg == 0 {
			r.intervalAvg = gap
		} else {
			r.intervalAvg = (r.intervalAvg + gap) / 2
		}
	}
	r.mu.Unlock()
}

func (r *slotReader) sleep(d time.Duration) bool {
	if d <= 0 {
		return r.ctx.Err() == nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-r.ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (r *slotReader) armed() {
	r.noteInterval()
	r.mu.Lock()
	r.lastHit = time.Now()
	r.mu.Unlock()
}

func (r *slotReader) hold(d, ceiling time.Duration) bool {
	r.mu.Lock()
	if time.Since(r.lastHit) > r.eagerWindow && ceiling < r.maxPollInterval {
		ceiling = r.maxPollInterval
	}
	r.mu.Unlock()

	if d > ceiling {
		d = ceiling
	}
	if d < 5*time.Millisecond {
		d = 5 * time.Millisecond
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-r.ctx.Done():
		return false
	case <-r.wake:
		return true
	case <-timer.C:
		return true
	}
}

func (r *slotReader) consumed(seq int64) {
	r.mu.Lock()
	r.seq = seq + 1
	r.started = true
	r.ackWanted = seq
	r.mu.Unlock()

	r.armed()
}

func (r *slotReader) ackLoop() {
	wait := r.tune.ackRetry
	for {
		if r.ctx.Err() != nil {
			return
		}
		r.mu.Lock()
		want, acked := r.ackWanted, r.acked
		r.mu.Unlock()

		if want > acked && want-acked >= int64(r.tune.ackStride) {
			if err := r.store.writeWatermark(r.ctx, r.table.Watermark, want); err == nil {
				r.mu.Lock()
				if want > r.acked {
					r.acked = want
				}
				r.mu.Unlock()
				wait = r.tune.ackRetry
				continue
			}
			// Every failed acknowledgement is retried, and a retry that
			// hammers Drive starves the very poll the writer is waiting on,
			// because both share one in-flight pool. Back off instead.
			wait *= 2
			if wait > 5*time.Second {
				wait = 5 * time.Second
			}
		}
		select {
		case <-r.ctx.Done():
			return
		case <-time.After(wait):
		}
	}
}

func (r *slotReader) stop(reason string) {
	r.mu.Lock()
	if r.stopped == "" {
		r.stopped = reason
	}
	r.mu.Unlock()
}

func (r *slotReader) Stopped() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.stopped
}

func (r *slotReader) Err() error {
	return errors.New(r.Stopped())
}
