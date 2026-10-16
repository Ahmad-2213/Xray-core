package xdrive

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"hash/crc32"
	"io"
	"strconv"
	"sync"
	"time"

	"github.com/xtls/xray-core/common/errors"
)

const (
	slotMagic       = "XDRVSL02"
	slotMagicV1     = "XDRVSL01"
	slotInlineLimit = 14 * 1024
	slotDepthLimit  = 8

	slotModeInline = 1
	slotModeMedia  = 2
	slotModeEnd    = 3
	slotModeErr    = 4

	slotHeaderLen   = len(slotMagic) + 8 + 1 + 4 + 4
	slotHeaderLenV1 = len(slotMagicV1) + 8 + 1 + 4
)

var (
	slotHuntCeiling     = 200 * time.Millisecond
	slotHuntFloor       = 50 * time.Millisecond
	slotTailMin         = 150 * time.Millisecond
	slotTailMax         = 800 * time.Millisecond
	slotFetchAhead      = 6
	slotAckStride       = 1
	slotAckRetry        = 100 * time.Millisecond
	slotWriteAttempts   = 3
	slotNotFoundTimeout = 60 * time.Second
	slotWatermarkTTL    = 250 * time.Millisecond
	slotStallTimeout    = 60 * time.Second
	slotCloseGrace      = 10 * time.Second
)

type slotTiming struct {
	ackRetry        time.Duration
	ackStride       int
	ackTTL          time.Duration
	stallTimeout    time.Duration
	closeGrace      time.Duration
	huntFloor       time.Duration
	huntCeiling     time.Duration
	tailMin         time.Duration
	tailMax         time.Duration
	fetchAhead      int
	notFoundTimeout time.Duration
}

func takeSlotTiming() slotTiming {
	return slotTiming{
		ackRetry:        slotAckRetry,
		ackStride:       slotAckStride,
		ackTTL:          slotWatermarkTTL,
		stallTimeout:    slotStallTimeout,
		closeGrace:      slotCloseGrace,
		huntFloor:       slotHuntFloor,
		huntCeiling:     slotHuntCeiling,
		tailMin:         slotTailMin,
		tailMax:         slotTailMax,
		fetchAhead:      slotFetchAhead,
		notFoundTimeout: slotNotFoundTimeout,
	}
}

type slotFrame struct {
	Seq     int64
	Mode    uint8
	Data    []byte
	Len     int
	Crc     uint32
	Checked bool
}

func newInlineFrame(seq int64, data []byte) slotFrame {
	return slotFrame{
		Seq:     seq,
		Mode:    slotModeInline,
		Data:    data,
		Len:     len(data),
		Crc:     crc32.ChecksumIEEE(data),
		Checked: true,
	}
}

func newMediaFrame(seq int64, data []byte) slotFrame {
	return slotFrame{
		Seq:     seq,
		Mode:    slotModeMedia,
		Len:     len(data),
		Crc:     crc32.ChecksumIEEE(data),
		Checked: true,
	}
}

func encodeSlot(f slotFrame) string {
	buf := make([]byte, slotHeaderLen+len(f.Data))
	copy(buf, slotMagic)
	pos := len(slotMagic)
	binary.BigEndian.PutUint64(buf[pos:], uint64(f.Seq))
	pos += 8
	buf[pos] = f.Mode
	pos++
	binary.BigEndian.PutUint32(buf[pos:], uint32(f.Len))
	pos += 4
	binary.BigEndian.PutUint32(buf[pos:], f.Crc)
	pos += 4
	copy(buf[pos:], f.Data)
	return base64.StdEncoding.EncodeToString(buf)
}

func decodeSlot(description string) (slotFrame, bool) {
	if description == "" {
		return slotFrame{}, false
	}
	raw, err := base64.StdEncoding.DecodeString(description)
	if err != nil || len(raw) < slotHeaderLenV1 {
		return slotFrame{}, false
	}
	switch string(raw[:len(slotMagic)]) {
	case slotMagic:
		return decodeSlotV2(raw)
	case slotMagicV1:
		return decodeSlotV1(raw)
	default:
		return slotFrame{}, false
	}
}

func decodeSlotV2(raw []byte) (slotFrame, bool) {
	if len(raw) < slotHeaderLen {
		return slotFrame{}, false
	}
	pos := len(slotMagic)
	f := slotFrame{Checked: true, Seq: int64(binary.BigEndian.Uint64(raw[pos:]))}
	pos += 8
	f.Mode = raw[pos]
	pos++
	f.Len = int(binary.BigEndian.Uint32(raw[pos:]))
	pos += 4
	f.Crc = binary.BigEndian.Uint32(raw[pos:])
	pos += 4
	if f.Mode != slotModeInline {
		return f, true
	}
	if f.Len <= 0 || f.Len != len(raw)-pos {
		return slotFrame{}, false
	}
	payload := raw[pos:]
	if crc32.ChecksumIEEE(payload) != f.Crc {
		return slotFrame{}, false
	}
	f.Data = payload
	return f, true
}

func decodeSlotV1(raw []byte) (slotFrame, bool) {
	pos := len(slotMagicV1)
	f := slotFrame{Seq: int64(binary.BigEndian.Uint64(raw[pos:]))}
	pos += 8
	f.Mode = raw[pos]
	pos++
	length := int(binary.BigEndian.Uint32(raw[pos:]))
	pos += 4
	if f.Mode != slotModeInline {
		return f, true
	}
	if length <= 0 || length != len(raw)-pos {
		return slotFrame{}, false
	}
	f.Data = raw[pos:]
	f.Len = length
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
	progAt   time.Time
	wake     func()

	slots   chan struct{}
	pending chan struct{}
	wg      sync.WaitGroup

	wmMu     sync.Mutex
	wmFlight *wmCall
}

type wmCall struct {
	done  chan struct{}
	acked int64
	err   error
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
		pending:  make(chan struct{}, 2*len(table.Slots)),
		acked:    -1,
		progAt:   time.Now(),
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

	w.mu.Unlock()
	select {
	case w.pending <- struct{}{}:
	case <-w.ctx.Done():
		w.mu.Lock()
		return w.ctx.Err()
	}
	select {
	case w.slots <- struct{}{}:
	case <-w.ctx.Done():
		<-w.pending
		w.mu.Lock()
		return w.ctx.Err()
	}
	w.mu.Lock()
	if len(w.buf) == 0 || w.err != nil {
		err := w.err
		if err == nil {
			err = w.ctx.Err()
		}
		<-w.slots
		<-w.pending
		if err != nil {
			return err
		}
		return nil
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

func (w *slotWriter) readWatermarkShared() (int64, error) {
	w.wmMu.Lock()
	if w.wmFlight != nil {
		f := w.wmFlight
		w.wmMu.Unlock()
		<-f.done
		return f.acked, f.err
	}
	f := &wmCall{done: make(chan struct{})}
	w.wmFlight = f
	w.wmMu.Unlock()
	f.acked, f.err = w.store.readWatermark(w.ctx, w.table.Watermark)
	w.wmMu.Lock()
	w.wmFlight = nil
	w.wmMu.Unlock()
	close(f.done)
	return f.acked, f.err
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
	acked, err := w.readWatermarkShared()
	if err != nil {
		return false, err
	}
	now := time.Now()
	w.mu.Lock()
	if acked > w.acked {
		w.acked = acked
		w.progAt = now
	}
	w.ackAt = now
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
		prog := w.progAt
		w.mu.Unlock()
		if time.Now().After(deadline) || (closing && time.Since(prog) > w.tune.closeGrace) {
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
	defer func() {
		<-w.pending
		w.unpark(part)
		w.wg.Done()
	}()

	var lastErr error
	if err := w.await(seq); err != nil {
		w.fail(errors.New("failed to store segment ", seq).Base(err))
		w.signalErr(id, seq)
		return
	}
	for attempt := 0; attempt < slotWriteAttempts; attempt++ {
		if attempt > 0 {
			select {
			case <-w.ctx.Done():
				return
			case <-time.After(time.Duration(attempt) * 200 * time.Millisecond):
			}
			if frame, ok, err := w.store.readSlot(w.ctx, id); err == nil && ok && frame.Seq >= seq {
				return
			}
		}
		mode := uint8(slotModeMedia)
		if len(chunk) <= slotInlineLimit {
			mode = slotModeInline
		}
		if err := w.store.writeSlot(w.ctx, id, seq, mode, chunk); err != nil {
			lastErr = err
			if w.ctx.Err() != nil {
				return
			}
			continue
		}
		return
	}
	w.fail(errors.New("failed to store segment ", seq).Base(lastErr))
	w.signalErr(id, seq)
}

func (w *slotWriter) signalErr(id string, seq int64) {
	_ = w.store.writeSlot(w.ctx, id, seq, slotModeErr, nil)
}

func (w *slotWriter) unpark(part bool) {
	<-w.slots
	w.release(part)
}

func (w *slotWriter) release(part bool) {
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
		w.signalErr(w.slotID(seq), seq)
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
	warnAt      time.Time
	eof         bool
	stopped     string
}

func newSlotReader(ctx context.Context, store slotStore, table slotTable, p params) *slotReader {
	r := &slotReader{
		ctx:       ctx,
		store:     store,
		table:     table,
		tune:      takeSlotTiming(),
		params:    p,
		ch:        make(chan []byte, p.concurrency),
		wake:      make(chan struct{}, 1),
		acked:     -1,
		ackWanted: -1,
		lastHit:   time.Now(),
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
	ready   chan struct{}
	body    []byte
	end     bool
	fatal   bool
	err     error
	nfSince time.Time
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
			r.mu.Lock()
			r.eof = true
			r.mu.Unlock()
			r.stop("the peer closed the stream")
			return
		}
		if fetch.err != nil {
			r.stop(fetch.err.Error())
			return
		}
		if fetch.fatal {
			r.stop("the peer failed to store segment " + strconv.FormatInt(head, 10))
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
			case slotModeErr:
				f.fatal = true
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
				if frame.Checked && (len(body) != frame.Len || crc32.ChecksumIEEE(body) != frame.Crc) {
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

		if err == errNotFound {
			if f.nfSince.IsZero() {
				f.nfSince = time.Now()
			} else if head && time.Since(f.nfSince) >= r.tune.notFoundTimeout {
				f.err = errors.New("slot ", r.slotID(seq), " for segment ", seq, " is gone").Base(errNotFound)
				close(f.ready)
				return
			}
		} else {
			f.nfSince = time.Time{}
			if err != nil && r.ctx.Err() == nil {
				warn := false
				r.mu.Lock()
				if time.Since(r.warnAt) >= 10*time.Second {
					r.warnAt = time.Now()
					warn = true
				}
				r.mu.Unlock()
				if warn {
					errors.LogWarning(r.ctx, "xdrive slot read failed: ", err)
				}
			}
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
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.eof {
		return io.EOF
	}
	return errors.New(r.stopped)
}
