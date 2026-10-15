package xdrive

// The slot transport.
//
// Why this exists, in numbers measured against the live Drive API:
//
//	files.create, inline description, 16 KiB     876 ms
//	files.create, multipart,          4 KiB    1646 ms
//	files.create, multipart,        256 KiB    1537 ms
//	files.create, multipart,          1 MiB    1929 ms
//	index lag before files.list sees it       ~450 ms
//	files.list with 64 exact names            665 ms
//	files.get by a known id, description      371 ms   5 quota units
//
// The first table is the whole problem. A Drive write costs about 0.9s and
// barely notices how much it is carrying, so segment size buys throughput and
// never latency. The second table is where the latency actually went: after
// the write returned, the old path still had to wait for Drive's index and
// then run a sixty-four term listing to learn a file id it could have been
// told once. That is well over a second of every hop spent finding bytes that
// already existed.
//
// A slot removes the finding. The two peers are handed the file ids of a
// fixed ring of slot objects when the session is announced, so a write is a
// patch against an id both sides already hold and a read is a get against an
// id both sides already hold. Nothing lists anything, and nothing waits for
// an index.
//
// The second win is that the read can start before the write finishes. The
// reader polls its slot continuously; the poll that happens to be in flight
// when the write commits returns the bytes immediately. Detection therefore
// costs the gap between poll intervals rather than a fresh round trip, and a
// hop comes to roughly one write instead of three dependent round trips.
//
// The ring is what makes reuse safe. Segment n lives in slot n%depth, so a
// slot is only touched again one lap later. The reader publishes how far it
// has got in a per-direction watermark object, and the writer will not reuse
// a slot until that watermark says the last occupant was taken. Without the
// rule a reader that fell behind would silently lose whatever a wrap erased.

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
	slotMagic = "XDRVSL01"

	// Drive accepts a description well past this and refuses one past roughly
	// 30k characters. The header plus base64 expansion has to fit inside it,
	// so 14 KiB of payload encodes to about 18.7k characters.
	slotInlineLimit = 14 * 1024

	slotDepthLimit = 8
)

// The timings below are variables rather than constants so the simulator can
// scale Drive's latencies and these together. Only the proportions between
// them decide how the transport behaves, so a test that shrinks one and not
// the other is testing something the real transport never does.
var (
	// The widest gap the reader will use while it still expects a segment.
	// Drive commits a write in about a second, so asking more often than this
	// only spends requests on answers that say "not yet". It is deliberately
	// tighter than the budget alone would allow: a probe is 5 quota units and
	// 371 ms, so the wait it adds is latency while the requests it costs are
	// the cheapest kind on the API.
	slotHuntCeiling = 120 * time.Millisecond

	// How far ahead of the reader's own acknowledgement a segment may be
	// written. Two is enough to keep the writer busy through a watermark
	// round trip without letting the reader fall out of the ring.
	slotAckStride = 2

	// How often the acknowledgement loop rechecks. A watermark write costs
	// about a second, so this is a floor, not a rate.
	slotAckRetry = 150 * time.Millisecond

	// A watermark that stops moving means the reader is gone. Holding the
	// writer is the safe answer; overwriting is not. The window is generous
	// because acknowledgements are now retried until Drive confirms them, so
	// reaching it really does mean the peer is gone rather than that one write
	// happened to fail.
	slotStallTimeout = 60 * time.Second

	// How long a closing writer waits before giving up on a slot.
	slotCloseGrace = 2 * time.Second

	// How long a cached reader watermark is trusted before it is fetched
	// again. A slot is only reused once a lap, so this trades a little
	// throughput for a lot fewer round trips.
	slotWatermarkTTL = 700 * time.Millisecond

	// How many segments behind the head the reader will chase at once. Every
	// one of them is a real Drive request, but a download costs about a
	// second whether it runs alone or in company, so overlapping six is
	// close to free and overlapping more just spends the request budget. Six
	// rather than three matters for page loads: a dozen responses arriving
	// together are collected six at a time rather than three, which is the
	// difference between one round of fetches and two.
	slotFetchAhead = 6

	// The gap a segment behind the head settles into: about one arrival
	// interval, bounded so an idle stream still checks in and a busy one
	// still keeps up.
	slotTailMin = 150 * time.Millisecond
	slotTailMax = 800 * time.Millisecond

	// Where the head starts hunting. A Drive write takes most of a second, so
	// starting well below the ceiling only buys extra requests that are
	// certain to come back empty.
	slotHuntFloor = 50 * time.Millisecond
)

const (
	slotModeInline = 1
	slotModeMedia  = 2
	slotModeEnd    = 3
)

const slotHeaderLen = len(slotMagic) + 8 + 1 + 4

// slotFrame is what a slot's description decodes to.
type slotFrame struct {
	Seq  int64
	Mode uint8
	Data []byte // inline payload only; a media segment keeps its body in content
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

// slotStore is the Drive side of the transport. Every call is addressed by a
// file id both peers were handed when the session was announced, so none of
// it ever lists anything and none of it depends on Drive's search index.
type slotStore interface {
	makeSlot(ctx context.Context, name string) (string, error)
	writeSlot(ctx context.Context, id string, seq int64, mode uint8, data []byte) error
	readSlot(ctx context.Context, id string) (slotFrame, bool, error)
	readSlotBody(ctx context.Context, id string) ([]byte, error)
	writeWatermark(ctx context.Context, id string, seq int64) error
	readWatermark(ctx context.Context, id string) (int64, error)
	Close() error
}

// slotTable names the objects one direction of a session is built from.
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

// ---------------------------------------------------------------------------
// writer

type slotWriter struct {
	ctx   context.Context
	store slotStore
	table slotTable
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
		params:   p,
		segBytes: p.segmentBytes,
		slots:    make(chan struct{}, len(table.Slots)),
		acked:    -1,
	}
	// A session that is only carrying small interactive writes should stay
	// small, because a small write is also the cheap kind: at the inline limit
	// it is a metadata patch rather than a body upload, which measured 880 ms
	// against 1270 ms.
	if start := slotRampStart(p.segmentBytes); start < p.segmentBytes {
		w.segBytes = start
	}
	go w.flushLoop()
	return w
}

// slotRampStart is the size a slot session opens with, capped at what the
// description field can carry.
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

// flushLoop turns a quiet stream into a segment without waiting for a write to
// fill one. The first tick after a write that grew the buffer holds off, which
// coalesces a burst without adding a round trip to a single small write.
//
// While the writer still has segments in flight the timer is ignored, unless
// the buffer has already grown past half a segment. That is the difference
// between a page load and a single request. A single request finds the pipe
// idle and goes out immediately, at the smallest size, which is the cheapest
// write there is. A page load arrives as a dozen responses inside a second,
// and sending each as its own small segment means a dozen Drive writes at
// roughly a second each, all competing for a handful of slots, which is how a
// four slot ring turns a page load into a queue. Holding them until the
// segment fills turns the same dozen responses into one or two writes.
//
// The wait is bounded by held regardless, so a stream that sends a trickle
// still moves.
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
	// The ramp is for sessions that only ever carry small interactive writes.
	// It must not shred a large burst, so once the buffer holds noticeably
	// more than the ramp size there is real volume here and the configured
	// segment goes out in one piece.
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

// drained reports whether the reader has taken the bytes this segment is about
// to overwrite.
//
// The watermark only moves once every few segments and Drive costs a round
// trip to read, so it is not fetched per attempt. Two things keep the cost
// down: a segment inside the first lap never needs it at all, and afterwards
// the cached value is reused for a moment before being refreshed. A slot is
// only reused once a lap, so being a fraction of a second stale costs
// throughput and never correctness.
func (w *slotWriter) drained(seq int64) (bool, error) {
	depth := int64(len(w.table.Slots))
	w.mu.Lock()
	known, refreshed := w.acked, time.Since(w.ackAt)
	w.mu.Unlock()
	if known >= seq-depth {
		return true, nil
	}
	if refreshed < slotWatermarkTTL {
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

// await waits until the slot is free, and is the only place the writer can
// block on the reader. Holding here is the backpressure: a slow reader stops
// the writer rather than losing a segment.
//
// A peer that stops reading altogether is a dead session rather than a slow
// one, so the wait is bounded. Closing shortens it further, because tearing a
// session down should not sit through the full timeout behind every upload
// that is still queued.
func (w *slotWriter) await(seq int64) error {
	start := time.Now()
	deadline := start.Add(slotStallTimeout)
	for {
		ok, err := w.drained(seq)
		if err == nil && ok {
			return nil
		}
		w.mu.Lock()
		closing := w.closed
		w.mu.Unlock()
		if time.Now().After(deadline) || (closing && time.Since(start) > slotCloseGrace) {
			return errors.New("the reader stopped acknowledging slot ", w.slotID(seq)).Base(err)
		}
		select {
		case <-w.ctx.Done():
			return w.ctx.Err()
		case <-time.After(100 * time.Millisecond):
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

// Err reports why the writer stopped.
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

// ---------------------------------------------------------------------------
// reader

// slotReader walks the ring in order, keeping a few segments in flight behind
// the head. Delivery is serial because the stream is ordered, but collection
// is not: a download costs about a second whether it runs alone or alongside
// its neighbours, so overlapping them is close to free right up to the point
// the request budget objects. The head is still hunted first and most
// tightly, so none of this is paid for in latency.
type slotReader struct {
	ctx   context.Context
	store slotStore
	table slotTable
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
		ctx:    ctx,
		store:  store,
		table:  table,
		params: p,
		ch:     make(chan []byte, p.concurrency),
		wake:   make(chan struct{}, 1),
		acked:  -1,
		// Start out hunting. With a zero lastHit the age test in hold reads as
		// "idle for ever" and the first segment of a session would be hunted on
		// the idle interval, which is exactly the latency that shows up as
		// time to first byte.
		lastHit: time.Now(),
	}
	go r.run()
	go r.ackLoop()
	return r
}

// Wake re-arms the hunt. The local writer calling it means the peer has been
// sent something and an answer is on its way, so the idle interval is no
// longer the right pace even if the pipe has been quiet.
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

// slotFetch is one sequence number being collected off the ring.
type slotFetch struct {
	ready chan struct{}
	body  []byte
	end   bool
}

// run delivers the ring in order. Every exit goes through here, so the reason
// a stream stopped is always one place, and a fetch goroutine only ever
// reports what it found.
func (r *slotReader) run() {
	defer close(r.ch)
	pending := make(map[int64]*slotFetch, slotFetchAhead+1)

	for {
		if r.ctx.Err() != nil {
			r.stop("the session ended: " + r.ctx.Err().Error())
			return
		}
		r.mu.Lock()
		head := r.seq
		started := r.started
		r.mu.Unlock()

		// Chasing segments behind the head only pays once the stream has shown
		// it is actually running. A request and its answer arrive one segment
		// apart, and a session that only ever carries a few hundred bytes would
		// otherwise spend most of its requests confirming that nothing has
		// arrived yet.
		window := 1
		if started {
			window = slotFetchAhead
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
		select {
		case r.ch <- fetch.body:
		case <-r.ctx.Done():
			r.stop("the session ended while delivering")
			return
		}
		delete(pending, head)
		r.consumed(head)
	}
}

// collect waits for one segment to land in its slot and brings the body back.
// A slot is a single object addressed by an id both peers already hold, so
// there is nothing to discover and nothing to wait for the index.
//
// offset says how far behind the head this segment is. The head hunts, because
// its segment is the whole latency budget. Everything else waits out its turn
// first, because a segment three places back is not due for seconds and every
// probe spent on it is a request the session does not have.
func (r *slotReader) collect(seq int64, offset int, f *slotFetch) {
	if offset > 0 {
		if !r.sleep(time.Duration(float64(offset) * r.interval())) {
			return
		}
	}
	// Pacing is per fetch, not shared. One backoff counter across the whole
	// reader has every lagging fetch resetting the head's, which is how a
	// reader ends up asking at the floor interval during a bulk transfer.
	head := offset == 0
	backoff, ceiling := slotHuntFloor, slotHuntCeiling
	if offset > 0 {
		// A segment this far back is roughly one arrival interval away, so
		// asking about once per interval is all it takes. Hunting it at the
		// head's rate would multiply the session's request rate by the number
		// of segments in flight, and that is the rate that trips Drive.
		backoff = time.Duration(r.interval() * float64(time.Second))
		if backoff < slotTailMin {
			backoff = slotTailMin
		}
		if backoff > slotTailMax {
			backoff = slotTailMax
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

// interval estimates how far apart segments are arriving, which is what tells
// a lagging fetch how long to stay out of the way. It is a simple average of
// recent deliveries, floored so an idle stream never parks a fetch for long.
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

// armed records that a segment arrived, which resets the idle clock and gives
// the next segment a fresh estimate of how far apart they are coming.
func (r *slotReader) armed() {
	r.noteInterval()
	r.mu.Lock()
	r.lastHit = time.Now()
	r.mu.Unlock()
}

// hold waits out a poll gap. It reports false when the session is over, which
// is the only way a fetch goroutine ever stops.
//
// The ceiling drops to the idle interval once nothing has arrived for a whole
// eager window. That rule is the one the old path was missing: it asked at
// the poll interval for the entire eager window on both sides of the session,
// which is about a hundred requests a second, well past Drive's per-user cap,
// and every 403 that cap returns arms a two second freeze for the whole
// session. That is where the twenty second stalls came from.
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

// consumed moves past a segment and records that the slot it was holding is
// free.
//
// The acknowledgement is not written here. It is handed to ackLoop, which
// retries until Drive has it, because this value is the only thing standing
// between a slow reader and a writer that will not touch a slot again. An
// earlier version published it in a detached goroutine and threw the error
// away; a single failed patch left the reader believing it had acknowledged,
// so it did not try again for another few segments, the writer's gate stayed
// shut, and after the stall timeout the writer tore the whole session down.
// A page load is exactly the workload that produces a transient Drive error,
// so that was not a rare path.
func (r *slotReader) consumed(seq int64) {
	r.mu.Lock()
	r.seq = seq + 1
	r.started = true
	r.ackWanted = seq
	r.mu.Unlock()

	r.noteInterval()
	r.armed()
}

// ackLoop keeps the peer's watermark current. It only records an
// acknowledgement once Drive has confirmed it, and it always republishes the
// furthest segment seen, so a failed write costs latency rather than the
// session.
func (r *slotReader) ackLoop() {
	for {
		if r.ctx.Err() != nil {
			return
		}
		r.mu.Lock()
		want, acked := r.ackWanted, r.acked
		r.mu.Unlock()

		if want > acked && want-acked >= int64(slotAckStride) {
			if err := r.store.writeWatermark(r.ctx, r.table.Watermark, want); err == nil {
				r.mu.Lock()
				if want > r.acked {
					r.acked = want
				}
				r.mu.Unlock()
				continue
			}
		}
		select {
		case <-r.ctx.Done():
			return
		case <-time.After(slotAckRetry):
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

// Stopped reports why the reader stopped. Without it a closed channel looks
// the same however it closed, which makes a stall indistinguishable from an
// orderly shutdown.
func (r *slotReader) Stopped() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.stopped
}
