package mux

// Server-side Resume/Ack handling. Resume arrives as the first frame of a
// fresh v2 carrier and rebinds a suspended worker's sessions; Ack only
// advances accounting.

import (
	"context"
	"time"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/net"
)

func serverUserOf(ctx context.Context) string {
	if in := ctx.Value("inboundUser"); in != nil {
		if s, ok := in.(string); ok {
			return s
		}
	}
	return ""
}

// countRx applies the shared counting rule to received frames. New is
// counted by the caller only after successful dispatch (a failed dispatch
// kills the worker today; counting it would ack a never-created session and
// hang the client stream on rebind instead of failing it).
func (w *ServerWorker) countRx(meta *FrameMetadata) {
	switch meta.SessionStatus {
	case SessionStatusNew:
		if meta.Target.Network == net.Network_TCP {
			w.rx.Next()
			w.maybeSendAck()
		}
	case SessionStatusKeep:
		if !meta.Option.Has(OptionData) {
			return
		}
		if meta.Target.Network == net.Network_UDP {
			return
		}
		w.rx.Next()
		w.maybeSendAck()
	case SessionStatusEnd:
		w.rx.Next()
		w.maybeSendAck()
	}
}

// maybeSendAck emits Ack{rxCount} throttled, via the gate once present.
func (w *ServerWorker) maybeSendAck() {
	if w.gate == nil {
		return
	}
	intervalMs := int64(500)
	now := time.Now()
	w.ackMu.mu.Lock()
	if now.Sub(w.ackMu.lastTime) < time.Duration(intervalMs)*time.Millisecond {
		w.ackMu.mu.Unlock()
		return
	}
	count := w.rx.Value()
	if count == w.ackMu.lastSent {
		w.ackMu.mu.Unlock()
		return
	}
	w.ackMu.lastSent = count
	w.ackMu.lastTime = now
	w.ackMu.mu.Unlock()
	w.gate.writeAck(count)
}

func (w *ServerWorker) handleStatusResume(meta *FrameMetadata, reader *buf.BufferedReader) error {
	if !meta.Option.Has(OptionData) {
		return nil
	}
	mb, err := NewStreamReader(reader).ReadMultiBuffer()
	if err != nil {
		return err
	}
	defer buf.ReleaseMulti(mb)
	if len(mb) == 0 || len(mb[0].Bytes()) < resumePayloadLen {
		return errors.New("short resume payload")
	}
	rp, err := decodeResume(mb[0].Bytes())
	if err != nil {
		return err
	}

	user := serverUserOf(context.Background())

	// Rebind takes precedence over announce: a fresh carrier bearing a
	// parked token reattaches sessions. Otherwise it is an announce.
	if _, ok := hsPeek(rp.Token); ok {
		entry, ok := hsTake(rp.Token)
		if !ok {
			return errors.New("expired resume token")
		}
		if entry.user != "" && user != "" && entry.user != user {
			hsPutBack(rp.Token, entry)
			return errors.New("resume user mismatch")
		}
		if rp.Epoch <= entry.epoch {
			hsPutBack(rp.Token, entry)
			return errors.New("stale resume epoch")
		}
		if rp.RxCount > entry.tx {
			hsPutBack(rp.Token, entry)
			return errors.New("resume count beyond sent")
		}
		// Adopt the parked table, gate (with retain store) and rx baseline.
		entry.manager.Reparent(w.sessionManager)
		w.rx.Set(entry.rx)
		if entry.gate != nil {
			w.gate = entry.gate
			w.gate.swapTarget(w.link.Writer)
			w.gate.setSuspended(false)
		}
		w.resumeToken = rp.Token
		w.resumeEpoch = rp.Epoch
		w.resumeHasToken = true
		w.resumeUser = user
		if w.gate == nil {
			w.gate = newCarrierGate(w.link.Writer, w.done.Wait(), DefaultResumePolicy())
			w.gate.setOnCarrierError(func() {
				w.parkForResume(context.Background())
			})
			go w.watchHalfOpen()
		}
		// Reply with our rx so the client replays exactly what we
		// missed, then flush our retained suffix past the client's rx.
		reply := FrameMetadata{SessionStatus: SessionStatusResume}
		reply.Option.Set(OptionData)
		rpayload := encodeResume(ResumePayload{Token: rp.Token, Epoch: rp.Epoch, RxCount: w.rx.Value()})
		if err := writeMetaWithFrame(w.link.Writer, reply, buf.MultiBuffer{rpayload}); err != nil {
			return err
		}
		replayed := 0
		if w.gate != nil {
			sent := w.gate.TxCount()
			if err := w.gate.flushSince(rp.RxCount); err != nil {
				return err
			}
			if sent > rp.RxCount {
				replayed = int(sent - rp.RxCount)
			}
		}
		errors.LogInfo(context.Background(), "mux resume: adopted token, replayed ", replayed, " frames")
		return nil
	}

	// Unknown token: fresh announce. Remember it, create the gate so all
	// later frames on this carrier are counted/retained, park on loss.
	w.resumeToken = rp.Token
	w.resumeEpoch = rp.Epoch
	w.resumeHasToken = true
	w.resumeUser = user
	if w.gate == nil {
		w.gate = newCarrierGate(w.link.Writer, w.done.Wait(), DefaultResumePolicy())
		w.gate.setOnCarrierError(func() {
			w.parkForResume(context.Background())
		})
		go w.watchHalfOpen()
	}
	return nil
}

func (w *ServerWorker) handleStatusAck(meta *FrameMetadata, reader *buf.BufferedReader) error {
	if !meta.Option.Has(OptionData) {
		return nil
	}
	mb, err := NewStreamReader(reader).ReadMultiBuffer()
	if err != nil {
		return err
	}
	defer buf.ReleaseMulti(mb)
	if len(mb) == 0 || len(mb[0].Bytes()) < ackPayloadLen {
		return errors.New("short ack payload")
	}
	ap, err := decodeAck(mb[0].Bytes())
	if err != nil {
		return err
	}
	if w.gate != nil {
		w.gate.ack(ap.RxCount)
	}
	return nil
}

// watchHalfOpen forces park when bytes sit unacked past AckTimeout.
func (w *ServerWorker) watchHalfOpen() {
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-w.done.Wait():
			return
		case <-t.C:
		}
		if w.gate == nil || w.done.Done() {
			continue
		}
		if w.gate.UnackedBytes() > 0 && time.Since(w.gate.lastAckRecv()) > 4*time.Second {
			// Park via a synthetic path: reuse run()'s park by closing
			// nothing, just parking directly.
			w.parkForResume(context.Background())
			return
		}
	}
}
