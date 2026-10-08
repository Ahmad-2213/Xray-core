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
// A trailing ack is scheduled when rate-limited with unacked rx, so a
// burst's last frame is never left unacked: without it the peer's
// half-open detector would trip on a healthy-but-quiet carrier.
func (w *ServerWorker) maybeSendAck() {
	if w.gate.Load() == nil {
		return
	}
	intervalMs := int64(500)
	interval := time.Duration(intervalMs) * time.Millisecond
	now := time.Now()
	w.ackMu.mu.Lock()
	count := w.rx.Value()
	advanced := count != w.ackMu.lastSent
	// Frame-count trigger: bulk traffic must not wait out the whole
	// timer per window; combined with the timer below this bounds both
	// ack latency and ack rate. 8 frames ≈ a quarter of the default
	// per-stream window, so a lone bulk stream still acks promptly.
	if advanced && (count-w.ackMu.lastSent >= 8 || now.Sub(w.ackMu.lastTime) >= interval) {
		w.ackMu.lastSent = count
		w.ackMu.lastTime = now
		w.ackMu.mu.Unlock()
		if g := w.gate.Load(); g != nil {
			g.writeAck(count)
		}
		return
	}
	if advanced && !w.ackMu.pending {
		w.ackMu.pending = true
		wait := interval - now.Sub(w.ackMu.lastTime)
		w.ackMu.mu.Unlock()
		time.AfterFunc(wait, w.sendTrailingAck)
		return
	}
	w.ackMu.mu.Unlock()
}

// sendTrailingAck emits the delayed ack scheduled by maybeSendAck.
func (w *ServerWorker) sendTrailingAck() {
	w.ackMu.mu.Lock()
	w.ackMu.pending = false
	count := w.rx.Value()
	if count == w.ackMu.lastSent {
		w.ackMu.mu.Unlock()
		return
	}
	w.ackMu.lastSent = count
	w.ackMu.lastTime = time.Now()
	w.ackMu.mu.Unlock()
	if g := w.gate.Load(); g != nil {
		g.writeAck(count)
	}
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

	// Rebind takes precedence over announce: a fresh carrier bearing a
	// parked token reattaches sessions. Validated and consumed under a
	// single lock so concurrent rebinds can't both pass.
	entry, found, err := hsAdopt(rp.Token, func(e *suspendedWorker) error {
		return validateRebind(e.tx, e.epoch, e.user, rp, w.localUser)
	})
	if err != nil {
		return err
	}
	if found {
		// Adopt the parked table, gate (with retain store) and rx baseline.
		entry.manager.Reparent(w.sessionManager.Load())
		w.rx.Set(entry.rx)
		w.rsMu.Lock()
		if entry.gate != nil {
			w.gate.Store(entry.gate)
			entry.gate.swapTarget(w.link.Writer)
			if entry.done != nil {
				entry.gate.swapDone(entry.done.wait())
			}
		}
		w.resumeToken = rp.Token
		w.resumeEpoch = rp.Epoch
		w.resumeHasToken = true
		w.resumeUser = entry.user
		w.resumeDone = entry.done
		if w.gate.Load() == nil {
			g := newCarrierGate(w.link.Writer, w.done.Wait(), DefaultResumePolicy())
			if w.resumeDone != nil {
				g.swapDone(w.resumeDone.wait())
			}
			w.gate.Store(g)
		}
		w.rsMu.Unlock()
		// Re-arm onto this worker: the adopted gate still points at the
		// parking worker's park callback (a no-op on its empty manager),
		// which would leave write errors spinning instead of parking.
		if g := w.gate.Load(); g != nil {
			g.setOnCarrierError(func() {
				w.parkForResume(context.Background())
			})
			go w.watchHalfOpen()
		}
		// Reply with our rx so the client replays exactly what we
		// missed, then flush our retained suffix past the client's rx,
		// and only then unsuspend: live writes must never precede the
		// replay, or the peer double-counts.
		reply := FrameMetadata{SessionStatus: SessionStatusResume}
		reply.Option.Set(OptionData)
		rpayload := encodeResume(ResumePayload{Token: rp.Token, Epoch: rp.Epoch, RxCount: w.rx.Value()})
		if err := writeMetaWithFrame(w.link.Writer, reply, buf.MultiBuffer{rpayload}); err != nil {
			return err
		}
		replayed := 0
		// Flush runs off the reader loop (bidirectional unacked data
		// above the pipe buffers would deadlock two inline flushes).
		// The carrier stays readable meanwhile.
		go func() {
			if g := w.gate.Load(); g != nil {
				sent := g.TxCount()
				if err := g.flushSince(rp.RxCount); err != nil {
					errors.LogInfoInner(context.Background(), err, "mux resume: rebind flush failed")
					return
				}
				// Free what the peer confirms and advance the ack
				// clock: retention would otherwise pin memory and the
				// half-open detector would re-trip right after rebind.
				g.ack(rp.RxCount)
				if sent > rp.RxCount {
					replayed = int(sent - rp.RxCount)
				}
			}
			if g := w.gate.Load(); g != nil {
				g.setSuspended(false)
			}
			errors.LogInfo(context.Background(), "mux resume: adopted token ", tokenString(rp.Token), " epoch ", rp.Epoch, " replayed ", replayed, " frames")
		}()
		return nil
	}

	// Unknown token: either a fresh announce, or a redial racing a park
	// (half-open: the old carrier still looks alive server-side). Answer
	// with epoch 0 ("nothing parked") instead of silence so the client
	// retries on cadence instead of stalling one redial for 8s. The client
	// ignores epoch-0 replies without resuming.
	w.rsMu.Lock()
	w.resumeToken = rp.Token
	w.resumeEpoch = rp.Epoch
	w.resumeHasToken = true
	w.resumeUser = w.localUser
	if w.gate.Load() == nil {
		g := newCarrierGate(w.link.Writer, w.done.Wait(), DefaultResumePolicy())
		if w.resumeDone == nil {
			w.resumeDone = newTokenDone()
		}
		g.swapDone(w.resumeDone.wait())
		g.setOnCarrierError(func() {
			w.parkForResume(context.Background())
		})
		w.gate.Store(g)
		go w.watchHalfOpen()
	}
	w.rsMu.Unlock()
	reply := FrameMetadata{SessionStatus: SessionStatusResume}
	reply.Option.Set(OptionData)
	rpayload := encodeResume(ResumePayload{Token: rp.Token, Epoch: 0, RxCount: w.rx.Value()})
	if err := writeMetaWithFrame(w.link.Writer, reply, buf.MultiBuffer{rpayload}); err != nil {
		return err
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
	if g := w.gate.Load(); g != nil {
		g.ack(ap.RxCount)
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
		if w.gate.Load() == nil || w.done.Done() {
			continue
		}
		g := w.gate.Load()
		idle := time.Since(g.lastAckRecv())
		if halfOpenTripped(g.UnackedBytes(), idle, g.unackedAge(), 4*time.Second) {
			// Park via a synthetic path: reuse run()'s park by closing
			// nothing, just parking directly.
			w.parkForResume(context.Background())
			return
		}
	}
}
