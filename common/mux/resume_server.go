package mux

// Server-side Resume/Ack handling. Resume arrives as the first frame of a
// fresh v2 carrier and rebinds a suspended worker's sessions; Ack only
// advances accounting in Phase 1 (no retain store yet).

import (
	"context"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/errors"
)

func serverUserOf(ctx context.Context) string {
	if in := ctx.Value("inboundUser"); in != nil {
		if s, ok := in.(string); ok {
			return s
		}
	}
	return ""
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
	w.rx.Next()

	user := serverUserOf(context.Background())

	if !w.resumeHasToken {
		// First sight: announce. Remember the token; park on carrier loss.
		w.resumeToken = rp.Token
		w.resumeEpoch = rp.Epoch
		w.resumeHasToken = true
		w.resumeUser = user
		return nil
	}

	// Rebind request on a fresh carrier: the token IS the lookup key.
	entry, ok := hsTake(rp.Token)
	if !ok {
		return errors.New("unknown or expired resume token")
	}
	if entry.user != "" && user != "" && entry.user != user {
		return errors.New("resume user mismatch")
	}
	if rp.Epoch <= entry.epoch {
		return errors.New("stale resume epoch")
	}
	if rp.RxCount > entry.tx {
		return errors.New("resume count beyond sent")
	}
	entry.manager.Reparent(w.sessionManager)
	w.resumeToken = rp.Token
	w.resumeEpoch = rp.Epoch
	w.resumeHasToken = true
	w.resumeUser = user
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
	buf.ReleaseMulti(mb)
	return nil
}

// tryAdoptResume is folded into handleStatusResume (first-frame announce /
// rebind). Kept as documentation of the ordering contract.
func (w *ServerWorker) tryAdoptResume(ctx context.Context, reader *buf.BufferedReader) bool {
	_ = ctx
	_ = reader
	_ = w
	return false
}
