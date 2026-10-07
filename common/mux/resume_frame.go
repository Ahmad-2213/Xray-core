package mux

// Resume/Ack wire payloads. v1 metadata bytes are untouched; Resume and Ack
// are new SessionStatus values (0x05/0x06) carrying fixed-size payloads after
// the metadata frame. Old peers hit the "unknown status" path and fail fast.

import (
	"encoding/binary"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/errors"
)

// ResumePayload binds a new carrier to a suspended worker.
type ResumePayload struct {
	Token   [16]byte
	Epoch   uint64
	RxCount uint64
}

const resumePayloadLen = 16 + 8 + 8

// AckPayload reports the receiver's cumulative counted-frame count.
type AckPayload struct {
	RxCount uint64
}

const ackPayloadLen = 8

func encodeResume(p ResumePayload) *buf.Buffer {
	b := buf.New()
	b.Extend(int32(resumePayloadLen))
	copy(b.BytesTo(16), p.Token[:])
	binary.BigEndian.PutUint64(b.BytesRange(16, 24), p.Epoch)
	binary.BigEndian.PutUint64(b.BytesRange(24, 32), p.RxCount)
	return b
}

func decodeResume(p []byte) (ResumePayload, error) {
	var r ResumePayload
	if len(p) < resumePayloadLen {
		return r, errors.New("short resume payload: ", len(p))
	}
	copy(r.Token[:], p[:16])
	r.Epoch = binary.BigEndian.Uint64(p[16:24])
	r.RxCount = binary.BigEndian.Uint64(p[24:32])
	return r, nil
}

func encodeAck(p AckPayload) *buf.Buffer {
	b := buf.New()
	b.Extend(int32(ackPayloadLen))
	binary.BigEndian.PutUint64(b.BytesRange(0, 8), p.RxCount)
	return b
}

func decodeAck(p []byte) (AckPayload, error) {
	var a AckPayload
	if len(p) < ackPayloadLen {
		return a, errors.New("short ack payload: ", len(p))
	}
	a.RxCount = binary.BigEndian.Uint64(p[:8])
	return a, nil
}

// Exported for unit tests (package mux_test).
func DecodeResumeForTest(p []byte) (ResumePayload, error) { return decodeResume(p) }
func DecodeAckForTest(p []byte) (AckPayload, error)       { return decodeAck(p) }
