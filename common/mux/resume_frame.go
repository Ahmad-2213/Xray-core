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

func decodeResume(b *buf.Buffer) (ResumePayload, error) {
	var p ResumePayload
	if b.Len() < resumePayloadLen {
		return p, errors.New("short resume payload: ", b.Len())
	}
	copy(p.Token[:], b.BytesRange(0, 16))
	p.Epoch = binary.BigEndian.Uint64(b.BytesRange(16, 24))
	p.RxCount = binary.BigEndian.Uint64(b.BytesRange(24, 32))
	return p, nil
}

func encodeAck(p AckPayload) *buf.Buffer {
	b := buf.New()
	b.Extend(int32(ackPayloadLen))
	binary.BigEndian.PutUint64(b.BytesRange(0, 8), p.RxCount)
	return b
}

func decodeAck(b *buf.Buffer) (AckPayload, error) {
	var p AckPayload
	if b.Len() < ackPayloadLen {
		return p, errors.New("short ack payload: ", b.Len())
	}
	p.RxCount = binary.BigEndian.Uint64(b.BytesRange(0, 8))
	return p, nil
}
