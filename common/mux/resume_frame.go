package mux

// Resume/Ack wire payloads. v1 metadata bytes are untouched; Resume and Ack
// are new SessionStatus values (0x05/0x06) carrying fixed-size payloads after
// the metadata frame. Old peers hit the "unknown status" path and fail fast.

import (
	"encoding/binary"
	"hash/crc32"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/errors"
)

// ResumePayload binds a new carrier to a suspended worker. Hash carries
// the sender's rolling content-chain tip at RxCount (see chainFP);
// HasHash reports whether it was present on the wire (peers from before
// the extension send 32 bytes: check skipped, fail open).
type ResumePayload struct {
	Token   [16]byte
	Epoch   uint64
	RxCount uint64
	Hash    uint64
	HasHash bool // wire presence only, never encoded
}

const resumePayloadLen = 16 + 8 + 8 + 8

// minResumePayloadLen accepts pre-extension 32-byte resumes (no hash).
const minResumePayloadLen = 16 + 8 + 8

// AckPayload reports the receiver's cumulative counted-frame count and
// content tip. Hash extends the drift tripwire to every ack (fail
// within one ack interval instead of only at rebind); absent on
// pre-extension 8-byte acks (check skipped, fail open).
type AckPayload struct {
	RxCount uint64
	Hash    uint64
	HasHash bool // wire presence only, never encoded
}

const ackPayloadLen = 8 + 8

// minAckPayloadLen accepts pre-extension 8-byte acks (no hash).
const minAckPayloadLen = 8

func encodeResume(p ResumePayload) *buf.Buffer {
	size := minResumePayloadLen
	if p.HasHash {
		size = resumePayloadLen
	}
	b := buf.New()
	b.Extend(int32(size))
	copy(b.BytesTo(16), p.Token[:])
	binary.BigEndian.PutUint64(b.BytesRange(16, 24), p.Epoch)
	binary.BigEndian.PutUint64(b.BytesRange(24, 32), p.RxCount)
	if p.HasHash {
		binary.BigEndian.PutUint64(b.BytesRange(32, 40), p.Hash)
	}
	return b
}

func decodeResume(p []byte) (ResumePayload, error) {
	var r ResumePayload
	if len(p) < minResumePayloadLen {
		return r, errors.New("short resume payload: ", len(p))
	}
	copy(r.Token[:], p[:16])
	r.Epoch = binary.BigEndian.Uint64(p[16:24])
	r.RxCount = binary.BigEndian.Uint64(p[24:32])
	if len(p) >= resumePayloadLen {
		r.Hash = binary.BigEndian.Uint64(p[32:40])
		r.HasHash = true
	}
	return r, nil
}

func encodeAck(p AckPayload) *buf.Buffer {
	size := minAckPayloadLen
	if p.HasHash {
		size = ackPayloadLen
	}
	b := buf.New()
	b.Extend(int32(size))
	binary.BigEndian.PutUint64(b.BytesRange(0, 8), p.RxCount)
	if p.HasHash {
		binary.BigEndian.PutUint64(b.BytesRange(8, 16), p.Hash)
	}
	return b
}

func decodeAck(p []byte) (AckPayload, error) {
	var a AckPayload
	if len(p) < minAckPayloadLen {
		return a, errors.New("short ack payload: ", len(p))
	}
	a.RxCount = binary.BigEndian.Uint64(p[:8])
	if len(p) >= ackPayloadLen {
		a.Hash = binary.BigEndian.Uint64(p[8:16])
		a.HasHash = true
	}
	return a, nil
}

// chainFP folds one counted frame into a cumulative content hash. Both
// ends feed identical (seq, sid, payloadLen, payloadCRC) for the same
// logical frame, so any divergence — reordered, duplicated, skipped or
// altered bytes — flips all downstream values and a single tip
// comparison detects drift. Payload only (post-metadata bytes), which
// readFullFrame reassembly reproduces exactly on both ends.
func chainFP(prev, seq uint64, sid uint16, ln int, crc uint32) uint64 {
	h := prev
	mix := func(x uint64) {
		h ^= x
		h *= 1099511628211
	}
	mix(seq)
	mix(uint64(sid))
	mix(uint64(ln))
	mix(uint64(crc))
	return h
}

// fpDigest returns the payload length and CRC for chaining. Nil (End,
// payload-less) digests as empty on both ends.
func fpDigest(mb buf.MultiBuffer) (int, uint32) {
	if mb == nil {
		return 0, 0
	}
	h := crc32.NewIEEE()
	ln := 0
	for _, b := range mb {
		by := b.Bytes()
		ln += len(by)
		h.Write(by)
	}
	return ln, h.Sum32()
}
