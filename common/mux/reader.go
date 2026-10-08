package mux

import (
	"io"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/crypto"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/serial"
)

// PacketReader is an io.Reader that reads whole chunk of Mux frames every time.
type PacketReader struct {
	reader io.Reader
	eof    bool
	dest   *net.Destination
}

// NewPacketReader creates a new PacketReader.
func NewPacketReader(reader io.Reader, dest *net.Destination) *PacketReader {
	return &PacketReader{
		reader: reader,
		eof:    false,
		dest:   dest,
	}
}

// ReadMultiBuffer implements buf.Reader.
func (r *PacketReader) ReadMultiBuffer() (buf.MultiBuffer, error) {
	if r.eof {
		return nil, io.EOF
	}

	size, err := serial.ReadUint16(r.reader)
	if err != nil {
		return nil, err
	}

	if size > buf.Size {
		return nil, errors.New("packet size too large: ", size)
	}

	b := buf.New()
	if _, err := b.ReadFullFrom(r.reader, int32(size)); err != nil {
		b.Release()
		return nil, err
	}
	r.eof = true
	if r.dest != nil && r.dest.Network == net.Network_UDP {
		b.UDP = r.dest
	}
	return buf.MultiBuffer{b}, nil
}

// NewStreamReader creates a new StreamReader.
func NewStreamReader(reader *buf.BufferedReader) buf.Reader {
	return crypto.NewChunkStreamReaderWithChunkCount(crypto.PlainChunkSizeParser{}, reader, 1)
}

// ReadFullFrameForTest exposes readFullFrame to unit tests.
func ReadFullFrameForTest(rr buf.Reader) (buf.MultiBuffer, error) {
	return readFullFrame(rr)
}

// readFullFrame accumulates one framed payload before delivery.
// ChunkStreamReaders may return it piece-wise (ReadAtMost) while keeping
// the remainder in per-reader state, and PacketReaders signal end with
// EOF after the single packet. Delivering a prefix before the carrier
// dies would duplicate it on replay (and counting it would lose the
// suffix instead) — either kills the inner stream. So nothing is
// delivered until the frame is whole (EOF, including the empty-chunk
// case) or known dead (any other error discards the prefix).
// Accumulation is capped at buf.Size (our writer never emits bigger
// chunks; PacketReader enforces the same bound): anything larger is a
// corrupt or hostile peer, fail it instead of growing memory.
// errFrameTooLarge aborts accumulation past buf.Size (our writer never
// emits bigger chunks; PacketReader enforces the same bound). Unlike
// transport loss it must fail fast, never park: callers must not wrap
// it as frameReadError, or the reader would spin on a desynced carrier.
var errFrameTooLarge = errors.New("frame exceeds size bound")

func readFullFrame(rr buf.Reader) (buf.MultiBuffer, error) {
	var mb buf.MultiBuffer
	for {
		part, err := rr.ReadMultiBuffer()
		mb = append(mb, part...)
		if mb.Len() > buf.Size {
			buf.ReleaseMulti(mb)
			return nil, errFrameTooLarge
		}
		if err != nil {
			if errors.Cause(err) == io.EOF {
				return mb, nil
			}
			buf.ReleaseMulti(mb)
			return nil, err
		}
	}
}
