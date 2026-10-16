package xdrive

import (
	"encoding/base64"
	"encoding/binary"
	"hash/crc32"
	"testing"
)

func TestSlotFrameRoundTrip(t *testing.T) {
	data := []byte("frame payload")
	f, ok := decodeSlot(encodeSlot(newInlineFrame(7, data)))
	if !ok {
		t.Fatal("inline frame rejected")
	}
	if f.Seq != 7 || f.Mode != slotModeInline || string(f.Data) != string(data) {
		t.Fatalf("inline frame = %+v", f)
	}
	if f.Len != len(data) || f.Crc != crc32.ChecksumIEEE(data) || !f.Checked {
		t.Fatalf("inline meta = %+v", f)
	}

	m, ok := decodeSlot(encodeSlot(newMediaFrame(9, data)))
	if !ok {
		t.Fatal("media frame rejected")
	}
	if m.Seq != 9 || m.Mode != slotModeMedia || m.Len != len(data) || m.Crc != crc32.ChecksumIEEE(data) || !m.Checked {
		t.Fatalf("media frame = %+v", m)
	}
	if len(m.Data) != 0 {
		t.Fatalf("media frame carries %d inline bytes", len(m.Data))
	}
}

func TestSlotFrameLegacy(t *testing.T) {
	data := []byte("legacy")
	raw := make([]byte, 0, 32)
	raw = append(raw, slotMagicV1...)
	var tmp [8]byte
	binary.BigEndian.PutUint64(tmp[:], 4)
	raw = append(raw, tmp[:]...)
	raw = append(raw, slotModeInline)
	var ln [4]byte
	binary.BigEndian.PutUint32(ln[:], uint32(len(data)))
	raw = append(raw, ln[:]...)
	raw = append(raw, data...)
	f, ok := decodeSlot(base64.StdEncoding.EncodeToString(raw))
	if !ok {
		t.Fatal("legacy inline frame rejected")
	}
	if f.Seq != 4 || string(f.Data) != string(data) || f.Checked {
		t.Fatalf("legacy frame = %+v", f)
	}
}

func TestSlotFrameTruncated(t *testing.T) {
	full, err := base64.StdEncoding.DecodeString(encodeSlot(newInlineFrame(1, []byte("truncated"))))
	if err != nil {
		t.Fatalf("decode test vector: %v", err)
	}
	if _, ok := decodeSlot(base64.StdEncoding.EncodeToString(full[:len(full)-3])); ok {
		t.Fatal("truncated inline frame accepted")
	}
	if _, ok := decodeSlot(encodeSlot(newInlineFrame(2, nil))); ok {
		t.Fatal("empty inline frame accepted")
	}
	raw, err := base64.StdEncoding.DecodeString(encodeSlot(newInlineFrame(3, []byte("crc"))))
	if err != nil {
		t.Fatalf("decode test vector: %v", err)
	}
	raw[len(raw)-1] ^= 0xff
	if _, ok := decodeSlot(base64.StdEncoding.EncodeToString(raw)); ok {
		t.Fatal("corrupt inline frame accepted")
	}
	if _, ok := decodeSlot("!!!"); ok {
		t.Fatal("non-base64 accepted")
	}
}

func TestSlotFrameEnd(t *testing.T) {
	f, ok := decodeSlot(encodeSlot(slotFrame{Seq: 5, Mode: slotModeEnd}))
	if !ok {
		t.Fatal("end frame rejected")
	}
	if f.Seq != 5 || f.Mode != slotModeEnd {
		t.Fatalf("end frame = %+v", f)
	}
}
