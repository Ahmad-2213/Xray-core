package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	saltUp   = 0x1111111111111111
	saltDown = 0x2222222222222222
)

func ts() string { return time.Now().Format("15:04:05.000") }

func splitmix(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

func fill(p []byte, salt, off uint64) {
	for k := range p {
		p[k] = byte(splitmix((off+uint64(k))^salt) >> 24)
	}
}

func verify(p []byte, salt, off uint64) int {
	for k := range p {
		if p[k] != byte(splitmix((off+uint64(k))^salt)>>24) {
			return k
		}
	}
	return -1
}

// classifyMismatch tests whether the received bytes match the stream at
// offset-n*8192 (duplicate replay) or offset+n*8192 (skipped frames) for
// n up to 8, and dumps 64 bytes either way for manual correlation with
// the flush#/reattached log lines.
func classifyMismatch(p []byte, salt, off uint64) string {
	limit := len(p)
	if limit > 8192 {
		limit = 8192
	}
	head := p[:limit]
	for n := 1; n <= 8; n++ {
		d := uint64(n * 8192)
		if off >= d {
			ok := true
			for k := range head {
				if head[k] != byte(splitmix((off-d+uint64(k))^salt)>>24) {
					ok = false
					break
				}
			}
			if ok {
				return fmt.Sprintf("duplicate: matches offset-%d (frame -%d)", d, n)
			}
		}
		ok := true
		for k := range head {
			if head[k] != byte(splitmix((off+d+uint64(k))^salt)>>24) {
				ok = false
				break
			}
		}
		if ok {
			return fmt.Sprintf("skip: matches offset+%d (frame +%d)", d, n)
		}
	}
	return "unclassified"
}

func dump64(p []byte, salt, off uint64) string {
	n := len(p)
	if n > 64 {
		n = 64
	}
	actual := ""
	expect := ""
	for k := 0; k < n; k++ {
		actual += fmt.Sprintf("%02x", p[k])
		expect += fmt.Sprintf("%02x", byte(splitmix((off+uint64(k))^salt)>>24))
	}
	return fmt.Sprintf("actual=%s expect=%s", actual, expect)
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("usage: live sink|relay|check|churn [flags]")
		os.Exit(2)
	}
	switch os.Args[1] {
	case "sink":
		runSink(os.Args[2:])
	case "relay":
		runRelay(os.Args[2:])
	case "check":
		os.Exit(runCheck(os.Args[2:]))
	case "churn":
		os.Exit(runChurn(os.Args[2:]))
	default:
		os.Exit(2)
	}
}

// ---------- sink: verifies uplink, produces deterministic downlink ----------

func runSink(args []string) {
	fs := flag.NewFlagSet("sink", flag.ExitOnError)
	listen := fs.String("listen", "127.0.0.1:9000", "")
	fs.Parse(args)
	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		fmt.Println(err)
		os.Exit(2)
	}
	var ids atomic.Int64
	for {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		go sinkConn(c, ids.Add(1))
	}
}

func sinkConn(c net.Conn, id int64) {
	defer c.Close()
	hdr := make([]byte, 1)
	if _, err := io.ReadFull(c, hdr); err != nil {
		return
	}
	if hdr[0] == 2 {
		sinkFinite(c)
		return
	}
	bulk := hdr[0] == 1
	start := time.Now()
	stop := make(chan struct{})
	defer close(stop)
	var down atomic.Uint64
	go func() {
		buf := make([]byte, 8192)
		var off uint64
		next := time.Now()
		for {
			select {
			case <-stop:
				return
			default:
			}
			fill(buf, saltDown, off)
			if _, err := c.Write(buf); err != nil {
				return
			}
			off += uint64(len(buf))
			down.Store(off)
			if !bulk {
				next = next.Add(125 * time.Millisecond) // 64 KB/s
				time.Sleep(time.Until(next))
			}
		}
	}()
	buf := make([]byte, 64*1024)
	var off uint64
	for {
		n, err := c.Read(buf)
		if n > 0 {
			if i := verify(buf[:n], saltUp, off); i >= 0 {
				fmt.Printf("%s SINK MISMATCH conn=%d bulk=%v offset=%d\n", ts(), id, bulk, off+uint64(i))
				return
			}
			off += uint64(n)
		}
		if err != nil {
			fmt.Printf("%s SINK conn=%d bulk=%v closed after %s up=%d down=%d err=%v\n",
				ts(), id, bulk, time.Since(start).Round(time.Millisecond), off, down.Load(), err)
			return
		}
	}
}

// ---------- relay: forwards TCP, RSTs all carriers on a timer ----------

type rconn struct {
	a, b   *net.TCPConn
	paused atomic.Bool
	dead   atomic.Bool
}

func runRelay(args []string) {
	fs := flag.NewFlagSet("relay", flag.ExitOnError)
	listen := fs.String("listen", "127.0.0.1:20002", "")
	target := fs.String("target", "127.0.0.1:20001", "")
	first := fs.Duration("first", 12*time.Second, "delay before first kill")
	every := fs.Duration("every", 15*time.Second, "kill interval (0 = never)")
	stall := fs.Duration("stall", 0, "freeze forwarding this long before RST (half-open)")
	fs.Parse(args)
	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		fmt.Println(err)
		os.Exit(2)
	}
	var mu sync.Mutex
	set := map[*rconn]struct{}{}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				d, err := net.Dial("tcp", *target)
				if err != nil {
					c.Close()
					return
				}
				r := &rconn{a: c.(*net.TCPConn), b: d.(*net.TCPConn)}
				mu.Lock()
				set[r] = struct{}{}
				mu.Unlock()
				var wg sync.WaitGroup
				pump := func(dst, src *net.TCPConn) {
					defer wg.Done()
					buf := make([]byte, 32*1024)
					for {
						n, err := src.Read(buf)
						if n > 0 {
							for r.paused.Load() && !r.dead.Load() {
								time.Sleep(10 * time.Millisecond)
							}
							if _, werr := dst.Write(buf[:n]); werr != nil {
								break
							}
						}
						if err != nil {
							break
						}
					}
					dst.Close()
					src.Close()
				}
				wg.Add(2)
				go pump(r.b, r.a)
				go pump(r.a, r.b)
				wg.Wait()
				mu.Lock()
				delete(set, r)
				mu.Unlock()
			}(c)
		}
	}()
	var kills atomic.Int64
	kill := func() {
		mu.Lock()
		list := make([]*rconn, 0, len(set))
		for r := range set {
			list = append(list, r)
		}
		mu.Unlock()
		if len(list) == 0 {
			return
		}
		fmt.Printf("%s KILL #%d conns=%d stall=%s\n", ts(), kills.Add(1), len(list), *stall)
		if *stall > 0 {
			for _, r := range list {
				r.paused.Store(true)
			}
			time.Sleep(*stall)
		}
		for _, r := range list {
			r.dead.Store(true)
			r.a.SetLinger(0)
			r.b.SetLinger(0)
			r.a.Close()
			r.b.Close()
		}
	}
	time.Sleep(*first)
	if *every <= 0 {
		select {}
	}
	for {
		kill()
		time.Sleep(*every)
	}
}

// ---------- check: SOCKS client, byte-exact both directions ----------

func socksDial(socks, target string) (net.Conn, error) {
	c, err := net.DialTimeout("tcp", socks, 5*time.Second)
	if err != nil {
		return nil, err
	}
	fail := func(e error) (net.Conn, error) { c.Close(); return nil, e }
	if _, err := c.Write([]byte{5, 1, 0}); err != nil {
		return fail(err)
	}
	r := make([]byte, 2)
	if _, err := io.ReadFull(c, r); err != nil || r[1] != 0 {
		return fail(fmt.Errorf("socks greeting failed: %v %v", err, r))
	}
	host, ps, err := net.SplitHostPort(target)
	if err != nil {
		return fail(err)
	}
	port, _ := strconv.Atoi(ps)
	ip := net.ParseIP(host).To4()
	if ip == nil {
		return fail(fmt.Errorf("target must be IPv4"))
	}
	req := append([]byte{5, 1, 0, 1}, ip...)
	req = append(req, byte(port>>8), byte(port))
	if _, err := c.Write(req); err != nil {
		return fail(err)
	}
	h := make([]byte, 4)
	if _, err := io.ReadFull(c, h); err != nil || h[1] != 0 {
		return fail(fmt.Errorf("socks connect failed: %v %v", err, h))
	}
	skip := 0
	switch h[3] {
	case 1:
		skip = 6
	case 4:
		skip = 18
	case 3:
		l := make([]byte, 1)
		if _, err := io.ReadFull(c, l); err != nil {
			return fail(err)
		}
		skip = int(l[0]) + 2
	}
	if _, err := io.ReadFull(c, make([]byte, skip)); err != nil {
		return fail(err)
	}
	return c, nil
}

func checkOne(socks, target string, deadline time.Time, bulk bool, readDelay time.Duration) (uint64, uint64, time.Duration, error) {
	c, err := socksDial(socks, target)
	if err != nil {
		return 0, 0, 0, err
	}
	defer c.Close()
	hdr := []byte{0}
	if bulk {
		hdr[0] = 1
	}
	if _, err := c.Write(hdr); err != nil {
		return 0, 0, 0, err
	}
	var sent, got atomic.Uint64
	var maxGap atomic.Int64
	errc := make(chan error, 2)
	go func() {
		buf := make([]byte, 8192)
		var off uint64
		next := time.Now()
		for time.Now().Before(deadline) {
			fill(buf, saltUp, off)
			if _, err := c.Write(buf); err != nil {
				errc <- fmt.Errorf("write failed after %d bytes: %w", off, err)
				return
			}
			off += uint64(len(buf))
			sent.Store(off)
			if !bulk {
				next = next.Add(125 * time.Millisecond)
				time.Sleep(time.Until(next))
			}
		}
		errc <- nil
	}()
	go func() {
		buf := make([]byte, 64*1024)
		var off uint64
		last := time.Now()
		for time.Now().Before(deadline) {
			n, err := c.Read(buf)
			now := time.Now()
			if n > 0 {
				if i := verify(buf[:n], saltDown, off); i >= 0 {
					badOff := off + uint64(i)
					errc <- fmt.Errorf("DOWNLINK MISMATCH at offset %d [%s] [%s] at %s (correlate reattached +-2s in client-error.log)",
						badOff, classifyMismatch(buf[i:n], saltDown, badOff), dump64(buf[i:n], saltDown, badOff), ts())
					return
				}
				off += uint64(n)
				got.Store(off)
				if g := now.Sub(last).Nanoseconds(); g > maxGap.Load() {
					maxGap.Store(g)
				}
				last = now
				if readDelay > 0 {
					time.Sleep(readDelay)
				}
			}
			if err != nil {
				errc <- fmt.Errorf("read failed after %d bytes: %w", off, err)
				return
			}
		}
		errc <- nil
	}()
	e := <-errc
	return sent.Load(), got.Load(), time.Duration(maxGap.Load()), e
}

func runCheck(args []string) int {
	fs := flag.NewFlagSet("check", flag.ExitOnError)
	socks := fs.String("socks", "127.0.0.1:10808", "")
	target := fs.String("target", "127.0.0.1:9000", "")
	dur := fs.Duration("dur", 75*time.Second, "")
	steady := fs.Int("steady", 2, "game-like 64KB/s connections")
	bulk := fs.Int("bulk", 1, "unthrottled connections")
	readDelay := fs.Duration("readDelay", 0, "sleep after each app-side read (throttled reader)")
	fs.Parse(args)
	deadline := time.Now().Add(*dur)
	var wg sync.WaitGroup
	var failed atomic.Bool
	for i := 0; i < *steady+*bulk; i++ {
		wg.Add(1)
		go func(id int, isBulk bool) {
			defer wg.Done()
			sent, got, gap, err := checkOne(*socks, *target, deadline, isBulk, *readDelay)
			if err == nil && (sent == 0 || got == 0) {
				err = fmt.Errorf("no data flowed")
			}
			if err != nil {
				failed.Store(true)
			}
			fmt.Printf("%s conn=%d bulk=%v sent=%d got=%d maxGap=%s err=%v\n",
				ts(), id, isBulk, sent, got, gap.Round(time.Millisecond), err)
		}(i, i >= *steady)
	}
	wg.Wait()
	if failed.Load() {
		fmt.Println("RESULT FAIL")
		return 1
	}
	fmt.Println("RESULT PASS")
	return 0
}

// ---------- churn: many short finite connections, New/End/half-close ----------

func sinkFinite(c net.Conn) {
	var h [8]byte
	if _, err := io.ReadFull(c, h[:]); err != nil {
		return
	}
	up, down := uint64(binary.BigEndian.Uint32(h[:4])), uint64(binary.BigEndian.Uint32(h[4:]))
	b := make([]byte, 8192)
	for off := uint64(0); off < up; {
		n := min(uint64(len(b)), up-off)
		if _, err := io.ReadFull(c, b[:n]); err != nil {
			return
		}
		if i := verify(b[:n], saltUp, off); i >= 0 {
			fmt.Printf("%s SINK MISMATCH finite offset=%d\n", ts(), off+uint64(i))
			return
		}
		off += n
	}
	for off := uint64(0); off < down; {
		n := min(uint64(len(b)), down-off)
		fill(b[:n], saltDown, off)
		if _, err := c.Write(b[:n]); err != nil {
			return
		}
		off += n
	}
}

func runChurn(args []string) int {
	fs := flag.NewFlagSet("churn", flag.ExitOnError)
	socks := fs.String("socks", "127.0.0.1:10808", "")
	target := fs.String("target", "127.0.0.1:9000", "")
	dur := fs.Duration("dur", 120*time.Second, "")
	rate := fs.Int("rate", 20, "new connections per second")
	fs.Parse(args)
	var ok, cerr, bad atomic.Int64
	var wg sync.WaitGroup
	rng := rand.New(rand.NewSource(1))
	tick := time.NewTicker(time.Second / time.Duration(*rate))
	defer tick.Stop()
	for end := time.Now().Add(*dur); time.Now().Before(end); {
		<-tick.C
		up, down := uint32(100+rng.Intn(100000)), uint32(100+rng.Intn(200000))
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := socksDial(*socks, *target)
			if err != nil {
				cerr.Add(1)
				return
			}
			defer c.Close()
			c.SetDeadline(time.Now().Add(60 * time.Second))
			h := make([]byte, 9)
			h[0] = 2
			binary.BigEndian.PutUint32(h[1:], up)
			binary.BigEndian.PutUint32(h[5:], down)
			if _, err := c.Write(h); err != nil {
				cerr.Add(1)
				return
			}
			b := make([]byte, 8192)
			for off := uint64(0); off < uint64(up); {
				n := min(uint64(len(b)), uint64(up)-off)
				fill(b[:n], saltUp, off)
				if _, err := c.Write(b[:n]); err != nil {
					cerr.Add(1)
					return
				}
				off += n
			}
			for off := uint64(0); off < uint64(down); {
				n := min(uint64(len(b)), uint64(down)-off)
				if _, err := io.ReadFull(c, b[:n]); err != nil {
					cerr.Add(1)
					return
				}
				if verify(b[:n], saltDown, off) >= 0 {
					bad.Add(1)
					return
				}
				off += n
			}
			ok.Add(1)
		}()
	}
	wg.Wait()
	fmt.Printf("%s churn ok=%d connErr=%d mismatch=%d\n", ts(), ok.Load(), cerr.Load(), bad.Load())
	if bad.Load() > 0 || ok.Load() == 0 {
		fmt.Println("RESULT FAIL")
		return 1
	}
	fmt.Println("RESULT PASS")
	return 0
}
