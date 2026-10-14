package xdrive

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/tls"
)

func liveQuantile(durs []time.Duration, q float64) time.Duration {
	if len(durs) == 0 {
		return 0
	}
	cp := append([]time.Duration(nil), durs...)
	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
	i := int(q*float64(len(cp)-1) + 0.5)
	if i < 0 {
		i = 0
	}
	if i >= len(cp) {
		i = len(cp) - 1
	}
	return cp[i].Round(time.Millisecond)
}

func liveRateLimited(status int, err error) bool {
	if status == http.StatusForbidden || status == http.StatusTooManyRequests {
		return true
	}
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "429") || strings.Contains(msg, "403") ||
		strings.Contains(msg, "rate limiting") || strings.Contains(msg, "rateLimitExceeded") ||
		strings.Contains(msg, "RESOURCE_EXHAUSTED")
}

func liveStepErr(status int, err error) string {
	if err == nil {
		if status == 0 || status == http.StatusOK {
			return ""
		}
		return fmt.Sprintf("status-%d", status)
	}
	if liveRateLimited(0, err) {
		return "ratelimit"
	}
	msg := strings.ReplaceAll(err.Error(), "\n", " ")
	if len(msg) > 80 {
		msg = msg[:80]
	}
	return "ERR " + msg
}

func liveParallel(n, workers int, fn func(i int) error) error {
	var wg sync.WaitGroup
	sem := make(chan struct{}, workers)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			errs[i] = fn(i)
		}(i)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

func liveDeleteAll(t *testing.T, s *driveStorage, prefix string) {
	t.Helper()
	found, err := s.query(context.Background(), "name contains '"+quoteDriveValue(flatten(prefix)+flatSeparator)+"'")
	if err != nil {
		t.Fatalf("cleanup list %s: %v", prefix, err)
	}
	flats := make([]string, 0, len(found))
	ids := make(map[string]string, len(found))
	for flat, file := range found {
		flats = append(flats, flat)
		ids[flat] = file.id
	}
	if err := liveParallel(len(flats), 16, func(i int) error {
		return s.deleteID(context.Background(), flats[i], ids[flats[i]])
	}); err != nil {
		t.Fatalf("cleanup delete %s: %v", prefix, err)
	}
}

func liveInlineBody(folder, flat, desc string) []byte {
	body, _ := json.Marshal(map[string]interface{}{
		"name": flat, "parents": []string{folder}, "description": desc,
	})
	return body
}

func liveMultipartBody(flat string, data []byte) (string, []byte) {
	metadata, _ := json.Marshal(map[string]interface{}{"name": flat})
	var body bytes.Buffer
	fmt.Fprintf(&body, "--%s\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n", driveBoundary)
	body.Write(metadata)
	fmt.Fprintf(&body, "\r\n--%s\r\nContent-Type: application/octet-stream\r\n\r\n", driveBoundary)
	body.Write(data)
	fmt.Fprintf(&body, "\r\n--%s--\r\n", driveBoundary)
	return "multipart/related; boundary=" + driveBoundary, body.Bytes()
}

func liveUpdateMeta(s *driveStorage, id, flat, desc string) (int, error) {
	body, _ := json.Marshal(map[string]interface{}{"name": flat, "description": desc})
	status, _, err := s.doMeta(context.Background(), http.MethodPatch, s.filesURL+"/"+url.PathEscape(id)+"?fields=id", "application/json; charset=UTF-8", body)
	return status, err
}

func liveUpdateMedia(s *driveStorage, id, flat string, data []byte) (int, error) {
	ctype, body := liveMultipartBody(flat, data)
	status, _, err := s.do(context.Background(), http.MethodPatch, strings.Split(driveUploadURL, "?")[0]+"/"+url.PathEscape(id)+"?uploadType=multipart", ctype, body)
	return status, err
}

func liveListURL(s *driveStorage, cond string) string {
	params := url.Values{
		"q":                         {"'" + quoteDriveValue(s.folder) + "' in parents and trashed = false and " + cond},
		"fields":                    {"nextPageToken,files(id,name,description)"},
		"pageSize":                  {fmt.Sprint(drivePageSize)},
		"supportsAllDrives":         {"true"},
		"includeItemsFromAllDrives": {"true"},
	}
	return s.filesURL + "?" + params.Encode()
}

func TestLiveDriveWritePrimitives(t *testing.T) {
	config := liveDriveConfig(t)
	ctx := context.Background()
	root := "streams/writelive"

	t.Logf("%-24s %-12s %-12s %-7s %-9s %-9s %-9s %s", "op", "extra", "n", "p50", "p95", "ok", "abort", "")

	newStorage := func(h2 bool) *driveStorage {
		t.Helper()
		var settings *internet.MemoryStreamConfig
		if h2 {
			settings = &internet.MemoryStreamConfig{
				SecuritySettings: &tls.Config{NextProtocol: []string{"h2"}},
			}
		}
		s, err := newDriveStorage(settings, config)
		if err != nil {
			t.Fatalf("newDriveStorage: %v", err)
		}
		return s
	}

	shared := newStorage(false)
	var stores []*driveStorage
	stores = append(stores, shared)
	defer func() {
		for _, s := range stores {
			s.Close()
		}
	}()
	defer liveDeleteAll(t, shared, root)

	data1k := make([]byte, 1024)
	if _, err := rand.Read(data1k); err != nil {
		t.Fatalf("rand: %v", err)
	}
	data16k := make([]byte, 16*1024)
	if _, err := rand.Read(data16k); err != nil {
		t.Fatalf("rand: %v", err)
	}
	desc1k := base64.StdEncoding.EncodeToString(data1k)
	desc16k := base64.StdEncoding.EncodeToString(data16k)

	measure := func(op, extra string, n int, fn func(i int) (int, error)) {
		var lat []time.Duration
		ok := 0
		abort := ""
		for i := 0; i < n; i++ {
			start := time.Now()
			status, err := fn(i)
			d := time.Since(start)
			if a := liveStepErr(status, err); a != "" {
				abort = a
				break
			}
			lat = append(lat, d)
			ok++
		}
		t.Logf("%-24s %-12s n=%-3d p50=%-9s p95=%-9s ok=%d/%d abort=%s", op, extra, n, liveQuantile(lat, 0.5), liveQuantile(lat, 0.95), ok, n, abort)
	}

	measure("create-inline", "1k", 30, func(i int) (int, error) {
		err := shared.putInline(ctx, fmt.Sprintf("%s/meta1k/%04d", root, i), data1k)
		return 0, err
	})
	liveDeleteAll(t, shared, root+"/meta1k")

	measure("create-multipart", "1k", 30, func(i int) (int, error) {
		err := shared.putMedia(ctx, fmt.Sprintf("%s/mp1k/%04d", root, i), data1k)
		return 0, err
	})
	liveDeleteAll(t, shared, root+"/mp1k")

	blankMeta := root + "/upd1k/blank"
	if err := shared.Put(ctx, blankMeta, nil); err != nil {
		t.Fatalf("blank create: %v", err)
	}
	blankMetaID, err := shared.resolveID(ctx, flatten(blankMeta))
	if err != nil {
		t.Fatalf("blank resolve: %v", err)
	}
	measure("update-meta", "1k", 30, func(i int) (int, error) {
		return liveUpdateMeta(shared, blankMetaID, flatten(blankMeta), desc1k)
	})
	liveDeleteAll(t, shared, root+"/upd1k")

	blankMedia := root + "/upm1k/blank"
	if err := shared.Put(ctx, blankMedia, nil); err != nil {
		t.Fatalf("blank create: %v", err)
	}
	blankMediaID, err := shared.resolveID(ctx, flatten(blankMedia))
	if err != nil {
		t.Fatalf("blank resolve: %v", err)
	}
	measure("update-media", "1k", 30, func(i int) (int, error) {
		return liveUpdateMedia(shared, blankMediaID, flatten(blankMedia), data1k)
	})
	liveDeleteAll(t, shared, root+"/upm1k")

	measure("create-inline", "16k", 30, func(i int) (int, error) {
		err := shared.putInline(ctx, fmt.Sprintf("%s/meta16k/%04d", root, i), data16k)
		return 0, err
	})
	liveDeleteAll(t, shared, root+"/meta16k")

	measure("create-multipart", "16k", 30, func(i int) (int, error) {
		err := shared.putMedia(ctx, fmt.Sprintf("%s/mp16k/%04d", root, i), data16k)
		return 0, err
	})
	liveDeleteAll(t, shared, root+"/mp16k")

	blankMeta16 := root + "/upd16k/blank"
	if err := shared.Put(ctx, blankMeta16, nil); err != nil {
		t.Fatalf("blank create: %v", err)
	}
	blankMeta16ID, err := shared.resolveID(ctx, flatten(blankMeta16))
	if err != nil {
		t.Fatalf("blank resolve: %v", err)
	}
	measure("update-meta", "16k", 30, func(i int) (int, error) {
		return liveUpdateMeta(shared, blankMeta16ID, flatten(blankMeta16), desc16k)
	})
	liveDeleteAll(t, shared, root+"/upd16k")

	blankMedia16 := root + "/upm16k/blank"
	if err := shared.Put(ctx, blankMedia16, nil); err != nil {
		t.Fatalf("blank create: %v", err)
	}
	blankMedia16ID, err := shared.resolveID(ctx, flatten(blankMedia16))
	if err != nil {
		t.Fatalf("blank resolve: %v", err)
	}
	measure("update-media", "16k", 30, func(i int) (int, error) {
		return liveUpdateMedia(shared, blankMedia16ID, flatten(blankMedia16), data16k)
	})
	liveDeleteAll(t, shared, root+"/upm16k")

	for _, rate := range []int{5, 10, 20, 40} {
		prefix := fmt.Sprintf("%s/burst%d", root, rate)
		interval := time.Second / time.Duration(rate)
		deadline := time.Now().Add(20 * time.Second)
		tick := time.NewTicker(interval)
		var mu sync.Mutex
		codes := map[int]int{}
		var lat []time.Duration
		abort := ""
		sent := 0
		var wg sync.WaitGroup
		for time.Now().Before(deadline) {
			<-tick.C
			mu.Lock()
			stopped := abort != ""
			mu.Unlock()
			if stopped {
				break
			}
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				opCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				defer cancel()
				start := time.Now()
				status, _, err := shared.doMeta(opCtx, http.MethodPost, shared.filesURL+"?fields=id", "application/json; charset=UTF-8", liveInlineBody(config.RemoteFolder, fmt.Sprintf("%s/%06d", flatten(prefix), i), desc1k))
				d := time.Since(start)
				mu.Lock()
				defer mu.Unlock()
				if abort == "" {
					abort = liveStepErr(status, err)
				}
				codes[status]++
				lat = append(lat, d)
			}(sent)
			sent++
		}
		tick.Stop()
		wg.Wait()
		mu.Lock()
		t.Logf("%-24s %-12s sent=%-4d codes=%v p50=%-9s p95=%-9s ok=%d/%d abort=%s", "burst-create", fmt.Sprintf("%dps", rate), sent, codes, liveQuantile(lat, 0.5), liveQuantile(lat, 0.95), len(lat), sent, abort)
		mu.Unlock()
		liveDeleteAll(t, shared, prefix)
	}

	for _, k := range []int{3, 16, 66, 130} {
		quoted := make([]string, 0, k)
		for i := 0; i < k; i++ {
			quoted = append(quoted, "name = '"+quoteDriveValue(fmt.Sprintf("probe~%d", i))+"'")
		}
		target := liveListURL(shared, "("+strings.Join(quoted, " or ")+")")
		var lat []time.Duration
		ok := 0
		abort := ""
		for i := 0; i < 30; i++ {
			start := time.Now()
			status, _, err := shared.doMeta(ctx, http.MethodGet, target, "", nil)
			d := time.Since(start)
			if a := liveStepErr(status, err); a != "" {
				abort = a
				break
			}
			lat = append(lat, d)
			ok++
		}
		t.Logf("%-24s %-12s n=%-3d urllen=%-5d p50=%-9s p95=%-9s ok=%d/%d abort=%s", "probe-shape", fmt.Sprintf("%d-names", k), 30, len(target), liveQuantile(lat, 0.5), liveQuantile(lat, 0.95), ok, 30, abort)
	}

	for _, n := range []int{0, 100, 1000} {
		prefix := fmt.Sprintf("%s/probe%d", root, n)
		if n > 0 {
			if err := liveParallel(n, 16, func(i int) error {
				return shared.Put(ctx, objectName(prefix, int64(i), segSuffix), []byte("x"))
			}); err != nil {
				t.Fatalf("populate %d: %v", n, err)
			}
		}
		for _, op := range []string{"probe", "list"} {
			var lat []time.Duration
			ok := 0
			abort := ""
			for i := 0; i < 10; i++ {
				start := time.Now()
				var err error
				if op == "probe" {
					_, err = shared.probe(ctx, prefix, 0)
					if err == errNotFound {
						err = nil
					}
				} else {
					_, err = shared.List(ctx, prefix)
				}
				d := time.Since(start)
				if a := liveStepErr(0, err); a != "" {
					abort = a
					break
				}
				lat = append(lat, d)
				ok++
			}
			t.Logf("%-24s %-12s n=%-3d p50=%-9s p95=%-9s ok=%d/%d abort=%s", op, fmt.Sprintf("files-%d", n), 10, liveQuantile(lat, 0.5), liveQuantile(lat, 0.95), ok, 10, abort)
		}
		liveDeleteAll(t, shared, prefix)
	}

	for _, size := range []int{8 * 1024, 12 * 1024, 16 * 1024, 20 * 1024, 24 * 1024, 32 * 1024} {
		prefix := fmt.Sprintf("%s/limit%dk", root, size/1024)
		data := make([]byte, size)
		if _, err := rand.Read(data); err != nil {
			t.Fatalf("rand: %v", err)
		}
		var lat []time.Duration
		ok := 0
		abort := ""
		descLen := 0
		consistent := true
		for i := 0; i < 10; i++ {
			name := fmt.Sprintf("%s/%02d", prefix, i)
			start := time.Now()
			err := shared.Put(ctx, name, data)
			d := time.Since(start)
			if a := liveStepErr(0, err); a != "" {
				abort = a
				break
			}
			found, err := shared.query(ctx, "name = '"+quoteDriveValue(flatten(name))+"'")
			if err != nil {
				if a := liveStepErr(0, err); a != "" {
					abort = a
					break
				}
			} else if file, present := found[flatten(name)]; present {
				if i == 0 {
					descLen = len(file.description)
				} else if len(file.description) != descLen {
					consistent = false
				}
			}
			lat = append(lat, d)
			ok++
		}
		extra := fmt.Sprintf("%dk-desc%d", size/1024, descLen)
		if !consistent {
			extra += "-mismatch"
		}
		t.Logf("%-24s %-12s n=%-3d p50=%-9s p95=%-9s ok=%d/%d abort=%s", "inline-limit", extra, 10, liveQuantile(lat, 0.5), liveQuantile(lat, 0.95), ok, 10, abort)
		liveDeleteAll(t, shared, prefix)
	}

	warm := map[bool]*driveStorage{false: shared}
	warmH2 := newStorage(true)
	stores = append(stores, warmH2)
	warm[true] = warmH2
	blankCold := root + "/coldblank/blank"
	if err := shared.Put(ctx, blankCold, nil); err != nil {
		t.Fatalf("blank create: %v", err)
	}
	blankColdID, err := shared.resolveID(ctx, flatten(blankCold))
	if err != nil {
		t.Fatalf("blank resolve: %v", err)
	}
	for _, h2 := range []bool{false, true} {
		proto := "h1"
		if h2 {
			proto = "h2"
		}
		for _, cold := range []bool{false, true} {
			conn := "warm"
			if cold {
				conn = "cold"
			}
			measureCold := func(op string, n int, fn func(s *driveStorage, i int) (int, error)) {
				var lat []time.Duration
				ok := 0
				abort := ""
				for i := 0; i < n; i++ {
					s := warm[h2]
					if cold {
						s = newStorage(h2)
					}
					start := time.Now()
					status, err := fn(s, i)
					d := time.Since(start)
					if cold {
						s.Close()
					}
					if a := liveStepErr(status, err); a != "" {
						abort = a
						break
					}
					lat = append(lat, d)
					ok++
				}
				t.Logf("%-24s %-12s n=%-3d p50=%-9s p95=%-9s ok=%d/%d abort=%s", op, proto+"-"+conn, n, liveQuantile(lat, 0.5), liveQuantile(lat, 0.95), ok, n, abort)
			}
			measureCold("create-inline", 30, func(s *driveStorage, i int) (int, error) {
				err := s.putInline(ctx, fmt.Sprintf("%s/%s-%s-c1k/%04d", root, proto, conn, i), data1k)
				return 0, err
			})
			measureCold("update-meta", 30, func(s *driveStorage, i int) (int, error) {
				return liveUpdateMeta(s, blankColdID, flatten(blankCold), desc1k)
			})
		}
	}
	liveDeleteAll(t, shared, root+"/coldblank")
	for _, h2 := range []bool{false, true} {
		for _, cold := range []bool{false, true} {
			proto := "h1"
			if h2 {
				proto = "h2"
			}
			conn := "warm"
			if cold {
				conn = "cold"
			}
			liveDeleteAll(t, shared, fmt.Sprintf("%s/%s-%s-c1k", root, proto, conn))
		}
	}
}
