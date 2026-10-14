package xdrive

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	stdnet "net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/transport/internet"
)

type fakeFile struct {
	id          string
	name        string
	data        []byte
	description string
}

type fakeDrive struct {
	server *httptest.Server

	mu            sync.Mutex
	files         map[string]*fakeFile
	nextID        int
	failOnce      map[string]bool
	failStatus    map[string]int
	failBody      map[string]string
	tokens        int
	hosts         map[string]bool
	addrs         map[string]bool
	orLists       int
	containsLists int
	lastQuery     string
	listDelay     time.Duration
	listDelayNext time.Duration
	createStatus  int
	mediaUploads  int
}

func newFakeDrive(t *testing.T) *fakeDrive {
	t.Helper()

	drive := &fakeDrive{
		files:      make(map[string]*fakeFile),
		failOnce:   make(map[string]bool),
		failStatus: make(map[string]int),
		failBody:   make(map[string]string),
		hosts:      make(map[string]bool),
		addrs:      make(map[string]bool),
	}
	mux := http.NewServeMux()
	mux.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	record := func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			drive.mu.Lock()
			drive.hosts[r.Host] = true
			drive.addrs[r.RemoteAddr] = true
			drive.mu.Unlock()
			next(w, r)
		}
	}
	mux.HandleFunc("/token", record(drive.handleToken))
	mux.HandleFunc("/upload", record(drive.handleUpload))
	mux.HandleFunc("/files", record(drive.handleFiles))
	mux.HandleFunc("/files/", record(drive.handleFile))
	drive.server = httptest.NewServer(mux)

	previous := []string{driveTokenURL, driveFilesURL, driveUploadURL}
	previousBackoff := driveInitialBackoff
	driveTokenURL = drive.server.URL + "/token"
	driveFilesURL = drive.server.URL + "/files"
	driveUploadURL = drive.server.URL + "/upload"
	driveInitialBackoff = 5 * time.Millisecond

	t.Cleanup(func() {
		driveTokenURL, driveFilesURL, driveUploadURL = previous[0], previous[1], previous[2]
		driveInitialBackoff = previousBackoff
		drive.server.Close()
	})
	return drive
}

func (d *fakeDrive) handleToken(w http.ResponseWriter, r *http.Request) {
	d.mu.Lock()
	d.tokens++
	d.mu.Unlock()
	json.NewEncoder(w).Encode(map[string]interface{}{
		"access_token": "fake-token",
		"expires_in":   3600,
	})
}

func (d *fakeDrive) handleUpload(w http.ResponseWriter, r *http.Request) {
	d.mu.Lock()
	d.mediaUploads++
	d.mu.Unlock()
	if d.shouldFail("upload") {
		if status, body := d.failure("upload"); status != 0 {
			w.WriteHeader(status)
			w.Write([]byte(body))
			return
		}
		w.WriteHeader(http.StatusTooManyRequests)
		return
	}

	_, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	reader := multipart.NewReader(r.Body, params["boundary"])

	metaPart, err := reader.NextPart()
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	var metadata struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(metaPart).Decode(&metadata); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	var data []byte
	if dataPart, err := reader.NextPart(); err == nil {
		data, _ = io.ReadAll(dataPart)
	}

	d.mu.Lock()
	d.nextID++
	id := fmt.Sprintf("id-%d", d.nextID)
	d.files[id] = &fakeFile{id: id, name: metadata.Name, data: data}
	d.mu.Unlock()

	json.NewEncoder(w).Encode(map[string]string{"id": id, "name": metadata.Name})
}

func (d *fakeDrive) handleFiles(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		d.handleCreate(w, r)
		return
	}
	d.handleList(w, r)
}

func (d *fakeDrive) handleCreate(w http.ResponseWriter, r *http.Request) {
	d.mu.Lock()
	status := d.createStatus
	d.mu.Unlock()
	if status != 0 {
		w.WriteHeader(status)
		return
	}
	if d.shouldFail("upload") {
		if status, body := d.failure("upload"); status != 0 {
			w.WriteHeader(status)
			w.Write([]byte(body))
			return
		}
		w.WriteHeader(http.StatusTooManyRequests)
		return
	}

	var meta struct {
		Name        string `json:"name"`
		Description string `json:"description"`
	}
	if err := json.NewDecoder(r.Body).Decode(&meta); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	d.mu.Lock()
	d.nextID++
	id := fmt.Sprintf("id-%d", d.nextID)
	d.files[id] = &fakeFile{id: id, name: meta.Name, description: meta.Description}
	d.mu.Unlock()

	json.NewEncoder(w).Encode(map[string]string{"id": id, "name": meta.Name})
}

func (d *fakeDrive) handleList(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query().Get("q")
	exact, prefix := parseFakeQuery(query)
	or := parseFakeOr(query)

	d.mu.Lock()
	d.lastQuery = query
	if len(or) > 0 {
		d.orLists++
	} else if prefix != "" {
		d.containsLists++
	}
	delay := d.listDelay
	d.listDelay = 0
	if delay == 0 {
		delay = d.listDelayNext
		d.listDelayNext = 0
	}
	d.mu.Unlock()
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-r.Context().Done():
			return
		}
	}
	if d.shouldFail("list") {
		if status, body := d.failure("list"); status != 0 {
			w.WriteHeader(status)
			w.Write([]byte(body))
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	type entry struct {
		ID          string `json:"id"`
		Name        string `json:"name"`
		Description string `json:"description,omitempty"`
	}
	result := struct {
		Files []entry `json:"files"`
	}{}

	d.mu.Lock()
	for _, file := range d.files {
		match := false
		switch {
		case len(or) > 0:
			for _, name := range or {
				if file.name == name {
					match = true
					break
				}
			}
		case exact != "":
			match = file.name == exact
		case prefix != "":
			match = strings.HasPrefix(file.name, prefix)
		}
		if match {
			result.Files = append(result.Files, entry{
				ID: file.id, Name: file.name, Description: file.description,
			})
		}
	}
	d.mu.Unlock()

	json.NewEncoder(w).Encode(result)
}

func (d *fakeDrive) handleFile(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/files/")

	d.mu.Lock()
	file, ok := d.files[id]
	if ok && r.Method == http.MethodDelete {
		delete(d.files, id)
	}
	d.mu.Unlock()

	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if r.Method == http.MethodDelete {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if strings.Contains(r.URL.RawQuery, "fields=description") {
		json.NewEncoder(w).Encode(map[string]string{"description": file.description})
		return
	}
	w.Write(file.data)
}

func (d *fakeDrive) shouldFail(kind string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.failOnce[kind] {
		d.failOnce[kind] = false
		return true
	}
	return false
}

func (d *fakeDrive) failOnceWith(kind string, status int, body string) {
	d.mu.Lock()
	d.failOnce[kind] = true
	d.failStatus[kind] = status
	d.failBody[kind] = body
	d.mu.Unlock()
}

func (d *fakeDrive) failure(kind string) (int, string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if status, ok := d.failStatus[kind]; ok {
		return status, d.failBody[kind]
	}
	return 0, ""
}

func (d *fakeDrive) failNext(kind string) {
	d.mu.Lock()
	d.failOnce[kind] = true
	d.mu.Unlock()
}

func (d *fakeDrive) seenHosts() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]string, 0, len(d.hosts))
	for h := range d.hosts {
		out = append(out, h)
	}
	return out
}

func (d *fakeDrive) seenAddrs() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]string, 0, len(d.addrs))
	for a := range d.addrs {
		out = append(out, a)
	}
	return out
}

func (d *fakeDrive) count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.files)
}

func parseFakeQuery(query string) (exact, prefix string) {
	if value, ok := cutQuoted(query, "name = '"); ok {
		return value, ""
	}
	if value, ok := cutQuoted(query, "name contains '"); ok {
		return "", value
	}
	return "", ""
}

func parseFakeOr(query string) []string {
	if !strings.Contains(query, " or ") {
		return nil
	}
	var names []string
	for _, part := range strings.Split(query, " or ") {
		if value, ok := cutQuoted(part, "name = '"); ok {
			names = append(names, value)
		}
	}
	return names
}

func cutQuoted(query, marker string) (string, bool) {
	start := strings.Index(query, marker)
	if start < 0 {
		return "", false
	}
	rest := query[start+len(marker):]
	end := strings.Index(rest, "'")
	if end < 0 {
		return "", false
	}
	return rest[:end], true
}

func driveSettings() *internet.MemoryStreamConfig {
	return &internet.MemoryStreamConfig{
		ProtocolName: protocolName,
		ProtocolSettings: &Config{
			RemoteFolder:      "folder-id",
			Service:           "Google Drive",
			Secrets:           []string{"client", "secret", "refresh"},
			FlushIntervalMs:   5,
			PollIntervalMs:    5,
			MaxPollIntervalMs: 20,
			SessionTtlSeconds: 1,
		},
	}
}

func newDriveBackend(t *testing.T) *driveStorage {
	t.Helper()

	storage, err := newDriveStorage(driveSettings(), driveSettings().ProtocolSettings.(*Config))
	if err != nil {
		t.Fatalf("newDriveStorage: %v", err)
	}
	return storage
}

func TestDriveSecrets(t *testing.T) {
	if _, err := newDriveStorage(nil, &Config{RemoteFolder: "f", Secrets: []string{"a", "b"}}); err == nil {
		t.Fatal("accepted two secrets")
	}
	if _, err := newDriveStorage(nil, &Config{Secrets: []string{"a", "b", "c"}}); err == nil {
		t.Fatal("accepted an empty remoteFolder")
	}
	if _, err := newDriveStorage(nil, &Config{RemoteFolder: "f", Secrets: []string{"a", "", "c"}}); err == nil {
		t.Fatal("accepted an empty secret")
	}
}

func TestDriveRoundTrip(t *testing.T) {
	drive := newFakeDrive(t)
	storage := newDriveBackend(t)
	ctx := context.Background()

	if err := storage.Put(ctx, "streams/abc/c2s/000000000.seg", []byte("hello")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	data, err := storage.Get(ctx, "streams/abc/c2s/000000000.seg")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(data) != "hello" {
		t.Fatalf("Get returned %q, want %q", data, "hello")
	}

	names, err := storage.List(ctx, "streams/abc/c2s")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(names) != 1 || names[0].Name != "000000000.seg" {
		t.Fatalf("List returned %v, want 1 segment", names)
	}

	if _, err := storage.Get(ctx, "streams/abc/c2s/000000009.seg"); err != errNotFound {
		t.Fatalf("Get returned %v, want errNotFound", err)
	}

	if err := storage.Delete(ctx, "streams/abc/c2s/000000000.seg"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if drive.count() != 0 {
		t.Fatalf("fake drive still holds %d files", drive.count())
	}
}

func TestDriveListChildren(t *testing.T) {
	newFakeDrive(t)
	storage := newDriveBackend(t)
	ctx := context.Background()

	for _, name := range []string{
		"streams/one/c2s/000000000.seg",
		"streams/one/s2c/000000000.seg",
		"streams/two/c2s/000000000.seg",
	} {
		if err := storage.Put(ctx, name, []byte("x")); err != nil {
			t.Fatalf("Put %s: %v", name, err)
		}
	}

	names, err := storage.List(ctx, "streams")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(names) != 2 {
		t.Fatalf("List returned %v, want 2 sessions", names)
	}
}

func TestDriveDeleteSession(t *testing.T) {
	drive := newFakeDrive(t)
	storage := newDriveBackend(t)
	ctx := context.Background()

	for _, name := range []string{
		"streams/one/c2s/000000000.seg",
		"streams/one/c2s/000000001.end",
		"streams/one/s2c/000000000.seg",
		"streams/two/c2s/000000000.seg",
	} {
		if err := storage.Put(ctx, name, []byte("x")); err != nil {
			t.Fatalf("Put %s: %v", name, err)
		}
	}

	if err := storage.Delete(ctx, "streams/one"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if drive.count() != 1 {
		t.Fatalf("fake drive holds %d files, want 1", drive.count())
	}
}

func TestDriveRetry(t *testing.T) {
	drive := newFakeDrive(t)
	storage := newDriveBackend(t)
	ctx := context.Background()

	drive.failNext("upload")
	if err := storage.Put(ctx, "sessions/abc", nil); err != nil {
		t.Fatalf("Put did not survive a 429: %v", err)
	}

	drive.failNext("list")
	if _, err := storage.List(ctx, "sessions"); err != nil {
		t.Fatalf("List did not survive a 503: %v", err)
	}
}

func TestDriveTokenCache(t *testing.T) {
	drive := newFakeDrive(t)
	storage := newDriveBackend(t)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		if err := storage.Put(ctx, fmt.Sprintf("sessions/s%d", i), nil); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	if drive.tokens != 1 {
		t.Fatalf("token endpoint hit %d times, want 1", drive.tokens)
	}
}

func TestDriveTransport(t *testing.T) {
	newFakeDrive(t)

	client, server, cleanup := pairWith(t, driveSettings())
	defer cleanup()

	if _, err := client.Write([]byte("ping")); err != nil {
		t.Fatalf("client write: %v", err)
	}
	expectRead(t, server, "ping")

	if _, err := server.Write([]byte("pong")); err != nil {
		t.Fatalf("server write: %v", err)
	}
	expectRead(t, client, "pong")
}

func TestDriveLargeTransfer(t *testing.T) {
	newFakeDrive(t)

	client, server, cleanup := pairWith(t, driveSettings())
	defer cleanup()

	payload := make([]byte, 300000)
	for i := range payload {
		payload[i] = byte(i % 251)
	}

	go func() {
		client.Write(payload)
	}()

	if err := server.SetReadDeadline(time.Now().Add(60 * time.Second)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(server, got); err != nil {
		t.Fatalf("ReadFull: %v", err)
	}
	for i := range got {
		if got[i] != payload[i] {
			t.Fatalf("payload mismatch at byte %d", i)
		}
	}
}

func TestRateLimited(t *testing.T) {
	limited := []string{
		`{"error":{"code":403,"errors":[{"reason":"userRateLimitExceeded"}]}}`,
		`{"error":{"code":403,"errors":[{"reason":"rateLimitExceeded"}]}}`,
		`{"error":{"code":403,"errors":[{"reason":"sharingRateLimitExceeded"}]}}`,
		`{"error":{"status":"RESOURCE_EXHAUSTED"}}`,
	}
	for _, payload := range limited {
		if !rateLimited([]byte(payload)) {
			t.Fatalf("rateLimited missed %s", payload)
		}
	}

	permanent := []string{
		`{"error":{"code":403,"errors":[{"reason":"insufficientFilePermissions"}]}}`,
		`{"error":{"code":403,"errors":[{"reason":"storageQuotaExceeded"}]}}`,
		`not json at all`,
	}
	for _, payload := range permanent {
		if rateLimited([]byte(payload)) {
			t.Fatalf("rateLimited treated %s as temporary", payload)
		}
	}
}

func TestDriveRetryRateLimit(t *testing.T) {
	drive := newFakeDrive(t)
	storage := newDriveBackend(t)

	drive.failOnceWith("upload", http.StatusForbidden,
		`{"error":{"code":403,"errors":[{"reason":"userRateLimitExceeded"}]}}`)

	if err := storage.Put(context.Background(), "sessions/abc", nil); err != nil {
		t.Fatalf("Put did not survive a 403: %v", err)
	}
}

func TestDriveSessionClients(t *testing.T) {
	drive := newFakeDrive(t)

	first, err := newStorage(driveSettings())
	if err != nil {
		t.Fatalf("newStorage: %v", err)
	}
	second, err := newStorage(driveSettings())
	if err != nil {
		t.Fatalf("newStorage: %v", err)
	}
	if first == second {
		t.Fatal("same settings shared one storage")
	}

	ctx := context.Background()
	if err := first.Put(ctx, "streams/a/c2s/000000000.seg", []byte("x")); err != nil {
		t.Fatalf("first Put: %v", err)
	}
	if err := second.Put(ctx, "streams/b/c2s/000000000.seg", []byte("x")); err != nil {
		t.Fatalf("second Put: %v", err)
	}

	if addrs := drive.seenAddrs(); len(addrs) != 2 {
		t.Fatalf("fake drive saw %d client addresses, want 2", len(addrs))
	}
}

func TestDriveAttemptTimeoutFailsFast(t *testing.T) {
	newFakeDrive(t)

	ln, err := stdnet.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				time.Sleep(5 * time.Second)
				c.Close()
			}()
		}
	}()

	previousFilesURL := driveFilesURL
	previousTimeout := driveAttemptTimeout
	previousFastWait := driveFastRetryWait
	driveFilesURL = "http://" + ln.Addr().String()
	driveAttemptTimeout = 50 * time.Millisecond
	driveFastRetryWait = 5 * time.Millisecond
	t.Cleanup(func() {
		driveFilesURL = previousFilesURL
		driveAttemptTimeout = previousTimeout
		driveFastRetryWait = previousFastWait
	})

	storage := newDriveBackend(t)
	start := time.Now()
	_, _, err = storage.do(context.Background(), http.MethodPost, storage.filesURL+"?fields=id",
		"application/json; charset=UTF-8", []byte(`{"name":"hung"}`))
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("do() to a hung endpoint succeeded")
	}
	if elapsed > 5*time.Second {
		t.Fatalf("hung do() took %v, want fast failure", elapsed)
	}
}

func TestDriveFastRetryRecoversImmediately(t *testing.T) {
	drive := newFakeDrive(t)

	fakeAddr := strings.TrimPrefix(drive.server.URL, "http://")
	ln, err := stdnet.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	var mu sync.Mutex
	conns := 0
	go func() {
		for {
			downstream, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns++
			first := conns == 1
			mu.Unlock()
			if first {
				downstream.Close()
				continue
			}
			upstream, err := stdnet.Dial("tcp", fakeAddr)
			if err != nil {
				downstream.Close()
				continue
			}
			go func() {
				io.Copy(upstream, downstream)
				upstream.Close()
				downstream.Close()
			}()
			go func() {
				io.Copy(downstream, upstream)
				upstream.Close()
				downstream.Close()
			}()
		}
	}()

	previousFilesURL := driveFilesURL
	previousBackoff := driveInitialBackoff
	previousMaxBackoff := driveMaxBackoff
	previousFastWait := driveFastRetryWait
	previousFastRetries := driveFastRetries
	driveFilesURL = "http://" + ln.Addr().String()
	driveInitialBackoff = 2 * time.Second
	driveMaxBackoff = 2 * time.Second
	driveFastRetryWait = 5 * time.Millisecond
	driveFastRetries = 2
	t.Cleanup(func() {
		driveFilesURL = previousFilesURL
		driveInitialBackoff = previousBackoff
		driveMaxBackoff = previousMaxBackoff
		driveFastRetryWait = previousFastWait
		driveFastRetries = previousFastRetries
	})

	storage := newDriveBackend(t)
	start := time.Now()
	if err := storage.Put(context.Background(), "streams/flaky/000000000.seg", []byte("x")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("recovery after one transport failure took %v, want fast retry", elapsed)
	}
}

func TestDriveInlineListing(t *testing.T) {
	drive := newFakeDrive(t)
	storage := newDriveBackend(t)
	ctx := context.Background()

	payload := []byte("small enough to ride along with the listing")
	if err := storage.Put(ctx, "streams/abc/c2s/000000000.seg", payload); err != nil {
		t.Fatalf("Put: %v", err)
	}

	entries, err := storage.List(ctx, "streams/abc/c2s")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("List returned %v, want 1 entry", entries)
	}
	if string(entries[0].Inline) != string(payload) {
		t.Fatalf("listing carried %q, want %q", entries[0].Inline, payload)
	}

	d := drive
	d.mu.Lock()
	var stored *fakeFile
	for _, f := range d.files {
		stored = f
	}
	d.mu.Unlock()
	if len(stored.data) != 0 {
		t.Fatal("small payload was uploaded as content")
	}
}

func TestDriveLargeContent(t *testing.T) {
	drive := newFakeDrive(t)
	storage := newDriveBackend(t)
	ctx := context.Background()

	payload := make([]byte, driveInlineLimit+1)
	for i := range payload {
		payload[i] = byte(i)
	}
	if err := storage.Put(ctx, "streams/abc/c2s/000000000.seg", payload); err != nil {
		t.Fatalf("Put: %v", err)
	}

	entries, err := storage.List(ctx, "streams/abc/c2s")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(entries) != 1 || entries[0].Inline != nil {
		t.Fatalf("large payload was inlined, got %v", entries)
	}

	got, err := storage.Get(ctx, "streams/abc/c2s/000000000.seg")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if len(got) != len(payload) {
		t.Fatalf("Get returned %d bytes, want %d", len(got), len(payload))
	}
	_ = drive
}

func TestDriveInlineGet(t *testing.T) {
	newFakeDrive(t)
	storage := newDriveBackend(t)
	ctx := context.Background()

	payload := []byte("only in the description")
	if err := storage.Put(ctx, "sessions/abc", payload); err != nil {
		t.Fatalf("Put: %v", err)
	}

	got, err := storage.Get(ctx, "sessions/abc")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != string(payload) {
		t.Fatalf("Get returned %q, want %q", got, payload)
	}
}

func TestDriveMidInline(t *testing.T) {
	newFakeDrive(t)
	storage := newDriveBackend(t)
	ctx := context.Background()

	payload := make([]byte, 15000)
	for i := range payload {
		payload[i] = byte(i)
	}
	if err := storage.Put(ctx, "streams/abc/c2s/000000000.seg", payload); err != nil {
		t.Fatalf("Put: %v", err)
	}

	entries, err := storage.List(ctx, "streams/abc/c2s")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(entries) != 1 || string(entries[0].Inline) != string(payload) {
		t.Fatalf("listing did not carry the 15KB payload inline")
	}

	got, err := storage.Get(ctx, "streams/abc/c2s/000000000.seg")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != string(payload) {
		t.Fatalf("Get returned %d bytes, want %d", len(got), len(payload))
	}
}

func TestDriveFronting(t *testing.T) {
	drive := newFakeDrive(t)

	fake, err := url.Parse(drive.server.URL)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	port, err := net.PortFromString(fake.Port())
	if err != nil {
		t.Fatalf("port: %v", err)
	}

	driveTokenURL = "http://www.googleapis.com/token"
	driveFilesURL = "http://www.googleapis.com/files"
	driveUploadURL = "http://www.googleapis.com/upload"

	settings := driveSettings()
	settings.Destination = &net.Destination{
		Address: net.ParseAddress(fake.Hostname()),
		Port:    port,
		Network: net.Network_TCP,
	}

	storage, err := newDriveStorage(settings, settings.ProtocolSettings.(*Config))
	if err != nil {
		t.Fatalf("newDriveStorage: %v", err)
	}

	if err := storage.Put(context.Background(), "sessions/fronted", []byte("x")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if drive.count() != 1 {
		t.Fatalf("fake drive holds %d files, want 1", drive.count())
	}

	for _, host := range drive.seenHosts() {
		if host != "www.googleapis.com" {
			t.Fatalf("inner host was %q, want www.googleapis.com regardless of address", host)
		}
	}
}

func TestDriveWarmupFetchesToken(t *testing.T) {
	drive := newFakeDrive(t)
	storage := newDriveBackend(t)

	waitFor(t, "warmup token fetch", func() bool {
		drive.mu.Lock()
		defer drive.mu.Unlock()
		return drive.tokens == 1
	})
	time.Sleep(200 * time.Millisecond)
	drive.mu.Lock()
	tokens := drive.tokens
	drive.mu.Unlock()
	if tokens != 1 {
		t.Fatalf("fake saw %d token requests, want exactly 1", tokens)
	}
	if err := storage.Put(context.Background(), "sessions/warm", []byte("x")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	drive.mu.Lock()
	tokens = drive.tokens
	drive.mu.Unlock()
	if tokens != 1 {
		t.Fatalf("fake saw %d token requests after Put, want still 1", tokens)
	}
}

func TestProbeHedgeCapped(t *testing.T) {
	drive := newFakeDrive(t)
	storage := newDriveBackend(t)
	ctx := context.Background()

	if err := storage.Put(ctx, objectName("streams/hedgecap", 0, segSuffix), []byte("x")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	drive.mu.Lock()
	drive.listDelay = 1500 * time.Millisecond
	drive.mu.Unlock()
	start := time.Now()
	if _, err := storage.probe(ctx, "streams/hedgecap", 0); err != nil {
		t.Fatalf("probe: %v", err)
	}
	if d := time.Since(start); d >= 1300*time.Millisecond {
		t.Fatalf("first probe took %v, want under 1.3s", d)
	}

	drive.mu.Lock()
	drive.listDelay = 1500 * time.Millisecond
	drive.mu.Unlock()
	start = time.Now()
	if _, err := storage.probe(ctx, "streams/hedgecap", 0); err != nil {
		t.Fatalf("probe: %v", err)
	}
	if d := time.Since(start); d < 1300*time.Millisecond {
		t.Fatalf("second probe took %v, want at least 1.3s without a hedge", d)
	}
	waitFor(t, "slow queries to land", func() bool {
		orLists, _ := driveCounts(drive)
		return orLists == 3
	})
}

func TestProbeHedgeRecovers(t *testing.T) {
	drive := newFakeDrive(t)
	storage := newDriveBackend(t)
	ctx := context.Background()

	if err := storage.Put(ctx, objectName("streams/hedgeerr", 0, segSuffix), []byte("x")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	drive.mu.Lock()
	drive.listDelay = 1200 * time.Millisecond
	drive.listDelayNext = 300 * time.Millisecond
	drive.mu.Unlock()
	drive.failOnceWith("list", http.StatusNotFound, "gone")

	entries, err := storage.probe(ctx, "streams/hedgeerr", 0)
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("probe returned %d entries, want 1", len(entries))
	}
	waitFor(t, "hedged query to land", func() bool {
		orLists, _ := driveCounts(drive)
		return orLists == 2
	})
}

func TestProbeHedgeAfterFastProbes(t *testing.T) {
	drive := newFakeDrive(t)
	storage := newDriveBackend(t)
	ctx := context.Background()

	if err := storage.Put(ctx, objectName("streams/hedgefast", 0, segSuffix), []byte("x")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	for i := 0; i < 20; i++ {
		if _, err := storage.probe(ctx, "streams/hedgefast", 0); err != nil {
			t.Fatalf("probe: %v", err)
		}
	}

	drive.mu.Lock()
	drive.listDelay = 1500 * time.Millisecond
	drive.mu.Unlock()
	start := time.Now()
	if _, err := storage.probe(ctx, "streams/hedgefast", 0); err != nil {
		t.Fatalf("probe: %v", err)
	}
	if d := time.Since(start); d >= 1300*time.Millisecond {
		t.Fatalf("slow probe took %v, want under 1.3s", d)
	}
	waitFor(t, "hedged query to land", func() bool {
		orLists, _ := driveCounts(drive)
		return orLists == 22
	})
}

func TestPutRejectedFallsBackToMedia(t *testing.T) {
	drive := newFakeDrive(t)
	storage := newDriveBackend(t)
	ctx := context.Background()

	payload := []byte("hello media")
	drive.failOnceWith("upload", http.StatusBadRequest, "bad description")
	if err := storage.Put(ctx, "sessions/rejected", payload); err != nil {
		t.Fatalf("Put: %v", err)
	}
	drive.mu.Lock()
	uploads := drive.mediaUploads
	drive.mu.Unlock()
	if uploads != 1 {
		t.Fatalf("fake saw %d media uploads, want 1", uploads)
	}
	got, err := storage.Get(ctx, "sessions/rejected")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != string(payload) {
		t.Fatalf("Get returned %q, want %q", got, payload)
	}
}

func TestPutServerErrorSkipsMediaPath(t *testing.T) {
	drive := newFakeDrive(t)
	storage := newDriveBackend(t)
	ctx := context.Background()

	drive.mu.Lock()
	drive.createStatus = http.StatusInternalServerError
	drive.mu.Unlock()

	if err := storage.Put(ctx, "sessions/err500", []byte("x")); err == nil {
		t.Fatal("Put succeeded after persistent 500s")
	}
	drive.mu.Lock()
	uploads := drive.mediaUploads
	drive.mu.Unlock()
	if uploads != 0 {
		t.Fatalf("fake saw %d media uploads, want 0", uploads)
	}
}

func TestPutCancelledSkipsMediaPath(t *testing.T) {
	drive := newFakeDrive(t)
	storage := newDriveBackend(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := storage.Put(ctx, "sessions/cancelled", []byte("x"))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Put returned %v, want context.Canceled", err)
	}
	drive.mu.Lock()
	uploads := drive.mediaUploads
	drive.mu.Unlock()
	if uploads != 0 {
		t.Fatalf("fake saw %d media uploads, want 0", uploads)
	}
}

func TestNoCooldownAfterServerError(t *testing.T) {
	drive := newFakeDrive(t)
	storage := newDriveBackend(t)
	ctx := context.Background()

	drive.failOnceWith("upload", http.StatusInternalServerError, "boom")
	done := make(chan error, 1)
	go func() {
		done <- storage.Put(ctx, "sessions/abc", []byte("x"))
	}()
	time.Sleep(20 * time.Millisecond)

	start := time.Now()
	if _, err := storage.List(ctx, "sessions"); err != nil {
		t.Fatalf("List: %v", err)
	}
	if d := time.Since(start); d > 100*time.Millisecond {
		t.Fatalf("List after a 500 took %v, want under 100ms", d)
	}
	if err := <-done; err != nil {
		t.Fatalf("Put: %v", err)
	}
}

func TestCooldownAfterRateLimit(t *testing.T) {
	drive := newFakeDrive(t)
	storage := newDriveBackend(t)
	ctx := context.Background()

	drive.failNext("upload")
	if err := storage.Put(ctx, "sessions/abc", nil); err != nil {
		t.Fatalf("Put did not survive a 429: %v", err)
	}

	start := time.Now()
	if _, err := storage.List(ctx, "sessions"); err != nil {
		t.Fatalf("List: %v", err)
	}
	if d := time.Since(start); d < driveCooldown/2 {
		t.Fatalf("request after a 429 took %v, want at least %v", d, driveCooldown/2)
	}
}
