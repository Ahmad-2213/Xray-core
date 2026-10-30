package xdrive

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/xtls/xray-core/common/dice"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/transport/internet"
)

const (
	flatSeparator = "~"

	driveBoundary         = "xdrive-boundary"
	drivePageSize         = 1000
	driveMaxAttempts      = 8
	driveMaxInflight      = 32
	driveReservedInflight = 2
	slotHeaderAttempts    = 4
	driveInlineLimit      = 16 * 1024
	driveTimeout          = 60 * time.Second

	// A listing is on the reader's critical path, so it gets a short
	// per-attempt budget: one hung query must not stall delivery for the
	// whole total. Measured probe latency is well under two seconds.
	queryAttemptTimeout = 8 * time.Second
	queryTotalTimeout   = 30 * time.Second
	driveWarmTimeout    = 15 * time.Second
	driveCooldown       = 2 * time.Second

	driveMaxIDs = 4096

	minDeleteSamples = 3
	deleteLatencyNum = 3
	deleteLatencyDen = 2

	listCacheTTL = 40 * time.Millisecond

	probeWindowWidth = 64
	probeHedgeDelay  = time.Second
	probeHedgeGap    = 5 * time.Second
)

var (
	driveMaxBackoff     = 8 * time.Second
	driveTokenURL       = "https://oauth2.googleapis.com/token"
	driveFilesURL       = "https://www.googleapis.com/drive/v3/files"
	driveUploadURL      = "https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart&fields=id,name"
	driveUpdateURL      = "https://www.googleapis.com/upload/drive/v3/files"
	driveInitialBackoff = 200 * time.Millisecond

	driveAttemptTimeout = 30 * time.Second
	driveFastRetries    = 2
	driveFastRetryWait  = 200 * time.Millisecond

	driveMetaAttemptTimeout = 10 * time.Second
	driveMetaTotalTimeout   = 60 * time.Second

	slotReadAttemptTimeout  = 3 * time.Second
	slotWriteAttemptTimeout = 5 * time.Second
)

type driveStorage struct {
	folder       string
	clientID     string
	clientSecret string
	refreshToken string
	client       *http.Client
	tokenURL     string
	filesURL     string
	uploadURL    string
	updateURL    string
	backoff      time.Duration

	tokenMu     sync.Mutex
	token       string
	tokenExpiry time.Time

	inflight chan struct{}
	reserved chan struct{}

	coolMu    sync.Mutex
	coolUntil time.Time

	hedgeMu sync.Mutex
	hedgeAt time.Time

	idMu sync.Mutex
	ids  map[string]string

	listMu sync.Mutex
	lists  map[string]struct {
		entries []Entry
		at      time.Time
	}

	flightMu sync.Mutex
	flights  map[string]*listFlight

	delMu   sync.Mutex
	delAvg  time.Duration
	delBase time.Duration
	delN    int
}

type listFlight struct {
	done    chan struct{}
	entries []Entry
	err     error
}

func newDriveStorage(streamSettings *internet.MemoryStreamConfig, config *Config) (*driveStorage, error) {
	if config.RemoteFolder == "" {
		return nil, errors.New(`empty "remoteFolder", it must be a Google Drive folder id`)
	}
	if len(config.Secrets) != 3 {
		return nil, errors.New("Google Drive needs 3 secrets in order of ClientID, ClientSecret, RefreshToken")
	}
	for i, secret := range config.Secrets {
		if secret == "" {
			return nil, errors.New("Google Drive secret ", i, " is empty")
		}
	}

	s := &driveStorage{
		folder:       config.RemoteFolder,
		clientID:     config.Secrets[0],
		clientSecret: config.Secrets[1],
		refreshToken: config.Secrets[2],
		client:       newServiceClient(streamSettings, driveTimeout, driveMaxInflight),
		tokenURL:     driveTokenURL,
		filesURL:     driveFilesURL,
		uploadURL:    driveUploadURL,
		updateURL:    driveUpdateURL,
		backoff:      driveInitialBackoff,
		inflight:     make(chan struct{}, driveMaxInflight),
		reserved:     make(chan struct{}, driveReservedInflight),
		ids:          make(map[string]string),
		lists: make(map[string]struct {
			entries []Entry
			at      time.Time
		}),
		flights: make(map[string]*listFlight),
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), driveWarmTimeout)
		defer cancel()
		token, err := s.accessToken(ctx)
		if err != nil {
			return
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.filesURL+"/"+s.folder+"?fields=id&supportsAllDrives=true", nil)
		if err != nil {
			return
		}
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := s.client.Do(req)
		if err != nil {
			return
		}
		io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
	}()
	return s, nil
}

func flatten(name string) string {
	return strings.ReplaceAll(name, "/", flatSeparator)
}

func quoteDriveValue(value string) string {
	return strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(value)
}

func (s *driveStorage) accessToken(ctx context.Context) (string, error) {
	s.tokenMu.Lock()
	defer s.tokenMu.Unlock()

	if s.token != "" && time.Now().Before(s.tokenExpiry) {
		return s.token, nil
	}

	form := url.Values{
		"client_id":     {s.clientID},
		"client_secret": {s.clientSecret},
		"refresh_token": {s.refreshToken},
		"grant_type":    {"refresh_token"},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.tokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return "", errors.New("failed to build the token request").Base(err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := s.client.Do(req)
	if err != nil {
		return "", errors.New("failed to refresh the access token").Base(err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", errors.New("failed to read the token response").Base(err)
	}
	if resp.StatusCode != http.StatusOK {
		return "", errors.New("the token endpoint answered ", resp.StatusCode, ": ", string(body))
	}

	var parsed struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int64  `json:"expires_in"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return "", errors.New("failed to parse the token response").Base(err)
	}
	if parsed.AccessToken == "" {
		return "", errors.New("the token endpoint returned no access token")
	}

	lifetime := parsed.ExpiresIn
	if lifetime > 60 {
		lifetime -= 60
	}
	s.token = parsed.AccessToken
	s.tokenExpiry = time.Now().Add(time.Duration(lifetime) * time.Second)
	return s.token, nil
}

func jitter(backoff time.Duration) time.Duration {
	half := backoff / 2
	if half <= 0 {
		return backoff
	}
	return half + time.Duration(dice.Roll(int(half)))
}

func rateLimited(payload []byte) bool {
	var parsed struct {
		Error struct {
			Status string `json:"status"`
			Errors []struct {
				Reason string `json:"reason"`
			} `json:"errors"`
		} `json:"error"`
	}
	if json.Unmarshal(payload, &parsed) != nil {
		return false
	}
	for _, item := range parsed.Error.Errors {
		switch item.Reason {
		case "rateLimitExceeded", "userRateLimitExceeded", "sharingRateLimitExceeded":
			return true
		}
	}
	return parsed.Error.Status == "RESOURCE_EXHAUSTED"
}

func retryableStatus(status int) bool {
	switch status {
	case http.StatusTooManyRequests, http.StatusInternalServerError,
		http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	}
	return false
}

func (s *driveStorage) do(ctx context.Context, method, target, contentType string, body []byte) (int, []byte, error) {
	return s.doBudget(ctx, method, target, contentType, body, driveAttemptTimeout, 0)
}

func (s *driveStorage) doMeta(ctx context.Context, method, target, contentType string, body []byte) (int, []byte, error) {
	return s.doBudget(ctx, method, target, contentType, body, queryAttemptTimeout, queryTotalTimeout)
}

func (s *driveStorage) doMetaWrite(ctx context.Context, method, target, contentType string, body []byte) (int, []byte, error) {
	return s.doBudget(ctx, method, target, contentType, body, driveMetaAttemptTimeout, driveMetaTotalTimeout)
}

func (s *driveStorage) doBudget(ctx context.Context, method, target, contentType string, body []byte, attemptTimeout, totalTimeout time.Duration) (int, []byte, error) {
	return s.doBudgetPooled(ctx, method, target, contentType, body, attemptTimeout, totalTimeout, false)
}

func (s *driveStorage) doBudgetReserved(ctx context.Context, method, target, contentType string, body []byte, attemptTimeout, totalTimeout time.Duration) (int, []byte, error) {
	return s.doBudgetPooled(ctx, method, target, contentType, body, attemptTimeout, totalTimeout, true)
}

func (s *driveStorage) doBudgetPooled(ctx context.Context, method, target, contentType string, body []byte, attemptTimeout, totalTimeout time.Duration, reserved bool) (int, []byte, error) {
	backoff := s.backoff
	var lastErr error
	fastLeft := driveFastRetries
	lastTransport := false

	if totalTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, totalTimeout)
		defer cancel()
	}

	s.coolMu.Lock()
	until := s.coolUntil
	s.coolMu.Unlock()
	if d := time.Until(until); d > 0 {
		select {
		case <-ctx.Done():
			return 0, nil, ctx.Err()
		case <-time.After(d):
		}
	}

	for attempt := 0; attempt < driveMaxAttempts; attempt++ {
		if attempt > 0 {
			wait := jitter(backoff)
			if lastTransport && fastLeft > 0 {
				fastLeft--
				wait = jitter(driveFastRetryWait)
			} else {
				backoff *= 2
				if backoff > driveMaxBackoff {
					backoff = driveMaxBackoff
				}
			}
			select {
			case <-ctx.Done():
				return 0, nil, ctx.Err()
			case <-time.After(wait):
			}
		}

		token, err := s.accessToken(ctx)
		if err != nil {
			lastErr = err
			lastTransport = false
			continue
		}

		pool := s.inflight
		if reserved {
			pool = s.reserved
		}
		select {
		case pool <- struct{}{}:
		case <-ctx.Done():
			return 0, nil, ctx.Err()
		}

		var reader io.Reader
		if body != nil {
			reader = bytes.NewReader(body)
		}
		attemptCtx, cancel := context.WithTimeout(ctx, attemptTimeout)
		req, err := http.NewRequestWithContext(attemptCtx, method, target, reader)
		if err != nil {
			cancel()
			<-pool
			return 0, nil, errors.New("failed to build a Drive request").Base(err)
		}
		req.Header.Set("Authorization", "Bearer "+token)
		if contentType != "" {
			req.Header.Set("Content-Type", contentType)
		}

		resp, err := s.client.Do(req)
		if err != nil {
			cancel()
			<-pool
			if ctx.Err() != nil {
				return 0, nil, ctx.Err()
			}
			lastErr = errors.New("Drive request failed").Base(err)
			lastTransport = true
			errors.LogWarningInner(ctx, err, "retrying a failed Drive request, attempt ",
				attempt+1, " of ", driveMaxAttempts)
			continue
		}
		payload, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		cancel()
		<-pool
		if err != nil {
			if ctx.Err() != nil {
				return 0, nil, ctx.Err()
			}
			lastErr = errors.New("failed to read the Drive response").Base(err)
			lastTransport = true
			continue
		}
		lastTransport = false

		if resp.StatusCode == http.StatusUnauthorized {
			s.invalidateToken()
			lastErr = errors.New("Drive rejected the access token")
			continue
		}
		if resp.StatusCode == http.StatusForbidden && rateLimited(payload) {
			lastErr = errors.New("Drive is rate limiting: ", string(payload))
			errors.LogWarning(ctx, "rate limited by Drive, attempt ",
				attempt+1, " of ", driveMaxAttempts)
			s.coolMu.Lock()
			s.coolUntil = time.Now().Add(jitter(driveCooldown))
			s.coolMu.Unlock()
			continue
		}
		if retryableStatus(resp.StatusCode) {
			lastErr = errors.New("Drive answered ", resp.StatusCode, ": ", string(payload))
			errors.LogWarning(ctx, "retrying after Drive answered ", resp.StatusCode,
				", attempt ", attempt+1, " of ", driveMaxAttempts)
			if resp.StatusCode == http.StatusTooManyRequests {
				s.coolMu.Lock()
				s.coolUntil = time.Now().Add(jitter(driveCooldown))
				s.coolMu.Unlock()
			}
			continue
		}
		return resp.StatusCode, payload, nil
	}

	return 0, nil, lastErr
}

func (s *driveStorage) invalidateToken() {
	s.tokenMu.Lock()
	s.token = ""
	s.tokenMu.Unlock()
}

func (s *driveStorage) rememberID(name, id string) {
	s.idMu.Lock()
	for len(s.ids) >= driveMaxIDs {
		for k := range s.ids {
			delete(s.ids, k)
			break
		}
	}
	s.ids[name] = id
	s.idMu.Unlock()
}

func (s *driveStorage) forgetID(name string) {
	s.idMu.Lock()
	delete(s.ids, name)
	s.idMu.Unlock()
}

func (s *driveStorage) cachedID(name string) (string, bool) {
	s.idMu.Lock()
	defer s.idMu.Unlock()
	id, ok := s.ids[name]
	return id, ok
}

type driveFile struct {
	id          string
	description string
}

func (s *driveStorage) hedgedQuery(ctx context.Context, cond string) (map[string]driveFile, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	type outcome struct {
		found map[string]driveFile
		err   error
	}
	ch := make(chan outcome, 2)
	ask := func() {
		found, err := s.query(ctx, cond)
		select {
		case ch <- outcome{found, err}:
		case <-ctx.Done():
		}
	}
	go ask()
	timer := time.NewTimer(probeHedgeDelay)
	defer timer.Stop()
	pending := 1
	var firstErr error
	for {
		select {
		case out := <-ch:
			pending--
			if out.err == nil {
				return out.found, nil
			}
			if firstErr == nil {
				firstErr = out.err
			}
			if pending == 0 {
				return nil, firstErr
			}
		case <-timer.C:
			if pending == 1 {
				now := time.Now()
				s.hedgeMu.Lock()
				allowed := now.Sub(s.hedgeAt) >= probeHedgeGap
				if allowed {
					s.hedgeAt = now
				}
				s.hedgeMu.Unlock()
				if allowed {
					pending = 2
					go ask()
				}
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (s *driveStorage) query(ctx context.Context, condition string) (map[string]driveFile, error) {
	found := make(map[string]driveFile)
	pageToken := ""

	for {
		params := url.Values{
			"q":                         {"'" + quoteDriveValue(s.folder) + "' in parents and trashed = false and " + condition},
			"fields":                    {"nextPageToken,files(id,name,description)"},
			"pageSize":                  {fmt.Sprint(drivePageSize)},
			"supportsAllDrives":         {"true"},
			"includeItemsFromAllDrives": {"true"},
		}
		if pageToken != "" {
			params.Set("pageToken", pageToken)
		}

		status, payload, err := s.doMeta(ctx, http.MethodGet, s.filesURL+"?"+params.Encode(), "", nil)
		if err != nil {
			return nil, err
		}
		if status != http.StatusOK {
			return nil, errors.New("Drive listing answered ", status, ": ", string(payload))
		}

		var parsed struct {
			NextPageToken string `json:"nextPageToken"`
			Files         []struct {
				ID          string `json:"id"`
				Name        string `json:"name"`
				Description string `json:"description"`
			} `json:"files"`
		}
		if err := json.Unmarshal(payload, &parsed); err != nil {
			return nil, errors.New("failed to parse the Drive listing").Base(err)
		}

		for _, file := range parsed.Files {
			found[file.Name] = driveFile{id: file.ID, description: file.Description}
			s.rememberID(file.Name, file.ID)
		}

		pageToken = parsed.NextPageToken
		if pageToken == "" {
			return found, nil
		}
	}
}

func (s *driveStorage) resolveID(ctx context.Context, flat string) (string, error) {
	if id, ok := s.cachedID(flat); ok {
		return id, nil
	}
	found, err := s.query(ctx, "name = '"+quoteDriveValue(flat)+"'")
	if err != nil {
		return "", err
	}
	if file, ok := found[flat]; ok {
		return file.id, nil
	}
	return "", errNotFound
}

func (s *driveStorage) probe(ctx context.Context, prefix string, seq int64) ([]Entry, error) {
	names := make([]string, 0, probeWindowWidth+2)
	for i := int64(0); i < probeWindowWidth; i++ {
		names = append(names, objectName(prefix, seq+i, segSuffix))
	}
	names = append(names, objectName(prefix, seq, endSuffix), objectName(prefix, seq, errSuffix))
	quoted := make([]string, 0, len(names))
	for _, name := range names {
		quoted = append(quoted, "name = '"+quoteDriveValue(flatten(name))+"'")
	}
	found, err := s.hedgedQuery(ctx, "("+strings.Join(quoted, " or ")+")")
	if err != nil {
		return nil, err
	}
	entries := make([]Entry, 0, len(names))
	for _, name := range names {
		file, ok := found[flatten(name)]
		if !ok {
			continue
		}
		entry := Entry{Name: name[strings.LastIndexByte(name, '/')+1:]}
		if file.description != "" {
			if data, err := base64.StdEncoding.DecodeString(file.description); err == nil {
				entry.Inline = data
			}
		}
		entries = append(entries, entry)
	}
	if len(entries) == 0 {
		return nil, errNotFound
	}
	return entries, nil
}

func (s *driveStorage) Put(ctx context.Context, name string, data []byte) error {
	if len(data) <= driveInlineLimit {
		if err := s.putInline(ctx, name, data); err == nil {
			return nil
		} else if errors.Cause(err) != errInlineRejected {
			return err
		}
	}
	return s.putMedia(ctx, name, data)
}

var errInlineRejected = errors.New("Drive rejected the inline upload body")

func (s *driveStorage) putInline(ctx context.Context, name string, data []byte) error {
	flat := flatten(name)

	body, err := json.Marshal(map[string]interface{}{
		"name":        flat,
		"parents":     []string{s.folder},
		"description": base64.StdEncoding.EncodeToString(data),
	})
	if err != nil {
		return errors.New("failed to build the inline metadata").Base(err)
	}

	status, payload, err := s.doMetaWrite(ctx, http.MethodPost, s.filesURL+"?fields=id",
		"application/json; charset=UTF-8", body)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		err := errors.New("Drive rejected the inline upload of ", name,
			" with ", status, ": ", string(payload))
		if status >= 400 && status < 500 {
			return err.Base(errInlineRejected)
		}
		return err
	}

	var parsed struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(payload, &parsed); err != nil {
		return errors.New("failed to parse the Drive upload response").Base(err)
	}
	if parsed.ID != "" {
		s.rememberID(flat, parsed.ID)
	}
	return nil
}

func (s *driveStorage) putMedia(ctx context.Context, name string, data []byte) error {
	flat := flatten(name)

	metadata, err := json.Marshal(map[string]interface{}{
		"name":    flat,
		"parents": []string{s.folder},
	})
	if err != nil {
		return errors.New("failed to build the upload metadata").Base(err)
	}

	var body bytes.Buffer
	fmt.Fprintf(&body, "--%s\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n", driveBoundary)
	body.Write(metadata)
	fmt.Fprintf(&body, "\r\n--%s\r\nContent-Type: application/octet-stream\r\n\r\n", driveBoundary)
	body.Write(data)
	fmt.Fprintf(&body, "\r\n--%s--\r\n", driveBoundary)

	status, payload, err := s.do(ctx, http.MethodPost, s.uploadURL,
		"multipart/related; boundary="+driveBoundary, body.Bytes())
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return errors.New("Drive upload of ", name, " answered ", status, ": ", string(payload))
	}

	var parsed struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(payload, &parsed); err != nil {
		return errors.New("failed to parse the Drive upload response").Base(err)
	}
	if parsed.ID != "" {
		s.rememberID(flat, parsed.ID)
	}
	return nil
}

func (s *driveStorage) putMultipartSlot(ctx context.Context, id string, seq int64, mode uint8, data []byte) (bool, error) {
	meta, err := json.Marshal(map[string]interface{}{
		"description": encodeSlot(newMediaFrame(seq, data)),
	})
	if err != nil {
		return false, err
	}
	var body bytes.Buffer
	fmt.Fprintf(&body, "--%s\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n", driveBoundary)
	body.Write(meta)
	fmt.Fprintf(&body, "\r\n--%s\r\nContent-Type: application/octet-stream\r\n\r\n", driveBoundary)
	body.Write(data)
	fmt.Fprintf(&body, "\r\n--%s--\r\n", driveBoundary)
	status, payload, err := s.do(ctx, http.MethodPatch,
		s.updateURL+"/"+url.PathEscape(id)+"?uploadType=multipart&fields=id",
		"multipart/related; boundary="+driveBoundary, body.Bytes())
	if err != nil {
		return false, err
	}
	if status == http.StatusOK || status == http.StatusCreated {
		return false, nil
	}
	multipartErr := errors.New("Drive refused the multipart slot update at segment ", seq,
		" with ", status, ": ", string(payload))
	if status >= 400 && status < 500 && status != http.StatusTooManyRequests && !rateLimited(payload) {
		return true, multipartErr
	}
	return false, multipartErr
}

func (s *driveStorage) putMediaBody(ctx context.Context, id string, data []byte) error {
	status, payload, err := s.do(ctx, http.MethodPatch,
		s.updateURL+"/"+url.PathEscape(id)+"?uploadType=media&fields=id",
		"application/octet-stream", data)
	if err != nil {
		return err
	}
	if status != http.StatusOK && status != http.StatusCreated {
		return errors.New("Drive refused the slot body with ", status, ": ", string(payload))
	}
	return nil
}

func (s *driveStorage) Get(ctx context.Context, name string) ([]byte, error) {
	flat := flatten(name)
	id, err := s.resolveID(ctx, flat)
	if err != nil {
		return nil, err
	}

	status, payload, err := s.do(ctx, http.MethodGet,
		s.filesURL+"/"+url.PathEscape(id)+"?alt=media&supportsAllDrives=true", "", nil)
	if err != nil {
		return nil, err
	}
	switch status {
	case http.StatusOK:
		if len(payload) > 0 {
			return payload, nil
		}
		return s.getInline(ctx, id)
	case http.StatusNotFound:
		s.forgetID(flat)
		return nil, errNotFound
	default:
		return nil, errors.New("Drive download of ", name, " answered ", status, ": ", string(payload))
	}
}

func (s *driveStorage) getInline(ctx context.Context, id string) ([]byte, error) {
	status, payload, err := s.doMeta(ctx, http.MethodGet,
		s.filesURL+"/"+url.PathEscape(id)+"?fields=description&supportsAllDrives=true", "", nil)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, errors.New("Drive answered ", status, " for inline data: ", string(payload))
	}

	var parsed struct {
		Description string `json:"description"`
	}
	if err := json.Unmarshal(payload, &parsed); err != nil {
		return nil, errors.New("failed to parse the inline data").Base(err)
	}
	if parsed.Description == "" {
		return nil, nil
	}
	data, err := base64.StdEncoding.DecodeString(parsed.Description)
	if err != nil {
		return nil, errors.New("the inline data is not valid base64").Base(err)
	}
	return data, nil
}

func (s *driveStorage) deleteID(ctx context.Context, flat, id string) error {
	start := time.Now()
	status, payload, err := s.do(ctx, http.MethodDelete,
		s.filesURL+"/"+url.PathEscape(id)+"?supportsAllDrives=true", "", nil)
	if err != nil {
		return err
	}
	s.forgetID(flat)
	switch status {
	case http.StatusOK, http.StatusNoContent:
		s.noteDelete(time.Since(start))
		return nil
	case http.StatusNotFound:
		return nil
	default:
		return errors.New("Drive deletion of ", flat, " answered ", status, ": ", string(payload))
	}
}

func (s *driveStorage) noteDelete(d time.Duration) {
	s.delMu.Lock()
	defer s.delMu.Unlock()
	if s.delN == 0 {
		s.delAvg = d
	} else {
		s.delAvg = (s.delAvg + d) / 2
	}
	s.delN++
	if s.delN == minDeleteSamples {
		s.delBase = s.delAvg
	} else if s.delN > minDeleteSamples && s.delAvg < s.delBase {
		s.delBase = s.delAvg
	}
}

func (s *driveStorage) shouldGrow() bool {
	s.delMu.Lock()
	defer s.delMu.Unlock()
	return s.delN >= minDeleteSamples && s.delAvg*deleteLatencyDen <= s.delBase*deleteLatencyNum
}

func (s *driveStorage) Delete(ctx context.Context, name string) error {
	flat := flatten(name)

	if id, err := s.resolveID(ctx, flat); err == nil {
		if err := s.deleteID(ctx, flat, id); err != nil {
			return err
		}
	} else if err != errNotFound {
		return err
	}

	if strings.HasSuffix(name, ".seg") || strings.HasSuffix(name, ".end") || strings.HasSuffix(name, ".err") {
		return nil
	}

	children, err := s.query(ctx, "name contains '"+quoteDriveValue(flat+flatSeparator)+"'")
	if err != nil {
		return err
	}
	for childName, file := range children {
		if err := s.deleteID(ctx, childName, file.id); err != nil {
			return err
		}
	}
	return nil
}

func (s *driveStorage) List(ctx context.Context, prefix string) ([]Entry, error) {
	flat := flatten(prefix) + flatSeparator

	s.flightMu.Lock()
	if f, ok := s.flights[flat]; ok {
		s.flightMu.Unlock()
		<-f.done
		if f.err != nil {
			return nil, f.err
		}
		entries := make([]Entry, len(f.entries))
		copy(entries, f.entries)
		return entries, nil
	}
	f := &listFlight{done: make(chan struct{})}
	s.flights[flat] = f
	s.flightMu.Unlock()

	s.listMu.Lock()
	if c, ok := s.lists[flat]; ok && time.Since(c.at) < listCacheTTL {
		entries := make([]Entry, len(c.entries))
		copy(entries, c.entries)
		s.listMu.Unlock()
		s.flightMu.Lock()
		f.entries = entries
		delete(s.flights, flat)
		close(f.done)
		s.flightMu.Unlock()
		return entries, nil
	}
	s.listMu.Unlock()

	found, err := s.query(ctx, "name contains '"+quoteDriveValue(flat)+"'")
	var entries []Entry
	if err == nil {
		seen := make(map[string]bool, len(found))
		entries = make([]Entry, 0, len(found))
		for name, file := range found {
			rest := strings.TrimPrefix(name, flat)
			if rest == "" {
				continue
			}
			direct := true
			if cut := strings.Index(rest, flatSeparator); cut >= 0 {
				rest = rest[:cut]
				direct = false
			}
			if seen[rest] {
				continue
			}
			seen[rest] = true

			entry := Entry{Name: rest}
			if direct && file.description != "" {
				if data, err := base64.StdEncoding.DecodeString(file.description); err == nil {
					entry.Inline = data
				}
			}
			entries = append(entries, entry)
		}
		s.listMu.Lock()
		s.lists[flat] = struct {
			entries []Entry
			at      time.Time
		}{entries: entries, at: time.Now()}
		for len(s.lists) > 256 {
			for k := range s.lists {
				delete(s.lists, k)
				break
			}
		}
		s.listMu.Unlock()
	}
	s.flightMu.Lock()
	f.entries = entries
	f.err = err
	delete(s.flights, flat)
	close(f.done)
	s.flightMu.Unlock()
	if err != nil {
		return nil, err
	}
	return entries, nil
}

func (s *driveStorage) Close() error {
	s.client.CloseIdleConnections()
	return nil
}

func (s *driveStorage) makeSlot(ctx context.Context, name string) (string, error) {
	body, err := json.Marshal(map[string]interface{}{
		"name":    flatten(name),
		"parents": []string{s.folder},
	})
	if err != nil {
		return "", errors.New("failed to build the slot metadata").Base(err)
	}
	status, payload, err := s.doMetaWrite(ctx, http.MethodPost, s.filesURL+"?fields=id",
		"application/json; charset=UTF-8", body)
	if err != nil {
		return "", err
	}
	if status != http.StatusOK {
		return "", errors.New("Drive refused the slot ", name, " with ", status, ": ", string(payload))
	}
	var parsed struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(payload, &parsed); err != nil {
		return "", errors.New("failed to parse the slot response").Base(err)
	}
	if parsed.ID == "" {
		return "", errors.New("Drive returned no id for slot ", name)
	}
	return parsed.ID, nil
}

func (s *driveStorage) writeSlot(ctx context.Context, id string, seq int64, mode uint8, data []byte) error {
	if mode == slotModeInline {
		body, err := json.Marshal(map[string]interface{}{
			"description": encodeSlot(newInlineFrame(seq, data)),
		})
		if err != nil {
			return errors.New("failed to build the slot update").Base(err)
		}
		status, payload, err := s.doBudget(ctx, http.MethodPatch,
			s.filesURL+"/"+url.PathEscape(id)+"?fields=id&supportsAllDrives=true",
			"application/json; charset=UTF-8", body, slotWriteAttemptTimeout, driveMetaTotalTimeout)
		if err != nil {
			return err
		}
		if status != http.StatusOK {
			return errors.New("Drive refused the slot update at segment ", seq,
				" with ", status, ": ", string(payload))
		}
		return nil
	}

	fallback, err := s.putMultipartSlot(ctx, id, seq, mode, data)
	if err == nil {
		return nil
	}
	if !fallback {
		return err
	}
	if err := s.putMediaBody(ctx, id, data); err != nil {
		return err
	}
	body, err := json.Marshal(map[string]interface{}{
		"description": encodeSlot(newMediaFrame(seq, data)),
	})
	if err != nil {
		return errors.New("failed to build the slot header").Base(err)
	}
	var lastErr error
	for attempt := 0; attempt < slotHeaderAttempts; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(attempt) * 250 * time.Millisecond):
			}
		}
		status, payload, err := s.doMetaWrite(ctx, http.MethodPatch,
			s.filesURL+"/"+url.PathEscape(id)+"?fields=id&supportsAllDrives=true",
			"application/json; charset=UTF-8", body)
		if err != nil {
			lastErr = err
			continue
		}
		if status != http.StatusOK {
			lastErr = errors.New("Drive refused the slot header at segment ", seq,
				" with ", status, ": ", string(payload))
			continue
		}
		return nil
	}
	return lastErr
}

func (s *driveStorage) readSlot(ctx context.Context, id string) (slotFrame, bool, error) {
	status, payload, err := s.doBudget(ctx, http.MethodGet,
		s.filesURL+"/"+url.PathEscape(id)+"?fields=description&supportsAllDrives=true", "", nil,
		slotReadAttemptTimeout, queryTotalTimeout)
	if err != nil {
		return slotFrame{}, false, err
	}
	if status == http.StatusNotFound {
		return slotFrame{}, false, errNotFound
	}
	if status != http.StatusOK {
		return slotFrame{}, false, errors.New("Drive answered ", status, " for a slot: ", string(payload))
	}
	var parsed struct {
		Description string `json:"description"`
	}
	if err := json.Unmarshal(payload, &parsed); err != nil {
		return slotFrame{}, false, errors.New("failed to parse the slot").Base(err)
	}
	frame, ok := decodeSlot(parsed.Description)
	return frame, ok, nil
}

func (s *driveStorage) readSlotBody(ctx context.Context, id string) ([]byte, error) {
	status, payload, err := s.do(ctx, http.MethodGet,
		s.filesURL+"/"+url.PathEscape(id)+"?alt=media&supportsAllDrives=true", "", nil)
	if err != nil {
		return nil, err
	}
	if status == http.StatusNotFound {
		return nil, errNotFound
	}
	if status != http.StatusOK {
		return nil, errors.New("Drive answered ", status, " for a slot body: ", string(payload))
	}
	return payload, nil
}

func (s *driveStorage) writeWatermark(ctx context.Context, id string, seq int64) error {
	body, err := json.Marshal(map[string]interface{}{"description": encodeWatermark(seq)})
	if err != nil {
		return errors.New("failed to build the watermark").Base(err)
	}
	status, payload, err := s.doBudgetReserved(ctx, http.MethodPatch,
		s.filesURL+"/"+url.PathEscape(id)+"?fields=id&supportsAllDrives=true",
		"application/json; charset=UTF-8", body,
		slotWriteAttemptTimeout, driveMetaTotalTimeout)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return errors.New("Drive refused the watermark at ", seq, " with ", status, ": ", string(payload))
	}
	return nil
}

func (s *driveStorage) readWatermark(ctx context.Context, id string) (int64, error) {
	status, payload, err := s.doBudget(ctx, http.MethodGet,
		s.filesURL+"/"+url.PathEscape(id)+"?fields=description&supportsAllDrives=true", "", nil,
		slotReadAttemptTimeout, queryTotalTimeout)
	if err != nil {
		return -1, err
	}
	if status == http.StatusNotFound {
		return -1, nil
	}
	if status != http.StatusOK {
		return -1, errors.New("Drive answered ", status, " for the watermark: ", string(payload))
	}
	var parsed struct {
		Description string `json:"description"`
	}
	if err := json.Unmarshal(payload, &parsed); err != nil {
		return -1, errors.New("failed to parse the watermark").Base(err)
	}
	return decodeWatermark(parsed.Description), nil
}
