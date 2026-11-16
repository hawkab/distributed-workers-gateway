package gateway

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"github.com/jackc/pgx/v5/pgxpool"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func testProfile(t *testing.T, resources string, caps ...string) *WorkerProfile {
	t.Helper()
	var r WorkerResources
	must(t, json.Unmarshal([]byte(resources), &r))
	p, e := (&WorkerProfile{Schema: 1, Capabilities: caps, Resources: &r}).normalized()
	must(t, e)
	return p
}
func profileRequest(t *testing.T, h http.Handler, method, path, token, body string, profile *WorkerProfile) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	if profile != nil {
		r.Header.Set("X-Worker-Profile", base64.RawURLEncoding.EncodeToString(profileBytes(profile)))
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}
func TestEnrollmentAuthenticationServerUUIDAndProfilePrivacy(t *testing.T) {
	s := fixture(t)
	ctx := context.Background()
	admin := NewToken()
	h := (&API{Store: s, AdminToken: admin}).Handler()
	p := testProfile(t, `{"cpu_available_cores":1.5,"gpu_status":"unknown"}`, "sha256")
	if request(t, h, "POST", "/v1/admin/enrollments", "", `{}`).Code != 401 {
		t.Fatal("anonymous enrollment")
	}
	w := request(t, h, "POST", "/v1/admin/enrollments", admin, `{}`)
	if w.Code != 201 {
		t.Fatal(w.Code, w.Body.String())
	}
	var created map[string]string
	must(t, json.Unmarshal(w.Body.Bytes(), &created))
	token := created["enrollment_token"]
	if request(t, h, "GET", "/v1/workers/me", token, "").Code != 401 {
		t.Fatal("pending credential has device rights")
	}
	for _, credential := range []string{"", admin, NewToken()} {
		if profileRequest(t, h, "POST", "/v1/workers/connect", credential, `{}`, p).Code != 401 {
			t.Fatal("connect without enrollment/device authorization")
		}
	}
	bad := httptest.NewRequest("POST", "/v1/workers/connect", strings.NewReader(`{}`))
	bad.Header.Set("Authorization", "Bearer "+NewToken())
	bad.Header.Set("X-Worker-Profile", "not-base64!")
	denied := httptest.NewRecorder()
	h.ServeHTTP(denied, bad)
	if denied.Code != 401 {
		t.Fatal("unknown credential reached profile parser", denied.Code)
	}
	if request(t, h, "POST", "/v1/workers/connect", token, `{}`).Code != 400 {
		t.Fatal("connect accepted no profile")
	}
	w = profileRequest(t, h, "POST", "/v1/workers/connect", token, `{}`, p)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	var connected struct {
		WorkerID string `json:"worker_id"`
		Schema   int    `json:"profile_schema"`
	}
	must(t, json.Unmarshal(w.Body.Bytes(), &connected))
	if !uuidPattern.MatchString(connected.WorkerID) || connected.Schema != 1 || strings.Contains(w.Body.String(), token) {
		t.Fatal("server identity/secret exposure")
	}
	// A new Store simulates a gateway restart after a lost connect response.
	restarted := NewStore(s.Pool, s.Schema)
	id, e := restarted.ConnectWorker(ctx, token, p)
	must(t, e)
	if id != connected.WorkerID {
		t.Fatal("connect identity changed on restart")
	}
	w = request(t, h, "GET", "/v1/workers/me?worker_id=foreign", token, "")
	if w.Code != 200 || !strings.Contains(w.Body.String(), connected.WorkerID) || !strings.Contains(w.Body.String(), "server_observed_at") {
		t.Fatal("own me response")
	}
	if request(t, h, "GET", "/v1/admin/workers", token, "").Code != 401 {
		t.Fatal("device can list fleet")
	}
	if profileRequest(t, h, "POST", "/v1/workers/heartbeat", token, `{"worker_id":"`+id+`","capabilities":["spoofed"]}`, p).Code != 400 {
		t.Fatal("heartbeat body overrides profile")
	}
	if profileRequest(t, h, "POST", "/v1/jobs/claim", token, `{"worker_id":"`+id+`","capabilities":["spoofed"]}`, p).Code != 400 {
		t.Fatal("claim body overrides profile")
	}
	if profileRequest(t, h, "POST", "/v1/workers/heartbeat", token, `{"worker_id":"`+id+`"}`, p).Code != 200 {
		t.Fatal("header capabilities not authoritative")
	}
	if _, e = s.WorkerByToken(ctx, token); e != nil {
		t.Fatal("enrollment bearer did not remain device credential")
	}
	legacy := register(t, s, "legacy-original")
	old, e := s.ConnectWorker(ctx, legacy, p)
	must(t, e)
	if old != "legacy-original" {
		t.Fatal("legacy worker ID changed")
	}
	_, e = s.Pool.Exec(ctx, `UPDATE `+s.table("workers")+` SET enabled=false WHERE id=$1`, id)
	must(t, e)
	if _, e = s.ConnectWorker(ctx, token, p); !errors.Is(e, ErrUnauthorized) {
		t.Fatal("disabled worker re-enrolled", e)
	}
}
func TestConcurrentEnrollmentConnectHasOneStableIdentity(t *testing.T) {
	s := fixture(t)
	ctx := context.Background()
	token := NewToken()
	must(t, s.CreateEnrollment(ctx, token))
	p := testProfile(t, `{}`, "sha256")
	const parallel = 24
	var wg sync.WaitGroup
	ids := make(chan string, parallel)
	errs := make(chan error, parallel)
	for range parallel {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id, e := s.ConnectWorker(ctx, token, p)
			if e != nil {
				errs <- e
			} else {
				ids <- id
			}
		}()
	}
	wg.Wait()
	close(ids)
	close(errs)
	for e := range errs {
		t.Fatal(e)
	}
	var expected string
	for id := range ids {
		if expected == "" {
			expected = id
		}
		if id != expected {
			t.Fatal("duplicate enrollment identities")
		}
	}
	if !uuidPattern.MatchString(expected) || count(t, s, "workers") != 1 || count(t, s, "worker_enrollments") != 1 {
		t.Fatal("duplicate persisted enrollment")
	}
}
func TestProfileHeaderStrictSchemaAndUnknownResources(t *testing.T) {
	invalidUTF8 := append([]byte(`{"schema":1,"capabilities":["`), 0xff)
	invalidUTF8 = append(invalidUTF8, []byte(`"],"resources":{}}`)...)
	invalidRequest := httptest.NewRequest("POST", "/", nil)
	invalidRequest.Header.Set("X-Worker-Profile", base64.RawURLEncoding.EncodeToString(invalidUTF8))
	if _, e := profileHeader(invalidRequest, true); !errors.Is(e, ErrInvalid) {
		t.Fatal("invalid UTF-8 profile accepted")
	}
	for _, raw := range []string{
		`{"schema":2,"capabilities":[],"resources":{}}`,
		`{"schema":1,"capabilities":[],"resources":{"hostname":"private-host"}}`,
		`{"schema":1,"capabilities":[],"resources":{"ram_available_bytes":-1}}`,
		`{"schema":1,"capabilities":[],"resources":{"gpus":[{"index":0},{"index":0}]}}`,
		`{"schema":1,"capabilities":[],"resources":{"gpus":[{"index":null}]}}`,
		`{"schema":1,"capabilities":[],"resources":{"gpu_status":"none","gpus":[{"index":0}]}}`,
	} {
		r := httptest.NewRequest("POST", "/", nil)
		r.Header.Set("X-Worker-Profile", base64.RawURLEncoding.EncodeToString([]byte(raw)))
		if _, e := profileHeader(r, true); !errors.Is(e, ErrInvalid) {
			t.Fatal("invalid profile accepted", raw, e)
		}
	}
	r := httptest.NewRequest("POST", "/", nil)
	r.Header.Set("X-Worker-Profile", base64.RawURLEncoding.EncodeToString([]byte(`{"schema":1,"capabilities":[],"resources":{"gpu_status":"unknown","gpus":[]}}`))+"=")
	if _, e := profileHeader(r, true); !errors.Is(e, ErrInvalid) {
		t.Fatal("padded base64url accepted")
	}
	for _, raw := range []string{`{"cpu_cores":null}`, `{"ram_bytes":-1}`, `{"invented":1}`} {
		var req ResourceRequirements
		if json.Unmarshal([]byte(raw), &req) == nil {
			t.Fatal("invalid resource requirements accepted", raw)
		}
	}
}
func TestFreshResourceClaimAndReadOnlyCurrentLease(t *testing.T) {
	s := fixture(t)
	ctx := context.Background()
	register(t, s, "resource-worker")
	p := testProfile(t, `{"cpu_available_cores":1.5,"ram_available_bytes":1000,"disk_available_bytes":4000,"gpu_status":"detected","gpus":[{"index":0,"compute_units":8,"memory_available_bytes":400,"unified_memory":true}]}`, "sha256")
	must(t, s.HeartbeatWithProfile(ctx, WorkerHeartbeat{WorkerID: "resource-worker"}, p))
	id, e := s.Enqueue(ctx, EnqueueJob{Operation: "sha256", Input: json.RawMessage(`{}`), ResourceRequirements: &ResourceRequirements{CPUCores: 2, RAMBytes: 800, DiskBytes: 50, GPUCount: 1, GPUMemoryBytes: 300}})
	must(t, e)
	if _, e = s.Claim(ctx, "resource-worker", []string{"sha256"}); !errors.Is(e, ErrNotFound) {
		t.Fatal("headerless claim reused stored resources", e)
	}
	if _, e = s.ClaimWithProfile(ctx, "resource-worker", nil, p); !errors.Is(e, ErrNotFound) {
		t.Fatal("fractional CPU oversubscribed", e)
	}
	cpu := 3.0
	p.Resources.CPUAvailableCores = &cpu
	if _, e = s.ClaimWithProfile(ctx, "resource-worker", nil, p); !errors.Is(e, ErrNotFound) {
		t.Fatal("shared RAM double-spent", e)
	}
	ram := int64(2000)
	p.Resources.RAMAvailableBytes = &ram
	j, e := s.ClaimWithProfile(ctx, "resource-worker", nil, p)
	must(t, e)
	if j.ID != id {
		t.Fatal("resource-compatible job not assigned")
	}
	current, e := s.CurrentJob(ctx, "resource-worker")
	must(t, e)
	if current.AttemptID != j.AttemptID || !current.LeaseUntil.Equal(j.LeaseUntil) {
		t.Fatal("current read mutated lease")
	}
	if _, e = s.CurrentJob(ctx, "foreign"); !errors.Is(e, ErrNotFound) {
		t.Fatal("current returned another worker task", e)
	}
	_, e = s.Complete(ctx, id, done(j, "resource-worker"))
	must(t, e)
	if _, e = s.CurrentJob(ctx, "resource-worker"); !errors.Is(e, ErrNotFound) {
		t.Fatal("completed job remained current", e)
	}
}
func TestResourceSQLUnknownGPUAllocationAndMixedMemoryBudgets(t *testing.T) {
	s := fixture(t)
	ctx := context.Background()
	cases := []struct {
		name, profile, requirements string
		want                        bool
	}{
		{"ordinary-legacy", `null`, `{}`, true},
		{"cpu-total-not-availability", `{"resources":{"cpu_logical_cores":8}}`, `{"cpu_cores":1}`, false},
		{"fractional-cpu", `{"resources":{"cpu_available_cores":1.5}}`, `{"cpu_cores":1.25}`, true},
		{"ram-total-unknown-available", `{"resources":{"ram_total_bytes":8192}}`, `{"ram_bytes":1}`, false},
		{"disk-total-unknown-available", `{"resources":{"disk_total_bytes":8192}}`, `{"disk_bytes":1}`, false},
		{"unknown-gpu", `{"resources":{"gpu_status":"unknown","gpus":[]}}`, `{"gpu_count":1}`, false},
		{"gpu-total-not-allocation", `{"resources":{"gpu_status":"detected","gpus":[{"index":0,"memory_total_bytes":8000,"unified_memory":false}]}}`, `{"gpu_memory_bytes":1}`, false},
		{"gpu-units-unknown", `{"resources":{"gpu_status":"detected","gpus":[{"index":0}]}}`, `{"gpu_compute_units":1}`, false},
		{"dedicated-preferred", `{"resources":{"ram_available_bytes":500,"gpu_status":"detected","gpus":[{"index":0,"memory_available_bytes":400,"unified_memory":true},{"index":1,"memory_available_bytes":400,"unified_memory":false}]}}`, `{"ram_bytes":500,"gpu_count":1,"gpu_memory_bytes":300}`, true},
		{"mixed-shared-ram-charge", `{"resources":{"ram_available_bytes":500,"gpu_status":"detected","gpus":[{"index":0,"memory_available_bytes":400,"unified_memory":true},{"index":1,"memory_available_bytes":400,"unified_memory":false}]}}`, `{"ram_bytes":500,"gpu_count":2,"gpu_memory_bytes":300}`, false},
		{"unknown-unification-conservative", `{"resources":{"ram_available_bytes":300,"gpu_status":"detected","gpus":[{"index":0,"memory_available_bytes":400}]}}`, `{"gpu_memory_bytes":300}`, true},
		{"unknown-unification-no-ram-proof", `{"resources":{"gpu_status":"detected","gpus":[{"index":0,"memory_available_bytes":400}]}}`, `{"gpu_memory_bytes":300}`, false},
		{"huge-count-does-not-overflow", `{"resources":{"gpu_status":"detected","gpus":[]}}`, `{"ram_bytes":9223372036854775807,"gpu_count":9223372036854775807,"gpu_memory_bytes":9223372036854775807}`, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var got bool
			must(t, s.Pool.QueryRow(ctx, `SELECT `+s.table("resources_match")+`($1::jsonb,$2::jsonb)`, c.profile, c.requirements).Scan(&got))
			if got != c.want {
				t.Fatal("resource eligibility mismatch", got, c.want)
			}
		})
	}
}
func TestUnfitResourcePrefixDoesNotStarveOrdinaryLegacyJob(t *testing.T) {
	s := fixture(t)
	ctx := context.Background()
	register(t, s, "ordinary-worker")
	tx, e := s.Pool.Begin(ctx)
	must(t, e)
	defer tx.Rollback(ctx)
	for range 129 {
		j, e := (EnqueueJob{Operation: "sha256", Input: json.RawMessage(`{}`), Priority: 100, ResourceRequirements: &ResourceRequirements{RAMBytes: 1 << 50}}).normalized()
		must(t, e)
		_, e = s.enqueue(ctx, tx, j)
		must(t, e)
	}
	must(t, tx.Commit(ctx))
	id := enqueue(t, s, "sha256", "ordinary-after-hard-prefix")
	j, e := s.Claim(ctx, "ordinary-worker", []string{"sha256"})
	must(t, e)
	if j.ID != id {
		t.Fatal("unfit prefix starved compatible job")
	}
}
func TestCurrentReadRevocationPreservesLateWritesAndAdoption(t *testing.T) {
	s, j, _ := artifactFixture(t)
	ctx := context.Background()
	old := identity(j, "stream-worker")
	u := newUpload(t, s, j, "current-acl")
	_, e := s.PutArtifactChunk(ctx, u.UploadID, old, 0, bytes.NewBufferString("body"))
	must(t, e)
	finish := FinishArtifact{AttemptIdentity: old, Length: 4}
	ref, e := s.FinishArtifact(ctx, u.UploadID, finish)
	must(t, e)
	// The read transaction must reuse its own connection, rather than requiring
	// a second pool slot while holding the first one.
	config, e := pgxpool.ParseConfig(os.Getenv("REGISTRY_TEST_DATABASE_URL"))
	must(t, e)
	config.MaxConns = 1
	pool, e := pgxpool.NewWithConfig(ctx, config)
	must(t, e)
	defer pool.Close()
	single := NewStore(pool, s.Schema)
	single.Artifacts = s.Artifacts
	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	file, _, e := single.OpenWorkerArtifact(readCtx, u.UploadID, old)
	cancel()
	must(t, e)
	file.Close()
	begin := beginEffect(j, "stream-worker", newID(), `{"url":"https://example.invalid/data","method":"GET"}`)
	effect, e := s.BeginEffect(ctx, j.ID, "current-acl", begin)
	must(t, e)
	response := finishEffect(j, "stream-worker", begin.ExecutionID, effect.RequestSHA256)
	_, e = s.FinishEffect(ctx, j.ID, "current-acl", response)
	must(t, e)
	expire(t, s, j.AttemptID)
	assertDenied := func(i AttemptIdentity) {
		t.Helper()
		if _, e := s.ArtifactStatus(ctx, u.UploadID, i); e == nil {
			t.Fatal("stale artifact status readable")
		}
		if f, _, e := s.OpenWorkerArtifact(ctx, u.UploadID, i); e == nil {
			f.Close()
			t.Fatal("stale artifact content readable")
		}
		if _, e := s.EffectStatus(ctx, j.ID, "current-acl", i); e == nil {
			t.Fatal("stale effect response readable")
		}
	}
	assertDenied(old)
	if _, e = s.BeginEffect(ctx, j.ID, "current-acl", begin); !errors.Is(e, ErrLeaseLost) {
		t.Fatal("stale begin replay leaked cached response", e)
	}
	if _, e = s.FinishArtifact(ctx, u.UploadID, finish); e != nil {
		t.Fatal("late artifact ACK lost", e)
	}
	if _, e = s.FinishEffect(ctx, j.ID, "current-acl", response); e != nil {
		t.Fatal("late effect ACK lost", e)
	}
	register(t, s, "replacement")
	next, e := s.Claim(ctx, "replacement", []string{"network.http"})
	must(t, e)
	current := identity(next, "replacement")
	if _, e = s.ArtifactStatus(ctx, u.UploadID, current); e != nil {
		t.Fatal("current same-job artifact adoption rejected", e)
	}
	f, _, e := s.OpenWorkerArtifact(ctx, u.UploadID, current)
	must(t, e)
	f.Close()
	if _, e = s.EffectStatus(ctx, j.ID, "current-acl", current); e != nil {
		t.Fatal("current same-job response replay rejected", e)
	}
	complete := done(next, "replacement")
	complete.ArtifactIDs = []string{ref.ArtifactID}
	_, e = s.Complete(ctx, j.ID, complete)
	must(t, e)
	assertDenied(current)
	ack, e := s.Complete(ctx, j.ID, complete)
	must(t, e)
	if !ack.Committed {
		t.Fatal("completed receipt replay lost")
	}
}
