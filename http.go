package gateway

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type API struct {
	Store        *Store
	AdminToken   string
	ReleaseDir   string
	MaxBodyBytes int64
}

var uuidPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", a.health)
	mux.HandleFunc("POST /v1/admin/enrollments", a.createEnrollment)
	mux.HandleFunc("POST /v1/workers/connect", a.connectWorker)
	mux.HandleFunc("GET /v1/workers/me", a.workerMe)
	mux.HandleFunc("GET /v1/jobs/current", a.currentJob)
	mux.HandleFunc("POST /v1/workers/heartbeat", a.workerHeartbeat)
	mux.HandleFunc("POST /v1/jobs/claim", a.claim)
	mux.HandleFunc("POST /v1/admin/jobs/wait", a.waitJobs)
	mux.HandleFunc("POST /v1/jobs/{id}/heartbeat", a.jobHeartbeat)
	mux.HandleFunc("POST /v1/jobs/{id}/complete", a.complete)
	mux.HandleFunc("POST /v1/jobs/{id}/artifacts", a.createArtifact)
	mux.HandleFunc("GET /v1/artifacts/{id}/status", a.artifactStatus)
	mux.HandleFunc("GET /v1/artifacts/{id}/content", a.workerArtifactContent)
	mux.HandleFunc("PUT /v1/artifacts/{id}/chunks/{offset}", a.putArtifactChunk)
	mux.HandleFunc("POST /v1/artifacts/{id}/finish", a.finishArtifact)
	mux.HandleFunc("POST /v1/artifacts/{id}/abort", a.abortArtifact)
	mux.HandleFunc("GET /v1/admin/artifacts/{id}/content", a.artifactContent)
	mux.HandleFunc("POST /v1/jobs/{id}/effects/{effect}/begin", a.beginEffect)
	mux.HandleFunc("POST /v1/jobs/{id}/effects/{effect}/finish", a.finishEffect)
	mux.HandleFunc("GET /v1/jobs/{id}/effects/{effect}/status", a.effectStatus)
	mux.HandleFunc("GET /v1/admin/workers", a.adminWorkers)
	mux.HandleFunc("POST /v1/admin/workers", a.registerWorker)
	mux.HandleFunc("PATCH /v1/admin/workers/{id}", a.workerPolicy)
	mux.HandleFunc("POST /v1/admin/workers/{id}/policy", a.workerPolicy)
	mux.HandleFunc("GET /v1/admin/jobs", a.adminJobs)
	mux.HandleFunc("POST /v1/admin/jobs", a.enqueue)
	mux.HandleFunc("GET /v1/admin/jobs/{id}", a.adminJob)
	mux.HandleFunc("GET /v1/admin/jobs/{id}/result", a.jobResult)
	mux.HandleFunc("POST /v1/admin/jobs/{id}/cancel", a.cancel)
	mux.HandleFunc("GET /v1/admin/metrics", a.metrics)
	mux.HandleFunc("POST /v1/admin/events", a.command)
	mux.HandleFunc("GET /releases/android-stable/{name}", a.release)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/v1/") && !strings.HasPrefix(r.URL.Path, "/v1/admin/") && !(r.Method == http.MethodPost && r.URL.Path == "/v1/workers/connect") {
			token := bearer(r)
			if token == "" {
				fail(w, ErrUnauthorized)
				return
			}
			id, err := a.Store.WorkerByToken(r.Context(), token)
			if err != nil {
				fail(w, err)
				return
			}
			r = r.WithContext(context.WithValue(r.Context(), deviceKey{}, id))
		}
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Cache-Control", "no-store")
		mux.ServeHTTP(w, r)
	})
}
func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(value)
}
func fail(w http.ResponseWriter, e error) {
	status, code := http.StatusInternalServerError, "internal_error"
	switch {
	case errors.Is(e, ErrUnauthorized):
		status, code = 401, "unauthorized"
	case errors.Is(e, ErrNotFound):
		status, code = 404, "not_found"
	case errors.Is(e, ErrInvalid):
		status, code = 400, "invalid_request"
	case errors.Is(e, ErrConflict):
		status, code = 409, "idempotency_conflict"
	case errors.Is(e, ErrLeaseLost):
		status, code = 409, "lease_lost"
	case errors.Is(e, ErrUnavailable):
		status, code = 503, "storage_unavailable"
	}
	writeJSON(w, status, map[string]string{"error": code})
}
func (a *API) decode(w http.ResponseWriter, r *http.Request, v any) bool {
	maxBytes := a.MaxBodyBytes
	if maxBytes <= 0 {
		maxBytes = 16 << 20
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
	d := json.NewDecoder(r.Body)
	if e := d.Decode(v); e != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(e, &tooLarge) {
			writeJSON(w, 413, map[string]string{"error": "request_body_too_large"})
		} else {
			fail(w, ErrInvalid)
		}
		return false
	}
	if e := d.Decode(new(any)); e != io.EOF {
		fail(w, ErrInvalid)
		return false
	}
	return true
}
func bearer(r *http.Request) string {
	h := r.Header.Get("Authorization")
	if !strings.HasPrefix(h, "Bearer ") {
		return ""
	}
	return strings.TrimPrefix(h, "Bearer ")
}
func (a *API) admin(w http.ResponseWriter, r *http.Request) bool {
	got := bearer(r)
	if got == "" || a.AdminToken == "" || subtle.ConstantTimeCompare(tokenHash(got), tokenHash(a.AdminToken)) != 1 {
		fail(w, ErrUnauthorized)
		return false
	}
	return true
}

type deviceKey struct{}

func (a *API) device(w http.ResponseWriter, r *http.Request, worker string) bool {
	if id, ok := r.Context().Value(deviceKey{}).(string); ok {
		if id != worker {
			fail(w, ErrUnauthorized)
			return false
		}
		return true
	}
	token := bearer(r)
	if token == "" {
		fail(w, ErrUnauthorized)
		return false
	}
	id, e := a.Store.WorkerByToken(r.Context(), token)
	if e != nil {
		fail(w, e)
		return false
	}
	if id != worker {
		fail(w, ErrUnauthorized)
		return false
	}
	return true
}
func (a *API) health(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if e := a.Store.Pool.Ping(ctx); e != nil {
		writeJSON(w, 503, map[string]string{"status": "database_unavailable"})
		return
	}
	writeJSON(w, 200, map[string]string{"status": "ok"})
}
func (a *API) workerHeartbeat(w http.ResponseWriter, r *http.Request) {
	var raw json.RawMessage
	var h WorkerHeartbeat
	if !a.decode(w, r, &raw) {
		return
	}
	if json.Unmarshal(raw, &h) != nil {
		fail(w, ErrInvalid)
		return
	}
	if !a.device(w, r, h.WorkerID) {
		return
	}
	profile, e := profileHeader(r, false)
	if e != nil {
		fail(w, e)
		return
	}
	var fields map[string]json.RawMessage
	_ = json.Unmarshal(raw, &fields)
	h.Capabilities, e = bodyCapabilities(profile, fields["capabilities"])
	if e != nil {
		fail(w, e)
		return
	}
	if e := a.Store.HeartbeatWithProfile(r.Context(), h, profile); e != nil {
		fail(w, e)
		return
	}
	policy, e := a.Store.WorkerPolicy(r.Context(), h.WorkerID)
	if e != nil {
		fail(w, e)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "update_poll_seconds": policy})
}
func (a *API) claim(w http.ResponseWriter, r *http.Request) {
	var req struct {
		WorkerID     string          `json:"worker_id"`
		SlotID       int             `json:"slot_id,omitempty"`
		Capabilities json.RawMessage `json:"capabilities"`
	}
	if !a.decode(w, r, &req) || !a.device(w, r, req.WorkerID) {
		return
	}
	profile, e := profileHeader(r, false)
	if e != nil {
		fail(w, e)
		return
	}
	caps, e := bodyCapabilities(profile, req.Capabilities)
	if e != nil {
		fail(w, e)
		return
	}
	if requestedWait(r) > 0 {
		w.Header().Set("Preference-Applied", "wait=20")
	}
	job, e := a.Store.claimSlotWait(r.Context(), req.WorkerID, caps, profile, req.SlotID, requestedWait(r))
	if errors.Is(e, ErrNotFound) {
		w.WriteHeader(204)
		return
	}
	if e != nil {
		fail(w, e)
		return
	}
	writeJSON(w, 200, map[string]any{"job": job})
}
func (a *API) jobHeartbeat(w http.ResponseWriter, r *http.Request) {
	var h JobHeartbeat
	if !a.decode(w, r, &h) || !a.device(w, r, h.WorkerID) {
		return
	}
	if !uuidPattern.MatchString(r.PathValue("id")) || !uuidPattern.MatchString(h.AttemptID) {
		fail(w, ErrInvalid)
		return
	}
	out, e := a.Store.JobHeartbeat(r.Context(), r.PathValue("id"), h)
	if e != nil {
		fail(w, e)
		return
	}
	writeJSON(w, 200, out)
}
func (a *API) complete(w http.ResponseWriter, r *http.Request) {
	var c Complete
	if !a.decode(w, r, &c) || !a.device(w, r, c.WorkerID) {
		return
	}
	if !uuidPattern.MatchString(r.PathValue("id")) || !uuidPattern.MatchString(c.AttemptID) {
		fail(w, ErrInvalid)
		return
	}
	out, e := a.Store.Complete(r.Context(), r.PathValue("id"), c)
	if e != nil {
		fail(w, e)
		return
	}
	writeJSON(w, 200, out)
}
func (a *API) registerWorker(w http.ResponseWriter, r *http.Request) {
	if !a.admin(w, r) {
		return
	}
	var req struct {
		WorkerID string `json:"worker_id"`
		Token    string `json:"token"`
	}
	if !a.decode(w, r, &req) {
		return
	}
	if req.Token == "" {
		req.Token = NewToken()
	}
	if subtle.ConstantTimeCompare(tokenHash(req.Token), tokenHash(a.AdminToken)) == 1 {
		fail(w, ErrInvalid)
		return
	}
	if e := a.Store.RegisterWorker(r.Context(), req.WorkerID, req.Token); e != nil {
		fail(w, e)
		return
	}
	writeJSON(w, 201, map[string]string{"worker_id": req.WorkerID, "token": req.Token})
}
func (a *API) adminWorkers(w http.ResponseWriter, r *http.Request) {
	if !a.admin(w, r) {
		return
	}
	b, e := a.Store.Workers(r.Context())
	if e != nil {
		fail(w, e)
		return
	}
	writeJSON(w, 200, map[string]any{"workers": b})
}
func (a *API) enqueue(w http.ResponseWriter, r *http.Request) {
	if !a.admin(w, r) {
		return
	}
	var req EnqueueJob
	if !a.decode(w, r, &req) {
		return
	}
	if req.IdempotencyKey == "" {
		req.IdempotencyKey = r.Header.Get("Idempotency-Key")
	}
	id, e := a.Store.Enqueue(r.Context(), req)
	if e != nil {
		fail(w, e)
		return
	}
	writeJSON(w, 201, map[string]string{"job_id": id})
}
func (a *API) adminJobs(w http.ResponseWriter, r *http.Request) {
	if !a.admin(w, r) {
		return
	}
	limit := 100
	if q := r.URL.Query().Get("limit"); q != "" {
		n, e := strconv.Atoi(q)
		if e != nil || n < 1 || n > 1000 {
			fail(w, ErrInvalid)
			return
		}
		limit = n
	}
	b, e := a.Store.Jobs(r.Context(), limit)
	if e != nil {
		fail(w, e)
		return
	}
	writeJSON(w, 200, map[string]any{"jobs": b, "limit": limit})
}
func (a *API) adminJob(w http.ResponseWriter, r *http.Request) {
	if !a.admin(w, r) {
		return
	}
	if !uuidPattern.MatchString(r.PathValue("id")) {
		fail(w, ErrInvalid)
		return
	}
	b, e := a.Store.Job(r.Context(), r.PathValue("id"))
	if e != nil {
		fail(w, e)
		return
	}
	writeJSON(w, 200, b)
}
func (a *API) cancel(w http.ResponseWriter, r *http.Request) {
	if !a.admin(w, r) {
		return
	}
	if !uuidPattern.MatchString(r.PathValue("id")) {
		fail(w, ErrInvalid)
		return
	}
	if e := a.Store.Cancel(r.Context(), r.PathValue("id")); e != nil {
		fail(w, e)
		return
	}
	writeJSON(w, 200, map[string]bool{"cancel_requested": true})
}
func (a *API) metrics(w http.ResponseWriter, r *http.Request) {
	if !a.admin(w, r) {
		return
	}
	b, e := a.Store.Metrics(r.Context())
	if e != nil {
		fail(w, e)
		return
	}
	writeJSON(w, 200, b)
}
func (a *API) command(w http.ResponseWriter, r *http.Request) {
	if !a.admin(w, r) {
		return
	}
	var c Command
	if !a.decode(w, r, &c) {
		return
	}
	receipt, e := a.Store.AcceptCommand(r.Context(), c)
	if e != nil {
		fail(w, e)
		return
	}
	writeJSON(w, 200, receipt)
}

var apkName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]*\.apk$`)

func (a *API) release(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if name != "manifest.json" && !apkName.MatchString(name) {
		http.NotFound(w, r)
		return
	}
	if a.ReleaseDir == "" {
		http.NotFound(w, r)
		return
	}
	root, e := os.OpenRoot(a.ReleaseDir)
	if e != nil {
		http.NotFound(w, r)
		return
	}
	defer root.Close()
	infoBefore, e := root.Lstat("android-stable/" + name)
	if e != nil || !infoBefore.Mode().IsRegular() {
		http.NotFound(w, r)
		return
	}
	file, e := root.Open("android-stable/" + name)
	if e != nil {
		http.NotFound(w, r)
		return
	}
	defer file.Close()
	info, e := file.Stat()
	if e != nil || !info.Mode().IsRegular() {
		http.NotFound(w, r)
		return
	}
	if name == "manifest.json" {
		w.Header().Set("Content-Type", "application/json")
	} else {
		w.Header().Set("Content-Type", "application/vnd.android.package-archive")
		w.Header().Set("Cache-Control", "public, max-age=3600")
	}
	http.ServeContent(w, r, name, info.ModTime(), file)
}

func (a *API) workerPolicy(w http.ResponseWriter, r *http.Request) {
	if !a.admin(w, r) {
		return
	}
	var req struct {
		UpdatePollSeconds int64 `json:"update_poll_seconds"`
	}
	if !a.decode(w, r, &req) {
		return
	}
	if req.UpdatePollSeconds <= 0 || req.UpdatePollSeconds > 2147483647 {
		fail(w, ErrInvalid)
		return
	}
	if e := a.Store.SetWorkerPolicy(r.Context(), r.PathValue("id"), req.UpdatePollSeconds); e != nil {
		fail(w, e)
		return
	}
	writeJSON(w, 200, map[string]any{"worker_id": r.PathValue("id"), "update_poll_seconds": req.UpdatePollSeconds})
}
