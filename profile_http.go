package gateway

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"net/http"
)

func (a *API) emptyObject(w http.ResponseWriter, r *http.Request) bool {
	var value map[string]json.RawMessage
	if !a.decode(w, r, &value) {
		return false
	}
	if value == nil || len(value) != 0 {
		fail(w, ErrInvalid)
		return false
	}
	return true
}
func (a *API) createEnrollment(w http.ResponseWriter, r *http.Request) {
	if !a.admin(w, r) || !a.emptyObject(w, r) {
		return
	}
	token := NewToken()
	if e := a.Store.CreateEnrollment(r.Context(), token); e != nil {
		fail(w, e)
		return
	}
	writeJSON(w, 201, map[string]string{"enrollment_token": token})
}
func (a *API) connectWorker(w http.ResponseWriter, r *http.Request) {
	token := bearer(r)
	if token == "" || subtle.ConstantTimeCompare(tokenHash(token), tokenHash(a.AdminToken)) == 1 {
		fail(w, ErrUnauthorized)
		return
	}
	// Authenticate before reading/parsing a potentially large resource header.
	// ConnectWorker repeats authorization under its binding transaction.
	if e := a.Store.ValidateConnectCredential(r.Context(), token); e != nil {
		fail(w, e)
		return
	}
	if !a.emptyObject(w, r) {
		return
	}
	profile, e := profileHeader(r, true)
	if e != nil {
		fail(w, e)
		return
	}
	id, e := a.Store.ConnectWorker(r.Context(), token, profile)
	if e != nil {
		fail(w, e)
		return
	}
	policy, e := a.Store.WorkerPolicy(r.Context(), id)
	if e != nil {
		fail(w, e)
		return
	}
	writeJSON(w, 200, map[string]any{"worker_id": id, "update_poll_seconds": policy, "profile_schema": 1})
}
func (a *API) workerMe(w http.ResponseWriter, r *http.Request) {
	id, _ := r.Context().Value(deviceKey{}).(string)
	if id == "" {
		fail(w, ErrUnauthorized)
		return
	}
	value, e := a.Store.WorkerMe(r.Context(), id)
	if e != nil {
		fail(w, e)
		return
	}
	writeJSON(w, 200, value)
}
func (a *API) currentJob(w http.ResponseWriter, r *http.Request) {
	id, _ := r.Context().Value(deviceKey{}).(string)
	if id == "" {
		fail(w, ErrUnauthorized)
		return
	}
	job, e := a.Store.CurrentJob(r.Context(), id)
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
