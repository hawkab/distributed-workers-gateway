package gateway

import (
	"mime"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func (a *API) streamIdentity(w http.ResponseWriter, r *http.Request) (AttemptIdentity, bool) {
	if r.Header.Get("X-Worker-Id") == "" || r.Header.Get("X-Attempt-Id") == "" || r.Header.Get("X-Fencing-Token") == "" {
		fail(w, ErrUnauthorized)
		return AttemptIdentity{}, false
	}
	fence, e := strconv.ParseInt(r.Header.Get("X-Fencing-Token"), 10, 64)
	i := AttemptIdentity{WorkerID: r.Header.Get("X-Worker-Id"), AttemptID: r.Header.Get("X-Attempt-Id"), FencingToken: fence}
	if e != nil || !i.valid() {
		fail(w, ErrInvalid)
		return i, false
	}
	if !a.device(w, r, i.WorkerID) {
		return i, false
	}
	return i, true
}
func (a *API) createArtifact(w http.ResponseWriter, r *http.Request) {
	var c CreateArtifact
	if !a.decode(w, r, &c) || !a.device(w, r, c.WorkerID) {
		return
	}
	if !uuidPattern.MatchString(r.PathValue("id")) {
		fail(w, ErrInvalid)
		return
	}
	out, created, e := a.Store.CreateArtifact(r.Context(), r.PathValue("id"), c)
	if e != nil {
		fail(w, e)
		return
	}
	status := 200
	if created {
		status = 201
	}
	writeJSON(w, status, out)
}
func (a *API) putSealedArtifact(w http.ResponseWriter, r *http.Request) {
	i, ok := a.streamIdentity(w, r)
	if !ok {
		return
	}
	q := r.URL.Query()
	c := CreateArtifact{AttemptIdentity: i, Key: q.Get("key"), Name: q.Get("name"), MediaType: q.Get("media_type")}
	if c.MediaType == "" {
		c.MediaType = r.Header.Get("Content-Type")
	}
	receipt, err := a.Store.PutSealedArtifact(r.Context(), r.PathValue("id"), c, r.ContentLength, q.Get("sha256"), r.Body)
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, receipt)
}
func (a *API) artifactStatus(w http.ResponseWriter, r *http.Request) {
	i, ok := a.streamIdentity(w, r)
	if !ok {
		return
	}
	if !uuidPattern.MatchString(r.PathValue("id")) {
		fail(w, ErrInvalid)
		return
	}
	out, e := a.Store.ArtifactStatus(r.Context(), r.PathValue("id"), i)
	if e != nil {
		fail(w, e)
		return
	}
	writeJSON(w, 200, out)
}
func (a *API) putArtifactChunk(w http.ResponseWriter, r *http.Request) {
	i, ok := a.streamIdentity(w, r)
	if !ok {
		return
	}
	offset, e := strconv.ParseInt(r.PathValue("offset"), 10, 64)
	if e != nil || offset < 0 || !uuidPattern.MatchString(r.PathValue("id")) {
		fail(w, ErrInvalid)
		return
	}
	// Deliberately no JSON decoder/body buffer, decompressor, overall deadline or
	// aggregate byte limit. A disconnect cannot publish a partial segment.
	out, e := a.Store.PutArtifactChunk(r.Context(), r.PathValue("id"), i, offset, r.Body)
	if e != nil {
		fail(w, e)
		return
	}
	writeJSON(w, 200, out)
}
func (a *API) finishArtifact(w http.ResponseWriter, r *http.Request) {
	var c FinishArtifact
	if !a.decode(w, r, &c) || !a.device(w, r, c.WorkerID) {
		return
	}
	if !uuidPattern.MatchString(r.PathValue("id")) {
		fail(w, ErrInvalid)
		return
	}
	out, e := a.Store.FinishArtifact(r.Context(), r.PathValue("id"), c)
	if e != nil {
		fail(w, e)
		return
	}
	writeJSON(w, 200, out)
}
func (a *API) abortArtifact(w http.ResponseWriter, r *http.Request) {
	var i AttemptIdentity
	if !a.decode(w, r, &i) || !a.device(w, r, i.WorkerID) {
		return
	}
	if !i.valid() || !uuidPattern.MatchString(r.PathValue("id")) {
		fail(w, ErrInvalid)
		return
	}
	out, e := a.Store.AbortArtifact(r.Context(), r.PathValue("id"), i)
	if e != nil {
		fail(w, e)
		return
	}
	writeJSON(w, 200, out)
}
func (a *API) artifactContent(w http.ResponseWriter, r *http.Request) {
	if !a.admin(w, r) {
		return
	}
	if !uuidPattern.MatchString(r.PathValue("id")) {
		fail(w, ErrInvalid)
		return
	}
	file, ref, e := a.Store.OpenArtifact(r.Context(), r.PathValue("id"))
	if e != nil {
		fail(w, e)
		return
	}
	defer file.Close()
	serveArtifactContent(w, r, file, ref)
}
func (a *API) workerArtifactContent(w http.ResponseWriter, r *http.Request) {
	i, ok := a.streamIdentity(w, r)
	if !ok {
		return
	}
	if !uuidPattern.MatchString(r.PathValue("id")) {
		fail(w, ErrInvalid)
		return
	}
	file, ref, e := a.Store.OpenWorkerArtifact(r.Context(), r.PathValue("id"), i)
	if e != nil {
		fail(w, e)
		return
	}
	defer file.Close()
	serveArtifactContent(w, r, file, ref)
}
func serveArtifactContent(w http.ResponseWriter, r *http.Request, file *os.File, ref ArtifactRef) {
	w.Header().Set("Content-Type", ref.MediaType)
	w.Header().Set("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": strings.ReplaceAll(ref.Name, "/", "_")}))
	w.Header().Set("ETag", `"sha256:`+ref.SHA256+`"`)
	http.ServeContent(w, r, ref.Name, time.Time{}, file)
}
func (a *API) beginEffect(w http.ResponseWriter, r *http.Request) {
	var c EffectBegin
	if !a.decode(w, r, &c) || !a.device(w, r, c.WorkerID) {
		return
	}
	out, e := a.Store.BeginEffect(r.Context(), r.PathValue("id"), r.PathValue("effect"), c)
	if e != nil {
		fail(w, e)
		return
	}
	writeJSON(w, 200, out)
}
func (a *API) finishEffect(w http.ResponseWriter, r *http.Request) {
	var c EffectFinish
	if !a.decode(w, r, &c) || !a.device(w, r, c.WorkerID) {
		return
	}
	out, e := a.Store.FinishEffect(r.Context(), r.PathValue("id"), r.PathValue("effect"), c)
	if e != nil {
		fail(w, e)
		return
	}
	writeJSON(w, 200, out)
}
func (a *API) effectStatus(w http.ResponseWriter, r *http.Request) {
	i, ok := a.streamIdentity(w, r)
	if !ok {
		return
	}
	if !uuidPattern.MatchString(r.PathValue("id")) || !validID(r.PathValue("effect")) {
		fail(w, ErrInvalid)
		return
	}
	out, e := a.Store.EffectStatus(r.Context(), r.PathValue("id"), r.PathValue("effect"), i)
	if e != nil {
		fail(w, e)
		return
	}
	writeJSON(w, 200, out)
}
