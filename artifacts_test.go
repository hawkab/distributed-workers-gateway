package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"sync"
	"testing"
)

func artifactFixture(t *testing.T) (*Store, *ClaimedJob, string) {
	t.Helper()
	s := fixture(t)
	var e error
	s.Artifacts, e = NewArtifactFiles(t.TempDir())
	must(t, e)
	token := register(t, s, "stream-worker")
	enqueue(t, s, "network.http", "stream-job")
	j, e := s.Claim(context.Background(), "stream-worker", []string{"network.http"})
	must(t, e)
	return s, j, token
}
func identity(j *ClaimedJob, worker string) AttemptIdentity {
	return AttemptIdentity{WorkerID: worker, AttemptID: j.AttemptID, FencingToken: j.FencingToken}
}
func newUpload(t *testing.T, s *Store, j *ClaimedJob, key string) ArtifactStatus {
	t.Helper()
	a, created, e := s.CreateArtifact(context.Background(), j.ID, CreateArtifact{AttemptIdentity: identity(j, "stream-worker"), Key: key})
	must(t, e)
	if !created {
		t.Fatal("not newly allocated")
	}
	return a
}

type patternReader struct {
	left    int64
	maxRead int
	fill    byte
}

func (r *patternReader) Read(p []byte) (int, error) {
	if len(p) > 32*1024 {
		return 0, errors.New("unbounded read buffer")
	}
	if len(p) > r.maxRead {
		r.maxRead = len(p)
	}
	if r.left == 0 {
		return 0, io.EOF
	}
	n := len(p)
	if int64(n) > r.left {
		n = int(r.left)
	}
	for i := range p[:n] {
		p[i] = r.fill
	}
	r.left -= int64(n)
	return n, nil
}

type brokenArtifactReader struct{ sent bool }

func (r *brokenArtifactReader) Read(p []byte) (int, error) {
	if r.sent {
		return 0, io.ErrUnexpectedEOF
	}
	r.sent = true
	copy(p, "incomplete")
	return len("incomplete"), nil
}
func TestArtifactLargeStreamBoundedMemoryDedupAndSingleChunkHardlink(t *testing.T) {
	s, j, _ := artifactFixture(t)
	ctx := context.Background()
	a := newUpload(t, s, j, "request/large")
	i := identity(j, "stream-worker")
	size := int64(32<<20) + 123
	reader := &patternReader{left: size, fill: 0x42}
	chunk, e := s.PutArtifactChunk(ctx, a.UploadID, i, 0, reader)
	must(t, e)
	if chunk.NextOffset != size || chunk.Length != size || reader.maxRead > 32*1024 {
		t.Fatal(chunk, reader.maxRead)
	}
	chunkInfo, e := os.Stat(s.Artifacts.chunkPath(a.UploadID, 0, chunk.SHA256))
	must(t, e)
	duplicate, e := s.PutArtifactChunk(ctx, a.UploadID, i, 0, &patternReader{left: size, fill: 0x42})
	must(t, e)
	if !reflect.DeepEqual(chunk, duplicate) {
		t.Fatal("chunk receipt changed")
	}
	if _, e = s.PutArtifactChunk(ctx, a.UploadID, i, 0, &patternReader{left: size, fill: 0x43}); !errors.Is(e, ErrConflict) {
		t.Fatal("changed bytes accepted", e)
	}
	var published int
	must(t, s.Pool.QueryRow(ctx, `SELECT count(*) FROM `+s.table("outbox")+` WHERE event_type='artifact.sealed'`).Scan(&published))
	if published != 0 {
		t.Fatal("partial upload was published")
	}
	ref, e := s.FinishArtifact(ctx, a.UploadID, FinishArtifact{AttemptIdentity: i, Length: size, SHA256: chunk.SHA256})
	must(t, e)
	if !ref.Committed || ref.CandidateSaved || ref.Length != size {
		t.Fatal(ref)
	}
	objectInfo, e := os.Stat(s.Artifacts.objectPath(ref.SHA256))
	must(t, e)
	if !os.SameFile(chunkInfo, objectInfo) {
		t.Fatal("single chunk copied instead of linked")
	}
	if _, e = os.Stat(s.Artifacts.chunkPath(a.UploadID, 0, chunk.SHA256)); !errors.Is(e, os.ErrNotExist) {
		t.Fatal("duplicate chunk retained", e)
	}
	status, e := s.ArtifactStatus(ctx, a.UploadID, i)
	must(t, e)
	if status.Status != "sealed" || status.Offset != size || status.SegmentsCount != 1 || status.LastSegment == nil {
		t.Fatal(status)
	}
	expire(t, s, j.AttemptID)
	again, e := s.FinishArtifact(ctx, a.UploadID, FinishArtifact{AttemptIdentity: i, Length: size})
	must(t, e)
	if !reflect.DeepEqual(ref, again) {
		t.Fatal("receipt changed after lease expiry")
	}
	if _, e = s.ArtifactStatus(ctx, a.UploadID, i); !errors.Is(e, ErrLeaseLost) {
		t.Fatal("expired creator retained status read", e)
	}
}
func TestArtifactAbortStreamNoPartialAndCommittedOffsetReconciles(t *testing.T) {
	s, j, _ := artifactFixture(t)
	ctx := context.Background()
	a := newUpload(t, s, j, "aborted-stream")
	i := identity(j, "stream-worker")
	if _, e := s.PutArtifactChunk(ctx, a.UploadID, i, 0, &brokenArtifactReader{}); !errors.Is(e, io.ErrUnexpectedEOF) {
		t.Fatal(e)
	}
	st, e := s.ArtifactStatus(ctx, a.UploadID, i)
	must(t, e)
	if st.Offset != 0 || st.SegmentsCount != 0 {
		t.Fatal("partial segment became visible", st)
	}
	files, e := os.ReadDir(filepath.Join(s.Artifacts.root, "tmp"))
	must(t, e)
	if len(files) != 0 {
		t.Fatal("failed stream temporary file retained")
	}
	one, e := s.PutArtifactChunk(ctx, a.UploadID, i, 0, bytes.NewBufferString("abc"))
	must(t, e)
	if one.NextOffset != 3 {
		t.Fatal(one)
	}
	two, e := s.PutArtifactChunk(ctx, a.UploadID, i, 3, bytes.NewBufferString("def"))
	must(t, e)
	if two.NextOffset != 6 {
		t.Fatal(two)
	}
	// Lost ACK is recovered from durable metadata, independent of client process.
	fresh := NewStore(s.Pool, s.Schema)
	fresh.Artifacts = s.Artifacts
	st, e = fresh.ArtifactStatus(ctx, a.UploadID, i)
	must(t, e)
	if st.Offset != 6 || st.SegmentsCount != 2 {
		t.Fatal(st)
	}
	sum := sha256.Sum256([]byte("abcdef"))
	ref, e := fresh.FinishArtifact(ctx, a.UploadID, FinishArtifact{AttemptIdentity: i, Length: 6, SHA256: hex.EncodeToString(sum[:])})
	must(t, e)
	f, _, e := fresh.OpenArtifact(ctx, ref.ArtifactID)
	must(t, e)
	raw, e := io.ReadAll(f)
	f.Close()
	must(t, e)
	if string(raw) != "abcdef" {
		t.Fatal("ordered merge changed bytes")
	}
}
func TestArtifactConcurrentSameOffsetHasOneDurableSegment(t *testing.T) {
	s, j, _ := artifactFixture(t)
	a := newUpload(t, s, j, "race")
	i := identity(j, "stream-worker")
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, e := s.PutArtifactChunk(context.Background(), a.UploadID, i, 0, bytes.NewBufferString("same"))
			errs <- e
		}()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		must(t, e)
	}
	st, e := s.ArtifactStatus(context.Background(), a.UploadID, i)
	must(t, e)
	if st.Offset != 4 || st.SegmentsCount != 1 {
		t.Fatal(st)
	}
}
func TestArtifactConcurrentFinishReturnsOneStableReceipt(t *testing.T) {
	s, j, _ := artifactFixture(t)
	ctx := context.Background()
	a := newUpload(t, s, j, "finish-race")
	i := identity(j, "stream-worker")
	_, e := s.PutArtifactChunk(ctx, a.UploadID, i, 0, &patternReader{left: 1 << 20, fill: 1})
	must(t, e)
	_, e = s.PutArtifactChunk(ctx, a.UploadID, i, 1<<20, &patternReader{left: 1 << 20, fill: 2})
	must(t, e)
	var wg sync.WaitGroup
	results := make(chan ArtifactReceipt, 16)
	errs := make(chan error, 16)
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, e := s.FinishArtifact(ctx, a.UploadID, FinishArtifact{AttemptIdentity: i, Length: 2 << 20})
			if e != nil {
				errs <- e
			} else {
				results <- r
			}
		}()
	}
	wg.Wait()
	close(results)
	close(errs)
	for e := range errs {
		must(t, e)
	}
	var first *ArtifactReceipt
	for r := range results {
		if first == nil {
			v := r
			first = &v
		} else if !reflect.DeepEqual(*first, r) {
			t.Fatal("finish ACK changed", first, r)
		}
	}
	var events int
	must(t, s.Pool.QueryRow(ctx, `SELECT count(*) FROM `+s.table("outbox")+` WHERE event_type='artifact.sealed'`).Scan(&events))
	if events != 1 {
		t.Fatal("duplicate seal events", events)
	}
}
func TestArtifactOptionalNameIsMetadataOnly(t *testing.T) {
	s, j, _ := artifactFixture(t)
	ctx := context.Background()
	c := CreateArtifact{AttemptIdentity: identity(j, "stream-worker"), Key: "opaque/request", Name: "Отчёт.pdf", MediaType: "application/pdf"}
	a, _, e := s.CreateArtifact(ctx, j.ID, c)
	must(t, e)
	_, e = s.PutArtifactChunk(ctx, a.UploadID, c.AttemptIdentity, 0, bytes.NewBufferString("pdf-fixture"))
	must(t, e)
	ref, e := s.FinishArtifact(ctx, a.UploadID, FinishArtifact{AttemptIdentity: c.AttemptIdentity, Length: 11})
	must(t, e)
	if ref.Name != c.Name {
		t.Fatal(ref)
	}
	duplicate, created, e := s.CreateArtifact(ctx, j.ID, c)
	must(t, e)
	if created || duplicate.UploadID != a.UploadID {
		t.Fatal("name metadata broke request idempotency")
	}
	if _, e = os.Stat(filepath.Join(s.Artifacts.root, c.Name)); !errors.Is(e, os.ErrNotExist) {
		t.Fatal("metadata name used as filesystem path")
	}
}
func TestWorkerArtifactReadScopeSupportsCurrentSameJobAdoption(t *testing.T) {
	s, j, _ := artifactFixture(t)
	ctx := context.Background()
	a := newUpload(t, s, j, "worker-read")
	i := identity(j, "stream-worker")
	_, e := s.PutArtifactChunk(ctx, a.UploadID, i, 0, bytes.NewBufferString("body"))
	must(t, e)
	_, e = s.FinishArtifact(ctx, a.UploadID, FinishArtifact{AttemptIdentity: i, Length: 4})
	must(t, e)
	f, _, e := s.OpenWorkerArtifact(ctx, a.UploadID, i)
	must(t, e)
	f.Close()
	expire(t, s, j.AttemptID)
	register(t, s, "new-reader")
	next, e := s.Claim(ctx, "new-reader", []string{"network.http"})
	must(t, e)
	f, _, e = s.OpenWorkerArtifact(ctx, a.UploadID, identity(next, "new-reader"))
	must(t, e)
	f.Close()
	register(t, s, "cross-job")
	enqueue(t, s, "other.operation", "different-job")
	other, e := s.Claim(ctx, "cross-job", []string{"other.operation"})
	must(t, e)
	if f, _, e = s.OpenWorkerArtifact(ctx, a.UploadID, identity(other, "cross-job")); !errors.Is(e, ErrUnauthorized) {
		if f != nil {
			f.Close()
		}
		t.Fatal("other job could read artifact", e)
	}
	expire(t, s, next.AttemptID)
	if f, _, e = s.OpenWorkerArtifact(ctx, a.UploadID, identity(next, "new-reader")); !errors.Is(e, ErrLeaseLost) {
		if f != nil {
			f.Close()
		}
		t.Fatal("noncreator stale reader retained adoption grant", e)
	}
}
func TestStaleArtifactPreservedCurrentAttemptCanAdoptSameJobOnly(t *testing.T) {
	s, j, _ := artifactFixture(t)
	ctx := context.Background()
	a := newUpload(t, s, j, "late")
	i := identity(j, "stream-worker")
	_, e := s.PutArtifactChunk(ctx, a.UploadID, i, 0, bytes.NewBufferString("late-result"))
	must(t, e)
	expire(t, s, j.AttemptID)
	register(t, s, "new-worker")
	next, e := s.Claim(ctx, "new-worker", []string{"network.http"})
	must(t, e)
	ref, e := s.FinishArtifact(ctx, a.UploadID, FinishArtifact{AttemptIdentity: i, Length: 11})
	must(t, e)
	if ref.Committed || !ref.CandidateSaved {
		t.Fatal("stale artifact became canonical", ref)
	}
	c := done(next, "new-worker")
	c.ArtifactIDs = []string{ref.ArtifactID}
	receipt, e := s.Complete(ctx, next.ID, c)
	must(t, e)
	if !receipt.Committed {
		t.Fatal(receipt)
	}
	other := enqueue(t, s, "network.http", "other-job")
	another, e := s.Claim(ctx, "new-worker", []string{"network.http"})
	must(t, e)
	if another.ID != other {
		t.Fatal(another)
	}
	bad := done(another, "new-worker")
	bad.ArtifactIDs = c.ArtifactIDs
	if _, e = s.Complete(ctx, other, bad); !errors.Is(e, ErrUnauthorized) {
		t.Fatal("cross-job artifact adopted", e)
	}
}
func TestSealedArtifactCleanupReconcileKeepsOnlyVerifiedObject(t *testing.T) {
	s, j, _ := artifactFixture(t)
	ctx := context.Background()
	a := newUpload(t, s, j, "cleanup")
	i := identity(j, "stream-worker")
	chunk, e := s.PutArtifactChunk(ctx, a.UploadID, i, 0, bytes.NewBufferString("kept"))
	must(t, e)
	ref, e := s.FinishArtifact(ctx, a.UploadID, FinishArtifact{AttemptIdentity: i, Length: 4})
	must(t, e)
	chunkPath := s.Artifacts.chunkPath(a.UploadID, 0, chunk.SHA256)
	must(t, os.MkdirAll(filepath.Dir(chunkPath), 0700))
	must(t, os.Link(s.Artifacts.objectPath(ref.SHA256), chunkPath))
	must(t, os.Remove(s.Artifacts.objectPath(ref.SHA256)))
	_, e = s.Pool.Exec(ctx, `UPDATE `+s.table("artifact_uploads")+` SET chunks_cleaned=false WHERE id=$1`, a.UploadID)
	must(t, e)
	if e = s.ReconcileArtifacts(ctx); e == nil {
		t.Fatal("missing CAS object accepted")
	}
	if _, e = os.Stat(chunkPath); e != nil {
		t.Fatal("last copy deleted", e)
	}
	must(t, os.Link(chunkPath, s.Artifacts.objectPath(ref.SHA256)))
	must(t, s.ReconcileArtifacts(ctx))
	if _, e = os.Stat(chunkPath); !errors.Is(e, os.ErrNotExist) {
		t.Fatal("redundant chunk not reclaimed", e)
	}
}
func TestArtifactHTTPStreamsBeyondJSONBudgetAndAdminOnlyDownload(t *testing.T) {
	s, j, token := artifactFixture(t)
	a := newUpload(t, s, j, "http")
	handler := (&API{Store: s, AdminToken: "admin-secret", MaxBodyBytes: 512}).Handler()
	server := httptest.NewServer(handler)
	defer server.Close()
	req, e := http.NewRequest("PUT", server.URL+"/v1/artifacts/"+a.UploadID+"/chunks/0", &patternReader{left: 1 << 20, fill: 0x61})
	must(t, e)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("X-Worker-Id", "stream-worker")
	req.Header.Set("X-Attempt-Id", j.AttemptID)
	req.Header.Set("X-Fencing-Token", strconv.FormatInt(j.FencingToken, 10))
	response, e := server.Client().Do(req)
	must(t, e)
	defer response.Body.Close()
	if response.StatusCode != 200 {
		t.Fatal(response.StatusCode)
	}
	var chunk ArtifactChunk
	must(t, json.NewDecoder(response.Body).Decode(&chunk))
	ref, e := s.FinishArtifact(context.Background(), a.UploadID, FinishArtifact{AttemptIdentity: identity(j, "stream-worker"), Length: 1 << 20, SHA256: chunk.SHA256})
	must(t, e)
	unauth, e := server.Client().Get(server.URL + "/v1/admin/artifacts/" + ref.ArtifactID + "/content")
	must(t, e)
	unauth.Body.Close()
	if unauth.StatusCode != 401 {
		t.Fatal(unauth.StatusCode)
	}
	request, e := http.NewRequest("GET", server.URL+"/v1/admin/artifacts/"+ref.ArtifactID+"/content", nil)
	must(t, e)
	request.Header.Set("Authorization", "Bearer admin-secret")
	got, e := server.Client().Do(request)
	must(t, e)
	sha, n, e := copyArtifact(context.Background(), io.Discard, got.Body)
	got.Body.Close()
	must(t, e)
	if got.StatusCode != 200 || n != ref.Length || sha != ref.SHA256 {
		t.Fatal("download altered bytes")
	}
}
