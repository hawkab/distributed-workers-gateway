package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"sync"
	"testing"
)

func TestWholeArtifactRetryIntegrityAndAttempt(t *testing.T) {
	s, j, _ := artifactFixture(t)
	ctx := context.Background()
	c := CreateArtifact{AttemptIdentity: identity(j, "stream-worker"), Key: "result/data", Name: "data.bin"}
	body := []byte("sealed binary\x00")
	sum := sha256.Sum256(body)
	sha := hex.EncodeToString(sum[:])
	put := func(c CreateArtifact, reader io.Reader) (ArtifactReceipt, error) {
		return s.PutSealedArtifact(ctx, j.ID, c, int64(len(body)), sha, reader)
	}
	if _, err := put(c, &brokenArtifactReader{}); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("interrupted: %v", err)
	}
	if _, err := put(c, bytes.NewReader(body[1:])); !errors.Is(err, ErrConflict) {
		t.Fatalf("short body: %v", err)
	}
	var events int
	countEvents := func() int {
		t.Helper()
		must(t, s.Pool.QueryRow(ctx, "SELECT count(*) FROM "+s.table("outbox")+" WHERE event_type='artifact.sealed'").Scan(&events))
		return events
	}
	if countEvents() != 0 {
		t.Fatal("interrupted transfer published")
	}
	const n = 6
	var wg sync.WaitGroup
	receipts := make([]ArtifactReceipt, n)
	errs := make([]error, n)
	for i := range n {
		wg.Add(1)
		go func(i int) { defer wg.Done(); receipts[i], errs[i] = put(c, bytes.NewReader(body)) }(i)
	}
	wg.Wait()
	for i := range n {
		must(t, errs[i])
		if receipts[i] != receipts[0] || !receipts[i].Committed {
			t.Fatalf("receipt %d: %+v", i, receipts[i])
		}
	}
	if countEvents() != 1 {
		t.Fatal("duplicate event")
	}
	d := c
	d.WorkerID = "foreign"
	if _, err := put(d, bytes.NewReader(body)); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("wrong identity: %v", err)
	}
	expire(t, s, j.AttemptID)
	c.Key = "late"
	later, err := put(c, bytes.NewReader(body))
	must(t, err)
	if later.Committed || !later.CandidateSaved {
		t.Fatal("expired lease marked live")
	}
	f, _, err := s.OpenArtifact(ctx, later.ArtifactID)
	must(t, err)
	defer f.Close()
	data, err := io.ReadAll(f)
	must(t, err)
	if !bytes.Equal(data, body) {
		t.Fatal("content mismatch")
	}
}

func TestMultiArtifactValidationInOrder(t *testing.T) {
	s, j, _ := artifactFixture(t)
	ctx := context.Background()
	opened := newUpload(t, s, j, "open")
	sealed := newUpload(t, s, j, "finished")
	_, err := s.FinishArtifact(ctx, sealed.UploadID, FinishArtifact{AttemptIdentity: identity(j, "stream-worker")})
	must(t, err)
	tx, err := s.Pool.Begin(ctx)
	must(t, err)
	defer tx.Rollback(ctx)
	for _, tc := range []struct {
		job  string
		ids  []string
		want error
	}{
		{j.ID, nil, nil}, {j.ID, []string{sealed.UploadID, sealed.UploadID}, nil},
		{j.ID, []string{opened.UploadID, newID()}, ErrConflict},
		{j.ID, []string{newID(), opened.UploadID}, ErrNotFound},
		{newID(), []string{sealed.UploadID, newID()}, ErrUnauthorized},
	} {
		if e := s.validateArtifacts(ctx, tx, tc.job, tc.ids); !errors.Is(e, tc.want) {
			t.Fatalf("wanted %v, got %v", tc.want, e)
		}
	}
}
