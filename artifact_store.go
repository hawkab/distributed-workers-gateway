package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"io"
	"math"
	"mime"
	"os"
	"strings"
)

func (s *Store) initArtifacts(ctx context.Context) error {
	_, e := s.Pool.Exec(ctx, fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %[1]s(
 id uuid PRIMARY KEY,job_id uuid NOT NULL REFERENCES %[2]s(id),attempt_id uuid NOT NULL REFERENCES %[3]s(id),
 worker_id text NOT NULL REFERENCES %[4]s(id),fencing_token bigint NOT NULL,artifact_key text NOT NULL,artifact_name text NOT NULL DEFAULT '',media_type text NOT NULL,
 request_hash bytea NOT NULL,status text NOT NULL DEFAULT 'open' CHECK(status IN('open','sealed','aborted')),
 byte_offset bigint NOT NULL DEFAULT 0 CHECK(byte_offset>=0),blob_sha256 text,receipt jsonb,chunks_cleaned boolean NOT NULL DEFAULT false,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),UNIQUE(attempt_id,artifact_key));
ALTER TABLE %[1]s ADD COLUMN IF NOT EXISTS artifact_name text NOT NULL DEFAULT '';
CREATE TABLE IF NOT EXISTS %[5]s(
 upload_id uuid NOT NULL REFERENCES %[1]s(id),byte_offset bigint NOT NULL,length bigint NOT NULL CHECK(length>0),sha256 text NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),PRIMARY KEY(upload_id,byte_offset));
CREATE INDEX IF NOT EXISTS artifact_job ON %[1]s(job_id);
CREATE TABLE IF NOT EXISTS %[6]s(
 job_id uuid NOT NULL REFERENCES %[2]s(id),effect_id text NOT NULL,request_sha256 text NOT NULL,replay_policy text NOT NULL,
 method text NOT NULL,idempotency_header text NOT NULL DEFAULT '',idempotency_key_hash bytea,
 worker_id text NOT NULL,attempt_id uuid NOT NULL REFERENCES %[3]s(id),fencing_token bigint NOT NULL,execution_id uuid NOT NULL,
 status text NOT NULL CHECK(status IN('started','uncertain','completed')),response jsonb,artifact_ids text[] NOT NULL DEFAULT '{}',
 finish_hash bytea,receipt jsonb,created_at timestamptz NOT NULL DEFAULT clock_timestamp(),updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(job_id,effect_id));
CREATE TABLE IF NOT EXISTS %[7]s(
 job_id uuid NOT NULL,effect_id text NOT NULL,attempt_id uuid NOT NULL,content_hash bytea NOT NULL,response jsonb NOT NULL,
 artifact_ids text[] NOT NULL DEFAULT '{}',created_at timestamptz NOT NULL DEFAULT clock_timestamp(),PRIMARY KEY(job_id,effect_id,attempt_id,content_hash));
CREATE TABLE IF NOT EXISTS %[8]s(
 job_id uuid NOT NULL,effect_id text NOT NULL,execution_id uuid NOT NULL,worker_id text NOT NULL,attempt_id uuid NOT NULL,
 fencing_token bigint NOT NULL,request_sha256 text NOT NULL,created_at timestamptz NOT NULL DEFAULT clock_timestamp(),PRIMARY KEY(job_id,effect_id,execution_id));`,
		s.table("artifact_uploads"), s.table("jobs"), s.table("attempts"), s.table("workers"), s.table("artifact_chunks"), s.table("effects"), s.table("effect_candidates"), s.table("effect_executions")))
	return e
}

type artifactUpload struct {
	id, job, attempt, worker, key, name, media, status, sha string
	fence, offset                                           int64
	receipt                                                 []byte
	cleaned                                                 bool
}

func (s *Store) upload(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, id string, identity AttemptIdentity, lock bool) (artifactUpload, error) {
	u, e := s.artifactByID(ctx, q, id, lock)
	if e != nil {
		return u, e
	}
	if u.worker != identity.WorkerID || u.attempt != identity.AttemptID || u.fence != identity.FencingToken {
		return u, ErrUnauthorized
	}
	return u, nil
}
func (s *Store) artifactByID(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, id string, lock bool) (artifactUpload, error) {
	var u artifactUpload
	sql := `SELECT id::text,job_id::text,attempt_id::text,worker_id,fencing_token,artifact_key,artifact_name,media_type,status,byte_offset,coalesce(blob_sha256,''),receipt,chunks_cleaned FROM ` + s.table("artifact_uploads") + ` WHERE id=$1`
	if lock {
		sql += ` FOR UPDATE`
	}
	e := q.QueryRow(ctx, sql, id).Scan(&u.id, &u.job, &u.attempt, &u.worker, &u.fence, &u.key, &u.name, &u.media, &u.status, &u.offset, &u.sha, &u.receipt, &u.cleaned)
	if errors.Is(e, pgx.ErrNoRows) {
		return u, ErrNotFound
	}
	if e != nil {
		return u, e
	}
	if u.name == "" {
		u.name = u.key
	}
	return u, nil
}
func (s *Store) CreateArtifact(ctx context.Context, job string, c CreateArtifact) (ArtifactStatus, bool, error) {
	var out ArtifactStatus
	if s.Artifacts == nil {
		return out, false, ErrUnavailable
	}
	if !uuidPattern.MatchString(job) || !c.AttemptIdentity.valid() || !artifactKeyPattern.MatchString(c.Key) {
		return out, false, ErrInvalid
	}
	if c.MediaType == "" {
		c.MediaType = "application/octet-stream"
	}
	if c.Name == "" {
		c.Name = c.Key
	}
	if strings.ContainsAny(c.Name, "\r\n\x00") {
		return out, false, ErrInvalid
	}
	if _, _, e := mime.ParseMediaType(c.MediaType); e != nil {
		return out, false, ErrInvalid
	}
	var owned bool
	e := s.Pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM `+s.table("attempts")+` WHERE id=$1 AND job_id=$2 AND worker_id=$3 AND fencing_token=$4)`, c.AttemptID, job, c.WorkerID, c.FencingToken).Scan(&owned)
	if e != nil {
		return out, false, e
	}
	if !owned {
		return out, false, ErrUnauthorized
	}
	id := newID()
	hash := digest(struct {
		Job     string
		Request CreateArtifact
	}{job, c})
	tag, e := s.Pool.Exec(ctx, `INSERT INTO `+s.table("artifact_uploads")+`(id,job_id,attempt_id,worker_id,fencing_token,artifact_key,artifact_name,media_type,request_hash) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT(attempt_id,artifact_key) DO NOTHING`, id, job, c.AttemptID, c.WorkerID, c.FencingToken, c.Key, c.Name, c.MediaType, hash)
	if e != nil {
		return out, false, e
	}
	created := tag.RowsAffected() == 1
	if !created {
		var old []byte
		e = s.Pool.QueryRow(ctx, `SELECT id::text,request_hash FROM `+s.table("artifact_uploads")+` WHERE attempt_id=$1 AND artifact_key=$2`, c.AttemptID, c.Key).Scan(&id, &old)
		if e != nil {
			return out, false, e
		}
		if !sameBytes(old, hash) {
			return out, false, ErrConflict
		}
	}
	out, e = s.artifactWriteStatus(ctx, id, c.AttemptIdentity)
	return out, created, e
}
func (s *Store) ArtifactStatus(ctx context.Context, id string, identity AttemptIdentity) (ArtifactStatus, error) {
	if !identity.valid() {
		return ArtifactStatus{}, ErrInvalid
	}
	tx, e := s.Pool.Begin(ctx)
	if e != nil {
		return ArtifactStatus{}, e
	}
	defer tx.Rollback(ctx)
	u, e := s.artifactByID(ctx, tx, id, false)
	if e != nil {
		return ArtifactStatus{}, e
	}
	if e = s.currentAttempt(ctx, tx, u.job, identity); e != nil {
		return ArtifactStatus{}, e
	}
	out, e := s.artifactStatusSnapshot(ctx, tx, u, identity.AttemptID)
	if e != nil {
		return out, e
	}
	if e = s.currentAttempt(ctx, tx, u.job, identity); e != nil {
		return ArtifactStatus{}, e
	}
	return out, tx.Commit(ctx)
}
func (s *Store) artifactWriteStatus(ctx context.Context, id string, identity AttemptIdentity) (ArtifactStatus, error) {
	u, e := s.upload(ctx, s.Pool, id, identity, false)
	if e != nil {
		return ArtifactStatus{}, e
	}
	return s.artifactStatusSnapshot(ctx, s.Pool, u, u.attempt)
}
func (s *Store) artifactStatusSnapshot(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, u artifactUpload, attempt string) (ArtifactStatus, error) {
	id := u.id
	out := ArtifactStatus{UploadID: id}
	out.Status = u.status
	out.Offset = u.offset
	e := q.QueryRow(ctx, `SELECT a.lease_until,j.cancel_requested FROM `+s.table("attempts")+` a JOIN `+s.table("jobs")+` j ON j.id=a.job_id WHERE a.id=$1`, attempt).Scan(&out.LeaseUntil, &out.CancelRequested)
	if e != nil {
		return out, e
	}
	e = q.QueryRow(ctx, `SELECT count(*) FROM `+s.table("artifact_chunks")+` WHERE upload_id=$1`, id).Scan(&out.SegmentsCount)
	if e != nil {
		return out, e
	}
	last := ArtifactChunk{UploadID: id}
	e = q.QueryRow(ctx, `SELECT byte_offset,length,sha256 FROM `+s.table("artifact_chunks")+` WHERE upload_id=$1 ORDER BY byte_offset DESC LIMIT 1`, id).Scan(&last.Offset, &last.Length, &last.SHA256)
	if e == nil {
		last.NextOffset = last.Offset + last.Length
		out.LastSegment = &last
	} else if !errors.Is(e, pgx.ErrNoRows) {
		return out, e
	}
	if len(u.receipt) > 0 {
		out.Artifact = &ArtifactReceipt{}
		if e = json.Unmarshal(u.receipt, out.Artifact); e != nil {
			return out, e
		}
	}
	return out, nil
}
func (s *Store) chunk(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, id string, offset int64) (ArtifactChunk, error) {
	c := ArtifactChunk{UploadID: id, Offset: offset}
	e := q.QueryRow(ctx, `SELECT length,sha256 FROM `+s.table("artifact_chunks")+` WHERE upload_id=$1 AND byte_offset=$2`, id, offset).Scan(&c.Length, &c.SHA256)
	c.NextOffset = offset + c.Length
	return c, e
}
func (s *Store) PutArtifactChunk(ctx context.Context, id string, identity AttemptIdentity, offset int64, body io.Reader) (ArtifactChunk, error) {
	var out ArtifactChunk
	if s.Artifacts == nil {
		return out, ErrUnavailable
	}
	if offset < 0 || !identity.valid() {
		return out, ErrInvalid
	}
	u, e := s.upload(ctx, s.Pool, id, identity, false)
	if e != nil {
		return out, e
	}
	old, e := s.chunk(ctx, s.Pool, id, offset)
	if e == nil {
		sha, n, e := copyArtifact(ctx, io.Discard, body)
		if e != nil {
			return out, e
		}
		if sha != old.SHA256 || n != old.Length {
			return out, ErrConflict
		}
		return old, nil
	}
	if !errors.Is(e, pgx.ErrNoRows) {
		return out, e
	}
	if u.status != "open" || u.offset != offset {
		return out, ErrConflict
	}
	staged, e := s.Artifacts.stage(ctx, id, body)
	if e != nil {
		return out, e
	}
	defer os.Remove(staged.path)
	if staged.length <= 0 || staged.length > math.MaxInt64-offset {
		return out, ErrInvalid
	}
	tx, e := s.Pool.Begin(ctx)
	if e != nil {
		return out, e
	}
	defer tx.Rollback(ctx)
	u, e = s.upload(ctx, tx, id, identity, true)
	if e != nil {
		return out, e
	}
	old, e = s.chunk(ctx, tx, id, offset)
	if e == nil {
		if old.SHA256 != staged.sha || old.Length != staged.length {
			return out, ErrConflict
		}
		return old, tx.Commit(ctx)
	}
	if !errors.Is(e, pgx.ErrNoRows) {
		return out, e
	}
	if u.status != "open" || u.offset != offset {
		return out, ErrConflict
	}
	if e = s.Artifacts.promoteChunk(ctx, staged, id, offset); e != nil {
		return out, e
	}
	_, e = tx.Exec(ctx, `INSERT INTO `+s.table("artifact_chunks")+`(upload_id,byte_offset,length,sha256) VALUES($1,$2,$3,$4)`, id, offset, staged.length, staged.sha)
	if e != nil {
		return out, e
	}
	_, e = tx.Exec(ctx, `UPDATE `+s.table("artifact_uploads")+` SET byte_offset=$2,updated_at=clock_timestamp() WHERE id=$1`, id, offset+staged.length)
	if e != nil {
		return out, e
	}
	out = ArtifactChunk{UploadID: id, Offset: offset, NextOffset: offset + staged.length, Length: staged.length, SHA256: staged.sha}
	return out, tx.Commit(ctx)
}
func (s *Store) AbortArtifact(ctx context.Context, id string, identity AttemptIdentity) (ArtifactStatus, error) {
	u, e := s.upload(ctx, s.Pool, id, identity, false)
	if e != nil {
		return ArtifactStatus{}, e
	}
	if u.status == "sealed" {
		return ArtifactStatus{}, ErrConflict
	}
	_, e = s.Pool.Exec(ctx, `UPDATE `+s.table("artifact_uploads")+` SET status='aborted',updated_at=clock_timestamp() WHERE id=$1 AND status='open'`, id)
	if e != nil {
		return ArtifactStatus{}, e
	}
	return s.artifactWriteStatus(ctx, id, identity)
}
