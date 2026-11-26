package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"regexp"
	"sync"
	"time"
)

type Store struct {
	Pool      *pgxpool.Pool
	Schema    string
	Lease     time.Duration
	Artifacts *ArtifactFiles
	notifyMu  sync.Mutex
	changed   chan struct{}
}

func NewStore(pool *pgxpool.Pool, schema string) *Store {
	if schema == "" {
		schema = "worker_fleet"
	}
	return &Store{Pool: pool, Schema: schema, Lease: 120 * time.Second, changed: make(chan struct{})}
}
func (s *Store) table(name string) string { return pgx.Identifier{s.Schema, name}.Sanitize() }
func (s *Store) leaseSeconds() float64 {
	if s.Lease <= 0 {
		return 120
	}
	return s.Lease.Seconds()
}
func (s *Store) Init(ctx context.Context) error {
	if !regexp.MustCompile(`^[a-z][a-z0-9_]*$`).MatchString(s.Schema) {
		return ErrInvalid
	}
	var exists bool
	if e := s.Pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname=$1)`, s.Schema).Scan(&exists); e != nil {
		return e
	}
	if !exists {
		if _, e := s.Pool.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS `+pgx.Identifier{s.Schema}.Sanitize()); e != nil {
			return e
		}
	}
	var e error
	_, e = s.Pool.Exec(ctx, fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %[1]s(
 id text PRIMARY KEY,token_hash bytea UNIQUE NOT NULL,enabled boolean NOT NULL DEFAULT true,
 capabilities text[] NOT NULL DEFAULT '{}',abis text[] NOT NULL DEFAULT '{}',version_code bigint NOT NULL DEFAULT 0,
 update_poll_seconds integer NOT NULL DEFAULT 900,version_name text NOT NULL DEFAULT '',state text NOT NULL DEFAULT 'registered',active_job_id text NOT NULL DEFAULT '',
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),last_seen_at timestamptz NOT NULL DEFAULT clock_timestamp());
ALTER TABLE %[1]s ADD COLUMN IF NOT EXISTS update_poll_seconds integer NOT NULL DEFAULT 900;
CREATE TABLE IF NOT EXISTS %[2]s(
 id uuid PRIMARY KEY,operation text NOT NULL,input jsonb NOT NULL,script text NOT NULL DEFAULT '',
 required_capabilities text[] NOT NULL DEFAULT '{}',priority integer NOT NULL DEFAULT 0,
 status text NOT NULL DEFAULT 'queued' CHECK(status IN('queued','running','succeeded','failed','cancelled')),
 cancel_requested boolean NOT NULL DEFAULT false,fencing_token bigint NOT NULL DEFAULT 0,active_attempt_id uuid,
 idempotency_key text UNIQUE,request_hash bytea NOT NULL,created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),finished_at timestamptz,not_before timestamptz NOT NULL DEFAULT clock_timestamp());
CREATE TABLE IF NOT EXISTS %[3]s(
 id uuid PRIMARY KEY,job_id uuid NOT NULL REFERENCES %[2]s(id),worker_id text NOT NULL REFERENCES %[1]s(id),
 fencing_token bigint NOT NULL,status text NOT NULL DEFAULT 'running',
 started_at timestamptz NOT NULL DEFAULT clock_timestamp(),last_heartbeat_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 lease_until timestamptz NOT NULL,progress jsonb NOT NULL DEFAULT '{}',completed_at timestamptz,
 completion_hash bytea,receipt jsonb,UNIQUE(job_id,fencing_token));
ALTER TABLE %[3]s ADD COLUMN IF NOT EXISTS slot_id integer NOT NULL DEFAULT 0;
CREATE INDEX IF NOT EXISTS attempts_live_slot ON %[3]s(worker_id,slot_id,lease_until) WHERE status='running';
CREATE INDEX IF NOT EXISTS attempts_live_worker ON %[3]s(worker_id,lease_until) WHERE status='running';
ALTER TABLE %[2]s ADD COLUMN IF NOT EXISTS network jsonb NOT NULL DEFAULT '{}';
CREATE INDEX IF NOT EXISTS jobs_ready ON %[2]s(priority DESC,created_at,id) WHERE status IN('queued','running');
CREATE TABLE IF NOT EXISTS %[4]s(
 job_id uuid PRIMARY KEY REFERENCES %[2]s(id),attempt_id uuid UNIQUE NOT NULL REFERENCES %[3]s(id),
 status text NOT NULL,result jsonb NOT NULL,error jsonb,completion_hash bytea NOT NULL,created_at timestamptz NOT NULL DEFAULT clock_timestamp());
CREATE TABLE IF NOT EXISTS %[5]s(
 id uuid PRIMARY KEY,job_id uuid NOT NULL REFERENCES %[2]s(id),attempt_id uuid NOT NULL REFERENCES %[3]s(id),
 worker_id text NOT NULL,status text NOT NULL,result jsonb NOT NULL,error jsonb,completion_hash bytea NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),UNIQUE(attempt_id,completion_hash));
ALTER TABLE %[4]s ADD COLUMN IF NOT EXISTS artifact_ids text[] NOT NULL DEFAULT '{}';
ALTER TABLE %[5]s ADD COLUMN IF NOT EXISTS artifact_ids text[] NOT NULL DEFAULT '{}';
CREATE TABLE IF NOT EXISTS %[6]s(
 event_id uuid PRIMARY KEY,event_type text NOT NULL,payload jsonb NOT NULL,created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 published_at timestamptz,publish_attempts bigint NOT NULL DEFAULT 0);
CREATE INDEX IF NOT EXISTS outbox_pending ON %[6]s(created_at,event_id) WHERE published_at IS NULL;
CREATE TABLE IF NOT EXISTS %[7]s(
 event_id text PRIMARY KEY,content_hash bytea NOT NULL,payload jsonb NOT NULL,status text NOT NULL,
 receipt jsonb NOT NULL,created_at timestamptz NOT NULL DEFAULT clock_timestamp());
CREATE TABLE IF NOT EXISTS %[8]s(
 event_id text NOT NULL,content_hash bytea NOT NULL,payload jsonb NOT NULL,created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(event_id,content_hash));`, s.table("workers"), s.table("jobs"), s.table("attempts"), s.table("results"), s.table("candidates"), s.table("outbox"), s.table("inbox"), s.table("inbox_conflicts")))
	if e != nil {
		return e
	}
	if e = s.initProfiles(ctx); e != nil {
		return e
	}
	return s.initArtifacts(ctx)
}
func (s *Store) WorkerByToken(ctx context.Context, token string) (string, error) {
	var id string
	e := s.Pool.QueryRow(ctx, `SELECT id FROM `+s.table("workers")+` WHERE token_hash=$1 AND enabled`, tokenHash(token)).Scan(&id)
	if errors.Is(e, pgx.ErrNoRows) {
		return "", ErrUnauthorized
	}
	return id, e
}
func (s *Store) RegisterWorker(ctx context.Context, id, token string) error {
	if !validID(id) || len(token) < 32 {
		return ErrInvalid
	}
	tag, e := s.Pool.Exec(ctx, `INSERT INTO `+s.table("workers")+`(id,token_hash) VALUES($1,$2) ON CONFLICT DO NOTHING`, id, tokenHash(token))
	if e == nil && tag.RowsAffected() == 0 {
		return ErrConflict
	}
	return e
}
func (s *Store) Heartbeat(ctx context.Context, h WorkerHeartbeat) error {
	return s.HeartbeatWithProfile(ctx, h, nil)
}
func (s *Store) HeartbeatWithProfile(ctx context.Context, h WorkerHeartbeat, profile *WorkerProfile) error {
	if profile != nil {
		var e error
		profile, e = profile.normalized()
		if e != nil {
			return e
		}
		h.Capabilities = profile.Capabilities
	}
	tag, e := s.Pool.Exec(ctx, `UPDATE `+s.table("workers")+` SET capabilities=$2,abis=$3,version_code=$4,version_name=$5,state=$6,active_job_id=$7,last_seen_at=clock_timestamp(),profile=CASE WHEN $8::jsonb IS NULL THEN profile ELSE $8::jsonb END,profile_observed_at=CASE WHEN $8::jsonb IS NULL THEN profile_observed_at ELSE clock_timestamp() END WHERE id=$1 AND enabled`, h.WorkerID, sortedUnique(h.Capabilities), sortedUnique(h.ABIs), h.VersionCode, h.VersionName, h.State, h.ActiveJobID, profileBytes(profile))
	if e == nil && tag.RowsAffected() == 0 {
		return ErrUnauthorized
	}
	return e
}
func (s *Store) lockWorker(ctx context.Context, tx pgx.Tx, id string) error {
	var x string
	e := tx.QueryRow(ctx, `SELECT id FROM `+s.table("workers")+` WHERE id=$1 AND enabled FOR UPDATE`, id).Scan(&x)
	if errors.Is(e, pgx.ErrNoRows) {
		return ErrUnauthorized
	}
	return e
}
func (s *Store) event(ctx context.Context, tx pgx.Tx, kind string, payload any) error {
	b, e := json.Marshal(payload)
	if e != nil {
		return e
	}
	_, e = tx.Exec(ctx, `INSERT INTO `+s.table("outbox")+`(event_id,event_type,payload) VALUES($1,$2,$3)`, newID(), kind, b)
	if e == nil && kind != "attempt.started" {
		_, e = tx.Exec(ctx, `SELECT pg_notify($1,$2)`, s.notificationChannel(), kind)
	}
	return e
}
func (s *Store) Enqueue(ctx context.Context, request EnqueueJob) (string, error) {
	j, e := request.normalized()
	if e != nil {
		return "", e
	}
	tx, e := s.Pool.Begin(ctx)
	if e != nil {
		return "", e
	}
	defer tx.Rollback(ctx)
	id, e := s.enqueue(ctx, tx, j)
	if e != nil {
		return "", e
	}
	return id, tx.Commit(ctx)
}
func (s *Store) enqueue(ctx context.Context, tx pgx.Tx, j EnqueueJob) (string, error) {
	id := newID()
	hash := digest(j)
	var saved string
	network := j.Network
	if len(network) == 0 {
		network = json.RawMessage(`{}`)
	}
	resources, _ := json.Marshal(j.ResourceRequirements)
	if j.ResourceRequirements == nil {
		resources = []byte(`{}`)
	}
	e := tx.QueryRow(ctx, `INSERT INTO `+s.table("jobs")+`(id,operation,input,script,required_capabilities,priority,idempotency_key,request_hash,network,resource_requirements) VALUES($1,$2,$3,$4,$5,$6,nullif($7,''),$8,$9,$10) ON CONFLICT(idempotency_key) DO NOTHING RETURNING id::text`, id, j.Operation, []byte(j.Input), j.Script, j.RequiredCapabilities, j.Priority, j.IdempotencyKey, hash, []byte(network), resources).Scan(&saved)
	if errors.Is(e, pgx.ErrNoRows) {
		var old []byte
		e = tx.QueryRow(ctx, `SELECT id::text,request_hash FROM `+s.table("jobs")+` WHERE idempotency_key=$1`, j.IdempotencyKey).Scan(&saved, &old)
		if e != nil {
			return "", e
		}
		if !sameBytes(old, hash) {
			return "", ErrConflict
		}
		return saved, nil
	}
	if e != nil {
		return "", e
	}
	if e = s.event(ctx, tx, "job.queued", map[string]any{"job_id": id, "operation": j.Operation}); e != nil {
		return "", e
	}
	return saved, nil
}
func sameBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func (s *Store) WorkerPolicy(ctx context.Context, id string) (int64, error) {
	var seconds int64
	e := s.Pool.QueryRow(ctx, `SELECT update_poll_seconds FROM `+s.table("workers")+` WHERE id=$1 AND enabled`, id).Scan(&seconds)
	if errors.Is(e, pgx.ErrNoRows) {
		return 0, ErrNotFound
	}
	return seconds, e
}
func (s *Store) SetWorkerPolicy(ctx context.Context, id string, seconds int64) error {
	if seconds <= 0 || seconds > 2147483647 {
		return ErrInvalid
	}
	tag, e := s.Pool.Exec(ctx, `UPDATE `+s.table("workers")+` SET update_poll_seconds=$2 WHERE id=$1`, id, seconds)
	if e == nil && tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return e
}
