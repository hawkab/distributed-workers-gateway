package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/jackc/pgx/v5"
)

func (s *Store) Workers(ctx context.Context) (json.RawMessage, error) {
	var b []byte
	e := s.Pool.QueryRow(ctx, `SELECT coalesce(jsonb_agg(x ORDER BY x.id),'[]'::jsonb) FROM (SELECT w.id,w.enabled,w.capabilities,w.abis,w.version_code,w.version_name,w.update_poll_seconds,w.state,w.active_job_id,w.created_at,w.last_seen_at,(w.last_seen_at>clock_timestamp()-$1::double precision*interval '1 second') AS online,(SELECT count(*) FROM `+s.table("attempts")+` a WHERE a.worker_id=w.id AND a.status='running' AND a.lease_until>clock_timestamp()) AS active_leases FROM `+s.table("workers")+` w)x`, s.leaseSeconds()).Scan(&b)
	return b, e
}
func (s *Store) Jobs(ctx context.Context, limit int) (json.RawMessage, error) {
	var b []byte
	e := s.Pool.QueryRow(ctx, `SELECT coalesce(jsonb_agg(x ORDER BY x.created_at DESC,x.id),'[]'::jsonb) FROM(SELECT j.id,j.operation,j.required_capabilities,j.priority,j.status,j.cancel_requested,j.fencing_token,j.active_attempt_id,j.created_at,j.updated_at,j.finished_at FROM `+s.table("jobs")+` j ORDER BY j.created_at DESC,j.id LIMIT $1)x`, limit).Scan(&b)
	return b, e
}
func (s *Store) Job(ctx context.Context, id string) (json.RawMessage, error) {
	var b []byte
	e := s.Pool.QueryRow(ctx, `SELECT jsonb_build_object('id',j.id,'operation',j.operation,'input',j.input,'network',j.network,'script',j.script,'required_capabilities',j.required_capabilities,'priority',j.priority,'status',j.status,'cancel_requested',j.cancel_requested,'fencing_token',j.fencing_token,'created_at',j.created_at,'updated_at',j.updated_at,'finished_at',j.finished_at,'result',(SELECT jsonb_build_object('attempt_id',r.attempt_id,'status',r.status,'result',r.result,'error',r.error,'artifact_ids',r.artifact_ids,'created_at',r.created_at) FROM `+s.table("results")+` r WHERE r.job_id=j.id),'attempts',coalesce((SELECT jsonb_agg(jsonb_build_object('id',a.id,'worker_id',a.worker_id,'fencing_token',a.fencing_token,'status',a.status,'lease_until',a.lease_until,'started_at',a.started_at,'last_heartbeat_at',a.last_heartbeat_at,'progress',a.progress,'completed_at',a.completed_at) ORDER BY a.fencing_token) FROM `+s.table("attempts")+` a WHERE a.job_id=j.id),'[]'::jsonb),'candidates',coalesce((SELECT jsonb_agg(jsonb_build_object('id',c.id,'attempt_id',c.attempt_id,'worker_id',c.worker_id,'status',c.status,'result',c.result,'error',c.error,'artifact_ids',c.artifact_ids,'created_at',c.created_at) ORDER BY c.created_at) FROM `+s.table("candidates")+` c WHERE c.job_id=j.id),'[]'::jsonb)) FROM `+s.table("jobs")+` j WHERE j.id=$1`, id).Scan(&b)
	if errors.Is(e, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return b, e
}
func (s *Store) Metrics(ctx context.Context) (json.RawMessage, error) {
	var b []byte
	e := s.Pool.QueryRow(ctx, `SELECT jsonb_build_object('jobs_by_status',(SELECT coalesce(jsonb_object_agg(status,n),'{}'::jsonb) FROM(SELECT status,count(*) n FROM `+s.table("jobs")+` GROUP BY status)x),'workers',(SELECT count(*) FROM `+s.table("workers")+`),'active_leases',(SELECT count(*) FROM `+s.table("attempts")+` WHERE status='running' AND lease_until>clock_timestamp()),'expired_leases',(SELECT count(*) FROM `+s.table("attempts")+` WHERE status='running' AND lease_until<=clock_timestamp()),'results',(SELECT count(*) FROM `+s.table("results")+`),'candidates',(SELECT count(*) FROM `+s.table("candidates")+`),'outbox_pending',(SELECT count(*) FROM `+s.table("outbox")+` WHERE published_at IS NULL),'inbox',(SELECT count(*) FROM `+s.table("inbox")+`),'inbox_conflicts',(SELECT count(*) FROM `+s.table("inbox_conflicts")+`))`).Scan(&b)
	return b, e
}
