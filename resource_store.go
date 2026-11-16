package gateway

import (
	"context"
	"fmt"
)

func (s *Store) initProfiles(ctx context.Context) error {
	_, e := s.Pool.Exec(ctx, fmt.Sprintf(`
ALTER TABLE %[1]s ADD COLUMN IF NOT EXISTS profile jsonb;
ALTER TABLE %[1]s ADD COLUMN IF NOT EXISTS profile_observed_at timestamptz;
ALTER TABLE %[2]s ADD COLUMN IF NOT EXISTS resource_requirements jsonb NOT NULL DEFAULT '{}';
CREATE TABLE IF NOT EXISTS %[3]s(
 token_hash bytea PRIMARY KEY,worker_id text UNIQUE REFERENCES %[1]s(id),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),connected_at timestamptz);
CREATE OR REPLACE FUNCTION %[4]s(p jsonb,r jsonb) RETURNS boolean
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $fleet_resource_match$
 WITH req AS (
  SELECT coalesce((r->>'cpu_cores')::numeric,0) cpu,
   coalesce((r->>'ram_bytes')::numeric,0) ram,
   coalesce((r->>'disk_bytes')::numeric,0) disk,
   coalesce((r->>'gpu_compute_units')::numeric,0) units,
   coalesce((r->>'gpu_memory_bytes')::numeric,0) memory,
   greatest(coalesce((r->>'gpu_count')::numeric,0),
    CASE WHEN coalesce((r->>'gpu_compute_units')::numeric,0)>0 OR coalesce((r->>'gpu_memory_bytes')::numeric,0)>0 THEN 1 ELSE 0 END) needed
 ), qualified AS (
  SELECT gpu FROM req,jsonb_array_elements(coalesce(p->'resources'->'gpus','[]'::jsonb)) gpu
  WHERE (req.units=0 OR (gpu->>'compute_units')::numeric>=req.units)
   AND (req.memory=0 OR (gpu->>'memory_available_bytes')::numeric>=req.memory)
 ), totals AS (
  SELECT count(*)::numeric available,
   count(*) FILTER(WHERE gpu->>'unified_memory'='false')::numeric dedicated FROM qualified
 )
 SELECT coalesce(
  (req.cpu=0 OR (p->'resources'->>'cpu_available_cores')::numeric>=req.cpu)
  AND (req.disk=0 OR (p->'resources'->>'disk_available_bytes')::numeric>=req.disk)
  AND (req.needed=0 OR (p->'resources'->>'gpu_status'='detected' AND totals.available>=req.needed))
  AND (req.ram+greatest(0,req.needed-totals.dedicated)*req.memory=0
   OR (p->'resources'->>'ram_available_bytes')::numeric>=req.ram+greatest(0,req.needed-totals.dedicated)*req.memory)
 ,false) FROM req,totals
$fleet_resource_match$;
`, s.table("workers"), s.table("jobs"), s.table("worker_enrollments"), s.table("resources_match")))
	return e
}
