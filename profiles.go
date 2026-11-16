package gateway

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"strings"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
)

type GPUResource struct {
	Index                *int64 `json:"index"`
	ComputeUnits         *int64 `json:"compute_units,omitempty"`
	MemoryTotalBytes     *int64 `json:"memory_total_bytes,omitempty"`
	MemoryAvailableBytes *int64 `json:"memory_available_bytes,omitempty"`
	UnifiedMemory        *bool  `json:"unified_memory,omitempty"`
}
type WorkerResources struct {
	CPULogicalCores    *int64        `json:"cpu_logical_cores,omitempty"`
	CPUAvailableCores  *float64      `json:"cpu_available_cores,omitempty"`
	RAMTotalBytes      *int64        `json:"ram_total_bytes,omitempty"`
	RAMAvailableBytes  *int64        `json:"ram_available_bytes,omitempty"`
	DiskTotalBytes     *int64        `json:"disk_total_bytes,omitempty"`
	DiskAvailableBytes *int64        `json:"disk_available_bytes,omitempty"`
	GPUStatus          string        `json:"gpu_status"`
	GPUs               []GPUResource `json:"gpus"`
}
type WorkerProfile struct {
	Schema       int              `json:"schema"`
	Capabilities []string         `json:"capabilities"`
	Resources    *WorkerResources `json:"resources"`
}
type ResourceRequirements struct {
	CPUCores        float64 `json:"cpu_cores,omitempty"`
	RAMBytes        int64   `json:"ram_bytes,omitempty"`
	DiskBytes       int64   `json:"disk_bytes,omitempty"`
	GPUCount        int64   `json:"gpu_count,omitempty"`
	GPUComputeUnits int64   `json:"gpu_compute_units,omitempty"`
	GPUMemoryBytes  int64   `json:"gpu_memory_bytes,omitempty"`
}

func strictJSON(raw []byte, value any) error {
	if !utf8.Valid(raw) {
		return ErrInvalid
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if e := d.Decode(value); e != nil {
		return ErrInvalid
	}
	if e := d.Decode(new(any)); e != io.EOF {
		return ErrInvalid
	}
	return nil
}
func (r *ResourceRequirements) UnmarshalJSON(raw []byte) error {
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil || fields == nil {
		return ErrInvalid
	}
	for _, value := range fields {
		if strings.TrimSpace(string(value)) == "null" {
			return ErrInvalid
		}
	}
	type plain ResourceRequirements
	if e := strictJSON(raw, (*plain)(r)); e != nil {
		return e
	}
	return r.validate()
}
func (r ResourceRequirements) validate() error {
	if r.CPUCores < 0 || math.IsNaN(r.CPUCores) || math.IsInf(r.CPUCores, 0) || r.RAMBytes < 0 || r.DiskBytes < 0 || r.GPUCount < 0 || r.GPUComputeUnits < 0 || r.GPUMemoryBytes < 0 {
		return ErrInvalid
	}
	return nil
}
func (p *WorkerProfile) normalized() (*WorkerProfile, error) {
	if p == nil || p.Schema != 1 || p.Capabilities == nil || p.Resources == nil {
		return nil, ErrInvalid
	}
	// Store APIs may receive the same profile from concurrent callers. Normalize
	// a private copy instead of rewriting caller-owned capabilities/resources.
	copy := *p
	resources := *p.Resources
	p = &copy
	p.Resources = &resources
	for _, c := range p.Capabilities {
		if strings.TrimSpace(c) == "" {
			return nil, ErrInvalid
		}
	}
	p.Capabilities = sortedUnique(p.Capabilities)
	r := p.Resources
	for _, n := range []*int64{r.CPULogicalCores, r.RAMTotalBytes, r.RAMAvailableBytes, r.DiskTotalBytes, r.DiskAvailableBytes} {
		if n != nil && *n < 0 {
			return nil, ErrInvalid
		}
	}
	if r.CPUAvailableCores != nil && (*r.CPUAvailableCores < 0 || math.IsInf(*r.CPUAvailableCores, 0) || math.IsNaN(*r.CPUAvailableCores)) {
		return nil, ErrInvalid
	}
	if r.GPUStatus == "" {
		r.GPUStatus = "unknown"
	}
	if r.GPUStatus != "unknown" && r.GPUStatus != "none" && r.GPUStatus != "detected" {
		return nil, ErrInvalid
	}
	if r.GPUStatus == "none" && len(r.GPUs) != 0 {
		return nil, ErrInvalid
	}
	seen := map[int64]bool{}
	for _, gpu := range r.GPUs {
		if gpu.Index == nil || *gpu.Index < 0 || seen[*gpu.Index] {
			return nil, ErrInvalid
		}
		seen[*gpu.Index] = true
		for _, n := range []*int64{gpu.ComputeUnits, gpu.MemoryTotalBytes, gpu.MemoryAvailableBytes} {
			if n != nil && *n < 0 {
				return nil, ErrInvalid
			}
		}
	}
	if r.GPUs == nil {
		r.GPUs = []GPUResource{}
	}
	return p, nil
}
func profileHeader(r *http.Request, required bool) (*WorkerProfile, error) {
	value := r.Header.Get("X-Worker-Profile")
	if value == "" {
		if required {
			return nil, ErrInvalid
		}
		return nil, nil
	}
	raw, e := base64.RawURLEncoding.Strict().DecodeString(value)
	if e != nil {
		return nil, ErrInvalid
	}
	var profile WorkerProfile
	if e = strictJSON(raw, &profile); e != nil {
		return nil, e
	}
	return profile.normalized()
}
func profileBytes(profile *WorkerProfile) []byte {
	if profile == nil {
		return nil
	}
	b, _ := json.Marshal(profile)
	return b
}
func bodyCapabilities(profile *WorkerProfile, raw json.RawMessage) ([]string, error) {
	var body []string
	if len(raw) != 0 {
		if e := json.Unmarshal(raw, &body); e != nil {
			return nil, ErrInvalid
		}
	}
	if profile == nil {
		return sortedUnique(body), nil
	}
	if len(raw) != 0 {
		if strings.TrimSpace(string(raw)) == "null" || !sameBytes(digest(sortedUnique(body)), digest(profile.Capabilities)) {
			return nil, ErrInvalid
		}
	}
	return profile.Capabilities, nil
}

func (s *Store) CreateEnrollment(ctx context.Context, token string) error {
	if len(token) < 32 {
		return ErrInvalid
	}
	tag, e := s.Pool.Exec(ctx, `INSERT INTO `+s.table("worker_enrollments")+`(token_hash) SELECT $1 WHERE NOT EXISTS(SELECT 1 FROM `+s.table("workers")+` WHERE token_hash=$1) ON CONFLICT DO NOTHING`, tokenHash(token))
	if e == nil && tag.RowsAffected() == 0 {
		return ErrConflict
	}
	return e
}
func (s *Store) ValidateConnectCredential(ctx context.Context, token string) error {
	if token == "" {
		return ErrUnauthorized
	}
	var allowed bool
	e := s.Pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM `+s.table("workers")+` WHERE token_hash=$1 AND enabled UNION ALL SELECT 1 FROM `+s.table("worker_enrollments")+` e LEFT JOIN `+s.table("workers")+` w ON w.id=e.worker_id WHERE e.token_hash=$1 AND (e.worker_id IS NULL OR w.enabled))`, tokenHash(token)).Scan(&allowed)
	if e != nil {
		return e
	}
	if !allowed {
		return ErrUnauthorized
	}
	return nil
}
func (s *Store) ConnectWorker(ctx context.Context, token string, profile *WorkerProfile) (string, error) {
	if token == "" {
		return "", ErrUnauthorized
	}
	tx, e := s.Pool.Begin(ctx)
	if e != nil {
		return "", e
	}
	defer tx.Rollback(ctx)
	var id string
	e = tx.QueryRow(ctx, `SELECT id FROM `+s.table("workers")+` WHERE token_hash=$1 AND enabled FOR UPDATE`, tokenHash(token)).Scan(&id)
	if errors.Is(e, pgx.ErrNoRows) {
		var bound *string
		e = tx.QueryRow(ctx, `SELECT worker_id FROM `+s.table("worker_enrollments")+` WHERE token_hash=$1 FOR UPDATE`, tokenHash(token)).Scan(&bound)
		if errors.Is(e, pgx.ErrNoRows) {
			return "", ErrUnauthorized
		}
		if e != nil {
			return "", e
		}
		if bound == nil {
			id = newID()
			_, e = tx.Exec(ctx, `INSERT INTO `+s.table("workers")+`(id,token_hash) VALUES($1,$2)`, id, tokenHash(token))
			if e != nil {
				return "", e
			}
			_, e = tx.Exec(ctx, `UPDATE `+s.table("worker_enrollments")+` SET worker_id=$2,connected_at=clock_timestamp() WHERE token_hash=$1`, tokenHash(token), id)
		} else {
			id = *bound
			e = s.lockWorker(ctx, tx, id)
		}
	}
	if e != nil {
		return "", e
	}
	profile, e = profile.normalized()
	if e != nil {
		return "", e
	}
	_, e = tx.Exec(ctx, `UPDATE `+s.table("workers")+` SET profile=$2,profile_observed_at=clock_timestamp(),capabilities=$3,last_seen_at=clock_timestamp() WHERE id=$1 AND enabled`, id, profileBytes(profile), profile.Capabilities)
	if e != nil {
		return "", e
	}
	return id, tx.Commit(ctx)
}
func (s *Store) WorkerMe(ctx context.Context, id string) (json.RawMessage, error) {
	var b []byte
	e := s.Pool.QueryRow(ctx, `SELECT jsonb_build_object('worker_id',id,'profile',profile,'server_observed_at',profile_observed_at,'update_poll_seconds',update_poll_seconds) FROM `+s.table("workers")+` WHERE id=$1 AND enabled`, id).Scan(&b)
	if errors.Is(e, pgx.ErrNoRows) {
		return nil, ErrUnauthorized
	}
	return b, e
}
