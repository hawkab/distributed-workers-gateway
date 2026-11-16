package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"github.com/jackc/pgx/v5"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// attemptState locks only the short metadata transaction, never a binary stream.
func (s *Store) attemptState(ctx context.Context, tx pgx.Tx, job string, i AttemptIdentity) (bool, error) {
	var active *string
	var fence int64
	var status string
	var cancelled bool
	e := tx.QueryRow(ctx, `SELECT active_attempt_id::text,fencing_token,status,cancel_requested FROM `+s.table("jobs")+` WHERE id=$1 FOR UPDATE`, job).Scan(&active, &fence, &status, &cancelled)
	if errors.Is(e, pgx.ErrNoRows) {
		return false, ErrNotFound
	}
	if e != nil {
		return false, e
	}
	var worker string
	var ownFence int64
	var live bool
	e = tx.QueryRow(ctx, `SELECT worker_id,fencing_token,status='running' AND lease_until>clock_timestamp() FROM `+s.table("attempts")+` WHERE id=$1 AND job_id=$2 FOR UPDATE`, i.AttemptID, job).Scan(&worker, &ownFence, &live)
	if errors.Is(e, pgx.ErrNoRows) {
		return false, ErrNotFound
	}
	if e != nil {
		return false, e
	}
	if worker != i.WorkerID || ownFence != i.FencingToken {
		return false, ErrUnauthorized
	}
	return !cancelled && active != nil && *active == i.AttemptID && fence == i.FencingToken && status == "running" && live, nil
}
func (s *Store) assembleArtifact(ctx context.Context, u artifactUpload) (string, error) {
	var count int64
	e := s.Pool.QueryRow(ctx, `SELECT count(*) FROM `+s.table("artifact_chunks")+` WHERE upload_id=$1`, u.id).Scan(&count)
	if e != nil {
		return "", e
	}
	if count == 1 {
		chunk, e := s.chunk(ctx, s.Pool, u.id, 0)
		if e != nil {
			return "", e
		}
		if chunk.Length != u.offset {
			return "", ErrConflict
		}
		source := s.Artifacts.chunkPath(u.id, 0, chunk.SHA256)
		if e = verifyArtifactFile(ctx, source, chunk.SHA256, chunk.Length); e != nil {
			return "", e
		}
		if e = linkArtifactFile(ctx, source, s.Artifacts.objectPath(chunk.SHA256), chunk.SHA256, chunk.Length); e != nil {
			return "", e
		}
		return chunk.SHA256, nil
	}
	// Query results are consumed one row at a time; neither bytes nor the whole
	// chunk manifest are accumulated in memory.
	rows, e := s.Pool.Query(ctx, `SELECT byte_offset,length,sha256 FROM `+s.table("artifact_chunks")+` WHERE upload_id=$1 ORDER BY byte_offset`, u.id)
	if e != nil {
		return "", e
	}
	defer rows.Close()
	file, e := os.CreateTemp(s.Artifacts.root+"/tmp", u.id+"-merge-")
	if e != nil {
		return "", e
	}
	defer func() { file.Close(); os.Remove(file.Name()) }()
	whole := sha256.New()
	writer := io.MultiWriter(file, whole)
	offset := int64(0)
	for rows.Next() {
		var start, length int64
		var sha string
		if e = rows.Scan(&start, &length, &sha); e != nil {
			return "", e
		}
		if start != offset {
			return "", ErrConflict
		}
		chunk, e := os.Open(s.Artifacts.chunkPath(u.id, start, sha))
		if e != nil {
			return "", e
		}
		actual, n, e := copyArtifact(ctx, writer, chunk)
		chunk.Close()
		if e != nil {
			return "", e
		}
		if actual != sha || n != length {
			return "", ErrConflict
		}
		offset += n
	}
	if e = rows.Err(); e != nil {
		return "", e
	}
	if offset != u.offset {
		return "", ErrConflict
	}
	if e = ctx.Err(); e != nil {
		return "", e
	}
	if e = file.Sync(); e != nil {
		return "", e
	}
	if e = file.Close(); e != nil {
		return "", e
	}
	sha := hex.EncodeToString(whole.Sum(nil))
	e = s.Artifacts.promoteObject(ctx, stagedArtifact{path: file.Name(), sha: sha, length: offset})
	return sha, e
}
func (s *Store) FinishArtifact(ctx context.Context, id string, c FinishArtifact) (ArtifactReceipt, error) {
	var out ArtifactReceipt
	if s.Artifacts == nil {
		return out, ErrUnavailable
	}
	if !c.AttemptIdentity.valid() || c.Length < 0 || (c.SHA256 != "" && !validSHA(c.SHA256)) {
		return out, ErrInvalid
	}
	u, e := s.upload(ctx, s.Pool, id, c.AttemptIdentity, false)
	if e != nil {
		return out, e
	}
	if u.status == "sealed" {
		if e = json.Unmarshal(u.receipt, &out); e != nil {
			return out, e
		}
		if out.Length != c.Length || (c.SHA256 != "" && out.SHA256 != c.SHA256) {
			return out, ErrConflict
		}
		return out, nil
	}
	if u.status != "open" || u.offset != c.Length {
		return out, ErrConflict
	}
	sha, e := s.assembleArtifact(ctx, u)
	if e != nil {
		// Another finish may have sealed and reclaimed segments after this
		// caller observed "open". Recover its durable ACK instead of failing
		// an otherwise identical concurrent retry on a removed chunk path.
		current, lookupErr := s.upload(ctx, s.Pool, id, c.AttemptIdentity, false)
		if lookupErr == nil && current.status == "sealed" {
			if decodeErr := json.Unmarshal(current.receipt, &out); decodeErr != nil {
				return out, decodeErr
			}
			if out.Length != c.Length || (c.SHA256 != "" && out.SHA256 != c.SHA256) {
				return out, ErrConflict
			}
			return out, nil
		}
		return out, e
	}
	if c.SHA256 != "" && c.SHA256 != sha {
		return out, ErrConflict
	}
	tx, e := s.Pool.Begin(ctx)
	if e != nil {
		return out, e
	}
	defer tx.Rollback(ctx)
	live, e := s.attemptState(ctx, tx, u.job, c.AttemptIdentity)
	if e != nil {
		return out, e
	}
	u, e = s.upload(ctx, tx, id, c.AttemptIdentity, true)
	if e != nil {
		return out, e
	}
	if u.status == "sealed" {
		if e = json.Unmarshal(u.receipt, &out); e != nil {
			return out, e
		}
		if out.Length != c.Length || out.SHA256 != sha {
			return out, ErrConflict
		}
		return out, tx.Commit(ctx)
	}
	if u.status != "open" || u.offset != c.Length {
		return out, ErrConflict
	}
	out = ArtifactReceipt{ArtifactRef: ArtifactRef{ArtifactID: id, Name: u.name, SHA256: sha, Length: c.Length, MediaType: u.media}, Committed: live, CandidateSaved: !live}
	receipt, _ := json.Marshal(out)
	_, e = tx.Exec(ctx, `UPDATE `+s.table("artifact_uploads")+` SET status='sealed',blob_sha256=$2,receipt=$3,updated_at=clock_timestamp() WHERE id=$1`, id, sha, receipt)
	if e != nil {
		return out, e
	}
	if e = s.event(ctx, tx, "artifact.sealed", map[string]any{"job_id": u.job, "attempt_id": u.attempt, "artifact": map[string]any{"artifact_id": out.ArtifactID, "sha256": out.SHA256, "length": out.Length}, "candidate_saved": out.CandidateSaved, "result_committed": false}); e != nil {
		return out, e
	}
	if e = tx.Commit(ctx); e != nil {
		return out, e
	}
	// Failure to reclaim redundant chunks cannot invalidate a durable receipt.
	// Maintenance retries cleanup after rechecking the CAS bytes and PG record.
	_ = s.cleanupArtifact(ctx, id, true)
	return out, nil
}
func (s *Store) cleanupArtifact(ctx context.Context, id string, alreadyVerified bool) error {
	if s.Artifacts == nil {
		return nil
	}
	var sha string
	var length int64
	var cleaned bool
	e := s.Pool.QueryRow(ctx, `SELECT blob_sha256,byte_offset,chunks_cleaned FROM `+s.table("artifact_uploads")+` WHERE id=$1 AND status='sealed'`, id).Scan(&sha, &length, &cleaned)
	if e != nil {
		return e
	}
	if cleaned {
		return nil
	}
	object := s.Artifacts.objectPath(sha)
	if !alreadyVerified {
		if e = verifyArtifactFile(ctx, object, sha, length); e != nil {
			return e
		}
	} else {
		info, e := os.Stat(object)
		if e != nil {
			return e
		}
		if info.Size() != length {
			return ErrConflict
		}
	}
	// Once sealed, no writer can promote another segment. Remove known and
	// orphaned pre-commit files only after the complete object is proven durable.
	if e = cleanArtifactDirectory(filepath.Join(s.Artifacts.root, "chunks", id), ""); e != nil {
		return e
	}
	if e = cleanArtifactDirectory(filepath.Join(s.Artifacts.root, "tmp"), id+"-"); e != nil {
		return e
	}
	_, e = s.Pool.Exec(ctx, `UPDATE `+s.table("artifact_uploads")+` SET chunks_cleaned=true WHERE id=$1 AND status='sealed' AND blob_sha256=$2`, id, sha)
	return e
}
func cleanArtifactDirectory(path, prefix string) error {
	directory, e := os.Open(path)
	if errors.Is(e, os.ErrNotExist) {
		return nil
	}
	if e != nil {
		return e
	}
	defer directory.Close()
	for {
		names, e := directory.Readdirnames(64)
		for _, name := range names {
			if prefix != "" && !strings.HasPrefix(name, prefix) {
				continue
			}
			if e := os.Remove(filepath.Join(path, name)); e != nil && !errors.Is(e, os.ErrNotExist) {
				return e
			}
		}
		if e == io.EOF {
			break
		}
		if e != nil {
			return e
		}
	}
	if e = directory.Sync(); e != nil {
		return e
	}
	if prefix == "" {
		if e = os.Remove(path); e != nil && !errors.Is(e, os.ErrNotExist) {
			return e
		}
		return syncArtifactDir(filepath.Dir(path))
	}
	return nil
}
func (s *Store) ReconcileArtifacts(ctx context.Context) error {
	if s.Artifacts == nil {
		return nil
	}
	last := "00000000-0000-0000-0000-000000000000"
	for {
		var id string
		e := s.Pool.QueryRow(ctx, `SELECT id::text FROM `+s.table("artifact_uploads")+` WHERE status='sealed' AND NOT chunks_cleaned AND id>$1::uuid ORDER BY id LIMIT 1`, last).Scan(&id)
		if errors.Is(e, pgx.ErrNoRows) {
			return nil
		}
		if e != nil {
			return e
		}
		if e = s.cleanupArtifact(ctx, id, false); e != nil {
			return e
		}
		last = id
	}
}
func (s *Store) validateArtifacts(ctx context.Context, tx pgx.Tx, job string, ids []string) error {
	for _, id := range ids {
		var storedJob, status string
		e := tx.QueryRow(ctx, `SELECT job_id::text,status FROM `+s.table("artifact_uploads")+` WHERE id=$1`, id).Scan(&storedJob, &status)
		if errors.Is(e, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if e != nil {
			return e
		}
		if storedJob != job {
			return ErrUnauthorized
		}
		if status != "sealed" {
			return ErrConflict
		}
	}
	return nil
}
func (s *Store) OpenArtifact(ctx context.Context, id string) (*os.File, ArtifactRef, error) {
	var ref ArtifactRef
	ref.ArtifactID = id
	if s.Artifacts == nil {
		return nil, ref, ErrUnavailable
	}
	e := s.Pool.QueryRow(ctx, `SELECT coalesce(nullif(artifact_name,''),artifact_key),media_type,blob_sha256,byte_offset FROM `+s.table("artifact_uploads")+` WHERE id=$1 AND status='sealed'`, id).Scan(&ref.Name, &ref.MediaType, &ref.SHA256, &ref.Length)
	if errors.Is(e, pgx.ErrNoRows) {
		return nil, ref, ErrNotFound
	}
	if e != nil {
		return nil, ref, e
	}
	file, e := os.Open(s.Artifacts.objectPath(ref.SHA256))
	return file, ref, e
}
func (s *Store) OpenWorkerArtifact(ctx context.Context, id string, i AttemptIdentity) (*os.File, ArtifactRef, error) {
	var ref ArtifactRef
	if !i.valid() {
		return nil, ref, ErrInvalid
	}
	var job, owner, attempt string
	var fence int64
	e := s.Pool.QueryRow(ctx, `SELECT job_id::text,worker_id,attempt_id::text,fencing_token FROM `+s.table("artifact_uploads")+` WHERE id=$1 AND status='sealed'`, id).Scan(&job, &owner, &attempt, &fence)
	if errors.Is(e, pgx.ErrNoRows) {
		return nil, ref, ErrNotFound
	}
	if e != nil {
		return nil, ref, e
	}
	if i.WorkerID == owner && i.AttemptID == attempt && i.FencingToken == fence {
		return s.OpenArtifact(ctx, id)
	}
	tx, e := s.Pool.Begin(ctx)
	if e != nil {
		return nil, ref, e
	}
	defer tx.Rollback(ctx)
	live, e := s.attemptState(ctx, tx, job, i)
	if errors.Is(e, ErrNotFound) {
		return nil, ref, ErrUnauthorized
	}
	if e != nil {
		return nil, ref, e
	}
	if !live {
		return nil, ref, ErrLeaseLost
	}
	if e = tx.Commit(ctx); e != nil {
		return nil, ref, e
	}
	return s.OpenArtifact(ctx, id)
}
