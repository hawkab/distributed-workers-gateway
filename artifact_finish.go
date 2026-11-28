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
	file, e := s.Artifacts.temporary(ctx, u.id, "merge-")
	if e != nil {
		return "", e
	}
	defer func() { file.Close(); removeArtifactStage(file.Name()) }()
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
	out, e = s.sealArtifact(ctx, tx, u, sha, c.Length, live)
	if e != nil {
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
func (s *Store) sealArtifact(ctx context.Context, tx pgx.Tx, u artifactUpload, sha string, length int64, live bool) (ArtifactReceipt, error) {
	id := u.id
	out := ArtifactReceipt{ArtifactRef: ArtifactRef{ArtifactID: id, Name: u.name, SHA256: sha, Length: length, MediaType: u.media}, Committed: live, CandidateSaved: !live}
	receipt, _ := json.Marshal(out)
	_, e := tx.Exec(ctx, `UPDATE `+s.table("artifact_uploads")+` SET status='sealed',blob_sha256=$2,receipt=$3,byte_offset=$4,updated_at=clock_timestamp() WHERE id=$1`, id, sha, receipt, length)
	if e != nil {
		return out, e
	}
	if e = s.event(ctx, tx, "artifact.sealed", map[string]any{"job_id": u.job, "attempt_id": u.attempt, "artifact": map[string]any{"artifact_id": out.ArtifactID, "sha256": out.SHA256, "length": out.Length}, "candidate_saved": out.CandidateSaved, "result_committed": false}); e != nil {
		return out, e
	}
	return out, nil
}

// PutSealedArtifact publishes one known byte sequence. createArtifactID reserves the
// same key as the resumable protocol. A dropped response can replay that key and
// body, including after lease expiry, without duplicating the seal event.
func (s *Store) PutSealedArtifact(ctx context.Context, job string, c CreateArtifact, length int64, sha string, body io.Reader) (ArtifactReceipt, error) {
	var out ArtifactReceipt
	if length < 0 || !validSHA(sha) {
		return out, ErrInvalid
	}
	id, _, err := s.createArtifactID(ctx, job, c)
	if err != nil {
		return out, err
	}
	staged, err := s.Artifacts.stage(ctx, id, body)
	if err != nil {
		return out, err
	}
	defer removeArtifactStage(staged.path)
	if sha != staged.sha || length != staged.length {
		return out, ErrConflict
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return out, err
	}
	defer tx.Rollback(ctx)
	live, err := s.attemptState(ctx, tx, job, c.AttemptIdentity)
	if err != nil {
		return out, err
	}
	u, err := s.upload(ctx, tx, id, c.AttemptIdentity, true)
	if err != nil {
		return out, err
	}
	if u.status == "sealed" {
		if err = json.Unmarshal(u.receipt, &out); err != nil {
			return out, err
		}
		if out.SHA256 != sha || out.Length != length {
			return ArtifactReceipt{}, ErrConflict
		}
		return out, tx.Commit(ctx)
	}
	if u.status != "open" || u.offset != 0 {
		return out, ErrConflict
	}
	if err = s.Artifacts.promoteObject(ctx, staged); err != nil {
		return out, err
	}
	out, err = s.sealArtifact(ctx, tx, u, sha, length, live)
	if err != nil {
		return out, err
	}
	if err = tx.Commit(ctx); err != nil {
		return out, err
	}
	_ = s.cleanupArtifact(ctx, id, true)
	return out, nil
}

func (s *Store) cleanupArtifact(ctx context.Context, id string, alreadyVerified bool) error {
	if s.Artifacts == nil {
		return nil
	}
	var sha string
	var length int64
	var cleaned, scopedTmp bool
	e := s.Pool.QueryRow(ctx, `SELECT blob_sha256,byte_offset,chunks_cleaned,scoped_tmp FROM `+s.table("artifact_uploads")+` WHERE id=$1 AND status='sealed'`, id).Scan(&sha, &length, &cleaned, &scopedTmp)
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
	if !scopedTmp {
		if e = cleanArtifactDirectory(filepath.Join(s.Artifacts.root, "tmp"), id+"-"); e != nil {
			return e
		}
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
		// Qualify the UUID input column: unqualified ORDER BY id resolves the
		// projected id::text alias and prevents the UUID index providing order.
		e := s.Pool.QueryRow(ctx, `SELECT u.id::text FROM `+s.table("artifact_uploads")+` u WHERE u.status='sealed' AND NOT u.chunks_cleaned AND u.id>$1::uuid ORDER BY u.id LIMIT 1`, last).Scan(&id)
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
	if len(ids) > 1 {
		// Preserve the error from the first invalid reference in caller order.
		var violation string
		err := tx.QueryRow(ctx, `SELECT CASE WHEN a.id IS NULL THEN 'missing'
                WHEN a.job_id <> $1::uuid THEN 'foreign' ELSE 'unfinished' END
            FROM unnest($2::uuid[]) WITH ORDINALITY ref(id, ord)
            LEFT JOIN `+s.table("artifact_uploads")+` a USING(id)
            WHERE a.id IS NULL OR a.job_id <> $1::uuid OR a.status <> 'sealed'
            ORDER BY ref.ord LIMIT 1`, job, ids).Scan(&violation)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		switch violation {
		case "missing":
			return ErrNotFound
		case "foreign":
			return ErrUnauthorized
		default:
			return ErrConflict
		}
	}
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
	return s.openArtifact(ctx, s.Pool, id)
}
func (s *Store) openArtifact(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, id string) (*os.File, ArtifactRef, error) {
	var ref ArtifactRef
	ref.ArtifactID = id
	if s.Artifacts == nil {
		return nil, ref, ErrUnavailable
	}
	e := q.QueryRow(ctx, `SELECT coalesce(nullif(artifact_name,''),artifact_key),media_type,blob_sha256,byte_offset FROM `+s.table("artifact_uploads")+` WHERE id=$1 AND status='sealed'`, id).Scan(&ref.Name, &ref.MediaType, &ref.SHA256, &ref.Length)
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
	var job string
	e := s.Pool.QueryRow(ctx, `SELECT job_id::text FROM `+s.table("artifact_uploads")+` WHERE id=$1 AND status='sealed'`, id).Scan(&job)
	if errors.Is(e, pgx.ErrNoRows) {
		return nil, ref, ErrNotFound
	}
	if e != nil {
		return nil, ref, e
	}
	tx, e := s.Pool.Begin(ctx)
	if e != nil {
		return nil, ref, e
	}
	defer tx.Rollback(ctx)
	if e = s.currentAttempt(ctx, tx, job, i); e != nil {
		return nil, ref, e
	}
	file, ref, e := s.openArtifact(ctx, tx, id)
	if e != nil {
		return nil, ref, e
	}
	if e = s.currentAttempt(ctx, tx, job, i); e != nil {
		file.Close()
		return nil, ref, e
	}
	if e = tx.Commit(ctx); e != nil {
		file.Close()
		return nil, ref, e
	}
	return file, ref, nil
}

func (s *Store) currentAttempt(ctx context.Context, tx pgx.Tx, job string, i AttemptIdentity) error {
	live, e := s.attemptState(ctx, tx, job, i)
	if errors.Is(e, ErrNotFound) {
		return ErrUnauthorized
	}
	if e != nil {
		return e
	}
	if !live {
		return ErrLeaseLost
	}
	var enabled bool
	e = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM `+s.table("workers")+` WHERE id=$1 AND enabled)`, i.WorkerID).Scan(&enabled)
	if e != nil {
		return e
	}
	if !enabled {
		return ErrUnauthorized
	}
	return nil
}
