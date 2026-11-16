package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
)

// ArtifactFiles is private server storage. Only opaque IDs/hashes enter receipts.
type ArtifactFiles struct{ root string }
type stagedArtifact struct {
	path, sha string
	length    int64
}

func NewArtifactFiles(root string) (*ArtifactFiles, error) {
	if root == "" {
		return nil, ErrInvalid
	}
	absolute, e := filepath.Abs(root)
	if e != nil {
		return nil, e
	}
	f := &ArtifactFiles{root: absolute}
	for _, name := range []string{"", "tmp", "chunks", "objects"} {
		p := filepath.Join(absolute, name)
		if e = os.MkdirAll(p, 0700); e != nil {
			return nil, e
		}
		if e = os.Chmod(p, 0700); e != nil {
			return nil, e
		}
	}
	if e = syncArtifactDir(absolute); e != nil {
		return nil, e
	}
	if e = syncArtifactDir(filepath.Dir(absolute)); e != nil {
		return nil, e
	}
	return f, nil
}

type contextArtifactReader struct {
	ctx context.Context
	r   io.Reader
}

func (r contextArtifactReader) Read(p []byte) (int, error) {
	if e := r.ctx.Err(); e != nil {
		return 0, e
	}
	return r.r.Read(p)
}
func copyArtifact(ctx context.Context, w io.Writer, r io.Reader) (string, int64, error) {
	h := sha256.New()
	n, e := io.CopyBuffer(io.MultiWriter(w, h), contextArtifactReader{ctx, r}, make([]byte, 32*1024))
	if e == nil {
		e = ctx.Err()
	}
	return hex.EncodeToString(h.Sum(nil)), n, e
}
func (f *ArtifactFiles) stage(ctx context.Context, id string, r io.Reader) (out stagedArtifact, e error) {
	file, e := os.CreateTemp(filepath.Join(f.root, "tmp"), id+"-stream-")
	if e != nil {
		return out, e
	}
	out.path = file.Name()
	defer func() {
		file.Close()
		if e != nil {
			os.Remove(out.path)
		}
	}()
	out.sha, out.length, e = copyArtifact(ctx, file, r)
	if e != nil {
		return out, e
	}
	if e = file.Sync(); e != nil {
		return out, e
	}
	e = file.Close()
	return out, e
}
func syncArtifactDir(path string) error {
	d, e := os.Open(path)
	if e != nil {
		return e
	}
	defer d.Close()
	return d.Sync()
}
func (f *ArtifactFiles) chunkPath(id string, offset int64, sha string) string {
	return filepath.Join(f.root, "chunks", id, strconv.FormatInt(offset, 10)+"-"+sha)
}
func (f *ArtifactFiles) objectPath(sha string) string {
	return filepath.Join(f.root, "objects", sha[:2], sha)
}
func verifyArtifactFile(ctx context.Context, path, sha string, length int64) error {
	file, e := os.Open(path)
	if e != nil {
		return e
	}
	defer file.Close()
	actual, n, e := copyArtifact(ctx, io.Discard, file)
	if e != nil {
		return e
	}
	if actual != sha || n != length {
		return ErrConflict
	}
	return nil
}
func linkArtifactFile(ctx context.Context, source, target, sha string, length int64) error {
	if e := os.MkdirAll(filepath.Dir(target), 0700); e != nil {
		return e
	}
	if e := syncArtifactDir(filepath.Dir(filepath.Dir(target))); e != nil {
		return e
	}
	if e := os.Link(source, target); e != nil {
		if !errors.Is(e, os.ErrExist) {
			return e
		}
		if e = verifyArtifactFile(ctx, target, sha, length); e != nil {
			return e
		}
	}
	return syncArtifactDir(filepath.Dir(target))
}
func (f *ArtifactFiles) promoteChunk(ctx context.Context, s stagedArtifact, id string, offset int64) error {
	return linkArtifactFile(ctx, s.path, f.chunkPath(id, offset, s.sha), s.sha, s.length)
}
func (f *ArtifactFiles) promoteObject(ctx context.Context, s stagedArtifact) error {
	return linkArtifactFile(ctx, s.path, f.objectPath(s.sha), s.sha, s.length)
}
