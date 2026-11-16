package gateway

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func request(t *testing.T, handler http.Handler, method, path, token, body string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	return w
}
func TestHTTPAuthenticationIsolationPolicyAndResultReceipt(t *testing.T) {
	s := fixture(t)
	admin := NewToken()
	api := &API{Store: s, AdminToken: admin}
	h := api.Handler()
	w := request(t, h, "POST", "/v1/admin/workers", admin, `{"worker_id":"tv-a"}`)
	if w.Code != 201 {
		t.Fatal(w.Code, w.Body.String())
	}
	var created map[string]string
	must(t, json.Unmarshal(w.Body.Bytes(), &created))
	token := created["token"]
	if request(t, h, "GET", "/v1/admin/workers", token, "").Code != 401 {
		t.Fatal("device has admin access")
	}
	if request(t, h, "POST", "/v1/workers/heartbeat", admin, `{"worker_id":"tv-a"}`).Code != 401 {
		t.Fatal("admin reused as device credential")
	}
	if request(t, h, "POST", "/v1/workers/heartbeat", token, `{"worker_id":"other"}`).Code != 401 {
		t.Fatal("device impersonation")
	}
	w = request(t, h, "POST", "/v1/workers/heartbeat", token, `{"worker_id":"tv-a","capabilities":["sha256"],"version_code":1,"abis":["armeabi-v7a"],"state":"idle"}`)
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"update_poll_seconds":900`) {
		t.Fatal(w.Code, w.Body.String())
	}
	w = request(t, h, "POST", "/v1/admin/workers/tv-a/policy", admin, `{"update_poll_seconds":30}`)
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	w = request(t, h, "POST", "/v1/workers/heartbeat", token, `{"worker_id":"tv-a"}`)
	if !strings.Contains(w.Body.String(), `"update_poll_seconds":30`) {
		t.Fatal("policy cached", w.Body.String())
	}
	if request(t, h, "POST", "/v1/jobs/claim", token, `{"worker_id":"tv-a","capabilities":["sha256"]}`).Code != 204 {
		t.Fatal("empty claim")
	}
	w = request(t, h, "POST", "/v1/admin/jobs", admin, `{"operation":"sha256","input":{"text":"hello"},"idempotency_key":"http-proof"}`)
	if w.Code != 201 {
		t.Fatal(w.Body.String())
	}
	var added map[string]string
	must(t, json.Unmarshal(w.Body.Bytes(), &added))
	w = request(t, h, "POST", "/v1/jobs/claim", token, `{"worker_id":"tv-a","capabilities":["sha256"]}`)
	var claimed struct {
		Job ClaimedJob `json:"job"`
	}
	must(t, json.Unmarshal(w.Body.Bytes(), &claimed))
	if claimed.Job.ID != added["job_id"] {
		t.Fatal(w.Body.String())
	}
	c := done(&claimed.Job, "tv-a")
	c.Result = json.RawMessage(`{"sha256":"2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"}`)
	body, _ := json.Marshal(c)
	path := "/v1/jobs/" + claimed.Job.ID + "/complete"
	one := request(t, h, "POST", path, token, string(body))
	two := request(t, h, "POST", path, token, string(body))
	if one.Code != 200 || !bytes.Equal(one.Body.Bytes(), two.Body.Bytes()) || !strings.Contains(one.Body.String(), `"committed":true`) {
		t.Fatal(one.Code, one.Body.String(), two.Body.String())
	}
	w = request(t, h, "GET", "/v1/admin/jobs/"+claimed.Job.ID, admin, "")
	if w.Code != 200 || !strings.Contains(w.Body.String(), "2cf24dba") {
		t.Fatal(w.Code, w.Body.String())
	}
	if request(t, h, "POST", "/v1/admin/jobs", admin, `{} {}`).Code != 400 {
		t.Fatal("trailing JSON accepted")
	}
}
func TestPublicReleaseHasStrictAllowlistAndNoPathEscape(t *testing.T) {
	root := t.TempDir()
	must(t, os.Mkdir(filepath.Join(root, "android-stable"), 0700))
	must(t, os.WriteFile(filepath.Join(root, "android-stable", "manifest.json"), []byte(`{"version_code":2}`), 0600))
	must(t, os.WriteFile(filepath.Join(root, "android-stable", "worker-2.apk"), []byte("APK fixture"), 0600))
	private := filepath.Join(t.TempDir(), "private.key")
	must(t, os.WriteFile(private, []byte("PRIVATE MATERIAL"), 0600))
	must(t, os.Symlink(private, filepath.Join(root, "android-stable", "leak.apk")))
	h := (&API{ReleaseDir: root}).Handler()
	w := request(t, h, "GET", "/releases/android-stable/manifest.json", "", "")
	if w.Code != 200 || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatal(w.Code, w.Header())
	}
	w = request(t, h, "GET", "/releases/android-stable/worker-2.apk", "", "")
	if w.Code != 200 || w.Body.String() != "APK fixture" {
		t.Fatal(w.Code)
	}
	for _, path := range []string{"/releases/android-stable/leak.apk", "/releases/android-stable/private.key", "/releases/android-stable/../private.key", "/releases/android-stable/%2e%2e%2fprivate.key"} {
		w = request(t, h, "GET", path, "", "")
		b, _ := io.ReadAll(w.Result().Body)
		if w.Code == 200 || strings.Contains(string(b), "PRIVATE MATERIAL") {
			t.Fatal("private path exposed", path, w.Code)
		}
	}
}
