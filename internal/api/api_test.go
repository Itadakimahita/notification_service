package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"notification-service/internal/domain"
	"notification-service/internal/store/dynamo"
)

type fakeStore struct {
	mu   sync.Mutex
	byID map[string]*domain.Notification
	idem map[string]string
}

func newFake() *fakeStore {
	return &fakeStore{byID: map[string]*domain.Notification{}, idem: map[string]string{}}
}

func (f *fakeStore) Create(_ context.Context, n *domain.Notification, key string) (string, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if key != "" {
		if id, ok := f.idem[n.Producer+"#"+key]; ok {
			return id, false, nil
		}
		f.idem[n.Producer+"#"+key] = n.ID
	}
	f.byID[n.ID] = n
	return n.ID, true, nil
}

func (f *fakeStore) Get(_ context.Context, id string) (*domain.Notification, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if n, ok := f.byID[id]; ok {
		return n, nil
	}
	return nil, dynamo.ErrNotFound
}

func do(t *testing.T, h http.Handler, method, path, key, idem, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if key != "" {
		req.Header.Set("Authorization", "ApiKey "+key)
	}
	if idem != "" {
		req.Header.Set("Idempotency-Key", idem)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func newHandler() (http.Handler, *fakeStore) {
	st := newFake()
	auth := NewAuth(map[string]string{"k1": "p1", "k2": "p2"}, 1000, 1000)
	return NewServer(st, nil, nil, auth).Router(), st
}

const validBody = `{"user_id":"u1","template_id":"otp","category":"transactional","data":{"code":"123"}}`

func TestAuthRequired(t *testing.T) {
	h, _ := newHandler()
	require.Equal(t, 401, do(t, h, "POST", "/v1/notifications", "", "", validBody).Code)
	require.Equal(t, 401, do(t, h, "POST", "/v1/notifications", "bad", "", validBody).Code)
}

func TestCreateAndIdempotentReplay(t *testing.T) {
	h, st := newHandler()
	first := do(t, h, "POST", "/v1/notifications", "k1", "abc", validBody)
	second := do(t, h, "POST", "/v1/notifications", "k1", "abc", validBody)
	require.Equal(t, 202, first.Code)
	require.Equal(t, 202, second.Code)
	require.Contains(t, second.Body.String(), `"duplicate":true`)
	require.Len(t, st.byID, 1)
}

func TestDefaultPriorityByCategory(t *testing.T) {
	n, err := createRequest{UserID: "u", TemplateID: "t", Category: domain.CategoryMarketing}.build("p", time.Now())
	require.NoError(t, err)
	require.Equal(t, domain.PriorityLow, n.Priority)
	n, err = createRequest{UserID: "u", TemplateID: "t", Category: domain.CategoryTransactional}.build("p", time.Now())
	require.NoError(t, err)
	require.Equal(t, domain.PriorityHigh, n.Priority)
}

func TestValidation(t *testing.T) {
	h, _ := newHandler()
	cases := map[string]string{
		"missing user":   `{"template_id":"t","category":"marketing"}`,
		"bad category":   `{"user_id":"u","template_id":"t","category":"spam"}`,
		"bad channel":    `{"user_id":"u","template_id":"t","category":"marketing","channels":["fax"]}`,
		"expired":        `{"user_id":"u","template_id":"t","category":"marketing","expires_at":"2000-01-01T00:00:00Z"}`,
		"unknown field":  `{"user_id":"u","template_id":"t","category":"marketing","x":1}`,
		"far send_at":    `{"user_id":"u","template_id":"t","category":"marketing","send_at":"2999-01-01T00:00:00Z"}`,
		"malformed json": `{`,
	}
	for name, body := range cases {
		require.Equal(t, 400, do(t, h, "POST", "/v1/notifications", "k1", "", body).Code, name)
	}
}

func TestGetIsScopedToProducer(t *testing.T) {
	h, _ := newHandler()
	rec := do(t, h, "POST", "/v1/notifications", "k1", "", validBody)
	require.Equal(t, 202, rec.Code)
	id := rec.Body.String()[len(`{"notification_id":"`) : len(`{"notification_id":"`)+36]
	require.Equal(t, 200, do(t, h, "GET", "/v1/notifications/"+id, "k1", "", "").Code)
	require.Equal(t, 404, do(t, h, "GET", "/v1/notifications/"+id, "k2", "", "").Code)
}

func TestBatchReportsPerItem(t *testing.T) {
	h, st := newHandler()
	body := `{"items":[` + validBody + `,{"user_id":"","template_id":"t","category":"marketing"}]}`
	rec := do(t, h, "POST", "/v1/notifications/batch", "k1", "", body)
	require.Equal(t, 207, rec.Code)
	require.Contains(t, rec.Body.String(), `"error"`)
	require.Len(t, st.byID, 1)
}

func TestRateLimit(t *testing.T) {
	st := newFake()
	h := NewServer(st, nil, nil, NewAuth(map[string]string{"k": "p"}, 0.001, 1)).Router()
	require.Equal(t, 202, do(t, h, "POST", "/v1/notifications", "k", "", validBody).Code)
	require.Equal(t, 429, do(t, h, "POST", "/v1/notifications", "k", "", validBody).Code)
}

type fakeUsers struct {
	profiles map[string]domain.Profile
	prefs    []domain.Preference
	devices  map[string]string
}

func (f *fakeUsers) PutProfile(_ context.Context, p domain.Profile) error {
	f.profiles[p.UserID] = p
	return nil
}

func (f *fakeUsers) PutPreferences(_ context.Context, _ string, p []domain.Preference) error {
	f.prefs = append(f.prefs, p...)
	return nil
}

func (f *fakeUsers) PutDevice(_ context.Context, _ string, d domain.Device) (string, error) {
	f.devices["d1"] = d.Token
	return "d1", nil
}

func (f *fakeUsers) DeleteDevice(_ context.Context, _, id string) error {
	delete(f.devices, id)
	return nil
}

type fakeTemplates struct{}

func (fakeTemplates) Put(_ context.Context, t domain.Template) (domain.Template, error) {
	t.Version = 1
	return t, nil
}

func TestUserEndpoints(t *testing.T) {
	users := &fakeUsers{profiles: map[string]domain.Profile{}, devices: map[string]string{}}
	h := NewServer(newFake(), users, fakeTemplates{}, NewAuth(map[string]string{"k": "p"}, 1000, 1000)).Router()

	require.Equal(t, 204, do(t, h, "PUT", "/v1/users/u1", "k", "", `{"locale":"kk","timezone":"Asia/Almaty","email":"a@b.c"}`).Code)
	require.Equal(t, "kk", users.profiles["u1"].Locale)
	require.Equal(t, 400, do(t, h, "PUT", "/v1/users/u1", "k", "", `{"timezone":"Mars/Base"}`).Code)

	require.Equal(t, 204, do(t, h, "PUT", "/v1/users/u1/preferences", "k", "", `{"preferences":[{"category":"marketing","channel":"push","enabled":true,"quiet_start":"22:00","quiet_end":"08:00"}]}`).Code)
	require.Len(t, users.prefs, 1)
	require.Equal(t, 400, do(t, h, "PUT", "/v1/users/u1/preferences", "k", "", `{"preferences":[{"category":"marketing","channel":"push","enabled":true,"quiet_start":"25:00","quiet_end":"08:00"}]}`).Code)
	require.Equal(t, 400, do(t, h, "PUT", "/v1/users/u1/preferences", "k", "", `{"preferences":[{"category":"x","channel":"push","enabled":true}]}`).Code)

	rec := do(t, h, "POST", "/v1/users/u1/devices", "k", "", `{"token":"abc","platform":"ios"}`)
	require.Equal(t, 201, rec.Code)
	require.Equal(t, 400, do(t, h, "POST", "/v1/users/u1/devices", "k", "", `{"token":"abc","platform":"palm"}`).Code)
	require.Equal(t, 204, do(t, h, "DELETE", "/v1/users/u1/devices/d1", "k", "", "").Code)
	require.Empty(t, users.devices)
}

func TestTemplateEndpointValidatesSyntax(t *testing.T) {
	h := NewServer(newFake(), nil, fakeTemplates{}, NewAuth(map[string]string{"k": "p"}, 1000, 1000)).Router()
	require.Equal(t, 201, do(t, h, "POST", "/v1/templates", "k", "", `{"template_id":"otp","channel":"sms","body":"code {{.code}}"}`).Code)
	require.Equal(t, 400, do(t, h, "POST", "/v1/templates", "k", "", `{"template_id":"otp","channel":"sms","body":"code {{.code"}`).Code)
	require.Equal(t, 400, do(t, h, "POST", "/v1/templates", "k", "", `{"template_id":"otp","channel":"fax","body":"x"}`).Code)
}
