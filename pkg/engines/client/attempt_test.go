package client

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/infinigence/octollm/pkg/internal/testhelper"
	"github.com/infinigence/octollm/pkg/octollm"
)

// newHangingSSEServer writes one SSE event and then keeps the connection open
// until the client goes away (or the test ends).
func newHangingSSEServer(t *testing.T, event string) *httptest.Server {
	t.Helper()
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "data: %s\n\n", event)
		w.(http.Flusher).Flush()
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	t.Cleanup(func() {
		close(release)
		srv.Close()
	})
	return srv
}

func newChatEndpoint(baseURL string) *GeneralEndpoint {
	return NewGeneralEndpoint(GeneralEndpointConfig{
		BaseURL: baseURL,
		Endpoints: map[octollm.APIFormat]string{
			octollm.APIFormatChatCompletions: "/v1/chat/completions",
		},
	})
}

// Closing the stream must cancel the real HTTP request so a producer parked in
// a network read finishes, instead of only unblocking its send.
func TestHTTPEndpoint_CloseInterruptsNetworkRead(t *testing.T) {
	srv := newHangingSSEServer(t, `{"error":{"type":"rate_limit_error"}}`)
	ep := newChatEndpoint(srv.URL)

	req := testhelper.CreateTestRequest()
	attemptReq, scope := req.BeginAttempt("svc-a")
	defer scope.Cancel()

	resp, err := ep.Process(attemptReq)
	require.NoError(t, err)
	require.NotNil(t, resp.Stream)

	select {
	case chunk := <-resp.Stream.Chan():
		require.NotNil(t, chunk)
	case <-time.After(2 * time.Second):
		t.Fatal("first chunk not received")
	}
	_, hasFirst := scope.State().FirstChunkTime()
	assert.True(t, hasFirst, "first chunk time must be written to the current attempt")

	// The producer is now blocked in scanner.Scan on the open connection.
	resp.Stream.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.True(t, resp.Stream.WaitDone(ctx), "producer must finish after Close")
	_, open := <-resp.Stream.Chan()
	assert.False(t, open)
	assert.NoError(t, resp.Stream.Err(), "transport close must not invent a terminal error")

	// A deliberate teardown is neither a stream failure nor an abort.
	assert.False(t, scope.State().Aborted())
	_, hasStreamErr := scope.State().StreamError()
	assert.False(t, hasStreamErr, "consumer close must not be recorded as a stream error")
}

// A request cancellation still records the read error, as before.
func TestHTTPEndpoint_RequestCancelRecordsStreamError(t *testing.T) {
	srv := newHangingSSEServer(t, `{"choices":[]}`)
	ep := newChatEndpoint(srv.URL)

	ctx, cancel := context.WithCancel(context.Background())
	req := testhelper.CreateTestRequest(testhelper.WithContext(ctx))
	attemptReq, scope := req.BeginAttempt("svc-a")
	defer scope.Cancel()

	resp, err := ep.Process(attemptReq)
	require.NoError(t, err)
	<-resp.Stream.Chan()
	cancel()
	waitCtx, cancelWait := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelWait()
	require.True(t, resp.Stream.WaitDone(waitCtx))
	streamErr, ok := scope.State().StreamError()
	require.True(t, ok)
	assert.ErrorIs(t, streamErr, ErrStreamScan)
}

func TestHTTPEndpoint_NormalEndMarksDone(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"choices\":[]}\n\ndata: [DONE]\n\n")
	}))
	defer srv.Close()

	resp, err := newChatEndpoint(srv.URL).Process(testhelper.CreateTestRequest())
	require.NoError(t, err)
	n := 0
	for range resp.Stream.Chan() {
		n++
	}
	assert.Equal(t, 2, n)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	assert.True(t, resp.Stream.WaitDone(ctx))
	assert.NoError(t, resp.Stream.Err())
}

func TestClientGetters_AttemptPriorityAndLegacy(t *testing.T) {
	// Legacy: no attempts, metadata is used.
	legacy := testhelper.CreateTestRequest()
	ts := time.Now()
	recordFirstChunkTime(legacy, ts)
	recordStreamError(legacy, errors.New("legacy"))
	got, ok := GetClientRecvFirstChunkTime(legacy)
	require.True(t, ok)
	assert.True(t, got.Equal(ts))
	_, ok = GetClientProcessStreamError(legacy)
	assert.True(t, ok)

	// With attempts: a stale metadata value is never returned.
	req := testhelper.CreateTestRequest()
	req.SetMetadataValue(clientRecvFirstChunkTime, ts)
	aReq, aScope := req.BeginAttempt("A")
	bReq, bScope := req.BeginAttempt("B")
	defer aScope.Cancel()
	defer bScope.Cancel()

	_, ok = GetClientRecvFirstChunkTime(req)
	assert.False(t, ok, "enabled but unselected must report no value")

	tb := ts.Add(time.Second)
	recordFirstChunkTime(bReq, tb)
	// A writes late, after B started.
	recordFirstChunkTime(aReq, ts.Add(2*time.Second))
	recordStreamError(aReq, errors.New("late A error"))

	req.SelectAttempt(bScope.State())
	got, ok = GetClientRecvFirstChunkTime(req)
	require.True(t, ok)
	assert.True(t, got.Equal(tb), "outer must read the selected attempt")
	_, ok = GetClientProcessStreamError(req)
	assert.False(t, ok, "A's late error must not pollute B")

	got, ok = GetClientRecvFirstChunkTime(aReq)
	require.True(t, ok)
	assert.True(t, got.Equal(ts.Add(2*time.Second)), "inside an attempt, read the current one")
}
