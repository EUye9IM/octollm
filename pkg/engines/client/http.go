package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"net/http/httputil"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/infinigence/octollm/pkg/errutils"
	"github.com/infinigence/octollm/pkg/octollm"
)

type StreamingType string

const (
	StreamingTypeSSE  StreamingType = "sse"
	StreamingTypeJSON StreamingType = "json"
)

type clientMetadataKey string

const (
	// clientRecvFirstChunkTime stores the timestamp (time.Time) when the first chunk
	// was received from the upstream HTTP endpoint.
	clientRecvFirstChunkTime clientMetadataKey = "recv_first_chunk_time"

	// clientProcessStreamError stores any error (error) that occurs during the processing of the stream response.
	// could be context cancellation, context deadline, scanner error, unexpected EOF, etc.
	clientProcessStreamError clientMetadataKey = "process_stream_error"
)

var ErrStreamScan = errors.New("scanner error reading stream body")

var sensitiveHeaders = []string{"Authorization", "X-Api-Key", "X-Auth-Token", "Api-Key", "X-Goog-Api-Key"}

func redactSensitiveHeaders(h http.Header) {
	for _, key := range sensitiveHeaders {
		if h.Get(key) != "" {
			h.Set(key, "[REDACTED]")
		}
	}
}

const maxIncomingRespDumpDebugLogBytes = 2000

// truncateIncomingRespDumpForDebugLog caps httputil.DumpResponse bytes used only for the
// "[http-endpoint] incoming response" debug log line (does not affect relayed bodies).
func truncateIncomingRespDumpForDebugLog(dump []byte) string {
	if len(dump) <= maxIncomingRespDumpDebugLogBytes {
		return string(dump)
	}
	return string(dump[:maxIncomingRespDumpDebugLogBytes]) + " ... [truncated, total=" + strconv.Itoa(len(dump)) + " bytes]"
}

// GetClientRecvFirstChunkTime returns when the first upstream chunk arrived.
//
// Inside an attempt it reads the current attempt; outside one, on a request
// whose load balancer uses attempts, it reads the selected attempt. In both
// cases a missing value is reported as such, never taken from stale shared
// metadata. Requests without attempts keep the legacy metadata behavior.
func GetClientRecvFirstChunkTime(req *octollm.Request) (time.Time, bool) {
	if req == nil {
		return time.Time{}, false
	}
	if state, ok := req.ObservedAttempt(); ok {
		return state.FirstChunkTime()
	}
	value, ok := req.GetMetadataValue(clientRecvFirstChunkTime)
	if !ok {
		return time.Time{}, false
	}
	t, ok := value.(time.Time)
	return t, ok
}

// GetClientProcessStreamError returns the transport-level stream error, read
// with the same attempt rules as GetClientRecvFirstChunkTime. It is separate
// from an attempt's abort error.
func GetClientProcessStreamError(req *octollm.Request) (error, bool) {
	if req == nil {
		return nil, false
	}
	if state, ok := req.ObservedAttempt(); ok {
		return state.StreamError()
	}
	value, ok := req.GetMetadataValue(clientProcessStreamError)
	if !ok {
		return nil, false
	}
	err, ok := value.(error)
	return err, ok
}

func recordFirstChunkTime(req *octollm.Request, t time.Time) {
	if state := req.CurrentAttempt(); state != nil {
		state.RecordFirstChunkTime(t)
		return
	}
	req.SetMetadataValue(clientRecvFirstChunkTime, t)
}

// errStreamClosedByConsumer is the cancel cause of a transport stopped by
// StreamChan.Close. The read error such a deliberate teardown causes is not a
// stream failure and is not recorded.
var errStreamClosedByConsumer = errors.New("stream closed by consumer")

// closedByConsumer reports whether ctx (the transport context) was canceled by
// the stream's Close rather than by the request.
func closedByConsumer(ctx context.Context) bool {
	return errors.Is(context.Cause(ctx), errStreamClosedByConsumer)
}

func recordStreamError(req *octollm.Request, err error) {
	if state := req.CurrentAttempt(); state != nil {
		state.RecordStreamError(err)
		return
	}
	req.SetMetadataValue(clientProcessStreamError, err)
}

// cancelOnCloseBody cancels the transport context once the response body is
// closed, so a fully read or abandoned body also releases the HTTP request.
type cancelOnCloseBody struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (b *cancelOnCloseBody) Close() error {
	err := b.ReadCloser.Close()
	b.cancel()
	return err
}

type HTTPEndpoint struct {
	client          *http.Client
	getURL          func(req *octollm.Request) (string, error)
	reqModifiers    []func(req *octollm.Request, hreq *http.Request) *http.Request
	nonstreamParser func(req *octollm.Request) octollm.Parser
	streamParser    func(req *octollm.Request) (octollm.Parser, StreamingType)
}

// HTTPEndpoint implements octollm.Endpoint
var _ octollm.Engine = (*HTTPEndpoint)(nil)

func NewHTTPEndpoint() *HTTPEndpoint {
	return &HTTPEndpoint{}
}

func (e *HTTPEndpoint) WithClient(client *http.Client) *HTTPEndpoint {
	e.client = client
	return e
}

func (e *HTTPEndpoint) WithURLGetter(getURL func(req *octollm.Request) (string, error)) *HTTPEndpoint {
	e.getURL = getURL
	return e
}

func (e *HTTPEndpoint) WithRequestModifier(reqModifier func(req *octollm.Request, hreq *http.Request) *http.Request) *HTTPEndpoint {
	e.reqModifiers = append(e.reqModifiers, reqModifier)
	return e
}

func (e *HTTPEndpoint) WithParser(nonstreamParser func(req *octollm.Request) octollm.Parser, streamParser func(req *octollm.Request) (octollm.Parser, StreamingType)) *HTTPEndpoint {
	e.nonstreamParser = nonstreamParser
	e.streamParser = streamParser
	return e
}

func (e *HTTPEndpoint) Process(req *octollm.Request) (*octollm.Response, error) {
	if e.getURL == nil {
		return nil, fmt.Errorf("getURL is not set")
	}

	url, err := e.getURL(req)
	if err != nil {
		return nil, fmt.Errorf("getURL error: %w", err)
	}

	if e.client == nil {
		e.client = http.DefaultClient
	}

	parsed, err := req.Body.Parsed()
	if err != nil {
		return nil, fmt.Errorf("parse request body error: %w", err)
	}

	bodyBytes, err := req.Body.Bytes()
	if err != nil {
		return nil, fmt.Errorf("get request body reader error: %w", err)
	}

	// The transport lifetime is its own cancelable context: closing the
	// returned stream (or body) must cancel the real HTTP request, which is
	// what unblocks a producer parked in a body read.
	transportCtx, cancelTransportCause := context.WithCancelCause(req.Context())
	cancelTransport := func() { cancelTransportCause(nil) }
	handedOff := false
	defer func() {
		if !handedOff {
			cancelTransport()
		}
	}()

	httpReq, err := http.NewRequestWithContext(
		transportCtx,
		http.MethodPost,
		url,
		bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("new request error: %w", err)
	}

	httpReq.Header = req.Header.Clone()
	for _, modifier := range e.reqModifiers {
		if modifier == nil {
			continue
		}
		httpReq = modifier(req, httpReq)
	}
	if httpReq.Header.Get("Content-Type") == "" {
		httpReq.Header.Set("Content-Type", "application/json")
	}
	// Mark the request replayable so net/http's Transport will transparently retry
	// it on a fresh connection when a pooled keep-alive conn is closed by the server
	// at the same moment we write the request (errServerClosedIdle / nothingWrittenError).
	// Without an Idempotency-Key header, Request.isReplayable() returns false for POST
	// and shouldRetryRequest gives up, surfacing the race as a spurious request error.
	// See net/http transport.go (shouldRetryRequest) and request.go (isReplayable).
	// The value only needs to be present, not stable: the Transport reuses the same
	// *Request for its one-shot replay, while load-balancer-level retries are new
	// logical attempts that legitimately get a new key.
	httpReq.Header.Set("X-Idempotency-Key", uuid.New().String())

	dumpReq := httpReq.Clone(httpReq.Context())
	redactSensitiveHeaders(dumpReq.Header)
	// Debug logs stringify parsed with fmt %v; payloads may be huge.
	// Turn off debug logs, redact elsewhere, or extend this block for bounded output.
	if reqDump, dumpErr := httputil.DumpRequestOut(dumpReq, false); dumpErr == nil {
		slog.DebugContext(req.Context(), fmt.Sprintf("[http-endpoint] outgoing request:\n%s%v", string(reqDump), parsed))
	}

	resp, err := e.client.Do(httpReq)
	if err != nil {
		slog.ErrorContext(req.Context(), fmt.Sprintf("[http-endpoint] do request error: %v", err))
		return nil, &errutils.UpstreamHTTPError{
			Err: fmt.Errorf("do request error: %w", err),
		}
	}

	dumpRespBody := false
	if resp.StatusCode != http.StatusOK {
		dumpRespBody = true
	}
	if respDump, dumpErr := httputil.DumpResponse(resp, dumpRespBody); dumpErr == nil {
		slog.DebugContext(req.Context(), fmt.Sprintf("[http-endpoint] incoming response:\n%s", truncateIncomingRespDumpForDebugLog(respDump)))
	}

	if resp.StatusCode != http.StatusOK {
		bodyBytes, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return nil, &errutils.UpstreamHTTPError{
				Err:        fmt.Errorf("read response body error: %w", err),
				StatusCode: resp.StatusCode,
			}
		}
		return nil, &errutils.UpstreamRespError{
			StatusCode: resp.StatusCode,
			Header:     resp.Header,
			Body:       bodyBytes,
		}
	}

	ct := resp.Header.Get("Content-Type")
	slog.DebugContext(req.Context(), fmt.Sprintf("[http-endpoint] got response with status code %d, content-type %s", resp.StatusCode, ct))

	// Determine if response is streaming
	isStream := false
	if action, ok := octollm.GetCtxValue[string](req, octollm.ContextKeyAction); ok {
		isStream = octollm.IsStreamAction(action)
	} else {
		// Fallback: use Content-Type header to determine stream mode
		if mt, _, err := mime.ParseMediaType(ct); err == nil {
			isStream = strings.EqualFold(mt, "text/event-stream")
		} else {
			isStream = strings.HasPrefix(strings.ToLower(ct), "text/event-stream")
		}
	}
	if !isStream {
		// non-stream
		slog.DebugContext(req.Context(), "[http-endpoint] returning non-stream response")
		// The body stays readable until it is fully read or closed; either
		// releases the transport context.
		body := octollm.NewBodyFromReader(&cancelOnCloseBody{ReadCloser: resp.Body, cancel: cancelTransport}, e.nonstreamParser(req))
		llmresp := octollm.NewNonStreamResponse(resp.StatusCode, resp.Header, body)
		handedOff = true
		return llmresp, nil
	}

	// stream response
	streamParser, streamingType := e.streamParser(req)
	if streamingType != StreamingTypeSSE && streamingType != StreamingTypeJSON {
		resp.Body.Close()
		return nil, fmt.Errorf("unsupported streaming type %s", streamingType)
	}

	ch := make(chan *octollm.StreamChunk)
	streamChan := octollm.NewStreamChan(ch, func() { cancelTransportCause(errStreamClosedByConsumer) })
	llmresp := octollm.NewStreamResponse(resp.StatusCode, resp.Header, streamChan)
	ctx := transportCtx

	// The producers close the body and then ch themselves; finish releases
	// the transport context and only then reports completion.
	finish := func() {
		cancelTransport()
		streamChan.MarkDone()
	}
	handedOff = true
	switch streamingType {
	case StreamingTypeSSE:
		octollm.SafeGo(req, func() {
			defer finish()
			e.processSSEStream(ctx, req, resp, ch, streamParser)
		})
	case StreamingTypeJSON:
		octollm.SafeGo(req, func() {
			defer finish()
			e.processJSONStream(ctx, req, resp, ch, streamParser)
		})
	}

	slog.DebugContext(req.Context(), "[http-endpoint] returning stream response")
	return llmresp, nil
}

// processSSEStream reads SSE events into ch, then closes the body and ch.
// The caller reports completion once it returns.
func (e *HTTPEndpoint) processSSEStream(ctx context.Context, req *octollm.Request, resp *http.Response, ch chan *octollm.StreamChunk, streamParser octollm.Parser) {
	defer close(ch)
	defer resp.Body.Close()

	metaBuffer := make(map[string]string)
	bodyBuffer := make([]byte, 0, 512)

	var recvFirstChunkTime time.Time

	var totalChunkSent int
	scanner := newScanner(resp.Body)
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			slog.InfoContext(ctx, fmt.Sprintf("[http-endpoint] context error during stream response: %v", err))
			return
		}
		line := scanner.Bytes()

		// process the line according to https://html.spec.whatwg.org/multipage/server-sent-events.html#event-stream-interpretation
		if len(line) == 0 {
			// dispatch the event and continue; per spec, skip if data buffer is empty
			if len(bodyBuffer) == 0 {
				metaBuffer = make(map[string]string)
				continue
			}
			body := octollm.NewBodyFromBytes(bodyBuffer, streamParser)
			// bodyLen := len(bodyBuffer)
			bodyBuffer = make([]byte, 0, 512)
			chunk := &octollm.StreamChunk{Body: body}
			if len(metaBuffer) > 0 {
				chunk.Metadata = metaBuffer
				metaBuffer = make(map[string]string)
			}
			select {
			case ch <- chunk:
				totalChunkSent++
				// slog.DebugContext(ctx, fmt.Sprintf("[http-endpoint] pushed stream chunk: len=%d", bodyLen))
			case <-ctx.Done():
				slog.InfoContext(ctx, fmt.Sprintf("[http-endpoint] context error during stream response: %v", ctx.Err()))
				return
			}
			continue
		}

		colonIdx := slices.Index(line, ':')
		if colonIdx == 0 {
			continue
		}
		var key string
		if colonIdx == -1 {
			key = string(line)
		} else {
			key = string(line[:colonIdx])
		}

		switch key {
		case "data":
			if recvFirstChunkTime.IsZero() {
				recvFirstChunkTime = time.Now()
				recordFirstChunkTime(req, recvFirstChunkTime)
			}
			// find the first non-space byte
			start := slices.IndexFunc(line[colonIdx+1:], func(b byte) bool {
				return b != ' '
			})
			if start != -1 {
				bodyBuffer = append(bodyBuffer, line[colonIdx+1+start:]...)
			}
		case "event", "id":
			value := strings.TrimLeft(string(line[colonIdx+1:]), " ")
			metaBuffer[key] = value
		default:
			// ignore other fields
			slog.DebugContext(ctx, fmt.Sprintf("[http-endpoint] ignore event line because of unknown key %s", key))
		}
	}
	if err := scanner.Err(); err != nil {
		if closedByConsumer(ctx) {
			// Deliberate teardown (e.g. after [DONE] or an abort): not a failure.
			slog.DebugContext(ctx, fmt.Sprintf("[http-endpoint] stream read stopped by consumer close: %v, total chunks sent: %d", err, totalChunkSent))
			return
		}
		recordStreamError(req, fmt.Errorf("%w: %w", ErrStreamScan, err))
		slog.WarnContext(ctx, fmt.Sprintf("[http-endpoint] scan response body error: %v, total chunks sent: %d", err, totalChunkSent))
	} else {
		slog.InfoContext(ctx, fmt.Sprintf("[http-endpoint] stream ended: normal completion, total chunks sent: %d", totalChunkSent))
	}
}

// processJSONStream reads a JSON array stream into ch, with the same resource
// contract as processSSEStream.
func (e *HTTPEndpoint) processJSONStream(ctx context.Context, req *octollm.Request, resp *http.Response, ch chan *octollm.StreamChunk, streamParser octollm.Parser) {
	defer close(ch)
	defer resp.Body.Close()

	dec := json.NewDecoder(resp.Body)

	// Read opening bracket '['
	t, err := dec.Token()
	if err != nil {
		slog.WarnContext(ctx, fmt.Sprintf("[http-endpoint] failed to read opening bracket: %v", err))
		return
	}
	// Verify it's an array opening bracket
	if delim, ok := t.(json.Delim); !ok || delim != '[' {
		slog.WarnContext(ctx, fmt.Sprintf("[http-endpoint] expected array opening bracket, got %T: %v", t, t))
		return
	}

	var recvFirstChunkTime time.Time
	var totalChunkSent int

	// Read array elements
	for dec.More() {
		if ctx.Err() != nil {
			slog.InfoContext(ctx, fmt.Sprintf("[http-endpoint] context error during JSON stream response: %v", ctx.Err()))
			return
		}
		// Decode one JSON object into RawMessage to preserve the raw bytes
		var rawMsg json.RawMessage
		if err := dec.Decode(&rawMsg); err != nil {
			slog.WarnContext(ctx, fmt.Sprintf("[http-endpoint] failed to decode JSON object: %v", err))
			return
		}

		if recvFirstChunkTime.IsZero() {
			recvFirstChunkTime = time.Now()
			recordFirstChunkTime(req, recvFirstChunkTime)
		}

		// Create chunk from raw JSON bytes
		body := octollm.NewBodyFromBytes(rawMsg, streamParser)
		chunk := &octollm.StreamChunk{Body: body}

		select {
		case ch <- chunk:
			totalChunkSent++
			// slog.DebugContext(ctx, fmt.Sprintf("[http-endpoint] pushed JSON stream chunk: len=%d", len(rawMsg)))
		case <-ctx.Done():
			slog.InfoContext(ctx, fmt.Sprintf("[http-endpoint] context error during JSON stream response: %v, total chunks sent: %d", ctx.Err(), totalChunkSent))
			return
		}
	}

	// Read closing bracket ']'
	_, err = dec.Token()
	if err != nil {
		if closedByConsumer(ctx) {
			slog.DebugContext(ctx, fmt.Sprintf("[http-endpoint] JSON stream read stopped by consumer close: %v", err))
			return
		}
		recordStreamError(req, err)
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			slog.InfoContext(ctx, fmt.Sprintf("[http-endpoint] stream ended: context cancelled: %v, total chunks sent: %d", err, totalChunkSent))
		} else {
			slog.WarnContext(ctx, fmt.Sprintf("[http-endpoint] stream ended: failed to read closing bracket (upstream engine may have crashed): %v, total chunks sent: %d", err, totalChunkSent))
		}
		return
	} else {
		slog.InfoContext(ctx, fmt.Sprintf("[http-endpoint] stream ended: normal completion, total chunks sent: %d", totalChunkSent))
	}
}
