package octollm

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// AttemptState holds the runtime state of one call to one backend (one
// "attempt"). A load balancer that retries creates a fresh state per attempt,
// so a late write from an abandoned attempt can never leak into the next one.
//
// It is deliberately free of gateway- or client-specific types: richer
// classification travels in the abort error itself. All methods are safe for
// concurrent use and are nil-receiver safe.
type AttemptState struct {
	backendName string

	mu             sync.Mutex
	firstChunkTime time.Time
	streamErr      error
	aborted        bool
	abortErr       error
}

// NewAttemptState returns a state for one attempt against backendName.
func NewAttemptState(backendName string) *AttemptState {
	return &AttemptState{backendName: backendName}
}

// BackendName is the backend this attempt targets. It is fixed at creation.
func (s *AttemptState) BackendName() string {
	if s == nil {
		return ""
	}
	return s.backendName
}

// RecordFirstChunkTime stores the time the first upstream chunk arrived. Only
// the first non-zero value is kept.
func (s *AttemptState) RecordFirstChunkTime(t time.Time) {
	if s == nil || t.IsZero() {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.firstChunkTime.IsZero() {
		s.firstChunkTime = t
	}
}

// FirstChunkTime returns the first-chunk time, if recorded.
func (s *AttemptState) FirstChunkTime() (time.Time, bool) {
	if s == nil {
		return time.Time{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.firstChunkTime, !s.firstChunkTime.IsZero()
}

// RecordStreamError stores a transport-level stream error (scanner, decode,
// cancellation). It is kept apart from the abort error so that a read error
// caused by tearing down the connection cannot replace the reason the attempt
// was abandoned. Only the first non-nil error is kept.
func (s *AttemptState) RecordStreamError(err error) {
	if s == nil || err == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.streamErr == nil {
		s.streamErr = err
	}
}

// StreamError returns the transport-level stream error, if any.
func (s *AttemptState) StreamError() (error, bool) {
	if s == nil {
		return nil, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.streamErr, s.streamErr != nil
}

// Abort marks the attempt's result as discarded and keeps the first abort
// reason. Callers must call it before the failed stream's end becomes visible
// to consumers (i.e. before closing the output channel).
func (s *AttemptState) Abort(err error) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.aborted {
		s.aborted = true
		s.abortErr = err
	}
}

// Aborted reports whether the attempt was abandoned.
func (s *AttemptState) Aborted() bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.aborted
}

// AbortError returns the reason the attempt was abandoned.
func (s *AttemptState) AbortError() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.abortErr
}

// attemptCtxKey carries the current *AttemptState in an attempt's context.
type attemptCtxKey struct{}

// WithoutAttempt returns ctx with the current attempt hidden. Use it when a
// context derived from an attempt starts an independent logical request
// (e.g. traffic replication).
func WithoutAttempt(ctx context.Context) context.Context {
	return context.WithValue(ctx, attemptCtxKey{}, (*AttemptState)(nil))
}

// selectedAttemptKey is the metadata key of the request-wide *attemptSelection.
// Metadata is shared by every shallow copy of a request, so the outer request
// observes what the load balancer publishes on a derived one.
type selectedAttemptKey struct{}

type attemptSelection struct {
	mu    sync.Mutex
	state *AttemptState
}

func (u *Request) attemptSelection(create bool) *attemptSelection {
	if u == nil {
		return nil
	}
	if u.metadata == nil {
		if !create {
			return nil
		}
		u.metadata = &sync.Map{}
	}
	if !create {
		v, ok := u.metadata.Load(selectedAttemptKey{})
		if !ok {
			return nil
		}
		sel, _ := v.(*attemptSelection)
		return sel
	}
	v, _ := u.metadata.LoadOrStore(selectedAttemptKey{}, &attemptSelection{})
	return v.(*attemptSelection)
}

// AttemptsEnabled reports whether the attempt mechanism is in use for this
// logical request (an attempt was begun on it or on a shallow copy). Getters
// use it to decide between attempt state and legacy metadata.
func (u *Request) AttemptsEnabled() bool {
	if u.CurrentAttempt() != nil {
		return true
	}
	return u.attemptSelection(false) != nil
}

// CurrentAttempt returns the attempt this request runs in, or nil outside an
// attempt. Engines inside one svc call (HTTP client, detectors, loggers) read
// their own attempt through it.
func (u *Request) CurrentAttempt() *AttemptState {
	if u == nil || u.ctx == nil {
		return nil
	}
	s, _ := u.ctx.Value(attemptCtxKey{}).(*AttemptState)
	return s
}

// SelectAttempt publishes attempt as the one whose result is returned for the
// whole logical request. Only attempt boundaries (load balancers, single-call
// wrappers) should call it.
func (u *Request) SelectAttempt(attempt *AttemptState) {
	sel := u.attemptSelection(true)
	if sel == nil {
		return
	}
	sel.mu.Lock()
	sel.state = attempt
	sel.mu.Unlock()
}

// SelectedAttempt returns the published attempt, or nil if none was
// published. Outer engines (token report, metrics) that are not inside an
// attempt read through it. The returned state is a live reference: a
// successful stream keeps updating it after Process returned.
func (u *Request) SelectedAttempt() *AttemptState {
	sel := u.attemptSelection(false)
	if sel == nil {
		return nil
	}
	sel.mu.Lock()
	defer sel.mu.Unlock()
	return sel.state
}

// ObservedAttempt is the attempt a getter should read: the current attempt
// inside one, otherwise the selected attempt. ok is false when the attempt
// mechanism is not in use, in which case callers fall back to legacy metadata.
// When ok is true but the state is nil, there is no value to report; callers
// must not fall back to metadata.
func (u *Request) ObservedAttempt() (state *AttemptState, ok bool) {
	if cur := u.CurrentAttempt(); cur != nil {
		return cur, true
	}
	if u.attemptSelection(false) != nil {
		return u.SelectedAttempt(), true
	}
	return nil, false
}

// DefaultAttemptFinishTimeout bounds how long [AttemptScope.FinishFailed]
// waits for a failed attempt's stream producer to finish.
const DefaultAttemptFinishTimeout = time.Second

// AttemptScope owns the lifecycle of one attempt: its state and a cancelable
// context that does not affect the parent request or sibling attempts.
type AttemptScope struct {
	state  *AttemptState
	ctx    context.Context
	cancel context.CancelFunc
}

// BeginAttempt starts one attempt against backendName. It returns a derived
// request whose context carries a fresh [AttemptState] and is canceled by the
// scope, plus the scope itself. Request-level data (context values, metadata,
// body) is inherited; the previous attempt's runtime state is not.
//
// The request-wide selection holder is created here, before the copy, so the
// original request can read what is published later.
func (u *Request) BeginAttempt(backendName string) (*Request, *AttemptScope) {
	u.attemptSelection(true)
	state := NewAttemptState(backendName)
	ctx, cancel := context.WithCancel(context.WithValue(u.Context(), attemptCtxKey{}, state))
	return u.WithContext(ctx), &AttemptScope{state: state, ctx: ctx, cancel: cancel}
}

// State returns the attempt's state.
func (s *AttemptScope) State() *AttemptState {
	if s == nil {
		return nil
	}
	return s.state
}

// Cancel cancels the attempt's context. Idempotent; it does not affect the
// parent request or other attempts.
func (s *AttemptScope) Cancel() {
	if s == nil || s.cancel == nil {
		return
	}
	s.cancel()
}

// BindResponseLifetime ties the attempt context to a successful response: the
// attempt is canceled when the response stream or body is closed rather than
// when Process returns, because a stream is still being produced afterwards.
func (s *AttemptScope) BindResponseLifetime(resp *Response) {
	if s == nil {
		return
	}
	if resp == nil {
		s.Cancel()
		return
	}
	switch {
	case resp.Stream != nil:
		resp.Stream.OnClose(s.Cancel)
	case resp.Body != nil:
		resp.Body.OnClose(s.Cancel)
	default:
		s.Cancel()
	}
}

// FinishFailed ends a failed attempt: it cancels the attempt, releases any
// response resources still held and waits for a stream producer to report
// completion. The close callbacks and the wait together are bounded by
// DefaultAttemptFinishTimeout, so neither an uncooperative producer nor a
// blocking close callback can stall the next attempt; a timeout is logged and
// the cleanup keeps running in the background.
func (s *AttemptScope) FinishFailed(resp *Response) {
	if s == nil {
		return
	}
	s.Cancel()
	if resp == nil || (resp.Body == nil && resp.Stream == nil) {
		return
	}
	s.finishWithin(resp, DefaultAttemptFinishTimeout)
}

func (s *AttemptScope) finishWithin(resp *Response, timeout time.Duration) {
	waitCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	finished := make(chan struct{})
	go func() {
		defer close(finished)
		if resp.Body != nil {
			resp.Body.Close()
		}
		if resp.Stream != nil {
			resp.Stream.Close()
			resp.Stream.WaitDone(waitCtx)
		}
	}()

	select {
	case <-finished:
		if resp.Stream != nil {
			select {
			case <-resp.Stream.Done():
				return
			default:
			}
		} else {
			return
		}
	case <-waitCtx.Done():
	}
	slog.WarnContext(s.ctx, "[attempt] failed attempt cleanup did not finish in time",
		"backend_name", s.state.BackendName(), "timeout", timeout)
}
