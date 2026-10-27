package octollm

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestStreamChan_SetErrorKeepsFirst(t *testing.T) {
	sc := NewStreamChan(make(chan *StreamChunk), nil)
	if sc.Err() != nil {
		t.Fatal("new stream must have no error")
	}
	sc.SetError(nil)
	first := errors.New("first")
	var wg sync.WaitGroup
	sc.SetError(first)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); sc.SetError(errors.New("later")) }()
	}
	wg.Wait()
	if !errors.Is(sc.Err(), first) {
		t.Fatalf("Err = %v, want first", sc.Err())
	}
}

func TestStreamChan_CloseIdempotentAndCallbacksOnce(t *testing.T) {
	var base, extra atomic.Int32
	sc := NewStreamChan(make(chan *StreamChunk), func() { base.Add(1) })
	sc.OnClose(func() { extra.Add(1) })

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); sc.Close() }()
	}
	wg.Wait()
	if base.Load() != 1 || extra.Load() != 1 {
		t.Fatalf("callbacks ran base=%d extra=%d, want 1 each", base.Load(), extra.Load())
	}
	select {
	case <-sc.Closed():
	default:
		t.Fatal("Closed must be closed after Close")
	}
	if sc.Err() != nil {
		t.Fatal("Close must not set an error")
	}

	// Registering after Close releases immediately, exactly once.
	var late atomic.Int32
	sc.OnClose(func() { late.Add(1) })
	sc.Close()
	if late.Load() != 1 {
		t.Fatalf("late callback ran %d times, want 1", late.Load())
	}
}

func TestStreamChan_DoneAndFinishStream(t *testing.T) {
	src := NewStreamChan(make(chan *StreamChunk), nil)
	srcErr := errors.New("terminal")
	src.SetError(srcErr)

	ch := make(chan *StreamChunk)
	out := NewStreamChan(ch, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if out.WaitDone(ctx) {
		t.Fatal("WaitDone must time out before MarkDone")
	}

	FinishStream(out, ch, src)
	if _, ok := <-ch; ok {
		t.Fatal("output channel must be closed")
	}
	if !errors.Is(out.Err(), srcErr) {
		t.Fatalf("Err = %v, want source error", out.Err())
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	if !out.WaitDone(ctx2) {
		t.Fatal("WaitDone must succeed after FinishStream")
	}
	out.MarkDone() // idempotent
}

func TestAttempt_IsolationAndSelection(t *testing.T) {
	req := NewEmptyRequest(context.Background())
	if req.AttemptsEnabled() {
		t.Fatal("attempts must be disabled before any attempt")
	}

	aReq, aScope := req.BeginAttempt("A")
	bReq, bScope := req.BeginAttempt("B")
	defer bScope.Cancel()

	if aReq.CurrentAttempt() != aScope.State() || bReq.CurrentAttempt() != bScope.State() {
		t.Fatal("each attempt request must see its own state")
	}
	if req.CurrentAttempt() != nil {
		t.Fatal("outer request is not inside an attempt")
	}
	if !req.AttemptsEnabled() {
		t.Fatal("outer request must see attempts enabled once one began")
	}
	if st, ok := req.ObservedAttempt(); !ok || st != nil {
		t.Fatal("before selection outer must observe 'enabled but no value'")
	}

	// A is abandoned, then writes late; B must not see it.
	aScope.FinishFailed(nil)
	if aReq.Context().Err() == nil {
		t.Fatal("FinishFailed must cancel the attempt context")
	}
	if bReq.Context().Err() != nil || req.Context().Err() != nil {
		t.Fatal("canceling one attempt must not affect others or the parent")
	}
	aScope.State().RecordFirstChunkTime(time.Now())
	aScope.State().RecordStreamError(errors.New("late read error"))
	if _, ok := bScope.State().FirstChunkTime(); ok {
		t.Fatal("B must not observe A's first chunk time")
	}

	req.SelectAttempt(bScope.State())
	if got, _ := req.ObservedAttempt(); got != bScope.State() {
		t.Fatal("outer must observe the selected attempt")
	}
	// A live reference: updates after selection are visible.
	ts := time.Now()
	bScope.State().RecordFirstChunkTime(ts)
	if got, ok := req.SelectedAttempt().FirstChunkTime(); !ok || !got.Equal(ts) {
		t.Fatal("selected attempt must be a live reference")
	}
}

func TestAttempt_AbortKeepsFirstAndSeparateFromStreamError(t *testing.T) {
	s := NewAttemptState("svc")
	abortErr := errors.New("rate limited")
	s.Abort(abortErr)
	s.Abort(errors.New("other"))
	s.RecordStreamError(context.Canceled)
	if !s.Aborted() || !errors.Is(s.AbortError(), abortErr) {
		t.Fatal("first abort reason must be kept")
	}
	if err, ok := s.StreamError(); !ok || !errors.Is(err, context.Canceled) {
		t.Fatal("stream error must be recorded separately")
	}
}

func TestAttemptScope_BindResponseLifetime(t *testing.T) {
	req := NewEmptyRequest(context.Background())
	aReq, scope := req.BeginAttempt("A")
	ch := make(chan *StreamChunk)
	resp := NewStreamResponse(200, nil, NewStreamChan(ch, nil))
	scope.BindResponseLifetime(resp)
	if aReq.Context().Err() != nil {
		t.Fatal("successful response must keep the attempt alive")
	}
	resp.Stream.Close()
	if aReq.Context().Err() == nil {
		t.Fatal("closing the response must end the attempt")
	}
}

func TestAttemptScope_FinishFailedIsBounded(t *testing.T) {
	req := NewEmptyRequest(context.Background())
	_, scope := req.BeginAttempt("A")
	// A producer that never reports completion.
	resp := NewStreamResponse(200, nil, NewStreamChan(make(chan *StreamChunk), nil))
	start := time.Now()
	scope.FinishFailed(resp)
	if d := time.Since(start); d > DefaultAttemptFinishTimeout+500*time.Millisecond {
		t.Fatalf("FinishFailed blocked %v", d)
	}
}

func TestAttemptScope_FinishFailedBoundsBlockingCloseCallback(t *testing.T) {
	req := NewEmptyRequest(context.Background())
	_, scope := req.BeginAttempt("A")
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	stream := NewStreamChan(make(chan *StreamChunk), func() { <-release })
	start := time.Now()
	scope.finishWithin(NewStreamResponse(200, nil, stream), 50*time.Millisecond)
	if d := time.Since(start); d > 500*time.Millisecond {
		t.Fatalf("FinishFailed blocked %v on a close callback", d)
	}
}

func TestAttemptScope_FinishFailedReturnsWhenDone(t *testing.T) {
	req := NewEmptyRequest(context.Background())
	_, scope := req.BeginAttempt("A")
	ch := make(chan *StreamChunk)
	stream := NewStreamChan(ch, nil)
	go func() {
		<-stream.Closed()
		FinishStream(stream, ch, nil)
	}()
	start := time.Now()
	scope.FinishFailed(NewStreamResponse(200, nil, stream))
	if d := time.Since(start); d > 200*time.Millisecond {
		t.Fatalf("cooperative producer took %v", d)
	}
}
