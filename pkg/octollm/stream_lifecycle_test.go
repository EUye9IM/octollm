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
