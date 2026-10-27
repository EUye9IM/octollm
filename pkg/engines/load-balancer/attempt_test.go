package loadbalancer

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/infinigence/octollm/pkg/engines/limiter"
	"github.com/infinigence/octollm/pkg/internal/testhelper"
	"github.com/infinigence/octollm/pkg/octollm"
)

type attemptLog struct {
	mu    sync.Mutex
	order []string
	ctxs  map[string]context.Context
}

func (l *attemptLog) record(name string, req *octollm.Request) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.order = append(l.order, name)
	if l.ctxs == nil {
		l.ctxs = map[string]context.Context{}
	}
	l.ctxs[name] = req.Context()
}

// firstProvider makes a backend the first candidate of both load balancers,
// so attempt order is deterministic.
type firstProvider struct{ name string }

func (p firstProvider) Resolve(*octollm.Request) ([]*PrioritizedBackend, AffinityCommitFunc, error) {
	if p.name == "" {
		return nil, nil, nil
	}
	return []*PrioritizedBackend{{Name: p.name, StrongCacheHit: true}}, nil, nil
}

func newAttemptLBs(t *testing.T, items []BackendItem, first ...string) map[string]octollm.Engine {
	t.Helper()
	mr := miniredis.RunT(t)
	rd := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rd.Close() })

	var provider AffinityProvider
	if len(first) > 0 {
		provider = firstProvider{name: first[0]}
	}
	wrr, err := NewShardKeyWeightedRoundRobin(items, 5*time.Second, 10, 0, provider, nil, rd)
	require.NoError(t, err)
	conc, err := NewShardKeyConcurrency(withTestConcurrencyKeys(items), 5*time.Second, 10, provider, rd, nil)
	require.NoError(t, err)
	return map[string]octollm.Engine{"wrr": wrr, "concurrency": conc}
}

func constMax(items []BackendItem) []BackendItem {
	for i := range items {
		items[i].MaxConcurrencyFn = func(*octollm.Request) int { return 10 }
	}
	return items
}

// The selected attempt is the one whose result is returned, and a successful
// stream keeps updating it after Process has returned.
func TestShardKeyLB_SelectedAttemptPairsWithSuccess(t *testing.T) {
	for _, lbName := range []string{"wrr", "concurrency"} {
		t.Run(lbName, func(t *testing.T) {
			log := &attemptLog{}
			lateWrite := make(chan struct{})
			lateDone := make(chan struct{})
			failA := octollm.EngineFunc(func(req *octollm.Request) (*octollm.Response, error) {
				log.record("A", req)
				state := req.CurrentAttempt()
				go func() {
					<-lateWrite
					// A's producer writes after B has started.
					state.RecordFirstChunkTime(time.Unix(1, 0))
					state.RecordStreamError(errors.New("late A"))
					close(lateDone)
				}()
				return nil, errors.New("A failed")
			})
			ch := make(chan *octollm.StreamChunk)
			okB := octollm.EngineFunc(func(req *octollm.Request) (*octollm.Response, error) {
				log.record("B", req)
				state := req.CurrentAttempt()
				stream := octollm.NewStreamChan(ch, nil)
				go func() {
					state.RecordFirstChunkTime(time.Unix(2, 0))
					octollm.FinishStream(stream, ch, nil)
				}()
				return octollm.NewStreamResponse(200, nil, stream), nil
			})
			items := constMax([]BackendItem{{Name: "A", Weight: 1, Engine: failA}, {Name: "B", Weight: 1, Engine: okB}})
			lb := newAttemptLBs(t, items, "A")[lbName]

			req := testhelper.CreateTestRequest()
			resp, err := lb.Process(req)
			require.NoError(t, err)
			close(lateWrite)
			<-lateDone // A wrote after B had started

			name, ok := GetSelectedBackendName(req)
			require.True(t, ok)
			assert.Equal(t, "B", name)

			for range resp.Stream.Chan() {
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			require.True(t, resp.Stream.WaitDone(ctx))

			sel := req.SelectedAttempt()
			ts, ok := sel.FirstChunkTime()
			require.True(t, ok, "state written after Process returned must be visible")
			assert.Equal(t, int64(2), ts.Unix())
			_, hasErr := sel.StreamError()
			assert.False(t, hasErr, "A's late error must not pollute B")

			log.mu.Lock()
			order := append([]string(nil), log.order...)
			bCtx := log.ctxs["B"]
			aCtx := log.ctxs["A"]
			log.mu.Unlock()
			require.Equal(t, []string{"A", "B"}, order)
			assert.NoError(t, bCtx.Err(), "successful attempt must stay alive until the response is closed")
			assert.Error(t, aCtx.Err(), "failed attempt must be canceled")
			resp.Stream.Close()
			assert.Error(t, bCtx.Err(), "closing the response ends the attempt")
		})
	}
}

// A later local reject never replaces the real call's error, nor its attempt.
func TestShardKeyLB_SelectedAttemptPairsWithReturnedError(t *testing.T) {
	realErr := errors.New("real upstream failure")
	for _, lbName := range []string{"wrr", "concurrency"} {
		t.Run(lbName, func(t *testing.T) {
			log := &attemptLog{}
			real := octollm.EngineFunc(func(req *octollm.Request) (*octollm.Response, error) {
				log.record("real", req)
				return nil, realErr
			})
			local := octollm.EngineFunc(func(req *octollm.Request) (*octollm.Response, error) {
				log.record("local", req)
				return nil, limiter.ErrRateLimitReached
			})
			items := constMax([]BackendItem{{Name: "real", Weight: 1, Engine: real}, {Name: "local", Weight: 1, Engine: local}})
			// The real failure happens first; the local reject comes after it.
			lb := newAttemptLBs(t, items, "real")[lbName]

			req := testhelper.CreateTestRequest()
			_, err := lb.Process(req)
			require.ErrorIs(t, err, realErr)
			log.mu.Lock()
			assert.Equal(t, []string{"real", "local"}, log.order)
			log.mu.Unlock()
			name, ok := GetSelectedBackendName(req)
			require.True(t, ok)
			assert.Equal(t, "real", name)
		})
	}
}

func TestShardKeyLB_AllLocalRejectsSelectFirstReject(t *testing.T) {
	for _, lbName := range []string{"wrr", "concurrency"} {
		t.Run(lbName, func(t *testing.T) {
			log := &attemptLog{}
			mk := func(n string) octollm.Engine {
				return octollm.EngineFunc(func(req *octollm.Request) (*octollm.Response, error) {
					log.record(n, req)
					return nil, limiter.ErrRateLimitReached
				})
			}
			items := constMax([]BackendItem{{Name: "x", Weight: 1, Engine: mk("x")}, {Name: "y", Weight: 1, Engine: mk("y")}})
			lb := newAttemptLBs(t, items)[lbName]

			req := testhelper.CreateTestRequest()
			_, err := lb.Process(req)
			require.ErrorIs(t, err, limiter.ErrRateLimitReached)
			name, ok := GetSelectedBackendName(req)
			require.True(t, ok)
			log.mu.Lock()
			defer log.mu.Unlock()
			require.NotEmpty(t, log.order)
			assert.Equal(t, log.order[0], name)
		})
	}
}

func TestShardKeyLB_NoAttemptNoSelection(t *testing.T) {
	items := constMax([]BackendItem{{Name: "zero", Weight: 1, Engine: octollm.EngineFunc(func(*octollm.Request) (*octollm.Response, error) {
		t.Fatal("must not be called")
		return nil, nil
	})}})
	items[0].MaxConcurrencyFn = func(*octollm.Request) int { return 0 }
	lb := newAttemptLBs(t, items)["concurrency"]
	req := testhelper.CreateTestRequest()
	_, err := lb.Process(req)
	require.Error(t, err)
	_, ok := GetSelectedBackendName(req)
	assert.False(t, ok)
}

// A second load balancer on the same request that returns without any attempt
// must not expose the first one's attempt.
func TestShardKeyLB_SequentialLBsResetSelection(t *testing.T) {
	for _, lbName := range []string{"wrr", "concurrency"} {
		t.Run(lbName, func(t *testing.T) {
			first := newAttemptLBs(t, constMax([]BackendItem{{Name: "A", Weight: 1, Engine: octollm.EngineFunc(func(*octollm.Request) (*octollm.Response, error) {
				return nil, errors.New("A failed")
			})}}))[lbName]
			emptyItems := constMax([]BackendItem{{Name: "Z", Weight: 1, Engine: octollm.EngineFunc(func(*octollm.Request) (*octollm.Response, error) {
				t.Fatal("must not be called")
				return nil, nil
			})}})
			emptyItems[0].MaxConcurrencyFn = func(*octollm.Request) int { return 0 }
			second := newAttemptLBs(t, emptyItems)["concurrency"]

			req := testhelper.CreateTestRequest()
			_, err := first.Process(req)
			require.Error(t, err)
			name, ok := GetSelectedBackendName(req)
			require.True(t, ok)
			require.Equal(t, "A", name)

			_, err = second.Process(req)
			require.Error(t, err)
			_, ok = GetSelectedBackendName(req)
			assert.False(t, ok, "no attempt in the second LB: nothing selected")
		})
	}
}

func TestShardKeyLB_CanceledBeforeFirstAttempt(t *testing.T) {
	for _, lbName := range []string{"wrr", "concurrency"} {
		t.Run(lbName, func(t *testing.T) {
			items := constMax([]BackendItem{{Name: "A", Weight: 1, Engine: octollm.EngineFunc(func(*octollm.Request) (*octollm.Response, error) {
				t.Fatal("must not start an attempt after cancellation")
				return nil, nil
			})}})
			lb := newAttemptLBs(t, items)[lbName]
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			req := testhelper.CreateTestRequest(testhelper.WithContext(ctx))
			_, err := lb.Process(req)
			require.ErrorIs(t, err, context.Canceled)
			_, ok := GetSelectedBackendName(req)
			assert.False(t, ok)
		})
	}
}

// cancelingAdmission cancels the request while admitting the given backend.
type cancelingAdmission struct {
	backend string
	cancel  context.CancelFunc
}

func (a *cancelingAdmission) BeforeAttempt(_ *octollm.Request, name string) (AttemptDoneFunc, bool) {
	if name == a.backend {
		a.cancel()
	}
	return nil, true
}

// A cancellation that happens during admission must not start the svc call,
// and a previous real error (with its attempt) is still what is returned.
func TestShardKeyLB_CanceledDuringAdmission(t *testing.T) {
	realErr := errors.New("real upstream failure")
	for _, lbName := range []string{"wrr", "concurrency"} {
		t.Run(lbName, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			log := &attemptLog{}
			items := constMax([]BackendItem{
				{Name: "A", Weight: 1, Engine: octollm.EngineFunc(func(req *octollm.Request) (*octollm.Response, error) {
					log.record("A", req)
					return nil, realErr
				})},
				{Name: "B", Weight: 1, Engine: octollm.EngineFunc(func(req *octollm.Request) (*octollm.Response, error) {
					t.Fatal("must not call B after cancellation during its admission")
					return nil, nil
				})},
			})
			admission := &cancelingAdmission{backend: "B", cancel: cancel}
			rd := newTestRedis(t)
			var lb octollm.Engine
			var err error
			if lbName == "wrr" {
				lb, err = NewShardKeyWeightedRoundRobin(items, 5*time.Second, 10, 0, firstProvider{name: "A"}, admission, rd)
			} else {
				lb, err = NewShardKeyConcurrency(withTestConcurrencyKeys(items), 5*time.Second, 10, firstProvider{name: "A"}, rd, admission)
			}
			require.NoError(t, err)

			req := testhelper.CreateTestRequest(testhelper.WithContext(ctx))
			_, err = lb.Process(req)
			require.ErrorIs(t, err, realErr)
			name, ok := GetSelectedBackendName(req)
			require.True(t, ok)
			assert.Equal(t, "A", name)
		})
	}
}
