package converter

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/infinigence/octollm/pkg/octollm"
	"github.com/infinigence/octollm/pkg/types/openai"
)

// failedSource returns a closed stream carrying datas and terminal error err.
func failedSource(err error, datas ...string) *octollm.StreamChan {
	ch := make(chan *octollm.StreamChunk, len(datas))
	sc := octollm.NewStreamChan(ch, nil)
	for _, d := range datas {
		ch <- &octollm.StreamChunk{Body: octollm.NewBodyFromBytes([]byte(d), &octollm.JSONParser[openai.ChatCompletionStreamChunk]{})}
	}
	sc.SetError(err)
	close(ch)
	sc.MarkDone()
	return sc
}

func drainAndWait(t *testing.T, s *octollm.StreamChan) {
	t.Helper()
	for range s.Chan() {
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.True(t, s.WaitDone(ctx), "converter must report completion")
}

func TestConverters_PropagateTerminalError(t *testing.T) {
	terminal := errors.New("terminal")
	chunk := `{"id":"c","model":"m","choices":[{"index":0,"delta":{"content":"hi"}}]}`
	cases := map[string][]string{
		"normal end":       {chunk},
		"early [DONE] end": {chunk, "[DONE]"},
	}
	for name, datas := range cases {
		t.Run("claude/"+name, func(t *testing.T) {
			httpReq, _ := http.NewRequest("POST", "http://localhost/v1/messages", nil)
			req := octollm.NewRequest(httpReq, octollm.APIFormatClaudeMessages)
			out, err := (&ChatCompletionToClaudeMessages{}).convertStreamResponse(req, failedSource(terminal, datas...))
			require.NoError(t, err)
			drainAndWait(t, out)
			assert.ErrorIs(t, out.Err(), terminal)
		})
		t.Run("responses/"+name, func(t *testing.T) {
			httpReq, _ := http.NewRequest("POST", "http://localhost/v1/responses", nil)
			req := octollm.NewRequest(httpReq, octollm.APIFormatResponses)
			out, err := (&ChatCompletionToResponses{}).convertStreamResponse(req, failedSource(terminal, datas...))
			require.NoError(t, err)
			drainAndWait(t, out)
			assert.ErrorIs(t, out.Err(), terminal)
		})
	}
}

// Closing the converted stream releases a converter parked on receive, even
// when the source producer ignores the stop request and never closes its
// channel.
func TestConverters_CloseReleasesBlockedReceive(t *testing.T) {
	httpReq, _ := http.NewRequest("POST", "http://localhost/v1/messages", nil)
	req := octollm.NewRequest(httpReq, octollm.APIFormatClaudeMessages)
	srcCh := make(chan *octollm.StreamChunk)
	src := octollm.NewStreamChan(srcCh, nil)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	go func() {
		<-release // uncooperative: only the test ends it
		octollm.FinishStream(src, srcCh, nil)
	}()
	out, err := (&ChatCompletionToClaudeMessages{}).convertStreamResponse(req, src)
	require.NoError(t, err)
	out.Close()
	drainAndWait(t, out)
}
