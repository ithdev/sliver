package core

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/bishopfox/sliver/protobuf/rpcpb"
	"github.com/bishopfox/sliver/protobuf/sliverpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type recvResult struct {
	data *sliverpb.TunnelData
	err  error
}

type fakeTunnelStream struct {
	ctx      context.Context
	received chan recvResult
}

func (stream *fakeTunnelStream) Header() (metadata.MD, error) {
	return nil, nil
}

func (stream *fakeTunnelStream) Trailer() metadata.MD {
	return nil
}

func (stream *fakeTunnelStream) CloseSend() error {
	return nil
}

func (stream *fakeTunnelStream) Context() context.Context {
	return stream.ctx
}

func (stream *fakeTunnelStream) Send(*sliverpb.TunnelData) error {
	return nil
}

func (stream *fakeTunnelStream) Recv() (*sliverpb.TunnelData, error) {
	select {
	case result := <-stream.received:
		return result.data, result.err
	case <-stream.ctx.Done():
		return nil, stream.ctx.Err()
	}
}

func (stream *fakeTunnelStream) SendMsg(interface{}) error {
	return nil
}

func (stream *fakeTunnelStream) RecvMsg(interface{}) error {
	return nil
}

func newTestTunnelRegistry() *tunnels {
	return &tunnels{
		tunnels:     &map[uint64]*TunnelIO{},
		mutex:       &sync.RWMutex{},
		streamMutex: &sync.Mutex{},
	}
}

func TestTunnelLoopReconnectsAndResetsOldGeneration(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	first := &fakeTunnelStream{
		received: make(chan recvResult, 1),
	}
	first.received <- recvResult{err: status.Error(codes.Unavailable, "reset")}

	second := &fakeTunnelStream{
		received: make(chan recvResult),
	}
	secondStarted := make(chan struct{})
	var secondOnce sync.Once

	registry := newTestTunnelRegistry()
	oldTunnel := NewTunnelIO(42, "session")
	if err := oldTunnel.Open(); err != nil {
		t.Fatal(err)
	}
	(*registry.tunnels)[42] = oldTunnel

	var opens int
	openStream := func(streamCtx context.Context) (rpcpb.SliverRPC_TunnelDataClient, error) {
		opens++
		switch opens {
		case 1:
			first.ctx = streamCtx
			return first, nil
		case 2:
			second.ctx = streamCtx
			secondOnce.Do(func() { close(secondStarted) })
			return second, nil
		default:
			return nil, errors.New("unexpected extra stream")
		}
	}

	done := make(chan error, 1)
	go func() {
		done <- tunnelLoop(ctx, openStream, registry, time.Millisecond, 2*time.Millisecond)
	}()

	select {
	case <-secondStarted:
	case <-time.After(time.Second):
		t.Fatal("tunnel loop did not open a replacement stream")
	}

	deadline := time.Now().Add(time.Second)
	for {
		registry.streamMutex.Lock()
		activeStream := registry.stream
		registry.streamMutex.Unlock()
		if activeStream == second {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("replacement stream was not made active")
		}
		time.Sleep(time.Millisecond)
	}
	registry.mutex.RLock()
	registered := len(*registry.tunnels)
	registry.mutex.RUnlock()
	if registered != 0 {
		t.Fatal("tunnels from the failed stream remained registered")
	}

	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("unexpected tunnel loop error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("tunnel loop did not stop after cancellation")
	}
}
