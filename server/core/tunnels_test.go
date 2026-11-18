package core

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/bishopfox/sliver/protobuf/sliverpb"
)

func TestTunnelsCloseDoesNotBlockRegistry(t *testing.T) {
	const tunnelID uint64 = 42
	registry := &tunnels{
		tunnels: map[uint64]*Tunnel{
			tunnelID: NewTunnel(tunnelID, "session"),
		},
		mutex: &sync.Mutex{},
	}

	closed := make(chan error, 1)
	go func() {
		closed <- registry.Close(tunnelID)
	}()

	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close blocked with no ToImplant consumer")
	}

	locked := make(chan struct{})
	go func() {
		registry.mutex.Lock()
		registry.mutex.Unlock()
		close(locked)
	}()

	select {
	case <-locked:
	case <-time.After(time.Second):
		t.Fatal("Close retained the registry mutex")
	}
	if _, ok := registry.tunnels[tunnelID]; ok {
		t.Fatal("closed tunnel remained registered")
	}
}

func TestStalledTunnelDoesNotBlockAnotherTunnel(t *testing.T) {
	const (
		stalledID uint64 = 100
		healthyID uint64 = 200
	)
	registry := &tunnels{
		tunnels: map[uint64]*Tunnel{
			stalledID: NewTunnel(stalledID, "session"),
			healthyID: NewTunnel(healthyID, "session"),
		},
		mutex: &sync.Mutex{},
	}
	stalledTunnel := registry.tunnels[stalledID]
	healthyTunnel := registry.tunnels[healthyID]

	stalledResult := make(chan error, 1)
	go func() {
		stalledResult <- stalledTunnel.SendDataFromImplant(&sliverpb.TunnelData{
			TunnelID: stalledID,
			Data:     []byte("stalled"),
		})
	}()

	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	healthyPacket := make(chan *sliverpb.TunnelData, 1)
	go func() {
		select {
		case packet := <-healthyTunnel.FromImplant:
			healthyPacket <- packet
		case <-deadline.C:
		}
	}()

	healthyResult := make(chan error, 1)
	go func() {
		healthyResult <- healthyTunnel.SendDataFromImplant(&sliverpb.TunnelData{
			TunnelID: healthyID,
			Data:     []byte("healthy"),
		})
	}()

	select {
	case err := <-healthyResult:
		if err != nil {
			t.Fatal(err)
		}
	case <-deadline.C:
		t.Fatal("healthy tunnel was blocked by the stalled tunnel")
	}
	select {
	case packet := <-healthyPacket:
		if packet.TunnelID != healthyID {
			t.Fatalf("received packet for tunnel %d", packet.TunnelID)
		}
	case <-deadline.C:
		t.Fatal("healthy tunnel data was not delivered")
	}

	if err := registry.Close(stalledID); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-stalledResult:
		if !errors.Is(err, ErrTunnelClosed) {
			t.Fatalf("stalled send returned %v, want %v", err, ErrTunnelClosed)
		}
	case <-deadline.C:
		t.Fatal("closing the tunnel did not release the stalled send")
	}

	if err := stalledTunnel.SendDataFromImplant(&sliverpb.TunnelData{
		TunnelID: stalledID,
		Data:     []byte("after-close"),
	}); !errors.Is(err, ErrTunnelClosed) {
		t.Fatalf("send after close returned %v, want %v", err, ErrTunnelClosed)
	}
}
