package core

import (
	"sync"
	"testing"
	"time"
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
