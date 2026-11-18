package core

/*
	Sliver Implant Framework
	Copyright (C) 2021  Bishop Fox

	This program is free software: you can redistribute it and/or modify
	it under the terms of the GNU General Public License as published by
	the Free Software Foundation, either version 3 of the License, or
	(at your option) any later version.

	This program is distributed in the hope that it will be useful,
	but WITHOUT ANY WARRANTY; without even the implied warranty of
	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
	GNU General Public License for more details.

	You should have received a copy of the GNU General Public License
	along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/

import (
	"sync"

	"github.com/bishopfox/sliver/protobuf/rpcpb"
	"github.com/bishopfox/sliver/protobuf/sliverpb"
)

var (
	// TunSocksTunnels - Interacting with duplex SocksTunnels
	SocksTunnels = tcpTunnel{
		tunnels: map[uint64]*TcpTunnel{},
		mutex:   &sync.Mutex{},
	}
)

type TcpTunnel struct {
	ID                uint64
	SessionID         string
	ToImplantSequence uint64

	FromImplant         chan *sliverpb.SocksData
	FromImplantSequence uint64
	Client              rpcpb.SliverRPC_SocksProxyServer

	mutex    sync.RWMutex
	done     chan struct{}
	doneOnce sync.Once
	closed   bool
}

type tcpTunnel struct {
	tunnels map[uint64]*TcpTunnel
	mutex   *sync.Mutex
}

func (t *tcpTunnel) Create(sessionID string) *TcpTunnel {
	tunnelID := NewTunnelID()
	session := Sessions.Get(sessionID)
	tunnel := &TcpTunnel{
		ID:        tunnelID,
		SessionID: session.ID,
		//ToImplant:   make(chan []byte),
		FromImplant: make(chan *sliverpb.SocksData),
		done:        make(chan struct{}),
	}
	t.mutex.Lock()
	defer t.mutex.Unlock()
	t.tunnels[tunnel.ID] = tunnel

	return tunnel
}

func (t *tcpTunnel) Close(tunnelID uint64) error {
	return t.closeTunnel(tunnelID, nil)
}

// CloseTunnel closes tunnel only if it is still the registered instance for
// its ID. A worker from an older tunnel must not remove a replacement.
func (t *tcpTunnel) CloseTunnel(tunnel *TcpTunnel) error {
	return t.closeTunnel(tunnel.ID, tunnel)
}

func (t *tcpTunnel) closeTunnel(tunnelID uint64, expected *TcpTunnel) error {
	t.mutex.Lock()
	tunnel := t.tunnels[tunnelID]
	if tunnel == nil || expected != nil && tunnel != expected {
		t.mutex.Unlock()
		return ErrInvalidTunnelID
	}
	delete(t.tunnels, tunnelID)
	t.mutex.Unlock()
	//close(tunnel.ToImplant)
	tunnel.close()
	return nil
}

func (t *TcpTunnel) Done() <-chan struct{} {
	return t.done
}

func (t *TcpTunnel) close() {
	t.doneOnce.Do(func() {
		t.mutex.Lock()
		t.closed = true
		close(t.done)
		t.mutex.Unlock()
	})
}

func (t *TcpTunnel) BindClient(stream rpcpb.SliverRPC_SocksProxyServer) (newBinding bool, owns bool) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	if t.closed {
		return false, false
	}
	if t.Client == nil {
		t.Client = stream
		return true, true
	}
	return false, t.Client == stream
}

func (t *TcpTunnel) ClientStream() rpcpb.SliverRPC_SocksProxyServer {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	return t.Client
}

func (t *TcpTunnel) SendDataFromImplant(tunnelData *sliverpb.SocksData) error {
	select {
	case t.FromImplant <- tunnelData:
		return nil
	case <-t.done:
		return ErrTunnelClosed
	}
}

func (t *tcpTunnel) CloseForClient(stream rpcpb.SliverRPC_SocksProxyServer) []uint64 {
	t.mutex.Lock()
	tunnels := make([]*TcpTunnel, 0)
	for _, tunnel := range t.tunnels {
		if tunnel.ClientStream() == stream {
			tunnels = append(tunnels, tunnel)
		}
	}
	t.mutex.Unlock()

	tunnelIDs := make([]uint64, 0, len(tunnels))
	for _, tunnel := range tunnels {
		if t.CloseTunnel(tunnel) == nil {
			tunnelIDs = append(tunnelIDs, tunnel.ID)
		}
	}
	return tunnelIDs
}

func (t *tcpTunnel) Get(tunnelID uint64) *TcpTunnel {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	return t.tunnels[tunnelID]
}
