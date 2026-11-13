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
	"context"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bishopfox/sliver/protobuf/clientpb"
	"github.com/bishopfox/sliver/protobuf/commonpb"
	"github.com/bishopfox/sliver/protobuf/rpcpb"
	"github.com/bishopfox/sliver/protobuf/sliverpb"
	"github.com/bishopfox/sliver/util/leaky"
	"google.golang.org/grpc"
)

var (
	// SocksProxies - Struct instance that holds all the portfwds
	SocksProxies = socksProxy{
		tcpProxies: map[uint64]*SocksProxy{},
		mutex:      &sync.RWMutex{},
	}
	SocksProxyID = (uint64)(0)
)

type SocksProxyStream interface {
	Send(*sliverpb.SocksData) error
	Recv() (*sliverpb.SocksData, error)
	CloseSend() error
}

type SocksRPCClient interface {
	CreateSocks(context.Context, *sliverpb.Socks, ...grpc.CallOption) (*sliverpb.Socks, error)
	SocksProxy(context.Context, ...grpc.CallOption) (SocksProxyStream, error)
}

type sliverSocksRPCClient struct {
	client rpcpb.SliverRPCClient
}

func NewSocksRPCClient(client rpcpb.SliverRPCClient) SocksRPCClient {
	return &sliverSocksRPCClient{client: client}
}

func (rpc *sliverSocksRPCClient) CreateSocks(ctx context.Context, request *sliverpb.Socks, opts ...grpc.CallOption) (*sliverpb.Socks, error) {
	return rpc.client.CreateSocks(ctx, request, opts...)
}

func (rpc *sliverSocksRPCClient) SocksProxy(ctx context.Context, opts ...grpc.CallOption) (SocksProxyStream, error) {
	return rpc.client.SocksProxy(ctx, opts...)
}

// PortfwdMeta - Metadata about a portfwd listener
type SocksProxyMeta struct {
	ID        uint64
	SessionID string
	BindAddr  string
	Username  string
	Password  string
}
type TcpProxy struct {
	Rpc     SocksRPCClient
	Session *clientpb.Session

	Username        string
	Password        string
	BindAddr        string
	Listener        net.Listener
	KeepAlivePeriod time.Duration
	DialTimeout     time.Duration

	connectionsMutex  sync.Mutex
	connectionsClosed bool
	connections       map[uint64]net.Conn
	cancelMutex       sync.Mutex
	cancel            context.CancelFunc
}

func (tcp *TcpProxy) Stop() error {
	tcp.cancelMutex.Lock()
	cancel := tcp.cancel
	tcp.cancel = nil
	tcp.cancelMutex.Unlock()
	if cancel != nil {
		cancel()
	}

	err := tcp.Listener.Close()

	tcp.connectionsMutex.Lock()
	tcp.connectionsClosed = true
	connections := tcp.connections
	tcp.connections = nil
	tcp.connectionsMutex.Unlock()

	for _, conn := range connections {
		conn.Close()
	}

	return err
}

func (tcp *TcpProxy) setCancel(cancel context.CancelFunc) {
	tcp.cancelMutex.Lock()
	defer tcp.cancelMutex.Unlock()
	tcp.cancel = cancel
}

func (tcp *TcpProxy) trackConnection(tunnelID uint64, conn net.Conn) bool {
	tcp.connectionsMutex.Lock()
	defer tcp.connectionsMutex.Unlock()
	if tcp.connectionsClosed {
		conn.Close()
		return false
	}
	if tcp.connections == nil {
		tcp.connections = map[uint64]net.Conn{}
	}
	tcp.connections[tunnelID] = conn
	return true
}

func (tcp *TcpProxy) getConnection(tunnelID uint64) (net.Conn, bool) {
	tcp.connectionsMutex.Lock()
	defer tcp.connectionsMutex.Unlock()
	conn, ok := tcp.connections[tunnelID]
	return conn, ok
}

func (tcp *TcpProxy) closeConnection(tunnelID uint64) {
	tcp.connectionsMutex.Lock()
	conn, ok := tcp.connections[tunnelID]
	if ok {
		delete(tcp.connections, tunnelID)
	}
	tcp.connectionsMutex.Unlock()
	if ok {
		conn.Close()
	}
}

// SocksProxy - Tracks portfwd<->tcpproxy
type SocksProxy struct {
	ID           uint64
	ChannelProxy *TcpProxy
}

// GetMetadata - Get metadata about the portfwd
func (p *SocksProxy) GetMetadata() *SocksProxyMeta {
	return &SocksProxyMeta{
		ID:        p.ID,
		SessionID: p.ChannelProxy.Session.ID,
		BindAddr:  p.ChannelProxy.BindAddr,
		Username:  p.ChannelProxy.Username,
		Password:  p.ChannelProxy.Password,
	}
}

type socksProxy struct {
	tcpProxies map[uint64]*SocksProxy
	mutex      *sync.RWMutex
}

// Add - Add a TCP proxy instance
func (f *socksProxy) Add(tcpProxy *TcpProxy) *SocksProxy {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	Sockser := &SocksProxy{
		ID:           nextSocksProxyID(),
		ChannelProxy: tcpProxy,
	}
	f.tcpProxies[Sockser.ID] = Sockser

	return Sockser
}

func (f *socksProxy) Start(socks *SocksProxy) error {
	tcpProxy := socks.ChannelProxy
	ctx, cancel := context.WithCancel(context.Background())
	tcpProxy.setCancel(cancel)
	defer func() {
		cancel()
		tcpProxy.Stop()
		f.unregister(socks.ID)
		log.Printf("Socks Stop -> %s\n", tcpProxy.BindAddr)
	}()

	proxy, err := tcpProxy.Rpc.SocksProxy(ctx)
	if err != nil {
		return err
	}

	receiverDone := make(chan struct{})
	go func() {
		defer close(receiverDone)
		fromImplantSequence := 0
		for {
			socksData, err := proxy.Recv()
			if err != nil {
				log.Printf("Failed to Recv from proxy, %s\n", err)
				tcpProxy.Stop()
				return
			}

			if conn, ok := tcpProxy.getConnection(socksData.TunnelID); ok {
				if socksData.CloseConn {
					tcpProxy.closeConnection(socksData.TunnelID)
					continue
				}
				log.Printf("[socks] agent to Server To (Client to User) Data Sequence %d , Data Size %d \n", fromImplantSequence, len(socksData.Data))
				//fmt.Printf("recv data len %d \n", len(p.Data))
				_, err := conn.Write(socksData.Data)
				if err != nil {
					log.Printf("Failed to write data to proxy connection, %s\n", err)
					tcpProxy.closeConnection(socksData.TunnelID)
					continue
				}
				fromImplantSequence++
			}
		}
	}()

	var connectionHandlers sync.WaitGroup
	for {
		connection, err := tcpProxy.Listener.Accept()
		if err != nil {
			log.Printf("Failed to accept new listener, probably already closed: %s\n", err)
			break
		}
		rpcSocks, err := tcpProxy.Rpc.CreateSocks(ctx, &sliverpb.Socks{
			SessionID: tcpProxy.Session.ID,
		})
		if err != nil {
			log.Printf("Failed rcp call to create socks %s\n", err)
			connection.Close()
			break
		}

		frame := &sliverpb.SocksData{
			Username: tcpProxy.Username,
			Password: tcpProxy.Password,
			TunnelID: rpcSocks.TunnelID,
			Request:  &commonpb.Request{SessionID: rpcSocks.SessionID},
		}
		if !tcpProxy.trackConnection(frame.TunnelID, connection) {
			break
		}
		connectionHandlers.Add(1)
		go func(conn net.Conn, frame *sliverpb.SocksData) {
			defer connectionHandlers.Done()
			connect(conn, proxy, tcpProxy, frame)
		}(connection, frame)
	}
	cancel()
	tcpProxy.Stop()
	<-receiverDone
	connectionHandlers.Wait()
	proxy.CloseSend()
	return nil
}

// Remove - Remove a TCP proxy instance
func (f *socksProxy) Remove(socksId uint64) bool {
	f.mutex.Lock()
	socks, ok := f.tcpProxies[socksId]
	if ok {
		delete(f.tcpProxies, socksId)
	}
	f.mutex.Unlock()

	if ok {
		socks.ChannelProxy.Stop()
	}
	return ok
}

func (f *socksProxy) unregister(socksID uint64) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	delete(f.tcpProxies, socksID)
}

// List - List all TCP proxy instances
func (f *socksProxy) List() []*SocksProxyMeta {
	f.mutex.RLock()
	defer f.mutex.RUnlock()
	socksProxy := []*SocksProxyMeta{}
	for _, socks := range f.tcpProxies {
		socksProxy = append(socksProxy, socks.GetMetadata())
	}
	return socksProxy
}

func nextSocksProxyID() uint64 {
	return atomic.AddUint64(&SocksProxyID, 1)
}

const leakyBufSize = 4108 // data.len(2) + hmacsha1(10) + data(4096)

var leakyBuf = leaky.NewLeakyBuf(2048, leakyBufSize)

func connect(conn net.Conn, stream SocksProxyStream, proxy *TcpProxy, frame *sliverpb.SocksData) {

	defer func() {
		// It's neccessary to close and remove connection once we done with it
		proxy.closeConnection(frame.TunnelID)

		log.Printf("[socks] connection closed")
	}()

	log.Printf("tcp conn %q<--><-->%q \n", conn.LocalAddr(), conn.RemoteAddr())

	buff := leakyBuf.Get()
	defer leakyBuf.Put(buff)
	var ToImplantSequence uint64 = 0
	for {
		n, err := conn.Read(buff)

		if err != nil {
			log.Printf("[socks] (User to Client) failed to read data, %s ", err)
			// Error basically means that the connection is closed(EOF) OR deadline exceeded
			// In any of that cases, it's better to just giveup
			return
		}
		if n > 0 {
			frame.Data = buff[:n]
			frame.Sequence = ToImplantSequence
			log.Printf("[socks] (User to Client) to Server to agent  Data Sequence %d , Data Size %d \n", ToImplantSequence, len(frame.Data))
			err := stream.Send(frame)
			if err != nil {
				log.Printf("[socks] (User to Client) failed to send data, %s ", err)
				return
			}
			ToImplantSequence++

		}

	}
}
