package core

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/bishopfox/sliver/protobuf/clientpb"
	"github.com/bishopfox/sliver/protobuf/sliverpb"
	"google.golang.org/grpc"
)

type fakeSocksRPC struct {
	stream    SocksProxyStream
	streamErr error
}

func (rpc *fakeSocksRPC) CreateSocks(context.Context, *sliverpb.Socks, ...grpc.CallOption) (*sliverpb.Socks, error) {
	return &sliverpb.Socks{SessionID: "session"}, nil
}

func (rpc *fakeSocksRPC) SocksProxy(context.Context, ...grpc.CallOption) (SocksProxyStream, error) {
	return rpc.stream, rpc.streamErr
}

type blockingSocksStream struct {
	started chan struct{}
	release chan struct{}
	once    bool
}

func newBlockingSocksStream() *blockingSocksStream {
	return &blockingSocksStream{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (stream *blockingSocksStream) Send(*sliverpb.SocksData) error {
	return nil
}

func (stream *blockingSocksStream) Recv() (*sliverpb.SocksData, error) {
	if !stream.once {
		stream.once = true
		close(stream.started)
	}
	<-stream.release
	return nil, errors.New("stream reset")
}

func (stream *blockingSocksStream) CloseSend() error {
	return nil
}

func newTestSocksProxy(t *testing.T, rpc SocksRPCClient, sessionID string) (*SocksProxy, *TcpProxy) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	tcpProxy := &TcpProxy{
		Rpc:      rpc,
		Session:  &clientpb.Session{ID: sessionID},
		BindAddr: listener.Addr().String(),
		Listener: listener,
	}
	return &SocksProxy{ID: nextSocksProxyID(), ChannelProxy: tcpProxy}, tcpProxy
}

func newTestRegistry() *socksProxy {
	return &socksProxy{
		tcpProxies: map[uint64]*SocksProxy{},
		mutex:      new(sync.RWMutex),
	}
}

func TestSocksProxyStartCleansUpOnReceiverFailure(t *testing.T) {
	registry := newTestRegistry()
	stream := newBlockingSocksStream()
	failing, failingTCP := newTestSocksProxy(t, &fakeSocksRPC{stream: stream}, "failing")
	registry.tcpProxies[failing.ID] = failing

	peer, peerTCP := newTestSocksProxy(t, &fakeSocksRPC{}, "peer")
	registry.tcpProxies[peer.ID] = peer
	defer registry.Remove(peer.ID)

	failingClient, failingServer := net.Pipe()
	defer failingClient.Close()
	if !failingTCP.trackConnection(uint64(1), failingServer) {
		t.Fatal("failed to track failing proxy connection")
	}

	peerClient, peerServer := net.Pipe()
	defer peerClient.Close()
	defer peerServer.Close()
	if !peerTCP.trackConnection(uint64(2), peerServer) {
		t.Fatal("failed to track peer proxy connection")
	}

	done := make(chan error, 1)
	go func() {
		done <- registry.Start(failing)
	}()

	select {
	case <-stream.started:
	case <-time.After(time.Second):
		t.Fatal("proxy receiver did not start")
	}
	close(stream.release)

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("proxy did not stop after stream failure")
	}

	if _, ok := registry.tcpProxies[failing.ID]; ok {
		t.Fatal("failed proxy remained registered")
	}
	if _, ok := registry.tcpProxies[peer.ID]; !ok {
		t.Fatal("unrelated proxy was unregistered")
	}
	assertConnectionClosed(t, failingClient)

	peerClient.SetDeadline(time.Now().Add(time.Second))
	peerServer.SetDeadline(time.Now().Add(time.Second))
	buf := make([]byte, 2)
	readDone := make(chan error, 1)
	go func() {
		_, err := peerServer.Read(buf)
		readDone <- err
	}()
	if _, err := peerClient.Write([]byte("ok")); err != nil {
		t.Fatalf("unrelated proxy connection was closed: %v", err)
	}
	if err := <-readDone; err != nil {
		t.Fatalf("unrelated proxy connection is unusable: %v", err)
	}
}

func TestSocksProxyStartCleansUpOnStreamCreationFailure(t *testing.T) {
	registry := newTestRegistry()
	socks, tcpProxy := newTestSocksProxy(t, &fakeSocksRPC{streamErr: errors.New("stream failed")}, "session")
	registry.tcpProxies[socks.ID] = socks

	err := registry.Start(socks)
	if err == nil {
		t.Fatal("expected stream creation error")
	}
	if _, ok := registry.tcpProxies[socks.ID]; ok {
		t.Fatal("proxy remained registered after stream creation failure")
	}
	conn, err := net.DialTimeout("tcp", tcpProxy.BindAddr, 100*time.Millisecond)
	if err == nil {
		conn.Close()
		t.Fatal("listener remained open after stream creation failure")
	}
}

func assertConnectionClosed(t *testing.T, conn net.Conn) {
	t.Helper()
	conn.SetReadDeadline(time.Now().Add(time.Second))
	buf := make([]byte, 1)
	_, err := conn.Read(buf)
	if err == nil {
		t.Fatal("connection remained open")
	}
}
