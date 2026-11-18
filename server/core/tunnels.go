package core

/*
	Sliver Implant Framework
	Copyright (C) 2019  Bishop Fox

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
	"crypto/rand"
	"encoding/binary"
	"errors"
	"sync"
	"time"

	"github.com/bishopfox/sliver/protobuf/rpcpb"
	"github.com/bishopfox/sliver/protobuf/sliverpb"
	"google.golang.org/protobuf/proto"
)

var (
	// Tunnels - Interacting with duplex tunnels
	Tunnels = tunnels{
		tunnels: map[uint64]*Tunnel{},
		mutex:   &sync.Mutex{},
	}

	// ErrInvalidTunnelID - Invalid tunnel ID value
	ErrInvalidTunnelID = errors.New("invalid tunnel ID")

	// ErrTunnelClosed - The tunnel lifetime ended before the operation completed
	ErrTunnelClosed = errors.New("tunnel closed")
)

const (
	// delayBeforeClose - delay before closing the tunnel.
	// I assume 10 seconds may be an overkill for a good connection, but it looks good enough for less stable one.
	delayBeforeClose = 10 * time.Second
)

// Tunnel  - Essentially just a mapping between a specific client and sliver
// with an identifier, these tunnels are full duplex. The server doesn't really
// care what data gets passed back and forth it just facilitates the connection
type Tunnel struct {
	ID        uint64
	SessionID string

	ToImplant         chan []byte
	ToImplantSequence uint64

	FromImplant         chan *sliverpb.TunnelData
	FromImplantSequence uint64

	Client rpcpb.SliverRPC_TunnelDataServer

	mutex               *sync.RWMutex
	lastDataMessageTime time.Time
	done                chan struct{}
	doneOnce            sync.Once
	closed              bool
}

func NewTunnel(id uint64, sessionID string) *Tunnel {
	return &Tunnel{
		ID:          id,
		SessionID:   sessionID,
		ToImplant:   make(chan []byte),
		FromImplant: make(chan *sliverpb.TunnelData),

		mutex:               &sync.RWMutex{},
		lastDataMessageTime: time.Now(), // need to be initialized
		done:                make(chan struct{}),
	}
}

// Done is closed once the tunnel lifetime ends. Data channels remain open so
// an in-flight sender can select on Done instead of racing with a close.
func (t *Tunnel) Done() <-chan struct{} {
	return t.done
}

func (t *Tunnel) close() {
	t.doneOnce.Do(func() {
		t.mutex.Lock()
		t.closed = true
		close(t.done)
		t.mutex.Unlock()
	})
}

// BindClient associates a client stream with a tunnel. The first caller binds,
// and later callers are told whether they own the existing binding.
func (t *Tunnel) BindClient(stream rpcpb.SliverRPC_TunnelDataServer) (newBinding bool, owns bool) {
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

// ClientStream returns the currently bound client stream, if any.
func (t *Tunnel) ClientStream() rpcpb.SliverRPC_TunnelDataServer {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	return t.Client
}

func (t *Tunnel) setLastMessageTime() {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	t.lastDataMessageTime = time.Now()
}

func (t *Tunnel) GetLastMessageTime() time.Time {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	return t.lastDataMessageTime
}

func (t *Tunnel) SendDataFromImplant(tunnelData *sliverpb.TunnelData) error {
	// Setting the date right before and right after message, since channel can be blocked for some amount of time
	t.setLastMessageTime()
	defer t.setLastMessageTime()

	select {
	case t.FromImplant <- tunnelData:
		return nil
	case <-t.done:
		return ErrTunnelClosed
	}
}

// SendToImplant delivers client data unless the tunnel has already ended.
func (t *Tunnel) SendToImplant(data []byte) error {
	select {
	case t.ToImplant <- data:
		return nil
	case <-t.done:
		return ErrTunnelClosed
	}
}

type tunnels struct {
	tunnels map[uint64]*Tunnel
	mutex   *sync.Mutex
}

func (t *tunnels) Create(sessionID string) *Tunnel {
	tunnelID := NewTunnelID()
	session := Sessions.Get(sessionID)

	tunnel := NewTunnel(
		tunnelID,
		session.ID,
	)

	t.mutex.Lock()
	defer t.mutex.Unlock()
	t.tunnels[tunnel.ID] = tunnel

	return tunnel
}

// ScheduleClose - schedules a close for tunnel, must be called as routine.
// will close it once there is no data for at least delayBeforeClose delay since last message
// This is _necessary_ since we processing messages asynchronously
// and if tunnelCloseHandler routine will fire before tunnelDataHandler routine we will lose some data
// (this is what happens for socks and portfwd)
// There is no another way around it, if we want to stick to async processing as we do now.
// All additional changes requires changes on implants(like sequencing for close messages),
// and as there is a goal to keep compatibility we don't do that at the moment.
// So there is trade off - more stability or more speed. Or rewriting implant logic.
// At the moment, i see it affects only `shell` command and locking it for 10 seconds on exit. Not a big deal.
func (t *tunnels) ScheduleClose(tunnelID uint64) {
	tunnel := t.Get(tunnelID)
	if tunnel == nil {
		return
	}

	timeDelta := time.Since(tunnel.GetLastMessageTime())

	coreLog.Printf("Scheduled close for channel %d (delta: %v)", tunnelID, timeDelta)

	if timeDelta >= delayBeforeClose {
		coreLog.Printf("Closing channel %d", tunnelID)
		t.Close(tunnelID)
	} else {
		// Reschedule
		coreLog.Printf("Rescheduling closing channel %d", tunnelID)
		time.Sleep(delayBeforeClose - timeDelta + time.Second)
		go t.ScheduleClose(tunnelID)
	}
}

// Close - closing tunnel
// It's preferred to use ScheduleClose function if you don't 100% sure there is no more data to receive
func (t *tunnels) Close(tunnelID uint64) error {
	return t.closeTunnel(tunnelID, nil)
}

// CloseTunnel closes tunnel only if it is still the registered instance for
// its ID. A worker from an older tunnel must not remove a replacement.
func (t *tunnels) CloseTunnel(tunnel *Tunnel) error {
	return t.closeTunnel(tunnel.ID, tunnel)
}

func (t *tunnels) closeTunnel(tunnelID uint64, expected *Tunnel) error {
	t.mutex.Lock()
	tunnel := t.tunnels[tunnelID]
	if tunnel == nil || expected != nil && tunnel != expected {
		t.mutex.Unlock()
		return ErrInvalidTunnelID
	}
	delete(t.tunnels, tunnelID)
	t.mutex.Unlock()

	tunnelClose, err := proto.Marshal(&sliverpb.TunnelData{
		TunnelID:  tunnel.ID,
		SessionID: tunnel.SessionID,
		Closed:    true,
	})
	if err != nil {
		return err
	}
	data, err := proto.Marshal(&sliverpb.Envelope{
		Type: sliverpb.MsgTunnelClose,
		Data: tunnelClose,
	})
	if err != nil {
		return err
	}

	// A dead client stream may leave no consumer on ToImplant. Do not block
	// tunnel creation and lookup behind the registry mutex in that case.
	select {
	case tunnel.ToImplant <- data:
	default:
	}

	tunnel.close()
	return nil
}

// CloseForClient retires every tunnel bound to a specific client stream.
func (t *tunnels) CloseForClient(stream rpcpb.SliverRPC_TunnelDataServer) []uint64 {
	t.mutex.Lock()
	tunnels := make([]*Tunnel, 0)
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

// Get - Get a tunnel
func (t *tunnels) Get(tunnelID uint64) *Tunnel {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	return t.tunnels[tunnelID]
}

// NewTunnelID - New 64-bit identifier
func NewTunnelID() uint64 {
	randBuf := make([]byte, 8)
	rand.Read(randBuf)
	return binary.LittleEndian.Uint64(randBuf)
}
