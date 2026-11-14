package core

/*
	Sliver Implant Framework
	Copyright (C) 2022  Bishop Fox

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
	"errors"
	"io"
	"log"
	"time"

	"github.com/bishopfox/sliver/protobuf/rpcpb"
)

const (
	initialTunnelReconnectDelay = time.Second
	maxTunnelReconnectDelay     = 30 * time.Second
)

type tunnelDataStreamFactory func(context.Context) (rpcpb.SliverRPC_TunnelDataClient, error)

// TunnelLoop - Parses incoming tunnel messages and distributes them
//
//	to session/tunnel objects
//	Expected to be called only once during initialization
func TunnelLoop(rpc rpcpb.SliverRPCClient) error {
	return tunnelLoop(
		context.Background(),
		func(ctx context.Context) (rpcpb.SliverRPC_TunnelDataClient, error) {
			return rpc.TunnelData(ctx)
		},
		GetTunnels(),
		initialTunnelReconnectDelay,
		maxTunnelReconnectDelay,
	)
}

func tunnelLoop(
	ctx context.Context,
	openStream tunnelDataStreamFactory,
	tunnels *tunnels,
	initialDelay time.Duration,
	maxDelay time.Duration,
) error {
	log.Println("Starting tunnel data loop ...")
	delay := initialDelay

	for {
		err := tunnelStreamLoop(ctx, openStream, tunnels, func() {
			delay = initialDelay
		})
		if ctx.Err() != nil {
			return ctx.Err()
		}
		log.Printf("Tunnel data stream ended: %s; reconnecting in %s", err, delay)

		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}

		delay *= 2
		if delay > maxDelay {
			delay = maxDelay
		}
	}
}

func tunnelStreamLoop(
	ctx context.Context,
	openStream tunnelDataStreamFactory,
	tunnels *tunnels,
	onConnected func(),
) error {
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream, err := openStream(streamCtx)
	if err != nil {
		return err
	}
	if onConnected != nil {
		onConnected()
	}
	defer stream.CloseSend()
	defer tunnels.ClearStream(stream)
	defer tunnels.Reset()

	tunnels.SetStream(stream)

	for {
		log.Printf("Waiting for TunnelData ...")
		incoming, err := stream.Recv()
		log.Printf("Recv stream msg: %v", incoming)
		log.Printf("Recv stream err: %s", err)

		if err == io.EOF {
			return errors.New("tunnel data stream closed")
		}
		if err != nil {
			return err
		}
		log.Printf("Received TunnelData for tunnel %d", incoming.TunnelID)
		tunnel := tunnels.Get(incoming.TunnelID)

		if tunnel != nil {
			data := incoming.GetData()

			log.Printf("This is data on tunnel %d: %s", tunnel.ID, data)

			if !incoming.Closed {
				log.Printf("Received data on tunnel %d", tunnel.ID)
				err = tunnel.RecvData(data)

				if err != nil {
					log.Printf("Warning! Error sending data to tunnel %d, %v", tunnel.ID, err)
				}
			} else {
				log.Printf("Closing tunnel %d", tunnel.ID)
				tunnels.Close(tunnel.ID)
			}
		} else {
			log.Printf("Received tunnel data for non-existent tunnel id %d", incoming.TunnelID)
		}
	}
}
