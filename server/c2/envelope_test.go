package c2

/*
	Sliver Implant Framework
	Copyright (C) 2019  Bishop Fox

	This program is free software: you can redistribute it and/or modify
	it under the terms of the GNU General Public License as published by
	the Free Software Foundation, either version 3 of the License, or
	(at your option) any later version.

	This program is distributed in the hope that it will be useful,
	but WITHOUT ANY WARRANTY; without even the implied warranty of
	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
	GNU General Public License for more details.

	You should have received a copy of the GNU General Public License
	along with this program. If not, see <https://www.gnu.org/licenses/>.
*/

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/bishopfox/sliver/protobuf/sliverpb"
	"google.golang.org/protobuf/proto"
)

func c2Frame(length uint32, body []byte) []byte {
	prefix := make([]byte, 4)
	binary.LittleEndian.PutUint32(prefix, length)
	return append(prefix, body...)
}

func TestReadC2EnvelopeRejectsOversizedFrameBeforeBody(t *testing.T) {
	body := []byte("must remain unread")
	reader := bytes.NewReader(c2Frame(maxC2EnvelopeLength+1, body))

	_, release, err := readC2Envelope(reader, newEnvelopeReadBudget(maxC2EnvelopeBytesInFlight))
	if !errors.Is(err, errC2EnvelopeTooLarge) {
		t.Fatalf("expected errC2EnvelopeTooLarge, got %v", err)
	}
	if release != nil {
		t.Fatal("oversized frame returned a budget release function")
	}
	if reader.Len() != len(body) {
		t.Fatalf("read %d body bytes before rejecting length", len(body)-reader.Len())
	}
}

func TestReadC2EnvelopeRejectsZeroLength(t *testing.T) {
	_, release, err := readC2Envelope(bytes.NewReader(c2Frame(0, nil)), newEnvelopeReadBudget(1))
	if !errors.Is(err, errC2EnvelopeZeroLength) {
		t.Fatalf("expected errC2EnvelopeZeroLength, got %v", err)
	}
	if release != nil {
		t.Fatal("zero-length frame returned a budget release function")
	}
}

func TestReadC2EnvelopeAcceptsValidFrameAndReleasesBudget(t *testing.T) {
	want := &sliverpb.Envelope{ID: 7, Type: sliverpb.MsgPing, Data: []byte("hello")}
	body, err := proto.Marshal(want)
	if err != nil {
		t.Fatalf("marshal test envelope: %v", err)
	}
	budget := newEnvelopeReadBudget(len(body))

	got, release, err := readC2Envelope(bytes.NewReader(c2Frame(uint32(len(body)), body)), budget)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !proto.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	if budget.reserve(1) {
		t.Fatal("budget allowed another reservation before release")
	}
	release()
	if !budget.reserve(1) {
		t.Fatal("budget remained exhausted after release")
	}
}

func TestReadC2EnvelopeRejectsWhenSharedBudgetIsExhausted(t *testing.T) {
	budget := newEnvelopeReadBudget(10)
	if !budget.reserve(10) {
		t.Fatal("failed to reserve test budget")
	}
	body := []byte{0x00}

	_, release, err := readC2Envelope(bytes.NewReader(c2Frame(1, body)), budget)
	if !errors.Is(err, errC2EnvelopeBudgetExhausted) {
		t.Fatalf("expected errC2EnvelopeBudgetExhausted, got %v", err)
	}
	if release != nil {
		t.Fatal("rejected frame returned a budget release function")
	}
}

func TestValidateC2EnvelopeLengthUsesSameBoundForWrites(t *testing.T) {
	if err := validateC2EnvelopeLength(maxC2EnvelopeLength); err != nil {
		t.Fatalf("maximum envelope length was rejected: %v", err)
	}
	if err := validateC2EnvelopeLength(maxC2EnvelopeLength + 1); !errors.Is(err, errC2EnvelopeTooLarge) {
		t.Fatalf("expected errC2EnvelopeTooLarge, got %v", err)
	}
}
