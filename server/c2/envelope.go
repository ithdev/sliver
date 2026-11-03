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
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/bishopfox/sliver/protobuf/sliverpb"
	"google.golang.org/protobuf/proto"
)

const (
	maxC2EnvelopeLength        = 64 * 1024 * 1024
	maxC2EnvelopeBytesInFlight = 128 * 1024 * 1024
)

var (
	errC2EnvelopeZeroLength      = errors.New("c2 envelope has zero length")
	errC2EnvelopeTooLarge        = errors.New("c2 envelope exceeds maximum length")
	errC2EnvelopeBudgetExhausted = errors.New("c2 envelope memory budget exhausted")

	c2EnvelopeBudget = newEnvelopeReadBudget(maxC2EnvelopeBytesInFlight)
)

type envelopeReadBudget struct {
	mu    sync.Mutex
	limit int
	used  int
}

func newEnvelopeReadBudget(limit int) *envelopeReadBudget {
	return &envelopeReadBudget{limit: limit}
}

func (b *envelopeReadBudget) reserve(size int) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if size > b.limit-b.used {
		return false
	}
	b.used += size
	return true
}

func (b *envelopeReadBudget) release(size int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if size > b.used {
		panic("c2 envelope memory budget released more bytes than reserved")
	}
	b.used -= size
}

func validateC2EnvelopeLength(length uint64) error {
	if length == 0 {
		return errC2EnvelopeZeroLength
	}
	if length > maxC2EnvelopeLength {
		return errC2EnvelopeTooLarge
	}
	return nil
}

// readC2Envelope validates the untrusted length prefix before allocating. The
// caller owns the returned release function and must call it after dispatching
// the envelope so concurrent connections cannot exceed the shared budget.
func readC2Envelope(r io.Reader, budget *envelopeReadBudget) (*sliverpb.Envelope, func(), error) {
	var lengthPrefix [4]byte
	if _, err := io.ReadFull(r, lengthPrefix[:]); err != nil {
		return nil, nil, fmt.Errorf("read c2 envelope length: %w", err)
	}

	length := binary.LittleEndian.Uint32(lengthPrefix[:])
	if err := validateC2EnvelopeLength(uint64(length)); err != nil {
		return nil, nil, err
	}

	size := int(length)
	if !budget.reserve(size) {
		return nil, nil, errC2EnvelopeBudgetExhausted
	}
	released := false
	release := func() {
		if released {
			panic("c2 envelope memory budget reservation released twice")
		}
		released = true
		budget.release(size)
	}

	data := make([]byte, size)
	if _, err := io.ReadFull(r, data); err != nil {
		release()
		return nil, nil, fmt.Errorf("read c2 envelope data: %w", err)
	}

	envelope := &sliverpb.Envelope{}
	if err := proto.Unmarshal(data, envelope); err != nil {
		release()
		return nil, nil, fmt.Errorf("decode c2 envelope: %w", err)
	}
	return envelope, release, nil
}
