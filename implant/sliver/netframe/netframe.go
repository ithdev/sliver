// Package netframe reads the length-prefixed frames used by socket transports.
package netframe

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
	"io"
)

const (
	// MaxLength is the largest frame the implant will allocate from a peer's
	// untrusted 4-byte length prefix.
	MaxLength = 64 * 1024 * 1024
)

var (
	// ErrZeroLength indicates an invalid empty frame.
	ErrZeroLength = errors.New("frame has zero length")
	// ErrTooLarge indicates a frame that exceeds MaxLength.
	ErrTooLarge = errors.New("frame exceeds maximum length")
)

// ValidateLength enforces the frame-size contract for reads and writes.
func ValidateLength(length uint64) error {
	if length == 0 {
		return ErrZeroLength
	}
	if length > MaxLength {
		return ErrTooLarge
	}
	return nil
}

// Read validates the length prefix before allocating the frame body.
func Read(r io.Reader) ([]byte, error) {
	var lengthPrefix [4]byte
	if _, err := io.ReadFull(r, lengthPrefix[:]); err != nil {
		return nil, err
	}

	length := binary.LittleEndian.Uint32(lengthPrefix[:])
	if err := ValidateLength(uint64(length)); err != nil {
		return nil, err
	}

	data := make([]byte, int(length))
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, err
	}
	return data, nil
}
