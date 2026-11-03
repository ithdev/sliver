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
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
)

func prefix(length uint32) []byte {
	data := make([]byte, 4)
	binary.LittleEndian.PutUint32(data, length)
	return data
}

func TestReadRejectsOversizedFrameBeforeBody(t *testing.T) {
	body := []byte("must remain unread")
	reader := bytes.NewReader(append(prefix(MaxLength+1), body...))

	_, err := Read(reader)
	if !errors.Is(err, ErrTooLarge) {
		t.Fatalf("expected ErrTooLarge, got %v", err)
	}
	if reader.Len() != len(body) {
		t.Fatalf("read %d body bytes before rejecting length", len(body)-reader.Len())
	}
}

func TestReadRejectsZeroLength(t *testing.T) {
	_, err := Read(bytes.NewReader(prefix(0)))
	if !errors.Is(err, ErrZeroLength) {
		t.Fatalf("expected ErrZeroLength, got %v", err)
	}
}

func TestReadAcceptsValidFrame(t *testing.T) {
	body := []byte("hello frame")
	frame := append(prefix(uint32(len(body))), body...)

	got, err := Read(bytes.NewReader(frame))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("got %q, want %q", got, body)
	}
}

func TestValidateLengthUsesSameBoundForWrites(t *testing.T) {
	if err := ValidateLength(MaxLength); err != nil {
		t.Fatalf("maximum frame length was rejected: %v", err)
	}
	if err := ValidateLength(MaxLength + 1); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("expected ErrTooLarge, got %v", err)
	}
}
