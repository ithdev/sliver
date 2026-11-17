package transport

import (
	"encoding/json"
	"testing"

	"github.com/bishopfox/sliver/protobuf/clientpb"
	"github.com/bishopfox/sliver/protobuf/commonpb"
	"github.com/bishopfox/sliver/protobuf/sliverpb"
	"github.com/bishopfox/sliver/server/core"
)

func TestGetActiveTargetIgnoresUnexpectedTypes(t *testing.T) {
	tests := []struct {
		name    string
		request interface{}
	}{
		{"beacon task", &clientpb.BeaconTask{Request: []byte("task request")}},
		{"null request", json.RawMessage(`{"Request":null}`)},
		{"string request", json.RawMessage(`{"Request":"text"}`)},
		{"non-string beacon ID", json.RawMessage(`{"Request":{"BeaconID":123}}`)},
		{"non-string session ID", json.RawMessage(`{"Request":{"SessionID":123}}`)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw, err := json.Marshal(tt.request)
			if err != nil {
				t.Fatal(err)
			}
			session, beacon, err := getActiveTarget(raw)
			if err != nil || session != nil || beacon != nil {
				t.Fatalf("getActiveTarget(%s) = (%v, %v, %v), want (nil, nil, nil)", raw, session, beacon, err)
			}
		})
	}
}

func TestGetActiveTargetMatchesSession(t *testing.T) {
	want := core.Sessions.Add(core.NewSession(core.NewImplantConnection("test", "")))
	t.Cleanup(func() { core.Sessions.Remove(want.ID) })

	raw, err := json.Marshal(&sliverpb.PwdReq{Request: &commonpb.Request{
		SessionID: want.ID,
	}})
	if err != nil {
		t.Fatal(err)
	}
	session, beacon, err := getActiveTarget(raw)
	if err != nil || session.GetID() != want.ID || beacon != nil {
		t.Fatalf("getActiveTarget(%s) = (%v, %v, %v), want session %q, no beacon, and no error", raw, session, beacon, err, want.ID)
	}
}
