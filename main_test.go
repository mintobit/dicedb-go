package dicedb

import (
	"bytes"
	"errors"
	"testing"

	"github.com/dicedb/dicedb-go/wire"
)

func areResponseValuesEqual(r1, r2 *wire.Response) bool {
	switch r1.Value.(type) {
	case *wire.Response_VStr:
		return r1.Value.(*wire.Response_VStr).VStr == r2.Value.(*wire.Response_VStr).VStr
	case *wire.Response_VInt:
		return r1.Value.(*wire.Response_VInt).VInt == r2.Value.(*wire.Response_VInt).VInt
	case *wire.Response_VFloat:
		return r1.Value.(*wire.Response_VFloat).VFloat == r2.Value.(*wire.Response_VFloat).VFloat
	case *wire.Response_VBytes:
		return bytes.Equal(r1.Value.(*wire.Response_VBytes).VBytes, r2.Value.(*wire.Response_VBytes).VBytes)
	case *wire.Response_VNil:
		return r1.Value.(*wire.Response_VNil) == r2.Value.(*wire.Response_VNil)
	}
	return false
}

func TestNewClient(t *testing.T) {
	tests := []struct {
		name    string
		host    string
		port    int
		wantNil bool
		err     error
	}{
		{
			name:    "valid connection",
			host:    "localhost",
			port:    7379,
			wantNil: false,
			err:     nil,
		},
		{
			name:    "invalid port",
			host:    "localhost",
			port:    -1,
			wantNil: true,
			err:     errors.New("dial tcp: address -1: invalid port"),
		},
		{
			name:    "unable to connect",
			host:    "localhost",
			port:    9999,
			wantNil: true,
			err:     errors.New("dial tcp 127.0.0.1:9999: connect: connection refused"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := NewClient(tt.host, tt.port)
			if (client == nil) != tt.wantNil {
				t.Errorf("NewClient() got = %v, %s, want nil = %v, err = %v", client, err, tt.wantNil, tt.err)
			}
			if err != nil && err.Error() != tt.err.Error() {
				t.Errorf("NewClient() got = %v, %s, want nil = %v, err = %v", client, err, tt.wantNil, tt.err)
			}
		})
	}
}

func TestClient_Fire(t *testing.T) {
	client, err := NewClient("localhost", 7379)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	tests := []struct {
		name     string
		mockConn *Client
		cmd      *wire.Command
		result   *wire.Response
		err      error
	}{
		{
			name:     "successful command",
			mockConn: client,
			cmd:      &wire.Command{Cmd: "PING"},
			result:   &wire.Response{Value: &wire.Response_VStr{VStr: "PONG"}},
			err:      nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := tt.mockConn.Fire(tt.cmd)
			if tt.err != nil && resp.Err != tt.err.Error() {
				t.Errorf("Fire() expected error: %v, want: %v", resp.Err, tt.err)
			}
			if !areResponseValuesEqual(resp, tt.result) {
				t.Errorf("Fire() unexpected response: %v, want: %v", resp, tt.result)
			}
		})
	}
}
