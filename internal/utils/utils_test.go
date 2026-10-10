package utils

import (
	"net"
	"sync/atomic"
	"testing"
)

func TestBitsToBytes(t *testing.T) {
	tests := []struct {
		name    string
		bits    []bool
		want    []byte
		wantErr bool
	}{
		{name: "empty", bits: []bool{}, want: []byte{}},
		{name: "single byte all set", bits: []bool{true, true, true, true, true, true, true, true}, want: []byte{0xFF}},
		{name: "single byte none set", bits: []bool{false, false, false, false, false, false, false, false}, want: []byte{0x00}},
		{name: "not multiple of 8", bits: []bool{true, false, true}, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := BitsToBytes(tc.bits)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("got %v, want %v", got, tc.want)
				}
			}
		})
	}
}

func TestBytesToBits(t *testing.T) {
	got := BytesToBits([]byte{0x80})
	want := []bool{true, false, false, false, false, false, false, false}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}

func TestBitsRoundTrip(t *testing.T) {
	original := []bool{
		true, false, true, true, false, false, true, false,
		false, true, true, false, true, false, false, true,
	}
	bytesOut, err := BitsToBytes(original)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	roundTripped := BytesToBits(bytesOut)
	if len(roundTripped) != len(original) {
		t.Fatalf("length mismatch: got %d, want %d", len(roundTripped), len(original))
	}
	for i := range original {
		if roundTripped[i] != original[i] {
			t.Fatalf("bit %d mismatch: got %v, want %v", i, roundTripped[i], original[i])
		}
	}
}

func TestRandomPeerID(t *testing.T) {
	id1 := RandomPeerID()
	if len(id1) != 20 {
		t.Fatalf("expected length 20, got %d", len(id1))
	}
	if string(id1[:8]) != "-qB0001-" {
		t.Fatalf("expected prefix -qB0001-, got %q", string(id1[:8]))
	}

	id2 := RandomPeerID()
	if string(id1[8:]) == string(id2[8:]) {
		t.Fatal("expected random suffixes to differ between calls, got identical suffixes")
	}
}

// timeoutErr is a minimal net.Error stand-in that always reports as a timeout,
// used to drive retry's retry-on-timeout branch deterministically.
type timeoutErr struct{}

func (timeoutErr) Error() string   { return "i/o timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }

func newPipeConn(t *testing.T) net.Conn {
	t.Helper()
	client, server := net.Pipe()
	t.Cleanup(func() {
		client.Close()
		server.Close()
	})
	return client
}

func TestRetrySucceedsFirstTry(t *testing.T) {
	conn := newPipeConn(t)
	var calls int32
	result, err := Retry(3, conn, func() (string, error) {
		atomic.AddInt32(&calls, 1)
		return "ok", nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != "ok" {
		t.Fatalf("expected result %q, got %q", "ok", result)
	}
	if calls != 1 {
		t.Fatalf("expected 1 call, got %d", calls)
	}
}

func TestRetryStopsOnNonTimeoutError(t *testing.T) {
	conn := newPipeConn(t)
	var calls int32
	wantErr := net.UnknownNetworkError("boom")
	_, err := Retry(5, conn, func() (string, error) {
		atomic.AddInt32(&calls, 1)
		return "", wantErr
	})
	if err != wantErr {
		t.Fatalf("expected error %v, got %v", wantErr, err)
	}
	if calls != 1 {
		t.Fatalf("expected exactly 1 call (no retry on non-timeout error), got %d", calls)
	}
}

func TestRetryExhaustsOnRepeatedTimeout(t *testing.T) {
	conn := newPipeConn(t)
	var calls int32
	_, err := Retry(4, conn, func() (string, error) {
		atomic.AddInt32(&calls, 1)
		return "", timeoutErr{}
	})
	if err == nil {
		t.Fatal("expected a timeout error, got nil")
	}
	if ne, ok := err.(net.Error); !ok || !ne.Timeout() {
		t.Fatalf("expected a timeout net.Error, got %v", err)
	}
	if calls != 4 {
		t.Fatalf("expected 4 calls (retries exhausted), got %d", calls)
	}
}
