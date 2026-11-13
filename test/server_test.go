package test

import (
	"bufio"
	"io"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/zegasega/go-redis-clone/internal/server"
)

func parse(input string) ([]string, error) {
	return server.ParseResp(bufio.NewReader(strings.NewReader(input)))
}

func TestParseRespValid(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  []string
	}{
		{"ping", "*1\r\n$4\r\nPING\r\n", []string{"PING"}},
		{"set", "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n", []string{"SET", "key", "value"}},
		{"get", "*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n", []string{"GET", "key"}},
		{"empty array", "*0\r\n", []string{}},
		{"empty bulk string", "*2\r\n$3\r\nSET\r\n$0\r\n\r\n", []string{"SET", ""}},
		{"binary safe", "*1\r\n$4\r\na\r\nb\r\n", []string{"a\r\nb"}},
		{"unicode", "*1\r\n$8\r\nçalış\r\n", []string{"çalış"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parse(tt.input)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestParseRespInvalid(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"empty input", ""},
		{"wrong array prefix", "+OK\r\n"},
		{"invalid array length", "*abc\r\n"},
		{"negative array length", "*-1\r\n"},
		{"wrong bulk prefix", "*1\r\n:4\r\n"},
		{"invalid bulk length", "*1\r\n$x\r\nPING\r\n"},
		{"null bulk string", "*1\r\n$-1\r\n"},
		{"bad terminator", "*1\r\n$4\r\nPINGxx"},
		{"truncated data", "*1\r\n$10\r\nPING\r\n"},
		{"missing elements", "*2\r\n$4\r\nPING\r\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := parse(tt.input); err == nil {
				t.Fatalf("expected error for input %q", tt.input)
			}
		})
	}
}

func TestParseRespMultipleCommands(t *testing.T) {
	input := "*1\r\n$4\r\nPING\r\n*2\r\n$3\r\nGET\r\n$1\r\nk\r\n"
	reader := bufio.NewReader(strings.NewReader(input))

	first, err := server.ParseResp(reader)
	if err != nil || !reflect.DeepEqual(first, []string{"PING"}) {
		t.Fatalf("first command: got %q, err %v", first, err)
	}

	second, err := server.ParseResp(reader)
	if err != nil || !reflect.DeepEqual(second, []string{"GET", "k"}) {
		t.Fatalf("second command: got %q, err %v", second, err)
	}

	if _, err := server.ParseResp(reader); err != io.EOF {
		t.Fatalf("expected io.EOF, got %v", err)
	}
}

func TestNewTCPServer(t *testing.T) {
	configs := []server.Config{
		{ListenAddr: ":0"},
		{ListenAddr: ":0", ShardCount: 4},
		{ListenAddr: ":0", ShardCount: -1, CleanerInterval: time.Second},
	}

	for _, cfg := range configs {
		if server.NewTCPServer(cfg) == nil {
			t.Fatalf("NewTCPServer returned nil for config %+v", cfg)
		}
	}
}

func TestRunFailsWhenAddressInUse(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	srv := server.NewTCPServer(server.Config{ListenAddr: ln.Addr().String()})

	errCh := make(chan error, 1)
	go func() { errCh <- srv.Run() }()

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected error when address is in use")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return an error in time")
	}
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr
}

func dial(t *testing.T, addr string) net.Conn {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.Dial("tcp", addr)
		if err == nil {
			return conn
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("could not connect to %s", addr)
	return nil
}

func TestServerAcceptsConnections(t *testing.T) {
	addr := freeAddr(t)
	srv := server.NewTCPServer(server.Config{ListenAddr: addr, ShardCount: 2, CleanerInterval: 50 * time.Millisecond})
	go srv.Run()

	conns := make([]net.Conn, 0, 5)
	for i := 0; i < 5; i++ {
		conn := dial(t, addr)
		conns = append(conns, conn)
	}

	for _, conn := range conns {
		if _, err := conn.Write([]byte("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n")); err != nil {
			t.Fatalf("write failed: %v", err)
		}
		if _, err := conn.Write([]byte("*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n")); err != nil {
			t.Fatalf("write failed: %v", err)
		}
		conn.Close()
	}
}

func TestServerClosesConnectionOnInvalidInput(t *testing.T) {
	addr := freeAddr(t)
	srv := server.NewTCPServer(server.Config{ListenAddr: addr})
	go srv.Run()

	conn := dial(t, addr)
	defer conn.Close()

	if _, err := conn.Write([]byte("INVALID\r\n")); err != nil {
		t.Fatalf("write failed: %v", err)
	}

	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 1)
	if _, err := conn.Read(buf); err != io.EOF {
		t.Fatalf("expected connection to be closed with io.EOF, got %v", err)
	}
}
