package server

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
)

func ParseResp(reader *bufio.Reader) ([]string, error) {
	prefix, err := reader.ReadByte()
	if err != nil {
		return nil, err
	}

	if prefix != '*' {
		return nil, fmt.Errorf("expected array prefix '*', got %q", prefix)
	}

	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}

	count, err := strconv.Atoi(line[:len(line)-2])
	if err != nil {
		return nil, fmt.Errorf("invalid array length: %w", err)
	}

	if count < 0 {
		return nil, fmt.Errorf("invalid array length: %d", count)
	}

	args := make([]string, 0, count)

	for i := 0; i < count; i++ {
		prefix, err := reader.ReadByte()
		if err != nil {
			return nil, err
		}

		if prefix != '$' {
			return nil, fmt.Errorf("expected bulk string prefix '$', got %q", prefix)
		}

		line, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}

		length, err := strconv.Atoi(line[:len(line)-2])
		if err != nil {
			return nil, fmt.Errorf("invalid bulk string length: %w", err)
		}

		if length < 0 {
			return nil, fmt.Errorf("null bulk strings are not supported")
		}

		data := make([]byte, length+2)

		if _, err := io.ReadFull(reader, data); err != nil {
			return nil, err
		}

		if data[length] != '\r' || data[length+1] != '\n' {
			return nil, fmt.Errorf("invalid bulk string terminator")
		}

		args = append(args, string(data[:length]))
	}

	return args, nil
}

func (s *TCPServer) handleConnection(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)

	for {
		args, err := ParseResp(reader)
		if err != nil {
			return
		}

		fmt.Println(args)
	}
}
