package main

import (
	"bufio"
	"io"
	"log"
	"net"
	"strconv"
)

// Can be tested using nc tool:
//    echo "asdad" | nc 127.0.0.1 27017
//
type TCPInput struct {
	data     chan []byte
	address  string
	listener net.Listener
	receivedCount int
}

func NewTCPInput(address string) (i *TCPInput) {
	i = new(TCPInput)
	i.data = make(chan []byte)
	i.address = address
	i.receivedCount = 0

	i.listen(address)

	return
}

func (i *TCPInput) Read(data []byte) (int, error) {
	buf := <-i.data
	copy(data, buf)
	i.receivedCount = i.receivedCount + 1
	log.Println("Received Count: " + strconv.Itoa((i.receivedCount)))

	return len(buf), nil
}

func (i *TCPInput) listen(address string) {
	listener, err := net.Listen("tcp", address)
	i.listener = listener

	if err != nil {
		log.Fatal("Can't start:", err)
	}

	go func() {
		for {
			conn, err := listener.Accept()

			if err != nil {
				log.Println("Error while Accept()", err)
				continue
			}

			go i.handleConnection(conn)
		}
	}()
}

func (i *TCPInput) handleConnection(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)

	for {
		buf, err := reader.ReadBytes('¶')
		if err == io.EOF {
			return
		} else if err != nil {
			log.Println("Unexpected error in input tcp connection", err)
			return
		}
		buf_len := len(buf)
		if buf_len > 0 {
			new_buf_len := len(buf) - 2
			if new_buf_len > 0 {
				new_buf := make([]byte, new_buf_len)
				copy(new_buf, buf[:new_buf_len])
				i.data <- new_buf
			}
		}
	}
}

func (i *TCPInput) String() string {
	return "TCP input: " + i.address
}
