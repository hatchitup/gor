package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"time"
)

type TCPOutput struct {
	address  string
	limit    int
	buf      chan []byte
	bufStats *GorStat
}

func NewTCPOutput(address string) io.Writer {
	o := new(TCPOutput)

	o.address = address

	o.buf = make(chan []byte, 100)
	if Settings.outputTCPStats {
		o.bufStats = NewGorStat("output_tcp")
	} else {
		log.Println("TCP Stats logging is disabled")
	}

	for i := 0; i < 10; i++ {
		go o.worker()
	}

	return o
}

func (o *TCPOutput) worker() {
	conn, err := o.connect(o.address)
	for ; err != nil; conn, err = o.connect(o.address) {
		time.Sleep(2 * time.Second)
	}

	defer conn.Close()

	for {
		_, err := conn.Write(<-o.buf)
		if err != nil {
			log.Println("Worker failed on write, exitings and starting new worker")
			go o.worker()
			break
		}
	}
}

func (o *TCPOutput) Write(data []byte) (n int, err error) {
	new_buf := make([]byte, len(data)+2)
	data = append(data, []byte("¶")...)
	copy(new_buf, data)
	o.buf <- new_buf
	if Settings.outputTCPStats {
		o.bufStats.Write(len(o.buf))
	}

	return len(data), nil
}

func (o *TCPOutput) connect(address string) (conn net.Conn, err error) {
	conn, err = net.Dial("tcp", address)

	if err != nil {
		log.Println("Connection error ", err, o.address)
	}

	return
}

func (o *TCPOutput) String() string {
	return fmt.Sprintf("TCP output %s, limit: %d", o.address, o.limit)
}
