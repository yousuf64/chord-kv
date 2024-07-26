package bootstrap

import (
	"fmt"
	"log"
	"net"
	"strings"
)

type RegisterStatus int

const (
	RegOk             RegisterStatus = 0
	RegOkOne          RegisterStatus = 1
	RegOkTwo          RegisterStatus = 2
	InvalidCommand    RegisterStatus = 9999
	AlreadyRegistered RegisterStatus = 9998
	AddrRegistered    RegisterStatus = 9997
	BSFull            RegisterStatus = 9996
)

type UnregisterStatus int

const (
	UnregOk    UnregisterStatus = 0
	UnregError UnregisterStatus = 9999
)

type Bootstrap struct {
	udp             *net.UDPConn
	RegisterReply   func(status RegisterStatus, nodeIPs []string)
	UnregisterReply func(status UnregisterStatus)
}

func New(addr string) *Bootstrap {
	remoteAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		panic(err)
	}

	udp, err := net.DialUDP("udp", nil, remoteAddr)
	if err != nil {
		panic(err)
	}

	b := &Bootstrap{udp, nil, nil}
	go b.listen()

	return b
}

func (b *Bootstrap) Register(addr string, username string) {
	split := strings.Split(addr, ":")
	msg := fmt.Sprintf("REG %s %s %s", split[0], split[1], username)
	msg = fmt.Sprintf("%04d %s", len(msg)+5, msg)

	_, err := b.udp.Write([]byte(msg))
	if err != nil {
		panic(err)
	}
}

func (b *Bootstrap) Unregister(addr string, username string) {
	split := strings.Split(addr, ":")
	msg := fmt.Sprintf("UNREG %s %s %s", split[0], split[1], username)
	msg = fmt.Sprintf("%04d %s", len(msg)+5, msg)

	_, err := b.udp.Write([]byte(msg))
	if err != nil {
		panic(err)
	}
}

func (b *Bootstrap) listen() {
	for {
		buffer := make([]byte, 1024)
		n, remoteAddr, err := b.udp.ReadFromUDP(buffer)
		if err != nil {
			fmt.Println(err)
			continue
		}

		msg := string(buffer[:n])
		split := strings.Split(msg, " ")

		if split[1] == "REGOK" {
			status := split[2]

			nodeIPs := make([]string, 0)
			var s RegisterStatus
			if status == "0" {
				s = RegOk
			} else if status == "1" {
				s = RegOkOne
				nodeIPs = append(nodeIPs, fmt.Sprintf("%s:%s", split[3], split[4]))
			} else if status == "2" {
				s = RegOkTwo
				nodeIPs = append(nodeIPs, fmt.Sprintf("%s:%s", split[3], split[4]))
				nodeIPs = append(nodeIPs, fmt.Sprintf("%s:%s", split[5], split[6]))
			} else if status == "9999" {
				s = InvalidCommand
			} else if status == "9998" {
				s = AlreadyRegistered
			} else if status == "9997" {
				s = AddrRegistered
			} else if status == "9996" {
				s = BSFull
			}

			if b.RegisterReply != nil {
				b.RegisterReply(s, nodeIPs)
			}
		} else if split[1] == "UNROK" {
			status := split[2]

			var s UnregisterStatus
			if status == "0" {
				s = UnregOk
			} else if status == "9999" {
				s = UnregError
			}

			if b.UnregisterReply != nil {
				b.UnregisterReply(s)
			}
		} else {
			log.Printf("Received unknown message: %s\n", string(buffer[:n]))
		}

		fmt.Printf("Received: %s from %s\n", string(buffer[:n]), remoteAddr)
	}
}
