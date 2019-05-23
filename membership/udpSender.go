// the struct to send udp messages
// Author: Ziang Wan

package membership

import (
	"encoding/json"
	"net"
	"strconv"
)

// the sender struct
type UdpSender int

// heartbeat structure
type HeartBeat struct {
	HostName string
	Type int
	UpdateBuffer []membershipEntry
}

// heartbeat type enum
const TypePing int = 0
const TypeAck int = 1
const TypeJoin int = 2

/* Helper Function */

func sendHeartBeatTo(hostName string, hbMessage *HeartBeat) error {
	// udp connection
	con, err := net.Dial("udp", combineHostAndPort(hostName, PortNumber))
	if err != nil {
		return err
	}
	defer con.Close()

	// marshal
	buffer, err := json.Marshal(*hbMessage)
	if err != nil {
		return err
	}

	// send
	_, err = con.Write(buffer)
	if err != nil {
		return err
	}

	return nil
}

func combineHostAndPort(hostName string, portNumber int) string {
	// combine hostName as a string and portNumber as an int
	retStr := hostName + ":" + strconv.Itoa(portNumber)
	return retStr
}

/* methods */

func (self *UdpSender) SendPing(hostName string, m *MembershipList) error {
	// send to the hostName a ping
	hbMessage := HeartBeat{HOSTNAME, TypePing, m.GetCurrentUpdateBuffer()}
	err := sendHeartBeatTo(hostName, &hbMessage)
	return err
}

func (self *UdpSender) ReplyPing(hostName string, m *MembershipList) error {
	// send to the hostName an acknowledgement
	hbMessage := HeartBeat{HOSTNAME, TypeAck, m.GetCurrentUpdateBuffer()}
	err := sendHeartBeatTo(hostName, &hbMessage)
	return err
}

/* special action for the introducer */

func (self *UdpSender) SendJoin(hostName string, m *MembershipList) error {
	// send to the introducer an join request
	hbMessage := HeartBeat{HOSTNAME, TypeJoin, m.GetCurrentUpdateBuffer()}
	err := sendHeartBeatTo(hostName, &hbMessage)
	return err
}

func (self *UdpSender) ReplyJoin(hostName string, m *MembershipList) error {
	// send to the introducer an join request
	hbMessage := HeartBeat{HOSTNAME, TypeAck, m.GetEntireMembershipList()}
	err := sendHeartBeatTo(hostName, &hbMessage)
	return err
}


