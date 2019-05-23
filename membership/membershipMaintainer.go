// the main program
// Author: Ziang Wan, Yan Xu

package membership

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
)

// the maintainer
type MembershipMaintainer struct {
	// sync
	swimWaitGroup sync.WaitGroup
	receiverThreadTermChan chan int
	heartBeatThreadTermChan chan int

	// book keeping
	pingMap *PingMap
	mbrList *MembershipList

	// flag
	joinedMulticast bool
}

// buffer size
const BufferSize int = 2048

/* Receiver Thread */

func (self *MembershipMaintainer) receiverThread() {
	// create the listening port
	pc, err := net.ListenPacket("udp", ":" + strconv.Itoa(PortNumber))
	if err != nil {
		fmt.Print("Receiver Thread Failed: ")
		fmt.Println(err)
		os.Exit(0)
	}
	defer pc.Close()

	// sender
	var sender UdpSender

	// keep receiving
	for{
		select{
		case <-self.receiverThreadTermChan:
			// terminate
			self.swimWaitGroup.Done()
			return
		default:
			// set a timeout
			pc.SetReadDeadline(time.Now().Add(2 * time.Second))

			// read
			inputBytes := make([]byte, BufferSize)
			length, _, err := pc.ReadFrom(inputBytes)
			if err != nil {
				continue
			}

			// decode
			var hbMessage HeartBeat
			err = json.Unmarshal(inputBytes[:length], &hbMessage)
			if err != nil {
				continue
			}

			if hbMessage.Type == TypePing {
				// receive a ping, reply with an ack
				self.mbrList.MergeWithUpdateBuffer(hbMessage.UpdateBuffer)
				sender.ReplyPing(hbMessage.HostName, self.mbrList)
			}else if hbMessage.Type == TypeJoin{
				// receive a join, only for the introducer, does not merge with local membership
				sender.ReplyJoin(hbMessage.HostName, self.mbrList)
			}else{
				// receive an ack, remove it from the pingMap
				self.mbrList.MergeWithUpdateBuffer(hbMessage.UpdateBuffer)
				self.pingMap.removeFromPingMap(hbMessage.HostName)
			}
		}

	}
}

/* Heartbeat Thread */

func (self *MembershipMaintainer) heartBeatThread() {
	// the sender
	var sender UdpSender

	for{
		select {
		case <-self.heartBeatThreadTermChan:
			// terminate
			self.swimWaitGroup.Done()
			return
		default:
			// send pings to gossip target
			pingTargets := self.mbrList.GetGossipTargets()
			for _, pingTarget := range pingTargets {
				// if not waiting for an ack
				err := sender.SendPing(pingTarget, self.mbrList)
				if err != nil {
					continue
				} else {
					self.pingMap.pingedTargetUpdateMap(pingTarget)
				}
			}

			// increment heartbeat
			self.mbrList.IncrementHeartBeat()

			// sleep and continue
			time.Sleep(time.Duration(MsPerRound) * time.Millisecond)

			// clear the update buffer
			self.mbrList.CleanBuffer()

			// check for timeout
			self.pingMap.pingMapIncrement()
			self.pingMap.checkTimeOut(self.mbrList)
		}
	}
}

/* Struct Methods */

func (self *MembershipMaintainer) joinExchangeWithIntroducer() error {
	// the introducer does not join
	if HOSTNAME == IntroducerAddr {
		return nil
	}

	var sender UdpSender
	sender.SendJoin(IntroducerAddr, self.mbrList)

	c1 := make(chan string, 1)

	// wait for an ack from the introducer
	go func() {
		pc, err := net.ListenPacket("udp", ":" + strconv.Itoa(PortNumber))
		if err != nil {
			c1 <- "UDP listening error"
			return
		}
		defer pc.Close()

		var hbMessage HeartBeat
		// read from network
		inputBytes := make([]byte, BufferSize)
		length, _, err := pc.ReadFrom(inputBytes)
		if err != nil {
			c1 <- "UDP reading error"
			return
		}

		// decode
		err = json.Unmarshal(inputBytes[:length], &hbMessage)
		if err != nil {
			c1 <- "malformed Response"
			return
		}

		// an ack from the introducer
		self.mbrList.MergeWithUpdateBuffer(hbMessage.UpdateBuffer)
		c1 <- "Success"
	}()

	// timeoutzz
	select {
    case res := <-c1:
    	if res != "Success"{
    		return errors.New(res)
    	}else{
    		return nil
    	}
    case <- time.After(time.Duration(MsPerRound * (1 +TimeoutRound)) * time.Millisecond):
        return errors.New("cannot contact the introducer: timeout")
    }
}

func (self *MembershipMaintainer) JoinMulticast() error {
	// reinit all the data field
	self.receiverThreadTermChan = make(chan int, 1)
	self.heartBeatThreadTermChan = make(chan int, 1)

	// join multicast
	err := self.joinExchangeWithIntroducer()
	if err != nil {
		return err
	}

	// swim protocol
	self.swimWaitGroup.Add(2)
	go self.receiverThread()
	go self.heartBeatThread()

	// set flag
	self.joinedMulticast = true
	return nil
}

func (self *MembershipMaintainer) LeaveMulticast() {
	// terminate all two swim threads
	self.receiverThreadTermChan <- 1
	self.heartBeatThreadTermChan <- 1
	self.swimWaitGroup.Wait()

	// clear out book keeping
	self.mbrList = createNewMembershipList()
	self.pingMap = createNewPingMap()

	// set flag
	self.joinedMulticast = false
}

func (self *MembershipMaintainer) GetCurrentMembers() []string {
	if self.joinedMulticast{
		return self.mbrList.GetCurrentMembers()
	}else{
		return []string{}
	}
}

func (self *MembershipMaintainer) GetCurrentLeader() string {
	var l []string = self.mbrList.GetCurrentMembers()
	if len(l) > 0{
		return l[0]
	}else{
		return "None"
	}
}

// MP4 added -> one backup master

func (self *MembershipMaintainer) GetBackupLeader() string {
	var l []string = self.mbrList.GetCurrentMembers()
	if len(l) > 1{
		return l[1]
	}else{
		return "None"
	}
}

/* package function */

func CreateMembershipMaintainer() *MembershipMaintainer {
	var mbrM MembershipMaintainer
	mbrM.joinedMulticast = false
	mbrM.mbrList = createNewMembershipList()
	mbrM.pingMap = createNewPingMap()
	return &mbrM
}

