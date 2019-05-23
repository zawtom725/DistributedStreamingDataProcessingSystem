// package membershipList
// Author: Ziang Wan, Yan Xu

package membership

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

// membership list entry
type membershipEntry struct{
	HostName string
	TimeStamp int64
	HeartbeatNumber int
	Alive bool
	BufferElapse int	// how many rounds left in the buffer
}

// the membershiplist struct
type MembershipList struct{
	Lock *sync.Mutex
	Members []membershipEntry
}

/* Helper Functions */

func mergeEntry(selfEntry membershipEntry, otherEntry membershipEntry) membershipEntry {
	// the hostname fields for the two entries are equal, see whether an update is necessary

	// firstly check timestamp <- the creation time (join time) of the membership list
	if selfEntry.TimeStamp < otherEntry.TimeStamp {
		otherEntry.BufferElapse = UpdateBufferRound
		return otherEntry
	}else if selfEntry.TimeStamp > otherEntry.TimeStamp {
		return selfEntry
	}

	// heart beat override
	if selfEntry.HeartbeatNumber < otherEntry.HeartbeatNumber {
		otherEntry.BufferElapse = UpdateBufferRound
		return otherEntry
	}else if selfEntry.HeartbeatNumber > otherEntry.HeartbeatNumber {
		return selfEntry
	}

	// same heartbeat: dead over alive
	if selfEntry.Alive && !otherEntry.Alive {
		otherEntry.BufferElapse = UpdateBufferRound
		return otherEntry
	}else{
		return selfEntry
	}
}

func (self *MembershipList) MergeWithUpdateBuffer(recentUpdates []membershipEntry) {
	self.Lock.Lock()
	defer self.Lock.Unlock()

	// no work shortcut
	if len(recentUpdates) == 0 {
		return
	}

	// self list is sorted, the recentUpdates list is sorted
	selfIdx := 0
	otherIdx := 0
	var newMembersList []membershipEntry

	// like the merge state of the merge sort
	for selfIdx < len(self.Members) && otherIdx < len(recentUpdates) {
		selfEntry := self.Members[selfIdx]
		otherEntry := recentUpdates[otherIdx]

		if selfEntry.HostName == otherEntry.HostName {
			// equal entry
			newMembersList = append(newMembersList, mergeEntry(selfEntry, otherEntry))
			selfIdx += 1
			otherIdx += 1
		}else if selfEntry.HostName < otherEntry.HostName {
			// append self
			newMembersList = append(newMembersList, selfEntry)
			selfIdx += 1
		}else{
			// append other - a new node, add to the buffer
			otherEntry.BufferElapse = UpdateBufferRound
			newMembersList = append(newMembersList, otherEntry)
			otherIdx += 1
		}
	}

	// append the remaining
	for selfIdx < len(self.Members) {
		newMembersList = append(newMembersList, self.Members[selfIdx])
		selfIdx += 1
	}
	for otherIdx < len(recentUpdates) {
		recentUpdates[otherIdx].BufferElapse = UpdateBufferRound
		newMembersList = append(newMembersList, recentUpdates[otherIdx])
		otherIdx += 1
	}

	// done
	self.Members = newMembersList
}

/* Increment Self Heartbeat */

func (self *MembershipList) IncrementHeartBeat() {
	self.Lock.Lock()
	defer self.Lock.Unlock()

	for idx, entry := range self.Members {
		if entry.HostName == HOSTNAME {
			self.Members[idx].HeartbeatNumber += 1

			// re-alive myself
			if !self.Members[idx].Alive {
				self.Members[idx].Alive = true
				self.Members[idx].BufferElapse = UpdateBufferRound
			}
		}
	}
}

/* Buffer Processing */

func (self *MembershipList) GetCurrentUpdateBuffer() []membershipEntry {
	// get the entries that are currently in the buffer
	self.Lock.Lock()
	defer self.Lock.Unlock()

	var bufferList []membershipEntry
	for _, entry := range self.Members {
		if entry.BufferElapse > 0 {
			bufferList = append(bufferList, entry)
		}
	}
	return bufferList
}

func (self *MembershipList) GetEntireMembershipList() []membershipEntry {
	// for newly joined nodes
	self.Lock.Lock()
	defer self.Lock.Unlock()

	return self.Members
}

func (self *MembershipList) CleanBuffer() {
	// remove timeout elements from the buffer
	self.Lock.Lock()
	defer self.Lock.Unlock()

	for idx, entry := range self.Members {
		if entry.BufferElapse > 0 {
			self.Members[idx].BufferElapse -= 1
		}
	}
}

/* for GetGossipTarget */

type pair struct {
	hostName string
	ringDistance int
}

func (self *MembershipList) prunedList() []string {
	// get a list of alive or suspected hostnames in order
	var notDead []string
	for _, entry := range self.Members {
		if entry.Alive {
			notDead = append(notDead, entry.HostName)
		}
	}
	return notDead
}

func computeRingDistance(idx1 int, idx2 int, n int) int {
	// absolute distance
	var absDistance int
	if idx1 >= idx2 {
		absDistance = idx1 - idx2
	}else{
		absDistance = idx2 - idx1
	}
	// ring distance
	var ringDistance int
	if absDistance > (n/2){
		ringDistance = n - absDistance
	}else{
		ringDistance = absDistance
	}
	return ringDistance
}

func (self *MembershipList) GetGossipTargets() []string {
	self.Lock.Lock()
	defer self.Lock.Unlock()

	// pruned the list to get the alive ring
	prunedMemberList := self.prunedList()

	// find self host
	selfIdx := -1
	for idx, hostName := range prunedMemberList {
		if hostName == HOSTNAME {
			selfIdx = idx
		}
	}

	// self not found -> serioues problem, return empty list
	if selfIdx == -1 {
		return []string{}
	}

	// find 4 with shortest ring distance
	var allPairs []pair
	for idx, hostName := range prunedMemberList {
		if idx != selfIdx {
			ringDistance := computeRingDistance(selfIdx, idx, len(prunedMemberList))
			allPairs = append(allPairs, pair{hostName, ringDistance})
		}
	}
	// sorting
	sort.Slice(allPairs, func(i, j int) bool {return allPairs[i].ringDistance < allPairs[j].ringDistance})
	// pick the smallest 4
	var gossipList []string
	for i := 0; i < len(allPairs) && i < 4; i++ {
		gossipList = append(gossipList, allPairs[i].hostName)
	}

	return gossipList
}

/* Time Out */

func (self *MembershipList) TimeOut(hostName string) {
	self.Lock.Lock()
	defer self.Lock.Unlock()

	fmt.Println("Timeout: " + hostName)
	for idx, entry := range self.Members {
		if entry.HostName == hostName {
			if self.Members[idx].Alive {
				self.Members[idx].Alive = false

				// add to the recent changed buffer
				self.Members[idx].BufferElapse = UpdateBufferRound
			}
		}
	}
}

/* Get Current Membership */

func (self *MembershipList) GetCurrentMembers() []string {
	self.Lock.Lock()
	defer self.Lock.Unlock()

	return self.prunedList()
}

/* Package Util Functions */

func createNewMembershipList() *MembershipList {
	// initialize the membership list, used when the process is born
	var mbl MembershipList
	mbl.Lock = &sync.Mutex{}
	mbl.Members = []membershipEntry{{HOSTNAME, time.Now().Unix(), 0, true, 2}}
	return &mbl
}
