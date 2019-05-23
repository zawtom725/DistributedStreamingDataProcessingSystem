package sdfs

import (
	"fmt"
	"hash/fnv"
	"sync"
)

type fileResidenceList struct {
	currentMembership []string
	replicaAddr map[string][]string
	version map[string]int
	lock *sync.Mutex
}

/* helper function */

func hash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32())
}

// get consecutive four elements followed by startIdx, wrap around if necessary, no duplicates
func listConsecFour(l []string, startIdx int) []string {
	if len(l) <= 4{
		return l
	}else{
		count := 0
		var retList []string
		for count < 4 {
			retList = append(retList, l[startIdx])
			count += 1
			startIdx += 1
			// wrap around if necessary
			if startIdx == len(l){
				startIdx = 0
			}
		}
		return retList
	}
}

func listIndexOf(target string, l []string) int {
	for idx, lEle := range l{
		if lEle == target{
			return idx
		}
	}
	return -1
}

// unexplainable difficult algorithm
func newMbrExtendReplica(oldReplicaAddrs []string, newMembership []string, sdfsFileName string) ([]string, []rereplicationAction){
	// guaranteed non-empty since only three simultaneous failure
	var existIdx []int
	var existedHost []string
	for _, oldReplicaAddr := range oldReplicaAddrs {
		thisIdx := listIndexOf(oldReplicaAddr, newMembership)
		if thisIdx != -1{
			existIdx = append(existIdx, thisIdx)
			existedHost = append(existedHost, oldReplicaAddr)
		}
	}

	// re-replication actions needed
	var retRRA []rereplicationAction

	startIdx := existIdx[0]
	currentIdx := startIdx + 1
	if currentIdx == len(newMembership){
		currentIdx = 0
	}

	for currentIdx != startIdx && len(existIdx) < 4{
		if listIndexOf(newMembership[currentIdx], oldReplicaAddrs) == -1{
			// find a new re-replicaion target
			existIdx = append(existIdx, currentIdx)
			retRRA = append(retRRA, rereplicationAction{existedHost, newMembership[currentIdx], sdfsFileName})
		}

		currentIdx += 1
		if currentIdx == len(newMembership){
			currentIdx = 0
		}
	}

	// construct the new replication list
	var newReplicaAddrs []string
	for _, idx := range existIdx{
		newReplicaAddrs = append(newReplicaAddrs, newMembership[idx])
	}

	return newReplicaAddrs, retRRA
}

/* Methods */

func (self *fileResidenceList) Put(sdfsFileName string) ([]string, int) {
	self.lock.Lock()
	defer self.lock.Unlock()

	if replicaAddrs, exist := self.replicaAddr[sdfsFileName]; exist{
		// the file already exists
		currentVersion := self.version[sdfsFileName]
		newVersion := currentVersion + 1
		self.version[sdfsFileName] = newVersion

		return replicaAddrs, newVersion
	}else{
		// create new entry for the file
		newVersion := 0
		self.version[sdfsFileName] = newVersion
		// pick 4 replica use hashing
		hashed := hash(sdfsFileName) % len(self.currentMembership)
		replicaAddrs := listConsecFour(self.currentMembership, hashed)
		self.replicaAddr[sdfsFileName] = replicaAddrs

		return replicaAddrs, newVersion
	}
}

func (self *fileResidenceList) Get(sdfsFileName string) []string {
	self.lock.Lock()
	defer self.lock.Unlock()

	if replicaAddrs, exist := self.replicaAddr[sdfsFileName]; exist{
		return replicaAddrs
	}else{
		return []string{}
	}
}

func (self *fileResidenceList) Delete(sdfsFileName string) []string {
	self.lock.Lock()
	defer self.lock.Unlock()

	if replicaAddrs, exist := self.replicaAddr[sdfsFileName]; exist{
		// delete entry
		delete(self.replicaAddr, sdfsFileName)
		delete(self.version, sdfsFileName)
		return replicaAddrs
	}else{
		return []string{}
	}
}

func (self *fileResidenceList) Print() {
	fmt.Print("Frl: ")
	fmt.Print(self.currentMembership)
	fmt.Print(" ,")
	fmt.Print(self.replicaAddr)
	fmt.Print(",")
	fmt.Print(self.version)
	fmt.Print("\n")
}

/* Membership Change -> Re-replication */

type rereplicationAction struct {
	fromAddr []string
	toAddr string
	sdfsFileName string
}

func (self *fileResidenceList) MembershipChangeLeader(newMembership []string) []rereplicationAction {
	self.lock.Lock()
	defer self.lock.Unlock()

	noMachineFails := true
	for _, mbr := range self.currentMembership {
		if listIndexOf(mbr, newMembership) == -1{
			noMachineFails = false
		}
	}

	// if no machine fails, do nothing
	if noMachineFails{
		self.currentMembership = newMembership
		return []rereplicationAction{}
	}

	// if some machines fail, do re-replication, version numbers remain unchanged
	var retList []rereplicationAction
	for sdfsFileName, replicaAddrs := range self.replicaAddr {
		newReplicaAddrs, replicationActions := newMbrExtendReplica(replicaAddrs, newMembership, sdfsFileName)
		self.replicaAddr[sdfsFileName] = newReplicaAddrs
		// append to returned action
		for _, singleAction := range replicationActions{
			retList = append(retList, singleAction)
		}
	}

	self.currentMembership = newMembership
	return retList
}

func (self *fileResidenceList) MembershipChangeNonLeader(newMembership []string) {
	self.lock.Lock()
	defer self.lock.Unlock()

	self.currentMembership = newMembership
}

/* Clear and Update */

func (self *fileResidenceList) Update(newCurrentMembers []string, newReplicaAddr map[string][]string, newVersion map[string]int){
	// for rereplication
	self.lock.Lock()
	defer self.lock.Unlock()

	self.currentMembership = newCurrentMembers
	self.replicaAddr = newReplicaAddr
	self.version = newVersion
}

func (self *fileResidenceList) Clear(){
	// for leave
	self.lock.Lock()
	defer self.lock.Unlock()

	self.replicaAddr = make(map[string][]string)
	self.version = make(map[string]int)
}

/* Function */

func createNewFileResidenceList(startingMembershipList []string) *fileResidenceList {
	var frl fileResidenceList
	frl.currentMembership = startingMembershipList
	frl.replicaAddr = make(map[string][]string)
	frl.version = make(map[string]int)
	frl.lock = &sync.Mutex{}
	return &frl
}