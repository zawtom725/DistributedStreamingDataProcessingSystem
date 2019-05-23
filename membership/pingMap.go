package membership

import (
	"sync"
)

type PingMap struct {
	pingMap map[string]int
	lock    *sync.Mutex
}

/* Methods */

func (self *PingMap) removeFromPingMap(hostName string) {
	// remove map entry
	self.lock.Lock()
	defer self.lock.Unlock()

	delete(self.pingMap, hostName)
}

func (self *PingMap) pingedTargetUpdateMap(hostName string){
	// add to map if not exist
	self.lock.Lock()
	defer self.lock.Unlock()

	if _, present := self.pingMap[hostName]; present == false {
		self.pingMap[hostName] = 0
	}
}

func (self *PingMap) checkExistence(hostName string) bool {
	self.lock.Lock()
	defer self.lock.Unlock()

	_, exists := self.pingMap[hostName]
	return exists
}

func (self *PingMap) pingMapIncrement() {
	self.lock.Lock()
	defer self.lock.Unlock()

	for key, _ := range self.pingMap {
		self.pingMap[key] += 1
	}
}

func (self *PingMap) checkTimeOut(mbrList *MembershipList) {
	self.lock.Lock()
	defer self.lock.Unlock()

	for hostName, round := range self.pingMap {
		if round > TimeoutRound {
			// timeout - update membership list
			mbrList.TimeOut(hostName)
			delete(self.pingMap, hostName)
		}
	}
}

/* package function */

func createNewPingMap() *PingMap {
	var pingMap PingMap
	pingMap.lock = &sync.Mutex{}
	pingMap.pingMap = make(map[string]int)
	return &pingMap
}
