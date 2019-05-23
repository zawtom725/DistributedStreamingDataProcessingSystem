package sdfs

import (
	"../membership"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

/* SDFS master: RPC Server */

type SdfsMasterRPC struct {
	MbrMaintainer *membership.MembershipMaintainer
	Frl *fileResidenceList
	Lock *sync.Mutex
}

/* master RPC Methods */

// master rpc forward

func (self *SdfsMasterRPC) Put(input SdfsMasterPutInput, output *SdfsMasterPutOutput) error {
	self.Lock.Lock()
	defer self.Lock.Unlock()

	// update frl
	replicaAddrs, version := self.Frl.Put(input.SdfsFileName)

	// forward to backup masters
	allMembers := self.MbrMaintainer.GetCurrentMembers()
	for _, member := range allMembers {
		// not forwarding to self
		if member == membership.HOSTNAME{
			continue
		}

		// RPC
		backupMaster, err := rpc.Dial("tcp", member + ":" + strconv.Itoa(SdfsPort))
		if err != nil {
			continue
		}
		var rpcOutput string
		backupMaster.Call("SdfsMasterRPC.PutForward", input, &rpcOutput)
		backupMaster.Close()
	}

	// output
	output.ReplicaAddrs = replicaAddrs
	output.Version = version

	return nil
}

// being forwarded

func (self *SdfsMasterRPC) PutForward(input SdfsMasterPutInput, reply *string) error {
	self.Lock.Lock()
	defer self.Lock.Unlock()

	self.Frl.Put(input.SdfsFileName)
	*reply = ResponseSuccess

	return nil
}

// get - no forwarding

func (self *SdfsMasterRPC) Get(input SdfsMasterGetInput, output *SdfsMasterGetOutput) error {
	self.Lock.Lock()
	defer self.Lock.Unlock()

	// update frl
	sdfsFileName := input.SdfsFileName
	replicaAddrs := self.Frl.Get(sdfsFileName)

	// output
	output.ReplicaAddrs = replicaAddrs

	return nil
}

// delete - forwarding

func (self *SdfsMasterRPC) Delete(input SdfsMasterDeleteInput, output *SdfsMasterDeleteOutput) error {
	self.Lock.Lock()
	defer self.Lock.Unlock()

	// update frl
	sdfsFileName := input.SdfsFileName
	replicaAddrs := self.Frl.Delete(sdfsFileName)

	// forward to backup masters
	allMembers := self.MbrMaintainer.GetCurrentMembers()
	for _, member := range allMembers {
		// not forwarding to self
		if member == membership.HOSTNAME{
			continue
		}

		// RPC
		backupMaster, err := rpc.Dial("tcp", member + ":" + strconv.Itoa(SdfsPort))
		if err != nil {
			continue
		}
		var rpcOutput string
		backupMaster.Call("SdfsMasterRPC.DeleteForward", input, &rpcOutput)
		backupMaster.Close()
	}

	// output
	output.ReplicaAddrs = replicaAddrs

	return nil
}

// being forwarded

func (self *SdfsMasterRPC) DeleteForward(input SdfsMasterDeleteInput, reply *string) error {
	self.Lock.Lock()
	defer self.Lock.Unlock()

	self.Frl.Delete(input.SdfsFileName)
	*reply = ResponseSuccess

	return nil
}

// update forward

func (self *SdfsMasterRPC) UpdateForward(input SdfsMasterRerepForwardInput, reply *string) error {
	self.Lock.Lock()
	defer self.Lock.Unlock()

	self.Frl.Update(input.CurrentMembers, input.ReplicaAddr, input.Version)
	*reply = ResponseSuccess

	return nil
}

/* Re-replication Thread */

func (self *SdfsMasterRPC) rereplicationThread(){
	// wait to enter re-replication process
	lastLeader := self.MbrMaintainer.GetCurrentLeader()
	lastBackupLeader := self.MbrMaintainer.GetBackupLeader()
	time.Sleep(time.Duration(ReconcileMsInterval) * time.Millisecond)

	for{
		// potential leader update, wait for a round to re-replicate
		currentLeader := self.MbrMaintainer.GetCurrentLeader()
		currentBackupLeader := self.MbrMaintainer.GetBackupLeader()
		if currentLeader != lastLeader || currentBackupLeader != lastBackupLeader {
			lastLeader = currentLeader
			lastBackupLeader = currentBackupLeader
			time.Sleep(time.Duration(ReconcileMsInterval) * time.Millisecond)
			continue
		}

		currentMembership := self.MbrMaintainer.GetCurrentMembers()

		// non leader action
		if currentLeader != membership.HOSTNAME{
			// backup master update self - and others as well to make frl function properly
			self.Frl.MembershipChangeNonLeader(currentMembership)
			time.Sleep(time.Duration(ReconcileMsInterval) * time.Millisecond)
			continue
		}

		// no leader change, I am the current leader, see the current membership list, empty if I quits
		if len(currentMembership) == 0 {
			self.Frl.Clear()
			time.Sleep(time.Duration(ReconcileMsInterval) * time.Millisecond)
			continue
		}

		// change start
		self.Lock.Lock()

		// test change
		rerepActions := self.Frl.MembershipChangeLeader(currentMembership)

		// re-replication needed
		if len(rerepActions) > 0 {
			// perform the re-replication
			for _, action := range rerepActions {
				// invoke client RPC
				replica, err := rpc.Dial("tcp", action.toAddr + ":" + strconv.Itoa(SdfsPort))
				if err != nil {
					continue
				}

				// replicate files
				var rpcInput SdfsReplicaReplicateInput = SdfsReplicaReplicateInput{action.fromAddr, action.sdfsFileName}
				var rpcOutput SdfsReplicaReplicateOutput
				err = replica.Call("SdfsMasterRPC.ReplicateFile", rpcInput, &rpcOutput)
				replica.Close()
				if err != nil{
					fmt.Println(err)
				}
			}

			// forward to backup master
			if currentBackupLeader != "None"{
				backupMaster, err := rpc.Dial("tcp", currentBackupLeader + ":" + strconv.Itoa(SdfsPort))
				if err != nil {
					continue
				}
				var rpcInput SdfsMasterRerepForwardInput = SdfsMasterRerepForwardInput{
					self.Frl.currentMembership, self.Frl.replicaAddr, self.Frl.version}
				var rpcOutput string
				backupMaster.Call("SdfsMasterRPC.UpdateForward", rpcInput, &rpcOutput)
				backupMaster.Close()
			}
		}

		// change end
		self.Lock.Unlock()

		time.Sleep(time.Duration(ReconcileMsInterval) * time.Millisecond)
	}
}

/* Function */

func LaunchSdfsMasterAndReplica(mbrMaintainer *membership.MembershipMaintainer) error {
	// recreate the storage
	os.RemoveAll(ReplicaStorageDir)
	os.Mkdir(ReplicaStorageDir, 0777)

	// recreate the machine log
	os.Remove(ReplicaLogFile)
	os.OpenFile(ReplicaLogFile, os.O_RDONLY | os.O_CREATE, 0666)

	// rpc
	var m SdfsMasterRPC
	m.Frl = createNewFileResidenceList(mbrMaintainer.GetCurrentMembers())
	m.MbrMaintainer = mbrMaintainer
	m.Lock = &sync.Mutex{}

	// create rpcer
	handler := rpc.NewServer()
	handler.Register(&m)
	listen, err := net.Listen("tcp",  ":" + strconv.Itoa(SdfsPort))
	if err != nil {
		return err
	}

	// start http serve
	go func(){
		for {
			context, err := listen.Accept()
			if err != nil {
				fmt.Println(err)
				continue
			}
			go handler.ServeConn(context)
		}
	}()

	// rereplication thread
	go m.rereplicationThread()

	return nil
}

