package crane

import (
	"../membership"
	"../sdfs"
	"fmt"
	"net"
	"net/rpc"
	"strconv"
)

type CraneMasterSupervisorRPC struct {
	mbrMaintainer 	*membership.MembershipMaintainer
	master     		*CraneMaster
	supervisor 		*CraneSupervisor
}

/* master rpc */

// exposed to client

func (this *CraneMasterSupervisorRPC) SubmitJob(input SubmitJobInput, output *SubmitJobOutput) error {
	// process topology
	fmt.Println("CraneRPC: Master SubmitJob")

	err := this.master.ProcessTopology(&input.ThisTask)
	if err != nil {
		output.Response = RPC_RESPONSE_FAILURE
		return nil
	}

	// forward to backup, RPC
	backupMasterHost := this.mbrMaintainer.GetBackupLeader()
	if backupMasterHost != "None" {
		// dial
		backupMaster, err := rpc.Dial("tcp", backupMasterHost + ":" + strconv.Itoa(CranePort))
		// rpc
		if err == nil {
			var rpcInput SubmitJobBackupSyncInput = SubmitJobBackupSyncInput{input.ThisTask}
			var rpcOutput SubmitJobBackupSyncOutput
			backupMaster.Call("CraneMasterSupervisorRPC.SubmitJobBackupSync", rpcInput, &rpcOutput)
			backupMaster.Close()
		}
	}

	// start job
	this.master.StartJob()

	// response
	output.Response = RPC_RESPONSE_SUCCESS

	return nil
}

func (this *CraneMasterSupervisorRPC) CheckJobProgress(input CheckJobProgressInput, output *CheckJobProgressOutput) error {
	output.Progress = this.master.GetProgress()
	output.AllFileReady = this.master.AllFileReady()
	output.FileNames = this.master.resultFileNames
	output.Response = RPC_RESPONSE_SUCCESS

	return nil
}

func (this *CraneMasterSupervisorRPC) StopJob(input StopJobInput, output *StopJobOutput) error {
	fmt.Println("CraneRPC: Master StopJob")

	ret := this.master.FinishJob()

	if ret {
		output.Response = RPC_RESPONSE_SUCCESS

		// forward to backup, RPC
		backupMasterHost := this.mbrMaintainer.GetBackupLeader()
		if backupMasterHost != "None" {
			// dial
			backupMaster, err := rpc.Dial("tcp", backupMasterHost + ":" + strconv.Itoa(CranePort))
			// rpc
			if err == nil {
				var rpcInput StopJobSyncInput = StopJobSyncInput{}
				var rpcOutput StopJobSyncOutput
				backupMaster.Call("CraneMasterSupervisorRPC.StopJobSync", rpcInput, &rpcOutput)
				backupMaster.Close()
			}
		}
	}else{
		output.Response = RPC_RESPONSE_FAILURE
	}

	return nil
}

// exposed to supervisor / worker

func (this *CraneMasterSupervisorRPC) UpdateJobProgress(input UpdateJobProgressInput, output *UpdateJobProgressOutput) error {
	fmt.Println("CraneRPC: UpdateJobProgress():", input.Progress)
	// store progress
	this.master.SetProgress(input.Progress)
	// success
	output.Response = RPC_RESPONSE_SUCCESS
	return nil
}

func (this *CraneMasterSupervisorRPC) FileReady(input FileReadyInput, output *FileReadyOutput) error {
	fmt.Println("CraneRPC: FileReady()", input.FileName)
	// file ready
	this.master.FileReady(input.FileName)

	// success
	output.Response = RPC_RESPONSE_SUCCESS

	return nil
}

// exposed as backup sync

func (this *CraneMasterSupervisorRPC) SubmitJobBackupSync(input SubmitJobBackupSyncInput, output *SubmitJobBackupSyncOutput) error {
	// simply process
	fmt.Println("CraneRPC: SubmitJobBackupSync()")

	this.master.BackupJob()

	this.master.ProcessTopology(&input.ThisTask)

	fmt.Println("Backup Tasks:", this.master.currentTasks)

	output.Response = RPC_RESPONSE_SUCCESS
	return nil
}

func (this *CraneMasterSupervisorRPC) StopJobSync(input StopJobSyncInput, output *StopJobSyncOutput) error {
	fmt.Println("CraneRPC: StopJobSync()")

	this.master.FinishBackupJob()
	output.Response = RPC_RESPONSE_SUCCESS
	return nil
}

/* supervisor rpc */

func (this *CraneMasterSupervisorRPC) StartTask(input StartTaskInput, output *StartTaskOutput) error {
	err := this.supervisor.StartWorker(&input.ThisTask)
	if err != nil {
		fmt.Println(err)
		output.Response = RPC_RESPONSE_FAILURE
	}else{
		output.Response = RPC_RESPONSE_SUCCESS
	}
	return nil
}

func (this *CraneMasterSupervisorRPC) TerminateTask(input TerminateTaskInput, output *TerminateTaskOutput) error {
	this.supervisor.EndWorker(&input.ThisTask)
	output.Response = RPC_RESPONSE_SUCCESS
	return nil
}

/* start */

func LaunchCraneMasterSupervisor(sdfsClient *sdfs.SdfsClient, mbrMaintainer *membership.MembershipMaintainer) error {
	var ms CraneMasterSupervisorRPC
	ms.mbrMaintainer = mbrMaintainer
	ms.master = CreateCraneMaster(mbrMaintainer)
	ms.supervisor = CreateCraneSupervisor(sdfsClient, mbrMaintainer)

	// start rpc server
	handler := rpc.NewServer()
	handler.Register(&ms)
	listen, err := net.Listen("tcp",  ":" + strconv.Itoa(CranePort))
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

	return nil
}
