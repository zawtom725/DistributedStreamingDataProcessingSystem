package crane

import (
	"../membership"
	"fmt"
	"net/rpc"
	"strconv"
	"sync"
	"time"
)

// const

const CHECK_MBR_INTERVAL_MS int = 2000

// struct

type CraneMaster struct {
	// sync lock
	lock *sync.Mutex

	// membership related
	mbrMaintainer  *membership.MembershipMaintainer
	lastMembership []string

	topology 	   *Topology // store the topology in case of failure
	isJobRunning   	bool      // job info -> sequential job processing,
	jobProgress 	float64

	// current tasks and result file names to query
	currentTasks    []Tasklet
	resultFileNames []string
	resultFileReady map[string]bool  // see if each file name is ready

	// used to terminate mbr checking routine
	checkerTerminateChnl chan int
	checkerDoneChnl chan int
}

// the struct to record each running task on the cluster
type Tasklet struct {
	// job info
	TopId              int    // corresponding topology id
	JobType            int    // spout/sink/bolt
	SourceFileName     string // the main executable/spout data source
	AdditionalFileName string // for spout: data file, for join: the name of the static db, for sink: the output file

	// VM info
	HostName   string   // the VM on which the tasks run
	ListenPort int      // bolt/sink: the port on which the listening thread runs
	ChildHosts [][]string // a list of list of full names for child hosts, parallelism considered

	// concurrency
	Concurrency int
}

// package function

// result file

func listContain(l []string, target string) bool {
	for _, lEle := range l {
		if lEle == target {
			return true
		}
	}
	return false
}

func (this *CraneMaster) FileReady(fileName string) {
	this.lock.Lock()
	defer this.lock.Unlock()

	if listContain(this.resultFileNames, fileName) {
		this.resultFileReady[fileName] = true
	}
}

func (this *CraneMaster) AllFileReady() bool {
	this.lock.Lock()
	defer this.lock.Unlock()

	for _, fileName := range this.resultFileNames {
		if !this.resultFileReady[fileName] {
			return false
		}
	}

	return true
}

// progress

func (this *CraneMaster) GetProgress() float64 {
	this.lock.Lock()
	defer this.lock.Unlock()

	return this.jobProgress
}

func (this *CraneMaster) SetProgress(progress float64) {
	this.lock.Lock()
	defer this.lock.Unlock()

	// handle async arrival
	if progress > this.jobProgress {
		this.jobProgress = progress
	}
}

func (this *CraneMaster) ProcessTopology(topology *Topology) error {
	this.lock.Lock()
	defer this.lock.Unlock()

	// store the topology
	this.topology = topology

	// job progress
	this.jobProgress = 0.0

	// membership
	currentMembers := this.mbrMaintainer.GetCurrentMembers()
	this.lastMembership = currentMembers

	// record / init
	this.currentTasks = topology.Schedule(currentMembers)
	this.resultFileNames = topology.GetResultFileNames()

	// readiness map
	this.resultFileReady = make(map[string]bool)
	for _, name := range this.resultFileNames {
		this.resultFileReady[name] = false
	}

	return nil
}

// mbr change checker & backup master thread

func listEqual(a []string, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for idx, aVal := range a {
		if aVal != b[idx] {
			return false
		}
	}
	return true
}

func (this *CraneMaster) checkerThread() {
	for {
		select {
		case <-this.checkerTerminateChnl:
			this.checkerDoneChnl <- 5
			fmt.Println("CraneMaster: checkerThread() channel terminate")
			return
		default:
			// check for membership changes
			currentMembership := this.mbrMaintainer.GetCurrentMembers()

			if !listEqual(currentMembership, this.lastMembership) {
				fmt.Println("CraneMaster: checkerThread() restart job")
				// update membership
				this.lastMembership = currentMembership
				// terminate current job
				this.terminateAllJobs()
				// reprocess
				this.ProcessTopology(this.topology)
				// restart jobs
				this.startAllJobs()
			}

			time.Sleep(time.Millisecond * time.Duration(CHECK_MBR_INTERVAL_MS))
		}
	}
}

func (this *CraneMaster) backupMasterThread() {
	for {
		select {
		case <-this.checkerTerminateChnl:
			this.checkerDoneChnl <- 5
			fmt.Println("CraneMaster: backupMasterThread() channel terminate")
			return
		default:
			// only do job if I become the primary master
			if membership.HOSTNAME == this.mbrMaintainer.GetCurrentLeader(){

				currentMembership := this.mbrMaintainer.GetCurrentMembers()
				// check for membership changes
				if !listEqual(currentMembership, this.lastMembership) {
					fmt.Println("CraneMaster: backupMasterThread() restart job")
					// update membership
					this.lastMembership = currentMembership
					// terminate current job
					this.terminateAllJobs()
					// reprocess
					this.ProcessTopology(this.topology)
					// restart jobs
					this.startAllJobs()
				}
			}else{

				currentMembership := this.mbrMaintainer.GetCurrentMembers()
				// check for membership changes
				if !listEqual(currentMembership, this.lastMembership){
					// update membership
					this.lastMembership = currentMembership
					// reprocess
					this.ProcessTopology(this.topology)
				}

			}

			time.Sleep(time.Millisecond * time.Duration(CHECK_MBR_INTERVAL_MS))
		}
	}
}

// job run and terminate -> regular execution

func sendTask(task Tasklet) error {
	// dial
	supervisor, err := rpc.Dial("tcp", task.HostName+ ":" + strconv.Itoa(CranePort))
	if err != nil{
		return err
	}
	defer supervisor.Close()

	// rpc
	var rpcInput StartTaskInput = StartTaskInput{task}
	var rpcOutput StartTaskOutput
	err = supervisor.Call("CraneMasterSupervisorRPC.StartTask", rpcInput, &rpcOutput)
	if err != nil {
		return err
	}

	return nil
}

func (this *CraneMaster) startAllJobs(){
	// rpc send job -> three pass: first sink, then bolt, finally spout
	for _, task := range this.currentTasks {
		if task.JobType == TYPE_SINK {
			fmt.Println("CraneMaster: startLAllJobs() starting sink task:", task)
			sendTask(task)
		}
	}
	for _, task := range this.currentTasks {
		if task.JobType == TYPE_BOLT {
			fmt.Println("CraneMaster: startLAllJobs() starting bolt task:", task)
			sendTask(task)
		}
	}
	for _, task := range this.currentTasks {
		if task.JobType == TYPE_SPOUT {
			fmt.Println("CraneMaster: startLAllJobs() starting spout task:", task)
			sendTask(task)
		}
	}
}

func (this *CraneMaster) StartJob() {
	this.lock.Lock()
	defer this.lock.Unlock()

	// fire up the checker thread
	this.checkerTerminateChnl = make(chan int)
	this.checkerDoneChnl = make(chan int)
	go this.checkerThread()

	// start the job
	this.startAllJobs()

	// flag
	this.isJobRunning = true
}

func (this *CraneMaster) terminateAllJobs(){
	// rpc terminate job
	for _, task := range this.currentTasks {
		fmt.Println("CraneMaster: terminateAllJobs() terminate task:", task)
		// dial
		supervisor, err := rpc.Dial("tcp", task.HostName+ ":" + strconv.Itoa(CranePort))
		if err != nil{
			continue
		}
		// rpc
		var rpcInput TerminateTaskInput = TerminateTaskInput{task}
		var rpcOutput TerminateTaskOutput
		supervisor.Call("CraneMasterSupervisorRPC.TerminateTask", rpcInput, &rpcOutput)
		supervisor.Close()
	}
}

func (this *CraneMaster) FinishJob() bool {
	if this.isJobRunning && this.AllFileReady() {
		this.lock.Lock()
		defer this.lock.Unlock()

		// terminate jobs
		this.terminateAllJobs()

		// terminate checker thread
		this.checkerTerminateChnl <- 5
		<- this.checkerDoneChnl

		// return the file content
		this.isJobRunning = false
		return true
	}else{
		return false
	}
}

func (this *CraneMaster) BackupJob() {
	this.lock.Lock()
	defer this.lock.Unlock()

	// fire up the backup job
	this.checkerTerminateChnl = make(chan int)
	this.checkerDoneChnl = make(chan int)
	go this.backupMasterThread()

	// flag also
	this.isJobRunning = true
}

func (this *CraneMaster) FinishBackupJob() {
	this.lock.Lock()
	defer this.lock.Unlock()

	// terminate checker thread
	if this.isJobRunning {
		this.checkerTerminateChnl <- 5
		<- this.checkerDoneChnl

		// return the file content
		this.isJobRunning = false
	}
}

// package methods

func CreateCraneMaster(mbrMaintainer *membership.MembershipMaintainer) *CraneMaster {
	var master CraneMaster
	master.mbrMaintainer = mbrMaintainer
	master.lock = &sync.Mutex{}

	master.isJobRunning = false
	return &master
}
