package crane

import(
	"../membership"
	"../sdfs"
	"sync"
)

// const

const POOL_THREAD_MAXIMUM int = 512

// struct

type CraneSupervisor struct {
	lock *sync.Mutex

	// membership list
	mbrMaintainer *membership.MembershipMaintainer
	sdfsClient *sdfs.SdfsClient

	// TopId -> currentWorker
	topIdToWorker map[int]*CraneWorker
}

// package method

func (this *CraneSupervisor) StartWorker(task *Tasklet) error {
	this.lock.Lock()
	defer this.lock.Unlock()

	// create the worker
	worker := CreateCraneWorker(this.sdfsClient, this.mbrMaintainer)

	// keep track
	this.topIdToWorker[task.TopId] = worker

	// prepare the task
	var workerTask WorkerTasklet
	workerTask.JobType = task.JobType
	workerTask.SelfPort = task.ListenPort
	workerTask.SourceFileName = task.SourceFileName
	workerTask.AdditionalFileName = task.AdditionalFileName
	workerTask.ChildHosts = task.ChildHosts

	// max pooling
	workerTask.MaxPooling = POOL_THREAD_MAXIMUM / task.Concurrency

	// run work
	return worker.StartTask(&workerTask)
}

func (this *CraneSupervisor) EndWorker(task *Tasklet) {
	this.lock.Lock()
	defer this.lock.Unlock()

	// find the worker
	for topId, worker := range this.topIdToWorker {
		if topId == task.TopId {
			worker.TerminateTask()
		}
	}

	// map delete to free resource
	delete(this.topIdToWorker, task.TopId)
}

// package function

func CreateCraneSupervisor(sdfsClient *sdfs.SdfsClient, mbrMaintainer *membership.MembershipMaintainer) *CraneSupervisor {
	var supervisor CraneSupervisor
	supervisor.lock = &sync.Mutex{}
	supervisor.mbrMaintainer = mbrMaintainer
	supervisor.sdfsClient = sdfsClient

	supervisor.topIdToWorker = make(map[int]*CraneWorker)
	return &supervisor
}


