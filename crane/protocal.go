package crane

// port

const CranePort int = 8888

// the base of worker port, will be incremented accordingly
const CraneWorkerPortBase int = 8889

// response str

const RPC_RESPONSE_SUCCESS string = "success"
const RPC_RESPONSE_FAILURE string = "failure"

// RPC protocal

// submit job

type SubmitJobInput struct {
	ThisTask Topology
}

type SubmitJobOutput struct {
	Response string
}

// check job progress

type CheckJobProgressInput struct {

}

type CheckJobProgressOutput struct {
	Progress float64
	AllFileReady bool
	FileNames []string
	Response string
}

// get result files

type StopJobInput struct {

}

type StopJobOutput struct {
	Response string
}

// update job progress

type UpdateJobProgressInput struct {
	Progress float64
}

type UpdateJobProgressOutput struct {
	Response string
}

// file ready

type FileReadyInput struct {
	FileName string
}

type FileReadyOutput struct {
	Response string
}

// SubmitJobBackupSync

type SubmitJobBackupSyncInput struct {
	ThisTask Topology
}

type SubmitJobBackupSyncOutput struct {
	Response string
}

// GetResultFilesSync

type StopJobSyncInput struct {

}

type StopJobSyncOutput struct {
	Response string
}

// StartTask

type StartTaskInput struct {
	ThisTask Tasklet
}

type StartTaskOutput struct {
	Response string
}

// TerminateTask

type TerminateTaskInput struct {
	ThisTask Tasklet
}

type TerminateTaskOutput struct {
	Response string
}


