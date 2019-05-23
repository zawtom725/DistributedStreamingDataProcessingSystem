package sdfs

/* SDFS master Protocal */

const SdfsPort int = 6666

const ReconcileMsInterval int = 2000

// put

type SdfsMasterPutInput struct {
	SdfsFileName string
}

type SdfsMasterPutOutput struct {
	ReplicaAddrs []string
	Version int
}

// get, get-version, ls

type SdfsMasterGetInput struct {
	SdfsFileName string
}

type SdfsMasterGetOutput struct {
	ReplicaAddrs []string
}

// delete

type SdfsMasterDeleteInput struct {
	SdfsFileName string
}

type SdfsMasterDeleteOutput struct {
	ReplicaAddrs []string
}

// re-replication frl forward

type SdfsMasterRerepForwardInput struct {
	CurrentMembers []string
	ReplicaAddr map[string][]string
	Version map[string]int
}

/* SDFS Replica Protocal */

// write

type SdfsReplicaWriteInput struct {
	SdfsFileName string
	FileContent []byte
}

type SdfsReplicaWriteOutput struct {
	Response string
}

// read

type SdfsReplicaReadInput struct {
	SdfsFileName string
	NumVersions int
}

type SdfsReplicaReadOutput struct {
	FileContents [][]byte
	FileNames []string
	Response string
}

// replica

type SdfsReplicaReplicateInput struct {
	FromHost []string
	SdfsFileName string
}

type SdfsReplicaReplicateOutput struct {
	Response string
}

// delete

type SdfsReplicaDeleteInput struct {
	SdfsFileName string
}

type SdfsReplicaDeleteOutput struct {
	Response string
}

