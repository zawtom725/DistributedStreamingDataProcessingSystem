package sdfs

import (
	"../membership"
	"errors"
	"fmt"
	"io/ioutil"
	"net/rpc"
	"os"
	"path/filepath"
	"strconv"
)

type SdfsClient struct {
	mbrMaintainer *membership.MembershipMaintainer
}

/* Helper Functions */

func getFileVersion(fileName string) string {
	extension := filepath.Ext(fileName)
	return "Version " + extension[1:]
}

func appendFile(fileName string, version int) string {
	return fileName + "." + strconv.Itoa(version)
}

func getVersionWrite(localFileName string, fileContents [][]byte, fileNames []string) {
	file, err := os.Create(localFileName)
	if err != nil{
		fmt.Println(err)
		return
	}
	defer file.Close()

	for idx := 0; idx < len(fileNames); idx++ {
		thisFileContent := fileContents[idx]
		thisFileVersion := getFileVersion(fileNames[idx])

		file.WriteString(fmt.Sprintf("<<<<<<<<<<<<< %s <<<<<<<<<<<<<\n", thisFileVersion))
		file.Write(thisFileContent)
		file.WriteString(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n")
	}
}

/* RPC Client */

func (self *SdfsClient) getMaster(sdfsFileName string) ([]string, error) {
	// master RPC
	master, err := rpc.Dial("tcp", self.mbrMaintainer.GetCurrentLeader() + ":" + strconv.Itoa(SdfsPort))
	if err != nil {
		return []string{}, err
	}
	defer master.Close()

	// replicate files from fa18-cs425-g58-01.cs.illinois.edu to this current machine
	var rpcInput SdfsMasterGetInput = SdfsMasterGetInput{sdfsFileName}
	var rpcOutput SdfsMasterGetOutput
	err = master.Call("SdfsMasterRPC.Get", rpcInput, &rpcOutput)
	if err != nil {
		return []string{}, err
	}

	return rpcOutput.ReplicaAddrs, nil
}

func (self *SdfsClient) putMaster(sdfsFileName string) ([]string, int, error){
	// master RPC
	master, err := rpc.Dial("tcp", self.mbrMaintainer.GetCurrentLeader() + ":" + strconv.Itoa(SdfsPort))
	if err != nil {
		return []string{}, 0 ,err
	}
	defer master.Close()

	// to use the put function of master
	var rpcInput SdfsMasterPutInput = SdfsMasterPutInput{sdfsFileName}
	var rpcOutput SdfsMasterPutOutput
	err = master.Call("SdfsMasterRPC.Put", rpcInput, &rpcOutput)
	if err != nil {
		return []string{}, 0, err
	}

	return rpcOutput.ReplicaAddrs, rpcOutput.Version, nil
}

func (self *SdfsClient) deleteMaster(sdfsFileName string) ([]string, error) {
	// master RPC
	master, err := rpc.Dial("tcp", self.mbrMaintainer.GetCurrentLeader() + ":" + strconv.Itoa(SdfsPort))
	if err != nil {
		return []string{}, err
	}
	defer master.Close()

	// replicate files from fa18-cs425-g58-01.cs.illinois.edu to this current machine
	var rpcInput SdfsMasterDeleteInput = SdfsMasterDeleteInput{sdfsFileName}
	var rpcOutput SdfsMasterDeleteOutput
	err = master.Call("SdfsMasterRPC.Delete", rpcInput, &rpcOutput)
	if err != nil {
		return []string{}, err
	}

	return rpcOutput.ReplicaAddrs, nil
}

/* Methods */

func (self *SdfsClient) Get(sdfsFileName string, localFileName string) error {
	// rpc
	replicaAddrs, err := self.getMaster(sdfsFileName)
	if err != nil{
		return err
	}

	if len(replicaAddrs) == 0{
		return errors.New("file doesn't exist: " + sdfsFileName)
	}

	// rpc read one
	for _, replicaAddr := range replicaAddrs{
		replica, err := rpc.Dial("tcp", replicaAddr + ":" + strconv.Itoa(SdfsPort))
		if err != nil {
			continue
		}

		var replicaRpcInput SdfsReplicaReadInput = SdfsReplicaReadInput{sdfsFileName, 1}
		var replicaRpcOutput SdfsReplicaReadOutput
		err = replica.Call("SdfsMasterRPC.ReadFile", replicaRpcInput, &replicaRpcOutput)
		replica.Close()
		if err != nil || replicaRpcOutput.Response == ResponseFailure{
			continue
		}

		// read one success, write file
		os.Remove(localFileName)
		ioutil.WriteFile(localFileName, replicaRpcOutput.FileContents[0], 0666)
		break
	}

	return nil
}

func (self *SdfsClient) GetVersion(sdfsFileName string, numVersion int, localFileName string) error {
	// rpc
	replicaAddrs, err := self.getMaster(sdfsFileName)
	if err != nil{
		return err
	}

	if len(replicaAddrs) == 0{
		return errors.New("file doesn't exist: " + sdfsFileName)
	}

	// rpc read one
	for _, replicaAddr := range replicaAddrs{
		replica, err := rpc.Dial("tcp", replicaAddr + ":" + strconv.Itoa(SdfsPort))
		if err != nil {
			continue
		}

		var replicaRpcInput SdfsReplicaReadInput = SdfsReplicaReadInput{sdfsFileName, numVersion}
		var replicaRpcOutput SdfsReplicaReadOutput
		err = replica.Call("SdfsMasterRPC.ReadFile", replicaRpcInput, &replicaRpcOutput)
		replica.Close()
		if err != nil || replicaRpcOutput.Response == ResponseFailure{
			continue
		}

		// read one success, write file -> get version
		os.Remove(localFileName)
		getVersionWrite(localFileName, replicaRpcOutput.FileContents, replicaRpcOutput.FileNames)
		break
	}

	return nil
}

func (self *SdfsClient) Put(localFileName string, sdfsFileName string) error {
	// read file
	content, err := ioutil.ReadFile(localFileName)
	if err != nil{
		return err
	}

	// RPC
	replicaAddrs, version, err := self.putMaster(sdfsFileName)
	if err != nil{
		return err
	}

	// client handles versioning
	remoteName := appendFile(sdfsFileName, version)

	// rpc write to each replica
	for _, replicaAddr := range replicaAddrs{
		replica, err := rpc.Dial("tcp", replicaAddr + ":" + strconv.Itoa(SdfsPort))
		if err != nil {
			continue
		}

		var replicaRpcInput SdfsReplicaWriteInput = SdfsReplicaWriteInput{remoteName, content}
		var replicaRpcOutput SdfsReplicaWriteOutput
		replica.Call("SdfsMasterRPC.WriteFile", replicaRpcInput, &replicaRpcOutput)
		replica.Close()
	}

	return nil
}

func (self *SdfsClient) Delete(sdfsFileName string) error {
	// RPC
	replicaAddrs, err := self.deleteMaster(sdfsFileName)
	if err != nil{
		return err
	}

	if len(replicaAddrs) == 0{
		return errors.New("file doesn't exist: " + sdfsFileName)
	}

	// rpc write to each replica
	for _, replicaAddr := range replicaAddrs{
		replica, err := rpc.Dial("tcp", replicaAddr + ":" + strconv.Itoa(SdfsPort))
		if err != nil {
			fmt.Println(err)
			continue
		}

		var replicaRpcInput SdfsReplicaDeleteInput = SdfsReplicaDeleteInput{sdfsFileName}
		var replicaRpcOutput SdfsReplicaDeleteOutput
		replica.Call("SdfsMasterRPC.DeleteFile", replicaRpcInput, &replicaRpcOutput)

		replica.Close()
	}

	return nil
}

func (self *SdfsClient) Ls(sdfsFileName string) error {
	// master RPC
	replicaAddrs, err := self.getMaster(sdfsFileName)
	if err != nil{
		return err
	}

	if len(replicaAddrs) == 0{
		return errors.New("file doesn't exist: " + sdfsFileName)
	}

	// print out
	fmt.Println("Replicas for file: " + sdfsFileName)
	for _, replicaAddr := range replicaAddrs{
		fmt.Println(replicaAddr)
	}

	return nil
}

func (self *SdfsClient) Store() error {
	files, err := ioutil.ReadDir(ReplicaStorageDir)
	if err != nil {
		return err
	}

	// print local storage
	fmt.Println("Local Storage:")
	for _, file := range files {
		fmt.Println(file.Name())
	}

	return nil
}

func (self *SdfsClient) ClearStorage() {
	// for leave
	os.RemoveAll(ReplicaStorageDir)
	os.Mkdir(ReplicaStorageDir, 0777)
}

/* Package Func */

func CreateSdfsClient(mbrMaintainer *membership.MembershipMaintainer) *SdfsClient{
	var sdfsClient SdfsClient
	sdfsClient.mbrMaintainer = mbrMaintainer
	return &sdfsClient
}
