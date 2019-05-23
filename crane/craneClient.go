package crane

import (
	"../membership"
	"../sdfs"
	"errors"
	"fmt"
	"io/ioutil"
	"net/rpc"
	"os/exec"
	"strconv"
	"time"
)

// crane client

type CraneClient struct {
	mbrMaintainer 	*membership.MembershipMaintainer
	sdfsClient 		*sdfs.SdfsClient
}

// package methods

func (this *CraneClient) submitJobSdfs(applicationFolderName string, topology *Topology) error {
	for _, topElement := range topology.Element {
		fmt.Println("CraneClient: submitJobSdfs() compile", topElement.SourceFileName)

		// compile application file
		appFilePath := "./" + applicationFolderName + "/" + topElement.SourceFileName
		goFilePath := appFilePath + ".go"
		cmd := exec.Command("go", "build", "-o", appFilePath, goFilePath)
		err := cmd.Run()
		if err != nil {
			return err
		}

		// submit application binary
		err = this.sdfsClient.Put(appFilePath, topElement.SourceFileName)
		if err != nil {
			return err
		}

		// submit supplementary file name
		if topElement.Type != TYPE_SINK && topElement.AdditionalFileName != "" {
			additionalFilePath := "./" + applicationFolderName + "/" + topElement.AdditionalFileName
			err = this.sdfsClient.Put(additionalFilePath, topElement.AdditionalFileName)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// check job progress -> master rpc

func (this *CraneClient) checkJobProgress() (float64, bool, []string, error) {
	// rpc : ask master for progress
	masterHostName := this.mbrMaintainer.GetCurrentLeader()
	master, err := rpc.Dial("tcp", masterHostName + ":" + strconv.Itoa(CranePort))
	if err != nil {
		return -1.0, false, nil, err
	}
	defer master.Close()

	var rpcInput CheckJobProgressInput = CheckJobProgressInput{}
	var rpcOutput CheckJobProgressOutput
	err = master.Call("CraneMasterSupervisorRPC.CheckJobProgress", rpcInput, &rpcOutput)
	if err != nil {
		return -1.0, false, nil, err
	}

	return rpcOutput.Progress, rpcOutput.AllFileReady, rpcOutput.FileNames, nil
}

// check result files -> master rpc

func (this *CraneClient) stopJob() error {
	// store result file on local fs
	masterHostName := this.mbrMaintainer.GetCurrentLeader()
	master, err := rpc.Dial("tcp", masterHostName + ":" + strconv.Itoa(CranePort))
	if err != nil {
		return err
	}
	defer master.Close()

	var rpcInput StopJobInput = StopJobInput{}
	var rpcOutput StopJobOutput
	err = master.Call("CraneMasterSupervisorRPC.StopJob", rpcInput, &rpcOutput)

	return err
}

func (this *CraneClient) SubmitJob(applicationFolderName string) error {
	topology := CreateTopology("./" + applicationFolderName + "/topology.txt")
	if topology == nil {
		return errors.New("topology parsing error")
	}

	err := this.submitJobSdfs(applicationFolderName, topology)
	if err != nil {
		return err
	}

	masterHostName := this.mbrMaintainer.GetCurrentLeader()
	master, err := rpc.Dial("tcp", masterHostName + ":" + strconv.Itoa(CranePort))
	if err != nil {
		return err
	}

	var rpcInput SubmitJobInput = SubmitJobInput{*topology}
	var rpcOutput SubmitJobOutput
	err = master.Call("CraneMasterSupervisorRPC.SubmitJob", rpcInput, &rpcOutput)
	if err != nil {
		return err
	}
	master.Close()

	if rpcOutput.Response == RPC_RESPONSE_FAILURE {
		return errors.New("master submission failure")
	}

	// keep track of progress, display every two seconds
	for {
		progress, partialResultAvailable, fileNames, err := this.checkJobProgress()

		if err != nil {
			continue
		}

		fmt.Printf("Application [%s] Progress: [%.2f]%%\n", applicationFolderName, progress * 100.0)

		if partialResultAvailable {
			for _, fileName := range fileNames {
				// get partial result
				this.sdfsClient.Get(fileName, fileName)
				fileContent, err := ioutil.ReadFile(fileName)
				if err != nil {
					fmt.Println(err)
					continue
				}
				// print
				fmt.Printf("Application [%s] Output [%s] Partial Result:\n%s\n", applicationFolderName,
					fileName, fileContent)
			}
		}

		// infinite loop as it is a streaming job

		/*
		if progress >= 1 && partialResultAvailable {
			fmt.Printf("Application [%s] Finished.\n", applicationFolderName)
			break
		}
		*/

		time.Sleep(time.Second * time.Duration(10))
	}

	// stop remote job
	this.stopJob()
	// done
	return nil
}

// package function

func CreateCraneClient(mbrMaintainer *membership.MembershipMaintainer, sdfsClient *sdfs.SdfsClient) *CraneClient {
	var client CraneClient
	client.mbrMaintainer = mbrMaintainer
	client.sdfsClient = sdfsClient
	return &client
}
