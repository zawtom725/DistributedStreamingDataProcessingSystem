package sdfs

import (
	"fmt"
	"io/ioutil"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
)

const ReplicaStorageDir string = "./fileStorage"
const ReplicaLogFile string = "machine.log"

const ResponseFailure string = "Failed"
const ResponseSuccess string = "Okay"

/* Helper Functions */

func max(x, y int) int {
	if x < y {
		return y
	}else{
		return x
	}
}

func writeLog(writeContent string) {
	f, err := os.OpenFile(ReplicaLogFile, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("Cannot open file:", ReplicaLogFile)
		return
	}
	defer f.Close()

	// write f
	f.WriteString(writeContent + "\n")
}

func removeExtension(sdfsFileName string) string {
	extension := filepath.Ext(sdfsFileName)
	return sdfsFileName[:len(sdfsFileName) - len(extension)]
}

// find all the versioned file
func findAllFiles(fileName string) ([]string, error) {
	files, err := filepath.Glob(ReplicaStorageDir + "/" + fileName + ".*")
	if err != nil {
		return []string{}, err
	}

	if files == nil{
		return []string{}, nil
	}else{
		return files, nil
	}
}

func writeFile(fileName string, fileContent []byte) error{
	err := ioutil.WriteFile(ReplicaStorageDir + "/" + fileName, fileContent, 0666)
	return err
}

func sortFileNames(fileNames []string) []string {
	fileMap := map[int]string{}
	var versionIdxs []int
	for i := 0; i < len(fileNames); i++  {
		idx, err := strconv.Atoi(filepath.Ext(fileNames[i])[1:])
		fileMap[idx] = fileNames[i]
		versionIdxs = append(versionIdxs, idx)

		if err != nil {
			fmt.Print(err)
		}
	}
	sort.Ints(versionIdxs)
	var sortedFileNames []string
	for i := 0; i < len(versionIdxs); i++ {
		sortedFileNames = append(sortedFileNames, fileMap[versionIdxs[i]])
	}
	return sortedFileNames
}


/* Methods */

func (self *SdfsMasterRPC) ReadFile(input SdfsReplicaReadInput, output *SdfsReplicaReadOutput) error {
	// logging
	writeLog("Reading file: " + input.SdfsFileName)

	// find file
	files, err := findAllFiles(input.SdfsFileName)
	if err != nil || len(files) == 0{
		fmt.Println("no such file")
		writeLog("Reading file" + input.SdfsFileName + "no such file")
		output.Response = ResponseFailure
		return nil
	}
	files = sortFileNames(files)

	// read file content
	numVersions := input.NumVersions
	for i := len(files) - 1; i >= max(0, (len(files) - numVersions)); i-- {
		content, err := ioutil.ReadFile(files[i])
		if err != nil {
			output.Response = ResponseFailure
			return err
		}
		output.FileContents = append(output.FileContents, content)
		output.FileNames = append(output.FileNames, files[i])
	}
	output.Response = ResponseSuccess

	return nil
}

func (self *SdfsMasterRPC) WriteFile(input SdfsReplicaWriteInput, output *SdfsReplicaWriteOutput) error {
	// logging
	writeLog("Writing file: " + input.SdfsFileName)

	// find files -> for versioning
	files, _ := findAllFiles(removeExtension(input.SdfsFileName))
	files = sortFileNames(files)

	// delete to remaining 4 versions
	if len(files) >= 5 {
		for i := 0; i < len(files) - 4; i++ {
			err := os.Remove(files[i])
			if err != nil {
				fmt.Println(err)
				continue
			}
		}
	}

	// write file
	err := writeFile(input.SdfsFileName, input.FileContent)
	if err != nil {
		fmt.Println("Write file failed")
		writeLog("write file" + input.SdfsFileName + "failed")
		output.Response = ResponseFailure
		return err
	}
	output.Response = ResponseSuccess

	return nil
}

func (self *SdfsMasterRPC) DeleteFile(input SdfsReplicaDeleteInput, output *SdfsReplicaDeleteOutput) error {
	// logging
	writeLog("Removing file: " + input.SdfsFileName)

	// remove
	files, _ := findAllFiles(input.SdfsFileName)
	for _, f := range files {
		os.Remove(f)
	}
	output.Response = ResponseSuccess

	return nil
}

func (self *SdfsMasterRPC) ReplicateFile(input SdfsReplicaReplicateInput, output *SdfsReplicaReplicateOutput) error {
	// logging
	writeLog("Re-replicate " + input.SdfsFileName)

	output.Response = ResponseFailure

	// read from any existing host
	for _, host := range input.FromHost{
		// remote read RPC
		client, err := rpc.Dial("tcp", host + ":" + strconv.Itoa(SdfsPort))
		if err != nil {
			continue
		}

		// replicate files
		var rpcInput SdfsReplicaReadInput = SdfsReplicaReadInput{input.SdfsFileName, 5}
		var rpcOutput SdfsReplicaReadOutput
		err = client.Call("SdfsMasterRPC.ReadFile", rpcInput, &rpcOutput)
		client.Close()
		if err != nil || rpcOutput.Response == ResponseFailure {
			continue
		}

		// write to local
		for i := 0; i < len(rpcOutput.FileNames); i++ {
			err := ioutil.WriteFile(rpcOutput.FileNames[i], rpcOutput.FileContents[i], 0666)
			if err != nil {
				fmt.Println(err)
				continue
			}
		}
		output.Response = ResponseSuccess
		break
	}

	return nil
}
