package main

import (
"../../ipcPipe"
"strings"
)

// define bolt

type thisBolt1 int

func (this *thisBolt1) Type() int {
	return ipcPipe.TYPE_TRANSFORM
}

func (this *thisBolt1) GetJoinFileName() string {
	// if the operation isn't join, this function will not be called
	return ""
}

func (this *thisBolt1) ProcessTuple(inputTuple ipcPipe.NamedTuple) []ipcPipe.NamedTuple {
	// needed by map/transform
	if timeStampStr, exist := inputTuple["timeStamp"]; exist {
		splitted := strings.Split(timeStampStr, " ")
		if len(splitted) == 2 && len(splitted[1]) >= 2 {
			outStr := splitted[1][0:2]

			var outputList []ipcPipe.NamedTuple
			outputTuple := make(ipcPipe.NamedTuple)
			outputTuple["timeStamp"] = outStr
			outputList = append(outputList, outputTuple)
			return outputList
		}
	}

	return []ipcPipe.NamedTuple{}
}

// the function -> need not to be changed

func main(){
	var tb thisBolt1
	ipcPipe.RunBolt(&tb)
}