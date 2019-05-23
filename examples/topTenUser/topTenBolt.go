package main

import (
	"../../ipcPipe"
)

// define bolt

type thisBolt int

func (this *thisBolt) Type() int {
	return ipcPipe.TYPE_TRANSFORM
}

func (this *thisBolt) GetJoinFileName() string {
	// if the operation isn't join, this function will not be called
	return ""
}

func (this *thisBolt) ProcessTuple(inputTuple ipcPipe.NamedTuple) []ipcPipe.NamedTuple {
	// needed by map/transform
	if nameStr, exist := inputTuple["name"]; exist {
		var outputList []ipcPipe.NamedTuple
		outputTuple := make(ipcPipe.NamedTuple)
		outputTuple["name"] = nameStr
		outputList = append(outputList, outputTuple)
		return outputList
	}

	return []ipcPipe.NamedTuple{}
}

// the function -> need not to be changed

func main(){
	var tb thisBolt
	ipcPipe.RunBolt(&tb)
}