package main

import (
	"../../ipcPipe"
	"bytes"
	"regexp"
	"strings"
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

func eliminateWebsiteSubstr(line string) string {
	websiteRegex := regexp.MustCompile("http")
	allIndex := websiteRegex.FindAllStringIndex(line, -1)

	var retString bytes.Buffer
	lastSliceEndIdx := 0

	for _, oneSlice := range allIndex {
		sliceStartIdx := oneSlice[0]

		if lastSliceEndIdx == len(line){
			break
		}else if sliceStartIdx <= lastSliceEndIdx{
			continue
		}

		// append rest
		retString.WriteString(line[lastSliceEndIdx:sliceStartIdx])

		// iterate until a whitespace or end of string
		sliceEndIdx := sliceStartIdx
		for sliceEndIdx < len(line){
			if line[sliceEndIdx:sliceEndIdx + 1] == " "{
				lastSliceEndIdx = sliceEndIdx
				break
			}else{
				sliceEndIdx += 1
			}
		}
		if sliceEndIdx == len(line){
			lastSliceEndIdx = sliceEndIdx
		}
	}

	// final append
	if lastSliceEndIdx < len(line){
		retString.WriteString(line[lastSliceEndIdx:])
	}

	return retString.String()
}

func eliminatePunctuation(line string) string {
	allPunctuations := []string{".", ",", "?", "!", ";", ":", "-", "{", "}", "(", ")", "'", "\"", "/", "#", "&", "@"}
	for _, punct := range allPunctuations {
		line = strings.Replace(line, punct, " ", -1)
	}
	return line
}

func (this *thisBolt) ProcessTuple(inputTuple ipcPipe.NamedTuple) []ipcPipe.NamedTuple {
	// input
	if msgStr, exist := inputTuple["msg"]; exist {
		if len(msgStr) > 0{
			msgStr = eliminateWebsiteSubstr(msgStr)
			msgStr = eliminatePunctuation(msgStr)

			// return
			var outputList []ipcPipe.NamedTuple
			outputTuple := make(ipcPipe.NamedTuple)
			outputTuple["msg"] = msgStr
			outputList = append(outputList, outputTuple)
			return outputList
		}
	}

	return []ipcPipe.NamedTuple{}
}

// the function -> need not to be changed

func main(){
	var tb thisBolt
	ipcPipe.RunBolt(&tb)
}
