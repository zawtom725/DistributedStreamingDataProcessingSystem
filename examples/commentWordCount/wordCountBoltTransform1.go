package main

import (
	"../../ipcPipe"
	"bytes"
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

// helper functions

func eliminateBrackets(line string) string {
	inPunctuation := false
	leftBracket := []rune("{")[0]
	rightBracket := []rune("}")[0]

	var retString bytes.Buffer

	for _, r := range line{
		if r == leftBracket && !inPunctuation {
			inPunctuation = true
		}else if r == rightBracket && inPunctuation {
			inPunctuation = false
			retString.WriteString(" ")
		}else if !inPunctuation{
			retString.WriteString(string(r))
		}
	}

	return retString.String()
}

func (this *thisBolt) ProcessTuple(inputTuple ipcPipe.NamedTuple) []ipcPipe.NamedTuple {
	// needed by map/transform
	if msgStr, exist := inputTuple["msg"]; exist {
		if len(msgStr) > 0{
			msgStr = eliminateBrackets(msgStr)

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