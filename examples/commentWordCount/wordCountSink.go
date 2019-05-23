package main

import (
	"../../ipcPipe"
	"bytes"
	"sort"
	"strconv"
	"strings"
)

// the sink definition

type thisSink struct {
	totalCount int
	wordCount map[string]int
}

func (this *thisSink) ProcessTuple(inputTuple ipcPipe.NamedTuple) {
	// needed by map/transform
	if msgStr, exist := inputTuple["msg"]; exist {
		for _, word := range strings.Split(msgStr, " "){
			// not empty word
			if len(word) == 0{
				continue
			}
			// not a number string
			if _, err := strconv.Atoi(word); err == nil{
				continue
			}
			// total count up
			this.totalCount += 1
			// corresponding count up
			if _, exist2 := this.wordCount[word]; exist2{
				this.wordCount[word] += 1
			}else{
				this.wordCount[word] = 1
			}
		}
	}
}

func (this *thisSink) GetResult() string {
	var buffer bytes.Buffer

	// output
	var allWords []string
	for key := range this.wordCount{
		allWords = append(allWords, key)
	}
	sort.Strings(allWords)

	for _, word := range allWords {
		if this.wordCount[word] >= 100 {
			buffer.WriteString(word + " : " + strconv.Itoa(this.wordCount[word]) + "\n")
		}
	}

	return buffer.String()
}

// main

func main(){
	var ts thisSink
	ts.totalCount = 0
	ts.wordCount = make(map[string]int)
	ipcPipe.RunSink(&ts)
}
