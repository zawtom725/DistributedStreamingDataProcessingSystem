package main

import(
	"../../ipcPipe"
	"bytes"
	"fmt"
	"sort"
)

// the sink definition

type thisSink struct {
	totalCount int
	timeCount map[string]int
}

func (this *thisSink) ProcessTuple(inputTuple ipcPipe.NamedTuple) {
	if value, exist := inputTuple["timeStamp"]; exist{
		// total count up
		this.totalCount += 1
		// corresponding count up
		if _, exist2 := this.timeCount[value]; exist2{
			this.timeCount[value] = this.timeCount[value] + 1
		}else{
			this.timeCount[value] = 1
		}
	}
}

func (this *thisSink) GetResult() string {
	var buffer bytes.Buffer

	// sort keys
	var keys []string
	for key := range this.timeCount {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		buffer.WriteString(fmt.Sprintf("%s : %d\n", key, this.timeCount[key]))
	}

	return buffer.String()
}

// main

func main(){
	var ts thisSink
	ts.totalCount = 0
	ts.timeCount = make(map[string]int)
	ipcPipe.RunSink(&ts)
}
