package main

import(
	"../../ipcPipe"
	"bytes"
	"fmt"
	"sort"
)

// the sink definition

type thisSink struct {
	nameCount map[string]int
}

func (this *thisSink) ProcessTuple(inputTuple ipcPipe.NamedTuple) {
	if nameStr, exist := inputTuple["name"]; exist{
		// corresponding count up
		if _, exist2 := this.nameCount[nameStr]; exist2 {
			this.nameCount[nameStr] += 1
		} else {
			this.nameCount[nameStr] = 1
		}
	}
}

// core func

type Pair struct {
	Key string
	Value int
}
type PairList []Pair
func (p PairList) Len() int { return len(p) }
func (p PairList) Less(i, j int) bool { return p[i].Value < p[j].Value }
func (p PairList) Swap(i, j int){ p[i], p[j] = p[j], p[i] }

func rankByFrequency(nameFrequency map[string]int) []Pair {
	pl := make(PairList, len(nameFrequency))
	i := 0
	for k, v := range nameFrequency {
		pl[i] = Pair{k, v}
		i++
	}
	sort.Sort(sort.Reverse(pl))
	return pl[:10]
}

func (this *thisSink) GetResult() string {
	var buffer bytes.Buffer

	// sort by frequency
	sortedPairs := rankByFrequency(this.nameCount)
	upperLimit := 0
	if len(sortedPairs) < 10{
		upperLimit = len(sortedPairs)
	}else{
		upperLimit = 10
	}
	for _, thisPair := range sortedPairs[:upperLimit]{
		buffer.WriteString(fmt.Sprintf("%s : %d\n", thisPair.Key, thisPair.Value))
	}

	return buffer.String()
}

// main

func main(){
	var ts thisSink
	ts.nameCount = make(map[string]int)
	ipcPipe.RunSink(&ts)
}
