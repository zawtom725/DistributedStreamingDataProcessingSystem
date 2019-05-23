package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"sort"
)

type NamedTuple map[string]string

func readDataFile(fileName string) []NamedTuple {
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	var retList []NamedTuple

	// read the first line (field names in)
	lines, err := csv.NewReader(file).ReadAll()
	fieldNames := lines[0]

	for _, line := range lines[1:] {
		// create the tuple
		var thisMap NamedTuple = make(NamedTuple)
		for idx, value := range line{
			thisMap[fieldNames[idx]] = value
		}
		// json encode
		retList = append(retList, thisMap)
	}

	return retList
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

func main(){
	tupList := readDataFile("./comment.csv")

	nameMap := make(map[string]int)

	for _, tuple := range tupList {
		if nameStr, exist := tuple["name"]; exist {
			if _, exist2 := nameMap[nameStr]; exist2 {
				nameMap[nameStr] += 1
			} else {
				nameMap[nameStr] = 1
			}
		}
	}

	// sort by frequency
	sortedPairs := rankByFrequency(nameMap)
	for _, thisPair := range sortedPairs[:10]{
		fmt.Printf("%s : %d\n", thisPair.Key, thisPair.Value)
	}
}
