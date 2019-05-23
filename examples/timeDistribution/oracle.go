package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
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

func main(){
	tupList := readDataFile("./comment.csv")

	count := 0
	timeMap := make(map[string]int)

	for _, tuple := range tupList {
		if timeStampStr, exist := tuple["timeStamp"]; exist {
			splitted := strings.Split(timeStampStr, " ")
			if len(splitted) == 2 && len(splitted[1]) >= 2 {
				timeStr := splitted[1][0:2]
				count += 1
				if _, exist2 := timeMap[timeStr]; exist2{
					timeMap[timeStr] += 1
				}else{
					timeMap[timeStr] = 1
				}
			}
		}
	}

	// print
	// sort keys
	var keys []string
	for key := range timeMap {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		fmt.Printf("%s : %d\n", key, timeMap[key])
	}
}
