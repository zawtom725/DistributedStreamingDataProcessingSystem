package main

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"regexp"
	"sort"
	"strconv"
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

func splitToWords(line string) []string {
	return strings.Split(line, " ")
}

func main(){
	var wordCount map[string]int = make(map[string]int)
	totalCount := 0

	tuples := readDataFile("comment.csv")
	for _, tuple := range tuples {
		thisLine := tuple["msg"]
		if len(thisLine) == 0{
			continue
		}

		thisLine = eliminateBrackets(thisLine)
		thisLine = eliminateWebsiteSubstr(thisLine)
		thisLine = eliminatePunctuation(thisLine)

		for _, word := range splitToWords(thisLine) {
			if len(word) > 0{
				if _, err := strconv.Atoi(word); err != nil{
					// non empty word -> count
					totalCount += 1
					if value, exist := wordCount[word]; exist {
						wordCount[word] = value + 1
					}else{
						wordCount[word] = 1
					}
				}
			}
		}
	}

	// output
	var allWords []string
	for key := range wordCount{
		allWords = append(allWords, key)
	}
	sort.Strings(allWords)

	for _, word := range allWords {
		if wordCount[word] >= 100 {
			fmt.Printf("%s : %d\n", word, wordCount[word])
		}
	}
}