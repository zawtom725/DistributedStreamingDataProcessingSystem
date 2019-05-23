package ipcPipe

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"
)

//////////////////////////
/* Tuple Representation */
//////////////////////////

// in order to represent a named tuple in the crane system, we use the built in go map map[string]string
// it maps a field name to a string representation of value
// right now it only supports scalar values. In other words, it is not supporting vector values like an array right now.
// for network communications, we transmit json marshalled map

type NamedTuple map[string]string

/////////////////////////
/* the Spout interface */
/////////////////////////

// GetFileName(): the source file name -> has to be a csv file with the first line defining column names
// EmitTuple(): it takes a line of the input data and output a str rep
// if it takes in a "terminate", it will terminate
// EmitInterval(): returns the emit interval in milliseconds

type Spout interface {
	GetFileName() string
	EmitInterval() int
}

////////////////////////
/* the Bolt interface */
////////////////////////

// jobType(): return the type of this operation
const TYPE_TRANSFORM int = 1
const TYPE_FILTER int = 2
const TYPE_JOIN int = 3

// GetJoinFileName(): return the name of the local static .csv file (with named fields)
// for Join: an inner join will be automatically performed
// ProcessTuple(): for map and transform, it accepts a string rep of a named tuple and returns
// a string rep of a processed named tuple.
// Notice, only one of the GetJoinFileName() and ProcessTuple() needs to be defined

type Bolt interface {
	Type() int
	GetJoinFileName() string
	ProcessTuple(NamedTuple) []NamedTuple
}

////////////////////////
/* the Sink Interface */
////////////////////////

// ProcessTuple(): feed in a str rep of named tuple. Notice, for sink, there is no output tuple.
// GetResult(): get a byte array of file content to be submitted

type Sink interface {
	ProcessTuple(NamedTuple)
	GetResult() string
}

///////////////////////////////////
// Special string representation //
///////////////////////////////////

// the one representing that the source stream has terminate -> the sink will upload the result
var TERMINATE_TUPLE NamedTuple = NamedTuple{"terminate":"terminate"}
// string rep
const STR_REP_TERMINATE string = "{\"terminate\":\"terminate\"}"

//////////////////////////
/* used to check tuples */
//////////////////////////

func IsTerminateTuple(tuple NamedTuple) bool {
	if value, exist := tuple["terminate"]; exist{
		return value == "terminate"
	}else{
		return false
	}
}

func createProgressTuple(progress float64) NamedTuple{
	ret := make(NamedTuple)
	ret["progress"] = fmt.Sprintf("%.2f", progress)
	return ret
}

//////////////////////////
/* Worker Runner Engine */
//////////////////////////

// helper

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

func innerJoin(inputTuple NamedTuple, staticDB []NamedTuple) []NamedTuple {
	// empty DB
	if len(staticDB) == 0 {
		return []NamedTuple{}
	}

	// get field name intersect
	var sharedFields []string
	var dbExtraFields []string
	for key, _ := range staticDB[0] {
		if _, exist := inputTuple[key]; exist {
			sharedFields = append(sharedFields, key)
		}else{
			dbExtraFields = append(dbExtraFields, key)
		}
	}

	// no shared fields
	if len(sharedFields) == 0 {
		return []NamedTuple{}
	}

	// do inner join
	var retList []NamedTuple

	for _, dbTuple := range staticDB {
		// check all equal
		allEqual := true
		for _, sharedKey := range sharedFields {
			if inputTuple[sharedKey] != dbTuple[sharedKey] {
				allEqual = false
			}
		}
		// if all equal, augment
		if allEqual {
			var newTuple NamedTuple = make(NamedTuple)
			for key, value := range inputTuple {
				newTuple[key] = value
			}
			for _, extraKey := range dbExtraFields {
				newTuple[extraKey] = dbTuple[extraKey]
			}
			retList = append(retList, newTuple)
		}
	}

	return retList
}

// spout

func RunSpout(spoutDefinition Spout){
	// the encoder
	encoder := json.NewEncoder(os.Stdout)

	// emit the source in the background
	go func(){
		// read file
		tupleList := readDataFile(spoutDefinition.GetFileName())
		tupleLenFloat := float64(len(tupleList))

		// keep track of progress and emit it
		lastEmittedProgress := 0.0

		for idx, tuple := range tupleList {
			// json encode
			encoder.Encode(tuple)

			// progress -> update every 5%
			currentProgress := float64(idx + 1) / tupleLenFloat
			if currentProgress - lastEmittedProgress >= 0.05 {
				lastEmittedProgress = currentProgress
				progressTuple := createProgressTuple(currentProgress)
				encoder.Encode(progressTuple)
			}

			// wait
			if spoutDefinition.EmitInterval() > 0 {
				time.Sleep(time.Duration(spoutDefinition.EmitInterval()) * time.Millisecond)
			}
		}

		// after the source is fully done. Emit a fully done progress tuple
		progressTuple := createProgressTuple(1)
		encoder.Encode(progressTuple)

		// no terminate tuple is emitted
		// encoder.Encode(TERMINATE_TUPLE)
	}()

	// wait for an terminate command
	reader := bufio.NewReader(os.Stdin)
	for {
		text, err := reader.ReadString('\n')
		if err != nil{
			continue
		}
		text = text[:len(text) - 1]

		var tuple NamedTuple
		err = json.Unmarshal([]byte(text), &tuple)
		if err != nil{
			continue
		}

		// if stdin pipe terminate -> terminate
		if IsTerminateTuple(tuple) {
			encoder.Encode(TERMINATE_TUPLE)
			break
		}
	}
}

// bolt

func RunBolt(boltDefinition Bolt){
	// the json encoder
	encoder := json.NewEncoder(os.Stdout)
	// read in line
	reader := bufio.NewReader(os.Stdin)

	// different procedure based on operation type: map & transform
	if boltDefinition.Type() == TYPE_TRANSFORM || boltDefinition.Type() == TYPE_FILTER {

		for{
			text, err := reader.ReadString('\n')
			if err != nil {
				continue
			}
			text = text[:len(text) - 1]

			// process input tuple
			var inputNamedTuple NamedTuple
			err = json.Unmarshal([]byte(text), &inputNamedTuple)
			if err != nil {
				continue
			}

			if IsTerminateTuple(inputNamedTuple){
				// terminate tuple
				encoder.Encode(TERMINATE_TUPLE)
				break
			}else{
				// regular tuple
				outputNamedTuples := boltDefinition.ProcessTuple(inputNamedTuple)
				for _, outputTuple := range outputNamedTuples {
					encoder.Encode(outputTuple)
				}
			}
		}

	}else{
		// join -> read static database
		staticDB := readDataFile(boltDefinition.GetJoinFileName())

		for{
			text, err := reader.ReadString('\n')
			if err != nil {
				continue
			}
			text = text[:len(text) - 1]

			// process input tuple
			var inputNamedTuple NamedTuple
			err = json.Unmarshal([]byte(text), &inputNamedTuple)
			if err != nil {
				continue
			}

			if IsTerminateTuple(inputNamedTuple){
				// terminate tuple
				encoder.Encode(TERMINATE_TUPLE)
				break
			}else{
				// regular tuple
				outputNamedTuples := innerJoin(inputNamedTuple, staticDB)
				for _, outputTuple := range outputNamedTuples {
					encoder.Encode(outputTuple)
				}
			}
		}

	}
}

// run sink

const STR_SINK_END_FILE = "EOF"
const SINK_EMIT_RESULT_INTERVAL_MS = 10000

func RunSink(sinkDefinition Sink) {
	// the reader
	reader := bufio.NewReader(os.Stdin)

	go func(){
		for{
			time.Sleep(time.Duration(SINK_EMIT_RESULT_INTERVAL_MS) * time.Millisecond)
			fmt.Print(sinkDefinition.GetResult())
			fmt.Println(STR_SINK_END_FILE)
		}
	}()

	for {
		text, err := reader.ReadString('\n')
		if err != nil {
			continue
		}
		text = text[:len(text)-1]

		// process input tuple
		var inputNamedTuple NamedTuple
		err = json.Unmarshal([]byte(text), &inputNamedTuple)
		if err != nil {
			continue
		}

		if IsTerminateTuple(inputNamedTuple) {
			// terminate tuple -> get result and emit it
			fmt.Print(sinkDefinition.GetResult())
			fmt.Println(STR_SINK_END_FILE)
			break
		}else{
			// regular tuple -> just process -> the sink
			sinkDefinition.ProcessTuple(inputNamedTuple)
		}
	}
}
