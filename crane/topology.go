package crane

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// constants

const STRING_SPOUT = "spout"
const STRING_BOLT = "bolt"
const STRING_SINK = "sink"

const TYPE_SPOUT = 1
const TYPE_BOLT = 2
const TYPE_SINK = 3

// define the topology

type TopologyElement struct {
	Id int
	Type int
	Parallelism int 			// bolt -> parallelism, other -> par = 1
	SourceFileName string
	AdditionalFileName string
	ChildIds []int 				// for bolt/spout: child ids, for sink: empty
}

// the external topology -> a tree with single Spout

// these fields are read-only -> no need for a lock
type Topology struct {
	SpoutId int
	Element []TopologyElement
}

// package methods

func (this *Topology) GetElementById(id int) *TopologyElement {
	for _, element := range this.Element{
		if element.Id == id {
			return &element
		}
	}
	return nil
}

func (this *Topology) GetSpoutId() int {
	return this.SpoutId
}

func (this *Topology) GetResultFileNames() []string {
	var retList []string
	for _, element := range this.Element {
		if element.Type == TYPE_SINK {
			retList = append(retList, element.AdditionalFileName)
		}
	}
	return retList
}

// core package method

/* Topology Scheduling Algorithm */

func (this *Topology) Schedule(membership []string) []Tasklet {
	// the algorithm is a three pass algorithm
	var topIdToTasks map[int][]Tasklet = make(map[int][]Tasklet)

	/* first pass, set up the map & job info */

	for _, element := range this.Element{
		var taskList []Tasklet
		for i := 0; i < element.Parallelism; i++{
			// create the tasklet
			var thisTask Tasklet
			// job info
			thisTask.TopId = element.Id
			thisTask.JobType = element.Type
			thisTask.SourceFileName = element.SourceFileName
			thisTask.AdditionalFileName = element.AdditionalFileName
			// append
			taskList = append(taskList, thisTask)
		}
		// entry
		topIdToTasks[element.Id] = taskList
	}

	// base ports
	var vmToNextAvailPort map[string]int = make(map[string]int)
	for _, vmHostName := range membership {
		vmToNextAvailPort[vmHostName] = CraneWorkerPortBase
	}

	// concurrencey records
	var vmToTaskCount map[string]int = make(map[string]int)
	for _, vmHostName := range membership {
		vmToTaskCount[vmHostName] = 0
	}

	// for childHosts
	var topIdToVM map[int][]string = make(map[int][]string)

	/* second pass, set up VM residence */

	idx := 0
	if len(membership) >= 3 {
		idx = 2
	}

	for _, element := range this.Element {
		topId := element.Id
		tasks := topIdToTasks[topId]

		// record
		topIdToVM[topId] = []string{}

		// all tasks assign VM and port
		for taskIdx := range tasks {
			// assign VM and increment
			assignedVM := membership[idx]
			if idx == len(membership) - 1 {
				idx = 0
			}else{
				idx = idx + 1
			}

			// assign port number and increment
			assignedPort := vmToNextAvailPort[assignedVM]
			vmToNextAvailPort[assignedVM] += 1

			// increment task count
			vmToTaskCount[assignedVM] += 1

			// task VM related
			topIdToTasks[topId][taskIdx].HostName = assignedVM
			topIdToTasks[topId][taskIdx].ListenPort = assignedPort

			// update
			topIdToVM[topId] = append(topIdToVM[topId], assignedVM + ":" + strconv.Itoa(assignedPort))
		}
	}

	/* Third Pass: update concurrency and childHosts */

	for topId, tasks := range topIdToTasks {
		for taskIdx, task := range tasks {
			// update childHosts
			var taskChildHosts [][]string
			for _, childTopId := range this.GetElementById(topId).ChildIds {
				taskChildHosts = append(taskChildHosts, topIdToVM[childTopId])
			}
			topIdToTasks[topId][taskIdx].ChildHosts = taskChildHosts

			// update concurrency
			topIdToTasks[topId][taskIdx].Concurrency = vmToTaskCount[task.HostName]
		}
	}

	// form the list and return

	var retList []Tasklet
	for _, element := range this.Element {
		tasks := topIdToTasks[element.Id]
		for _, task := range tasks {
			retList = append(retList, task)
		}
	}

	return retList
}

// validate function for init

func (this *Topology) validateRecurse(id int) bool{
	thisElement := this.GetElementById(id)

	if thisElement.Type == TYPE_SINK {
		// sink
		if len(thisElement.ChildIds) == 0{
			return true
		}else{
			return false
		}
	}else{
		// has to have some children
		if len(thisElement.ChildIds) == 0{
			return false
		}
		// recursively validate child
		for _, childId := range thisElement.ChildIds {
			if childId < thisElement.Id || !this.validateRecurse(childId) {
				return false
			}
		}
		return true
	}
}

func (this *Topology) Validate() bool {
	// invalid spout
	if this.SpoutId < 0 || this.GetElementById(this.SpoutId).Type != TYPE_SPOUT {
		return false
	}

	// validate the DAG structure recursively
	return this.validateRecurse(this.SpoutId)
}

// package function

func scanTopologyFromFile(fileName string) *Topology{
	// open the topology file
	file, err := os.Open(fileName)
	if err != nil {
		fmt.Println("Application topology doesn't exist")
		return nil
	}
	defer file.Close()

	// the map -> from id ->
	var parsedElements []TopologyElement
	var spoutId int = -1

	// read it line by line
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		// process
		splitted := strings.Split(scanner.Text(), " ")

		if len(splitted) >= 3 {
			// if it is the node definition line
			var thisElement TopologyElement

			// get self id 1:
			thisId, err := strconv.Atoi(splitted[0][:len(splitted[0]) - 1])
			if err != nil {
				panic(err)
			}
			thisElement.Id = thisId

			// further proceed based on type
			switch splitted[1]{
			case STRING_SPOUT :
				spoutId = thisId
				thisElement.Type = TYPE_SPOUT
				thisElement.Parallelism = 1

				thisElement.SourceFileName = splitted[2]

				if len(splitted) >= 4{
					thisElement.AdditionalFileName = splitted[3]
				}else{
					fmt.Println("Application topology parsing error: spout")
					return nil
				}
			case STRING_BOLT:
				thisElement.Type = TYPE_BOLT

				parallelism, err := strconv.Atoi(splitted[2])
				if err != nil || parallelism < 1 {
					fmt.Println("Application topology parsing error: bolt")
					return nil
				}
				thisElement.Parallelism = parallelism

				if len(splitted) >= 4{
					// source file name
					thisElement.SourceFileName = splitted[3]
					// optional additional file name for join
					if len(splitted) >= 5{
						thisElement.AdditionalFileName = splitted[4]
					}else{
						thisElement.AdditionalFileName = ""
					}
				}else{
					fmt.Println("Application topology parsing error: bolt")
					return nil
				}
			case STRING_SINK:
				thisElement.Type = TYPE_SINK
				thisElement.Parallelism = 1

				thisElement.SourceFileName = splitted[2]

				if len(splitted) >= 4{
					thisElement.AdditionalFileName = splitted[3]
				}else{
					fmt.Println("Application topology parsing error: sink")
					return nil
				}
			}

			parsedElements = append(parsedElements, thisElement)
		}else if len(splitted) == 1{
			// it is the graph definition line, split by ->
			match := strings.Split(splitted[0], "->")
			if len(match) == 2{
				// parse source and target
				sourceId, err := strconv.Atoi(match[0])
				if err != nil{
					continue
				}
				targetId, err := strconv.Atoi(match[1])
				if err != nil {
					continue
				}

				// get source node
				found := false
				for idx, element := range parsedElements {
					if element.Id == sourceId {
						parsedElements[idx].ChildIds = append(element.ChildIds, targetId)
						found = true
					}
				}
				if !found {
					panic("non existing source node.")
				}
			}
		}else{
			// empty line
			continue
		}
	}

	// create the topology
	var thisTopology Topology
	thisTopology.SpoutId = spoutId
	thisTopology.Element = parsedElements

	return &thisTopology
}

func CreateTopology(fileName string) *Topology {
	thisTopology := scanTopologyFromFile(fileName)

	// validate the topology
	if thisTopology != nil && thisTopology.Validate() {
		return thisTopology
	}else{
		fmt.Println("Invalid topology")
		return nil
	}
}
