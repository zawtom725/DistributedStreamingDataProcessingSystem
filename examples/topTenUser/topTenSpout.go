package main

import(
	"../../ipcPipe"
)

// define the spout interface

type thisSpout int

func (this *thisSpout) GetFileName() string {
	return "comment.csv"
}

func (this *thisSpout) EmitInterval() int{
	return 0 	// no sleep -> full speed
}

// the main function -> not to be changed

func main(){
	var ts thisSpout
	ipcPipe.RunSpout(&ts)
}
