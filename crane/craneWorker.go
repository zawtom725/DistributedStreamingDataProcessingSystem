package crane

import (
	"../ipcPipe"
	"../membership"
	"../sdfs"
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

// const

const TCP_TIMEOUT_MS int = 2000

// TCP buffer overflow problem
const TCP_BUFFER_SIZE int = 8 * 1024

const POOLING_SLEEP_INTERVAL_MS int = 500

// worker struct

type CraneWorker struct {
	// sync
	lock *sync.Mutex

	// initialized field
	sdfsClient    *sdfs.SdfsClient
	mbrMaintainer *membership.MembershipMaintainer

	// pooling stat
	maxPooling int
	currentPooling int
	poolingLock *sync.Mutex

	// job status
	jobType         int
	selfPort        int
	running         bool

	// ipc pipe
	inputEncoder  *json.Encoder // to communicate with the current running job
	outputScanner *bufio.Scanner

	// channels for terminating server listener
	listenerTerminateChnl chan int
	listenerDoneChnl      chan int
}

type WorkerTasklet struct {
	JobType            int      // spout/sink/join
	SelfPort           int      // the port number the job is listening on
	SourceFileName     string   // the main executable/spout data source
	AdditionalFileName string   // for join: the name of the static db, for sink: the output file
	ChildHosts         [][]string // for bolt/spout: child hostnames, for sink: empty
	MaxPooling 		   int
}

// networking -> many go routine -> deplete os resource
/* outgoing function */
// tcpSend()
// sendProgressToMaster()
// sendFileResultToSdfs()
/* Incoming */
// boltListeningThreadHandleConnection()
// sinkListeningThreadHandleConnection()

// outgoing

func (this *CraneWorker) tcpSend(tupleRep string, hostFull string) {
	// tcp connection
	conn, err := net.Dial("tcp", hostFull)
	if err != nil {
		// finish pooling
		this.poolingLock.Lock()
		this.currentPooling -= 1
		this.poolingLock.Unlock()
		return
	}

	// send
	conn.Write([]byte(tupleRep))
	conn.Close()

	// finish pooling
	this.poolingLock.Lock()
	this.currentPooling -= 1
	this.poolingLock.Unlock()
}

func (this *CraneWorker) sendProgressToMaster(progress float64) {
	// keep sending progress to master until succeeds
	currentMasterHost := this.mbrMaintainer.GetCurrentLeader()
	master, err := rpc.Dial("tcp", currentMasterHost + ":" + strconv.Itoa(CranePort))
	if err != nil {
		// finish pooling
		this.poolingLock.Lock()
		this.currentPooling -= 1
		this.poolingLock.Unlock()
		return
	}

	var rpcInput UpdateJobProgressInput = UpdateJobProgressInput{progress}
	var rpcOutput UpdateJobProgressOutput
	master.Call("CraneMasterSupervisorRPC.UpdateJobProgress", rpcInput, &rpcOutput)
	master.Close()

	// finish pooling
	this.poolingLock.Lock()
	this.currentPooling -= 1
	this.poolingLock.Unlock()
}

func (this *CraneWorker) sendFileResultToSdfs(fileName string, fileContent []byte) {
	// output to local fs
	os.Remove(fileName)
	ioutil.WriteFile(fileName, fileContent, 0644)

	// upload to sdfs
	err := this.sdfsClient.Put(fileName, fileName)
	if err != nil {
		// finish pooling
		this.poolingLock.Lock()
		this.currentPooling -= 1
		this.poolingLock.Unlock()
		return
	}

	// tell the master that this file is ready
	currentMasterHost := this.mbrMaintainer.GetCurrentLeader()
	master, err := rpc.Dial("tcp", currentMasterHost+":"+strconv.Itoa(CranePort))
	if err != nil {
		// finish pooling
		this.poolingLock.Lock()
		this.currentPooling -= 1
		this.poolingLock.Unlock()
		return
	}

	var rpcInput FileReadyInput = FileReadyInput{fileName}
	var rpcOutput FileReadyOutput
	master.Call("CraneMasterSupervisorRPC.FileReady", rpcInput, &rpcOutput)
	master.Close()

	// finish pooling
	this.poolingLock.Lock()
	this.currentPooling -= 1
	this.poolingLock.Unlock()
}

// incoming

func (this *CraneWorker) boltListeningThreadHandleConnection(conn net.Conn){
	// assume
	inputBytes := make([]byte, TCP_BUFFER_SIZE)
	length, err := conn.Read(inputBytes)
	if err != nil {
		// finish pooling
		this.poolingLock.Lock()
		this.currentPooling -= 1
		this.poolingLock.Unlock()
		return
	}

	var thisTuple ipcPipe.NamedTuple
	err = json.Unmarshal(inputBytes[:length], &thisTuple)
	if err != nil {
		// finish pooling
		this.poolingLock.Lock()
		this.currentPooling -= 1
		this.poolingLock.Unlock()
		return
	}

	// pipe into the ioWriter, a newline character is done
	this.inputEncoder.Encode(thisTuple)

	// finish pooling
	this.poolingLock.Lock()
	this.currentPooling -= 1
	this.poolingLock.Unlock()
}

func (this *CraneWorker) sinkListeningThreadHandleConnection(conn net.Conn){
	inputBytes := make([]byte, TCP_BUFFER_SIZE)
	length, err := conn.Read(inputBytes)
	if err != nil {
		// finish pooling
		this.poolingLock.Lock()
		this.currentPooling -= 1
		this.poolingLock.Unlock()
		return
	}

	var thisTuple ipcPipe.NamedTuple
	err = json.Unmarshal(inputBytes[:length], &thisTuple)
	if err != nil {
		// finish pooling
		this.poolingLock.Lock()
		this.currentPooling -= 1
		this.poolingLock.Unlock()
		return
	}

	// pipe into the ioWriter, a newline character is done
	this.inputEncoder.Encode(thisTuple)

	// finish pooling
	this.poolingLock.Lock()
	this.currentPooling -= 1
	this.poolingLock.Unlock()
}

// util

func checkProgressTuple(spoutOutput string) (bool, float64) {
	var thisTuple ipcPipe.NamedTuple
	err := json.Unmarshal([]byte(spoutOutput), &thisTuple)
	if err != nil{
		return false, 0.0
	}

	if value, exist := thisTuple["progress"]; exist {
		progress, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return false, 0.0
		}else{
			return true, progress
		}
	}else{
		return false, 0.0
	}
}

// the grouping strategy is random pick

func listRandPick(sl []string) string {
	if len(sl) == 0 {
		fmt.Println("CraneWorker: listRandPick() empty list")
		return ""
	}else{
		randIdx := rand.Intn(len(sl))
		return sl[randIdx]
	}
}

// run spout

func (this *CraneWorker) spoutEmittingThread(childHosts [][]string, cmd *exec.Cmd){

	for this.outputScanner.Scan() {
		m := this.outputScanner.Text()

		if isProgress, currentProgress := checkProgressTuple(m); isProgress{
			// send the progress to master in the background

			// pooling
			for{
				this.poolingLock.Lock()
				if this.currentPooling < this.maxPooling {
					// spawn and break
					this.currentPooling += 1
					go this.sendProgressToMaster(currentProgress)
					this.poolingLock.Unlock()
					break
				}else{
					this.poolingLock.Unlock()
					time.Sleep(time.Duration(POOLING_SLEEP_INTERVAL_MS) * time.Millisecond)
				}
			}

		}else{

			// represents the end of the source, terminate the worker
			if m == ipcPipe.STR_REP_TERMINATE {
				break
			}else{
				for _, host := range childHosts {

					// pooling
					for{
						this.poolingLock.Lock()
						if this.currentPooling < this.maxPooling {
							// spawn and break
							this.currentPooling += 1
							go this.tcpSend(m, listRandPick(host))
							this.poolingLock.Unlock()
							break
						}else{
							this.poolingLock.Unlock()
							time.Sleep(time.Duration(POOLING_SLEEP_INTERVAL_MS) * time.Millisecond)
						}
					}

				}
			}
		}
	}

	// self terminating
	this.inputEncoder.Encode(ipcPipe.TERMINATE_TUPLE)
	// wait for termination
	cmd.Wait()
	// all done -> set flag
	this.running = false

	fmt.Println("CraneWorker: spoutEmittingThread() terminate", time.Now().UTC())
}

func (this *CraneWorker) runSpout(task *WorkerTasklet) error {
	cmd := exec.Command("./" + task.SourceFileName)

	cmdStdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	cmdStdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}

	// the current cmdStdin progress to communicate with
	encoder := json.NewEncoder(cmdStdin)
	scanner := bufio.NewScanner(cmdStdout)
	this.inputEncoder = encoder
	this.outputScanner = scanner

	// channel - no need

	// run the command
	err = cmd.Start()
	if err != nil{
		panic(err)
	}
	this.running = true

	// start getting cmdStdout -> update progress / send it to children
	go this.spoutEmittingThread(task.ChildHosts, cmd)

	// successfully launched the spout
	return nil
}

// bolt

func (this *CraneWorker) boltEmittingThread(childHosts [][]string, cmd *exec.Cmd){
	for this.outputScanner.Scan() {
		m := this.outputScanner.Text()

		// the bolt will respond to terminate input, output a terminate and terminate itself
		if m == ipcPipe.STR_REP_TERMINATE {
			break
		}else{
			for _, host := range childHosts {

				// pooling
				for{
					this.poolingLock.Lock()
					if this.currentPooling < this.maxPooling {
						// spawn and break
						this.currentPooling += 1
						go this.tcpSend(m, listRandPick(host))
						this.poolingLock.Unlock()
						break
					}else{
						this.poolingLock.Unlock()
						time.Sleep(time.Duration(POOLING_SLEEP_INTERVAL_MS) * time.Millisecond)
					}
				}

			}
		}
	}

	// conclude
	cmd.Wait()
	this.running = false
	fmt.Println("CraneWorker: boltEmittingThread() terminate")
}

func (this *CraneWorker) boltListeningThread(finishChnl chan int){
	l, err := net.Listen("tcp", ":" + strconv.Itoa(this.selfPort))
	if err != nil {
		panic(err)
	}
	defer l.Close()

	// get the underlying listener for timeout
	tcpListener := l.(*net.TCPListener)

	// tell the outside the channel has finished setting up
	finishChnl <- 0

	// listen until receive a terminate tuple
	for {
		select {
		case <-this.listenerTerminateChnl:
			// terminate
			fmt.Println("CraneWorker: boltListeningThread() terminate")
			this.listenerDoneChnl <- 5
			return
		default:
			// set a timeout before accept
			tcpListener.SetDeadline(time.Now().Add(time.Millisecond * time.Duration(TCP_TIMEOUT_MS)))
			// listen on incoming transaction
			c, err := l.Accept()
			if err != nil{
				if !strings.Contains(err.Error(), "i/o timeout"){
					fmt.Println(err)
				}
				continue
			}

			for{
				this.poolingLock.Lock()
				if this.currentPooling < this.maxPooling {
					// spawn and break
					this.currentPooling += 1
					go this.boltListeningThreadHandleConnection(c)
					this.poolingLock.Unlock()
					break
				}else{
					this.poolingLock.Unlock()
					time.Sleep(time.Duration(POOLING_SLEEP_INTERVAL_MS) * time.Millisecond)
				}
			}

		}
	}
}

func (this *CraneWorker) runBolt(task *WorkerTasklet) error {
	cmd := exec.Command("./" + task.SourceFileName)

	cmdStdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	cmdStdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}

	// the current cmdStdin progress to communicate with
	encoder := json.NewEncoder(cmdStdin)
	scanner := bufio.NewScanner(cmdStdout)
	this.inputEncoder = encoder
	this.outputScanner = scanner

	// channel
	this.listenerTerminateChnl = make(chan int)
	this.listenerDoneChnl = make(chan int)

	// run the command
	err = cmd.Start()
	if err != nil {
		return err
	}

	this.running = true

	// start a reading thread
	go this.boltEmittingThread(task.ChildHosts, cmd)
	// for bolt -> need to listen to incoming tuple and pipe it into the cmdStdin, make sure the server is up
	finishChnl := make(chan int)
	go this.boltListeningThread(finishChnl)
	<-finishChnl

	// success
	return nil
}

// sink

func (this *CraneWorker) sinkEmittingThread(outputFileName string, cmd *exec.Cmd) {
	// read the content to form []byte
	var builder bytes.Buffer

	for this.outputScanner.Scan() {
		m := this.outputScanner.Text()
		if m != ipcPipe.STR_SINK_END_FILE {
			// file content
			builder.WriteString(m + "\n")
		}else{
			// send file result

			// pooling
			for{
				this.poolingLock.Lock()
				if this.currentPooling < this.maxPooling {
					// spawn and break
					this.currentPooling += 1
					go this.sendFileResultToSdfs(outputFileName, builder.Bytes())
					this.poolingLock.Unlock()
					break
				}else{
					this.poolingLock.Unlock()
					time.Sleep(time.Duration(POOLING_SLEEP_INTERVAL_MS) * time.Millisecond)
				}
			}

			// eof, wait for a new partial result
			builder = bytes.Buffer{}
		}
	}

	// wait for the termination of the working executable
	cmd.Wait()
	// finally set the flag
	this.running = false

	fmt.Println("CraneWorker: sinkEmittingThread() terminate", time.Now().UTC())
}

func (this *CraneWorker) sinkListeningThread(finishChnl chan int){
	l, err := net.Listen("tcp", ":" + strconv.Itoa(this.selfPort))
	if err != nil {
		panic(err)
	}
	defer l.Close()

	// get the underlying listener for timeout
	tcpListener := l.(*net.TCPListener)

	// tell the driver that the server has been set up
	finishChnl <- 5

	// listen until receive a terminate tuple
	for {
		select {
		case <-this.listenerTerminateChnl:
			// terminate
			fmt.Println("CraneWorker: sinkListeningThread() terminate")
			this.listenerDoneChnl <- 5
			return
		default:
			// set a timeout before accept
			tcpListener.SetDeadline(time.Now().Add(time.Millisecond * time.Duration(TCP_TIMEOUT_MS)))
			// listen on incoming transaction
			c, err := l.Accept()
			if err != nil{
				if !strings.Contains(err.Error(), "i/o timeout"){
					fmt.Println(err)
				}
				continue
			}

			// pooling
			for{
				this.poolingLock.Lock()
				if this.currentPooling < this.maxPooling {
					// spawn and break
					this.currentPooling += 1
					go this.sinkListeningThreadHandleConnection(c)
					this.poolingLock.Unlock()
					break
				}else{
					this.poolingLock.Unlock()
					time.Sleep(time.Duration(POOLING_SLEEP_INTERVAL_MS) * time.Millisecond)
				}
			}

		}
	}
}

func (this *CraneWorker) runSink(task *WorkerTasklet) error {
	cmd := exec.Command("./" + task.SourceFileName)

	cmdStdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	cmdStdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}

	// the current cmdStdin progress to communicate with
	encoder := json.NewEncoder(cmdStdin)
	scanner := bufio.NewScanner(cmdStdout)
	this.inputEncoder = encoder
	this.outputScanner = scanner

	// channel
	this.listenerTerminateChnl = make(chan int)
	this.listenerDoneChnl = make(chan int)

	// run the command
	cmd.Start()
	this.running = true

	// start a reading func
	go this.sinkEmittingThread(task.AdditionalFileName, cmd)

	// start a listening thread
	finishChnl := make(chan int)
	go this.sinkListeningThread(finishChnl)
	<-finishChnl

	return nil
}

// run task mutiplexer & test

func (this *CraneWorker) StartTask(task *WorkerTasklet) error {
	this.lock.Lock()
	defer this.lock.Unlock()

	fmt.Println("CraneWorker: StartTask()", task)

	// get necessary files
	err := this.sdfsClient.Get(task.SourceFileName, task.SourceFileName)
	if err != nil {
		return err
	}
	// change the file permission for source file
	cmd := exec.Command("chmod", "0766", task.SourceFileName)
	err = cmd.Run()
	if err != nil{
		return err
	}

	// get additional file if bolt
	if task.JobType != TYPE_SINK && task.AdditionalFileName != "" {
		err = this.sdfsClient.Get(task.AdditionalFileName, task.AdditionalFileName)
		if err != nil {
			return err
		}
	}

	// set port
	this.selfPort = task.SelfPort
	this.jobType = task.JobType

	// pooling
	this.maxPooling = task.MaxPooling
	this.currentPooling = 0
	this.poolingLock = &sync.Mutex{}

	// run the task
	switch task.JobType {
	case TYPE_SPOUT:
		err = this.runSpout(task)
	case TYPE_BOLT:
		err = this.runBolt(task)
	case TYPE_SINK:
		err = this.runSink(task)
	default:
		err = errors.New("no matching types")
	}

	if err != nil {
		fmt.Println(err)
		return err
	}else{
		fmt.Println("CraneWorker: StartTask() launched")
		return nil
	}
}

/*
func (this *CraneWorker) TestBeginTask(task *WorkerTasklet) error {
	this.lock.Lock()
	defer this.lock.Unlock()

	// set port
	this.selfPort = task.SelfPort
	this.jobType = task.JobType
	this.maxPooling = task.MaxPooling

	// run the task
	switch task.JobType {
	case TYPE_SPOUT:
		return this.runSpout(task)
	case TYPE_BOLT:
		return this.runBolt(task)
	case TYPE_SINK:
		return this.runSink(task)
	default:
		return errors.New("no matching types")
	}
}
*/

// terminate task

func (this *CraneWorker) TerminateTask(){
	// terminate the listening thread by channels
	this.lock.Lock()
	defer this.lock.Unlock()

	if this.jobType == TYPE_BOLT || this.jobType == TYPE_SINK {
		this.listenerTerminateChnl <- 5
		<- this.listenerDoneChnl
	}

	// terminate the emitter and the executable by piping a terminate tuple
	if this.running {
		this.inputEncoder.Encode(ipcPipe.TERMINATE_TUPLE)
	}

	for{
		this.poolingLock.Lock()
		if this.currentPooling == 0{
			break
		}
		this.poolingLock.Unlock()
		time.Sleep(time.Duration(POOLING_SLEEP_INTERVAL_MS) * time.Millisecond)
	}

	fmt.Println("CraneWorker: TerminateTask() thread pool cleared")

	// wait for all threads to be done

	this.running = false

	fmt.Println("CraneWorker: TerminateTask() done")
}

// package methods

func CreateCraneWorker(sdfsClient *sdfs.SdfsClient, mbrMaintainer *membership.MembershipMaintainer) *CraneWorker {
	var ret CraneWorker
	ret.lock = &sync.Mutex{}
	ret.sdfsClient = sdfsClient
	ret.mbrMaintainer = mbrMaintainer

	ret.running = false
	return &ret
}
