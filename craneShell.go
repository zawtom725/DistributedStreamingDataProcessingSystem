package main

import (
	"./crane"
	"./membership"
	"./sdfs"
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// struct

type craneShell struct {
	mbrMaintainer *membership.MembershipMaintainer
	sdfsClient *sdfs.SdfsClient
	craneClient *crane.CraneClient
}

// commands
const CMDListSelfId string = "self"
const CMDListMbrList string = "members"
const CMDJoinMulticast string = "join"
const CMDLeaveMulticast string = "leave"

const CMDPut string = "put"
const CMDDelete string = "delete"
const CMDGet string = "get"
const CMDGetVersion string = "get-version"
const CMDStore string = "store"
const CMDLs string = "ls"

// crane command
const CMDSubmit string = "submit"

/* Helper Function */

func printMembers(l []string){
	fmt.Println("Current Members:")
	for _, m := range l{
		fmt.Println(m)
	}
}

/* crane additional coomand */

func (this *craneShell) ProcessCraneCommand(command string) {
	// split the command
	commandSplitted := strings.Split(command, " ")

	// process based on the first keyword
	switch commandSplitted[0] {
	case CMDSubmit:
		// get self host name
		if len(commandSplitted) == 2{
			err := this.craneClient.SubmitJob(commandSplitted[1])
			if err != nil{
				fmt.Println(err)
			}else{
				fmt.Println("submit succeeds")
			}
		}else{
			fmt.Println(commandSplitted[0] + " command malformed")
		}
	default:
		fmt.Println("Unknown command:", command)
	}
}

/* main command processor */

func (self *craneShell) ProcessCommand(command string) {
	// split the command
	commandSplitted := strings.Split(command, " ")

	// process based on the first keyword
	switch commandSplitted[0] {
	case CMDListSelfId:
		// get self host name
		fmt.Println(membership.HOSTNAME)
	case CMDListMbrList:
		// get current membership list
		printMembers(self.mbrMaintainer.GetCurrentMembers())
	case CMDJoinMulticast:
		// join multicast
		err := self.mbrMaintainer.JoinMulticast()
		if err != nil {
			fmt.Println(err)
		}
	case CMDLeaveMulticast:
		// leave multicast
		self.mbrMaintainer.LeaveMulticast()
		self.sdfsClient.ClearStorage()
	case CMDPut:
		// sdfs put
		if len(commandSplitted) == 3{
			localFileName := commandSplitted[1]
			sdfsFileName := commandSplitted[2]
			// client put
			err := self.sdfsClient.Put(localFileName, sdfsFileName)
			if err != nil{
				fmt.Println(err)
			}else{
				fmt.Println("put succeeds")
			}
		}else{
			fmt.Println(commandSplitted[0] + " command malformed")
		}
	case CMDDelete:
		if len(commandSplitted) == 2{
			sdfsFileName := commandSplitted[1]
			// client delete
			err := self.sdfsClient.Delete(sdfsFileName)
			if err != nil{
				fmt.Println(err)
			}else{
				fmt.Println("delete succeeds")
			}
		}else{
			fmt.Println(commandSplitted[0] + " command malformed")
		}
	case CMDGet:
		if len(commandSplitted) == 3{
			sdfsFileName := commandSplitted[1]
			localFileName := commandSplitted[2]
			// client get
			err := self.sdfsClient.Get(sdfsFileName, localFileName)
			if err != nil{
				fmt.Println(err)
			}else{
				fmt.Println("get succeeds")
			}
		}else{
			fmt.Println(commandSplitted[0] + " command malformed")
		}
	case CMDGetVersion:
		if len(commandSplitted) == 4 {
			sdfsFileName := commandSplitted[1]
			localFileName := commandSplitted[3]
			numVersion, err := strconv.Atoi(commandSplitted[2])
			if err != nil {
				fmt.Println(commandSplitted[0] + " command malformed")
			}else{
				// client get version
				err := self.sdfsClient.GetVersion(sdfsFileName, numVersion, localFileName)
				if err != nil{
					fmt.Println(err)
				}else{
					fmt.Println("get-version succeeds")
				}
			}
		}else{
			fmt.Println(commandSplitted[0] + " command malformed")
		}
	case CMDStore:
		if len(commandSplitted) == 1{
			// client store
			err := self.sdfsClient.Store()
			if err != nil{
				fmt.Println(err)
			}else{
				fmt.Println("store succeeds")
			}
		}else{
			fmt.Println(commandSplitted[0] + " command malformed")
		}
	case CMDLs:
		if len(commandSplitted) == 2{
			sdfsFileName := commandSplitted[1]
			// client ls
			err := self.sdfsClient.Ls(sdfsFileName)
			if err != nil{
				fmt.Println(err)
			}else{
				fmt.Println("ls succeeds")
			}
		}else{
			fmt.Println(commandSplitted[0] + " command malformed")
		}
	default:
		// fallback
		self.ProcessCraneCommand(command)
	}
}

/* the main shell */

func main() {
	// the shell entity
	var shellEntity craneShell
	// membership list
	shellEntity.mbrMaintainer = membership.CreateMembershipMaintainer()
	// sdfs client
	shellEntity.sdfsClient = sdfs.CreateSdfsClient(shellEntity.mbrMaintainer)
	// crane client
	shellEntity.craneClient = crane.CreateCraneClient(shellEntity.mbrMaintainer, shellEntity.sdfsClient)

	// SDFS Launch
	sdfs.LaunchSdfsMasterAndReplica(shellEntity.mbrMaintainer)
	// Crane Launch
	crane.LaunchCraneMasterSupervisor(shellEntity.sdfsClient, shellEntity.mbrMaintainer)

	// command line reader
	reader := bufio.NewReader(os.Stdin)
	// pointer
	shell := &shellEntity

	// easy
	shell.ProcessCommand("join")

	for{
		// get a command
		fmt.Print("[T58 SDFS Shell] ")
		command, _ := reader.ReadString('\n')
		command = command[:len(command) - 1]

		// process command -> timed
		shell.ProcessCommand(command)
	}
}