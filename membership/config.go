// MP3 Membership Conifg
// Copied from MP2
// Author: Ziang Wan, Yan Xu

package membership

import(
	"os"
)

// SWIM port number
const PortNumber int = 5678

// hostnames
var HOSTNAME, _ = os.Hostname()

// introducer host name
const IntroducerAddr string = "fa18-cs425-g58-01.cs.illinois.edu"

// hb config
const TimeoutRound int = 10
const UpdateBufferRound int = 2
const MsPerRound int = 500