package mr

import (
	"time"
)

//
// RPC definitions.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type Args struct {
	Finished TaskType

	TimeStamp time.Time

	MapTaskNumber    int
	ReduceTaskNumber int
}

type Reply struct {
	Task TaskType
	Wait bool // true for wait
	Over bool // true for done

	Filename string
	M        int
	R        int

	MapTaskNumber    int
	ReduceTaskNumber int

	TimeStamp time.Time
}
