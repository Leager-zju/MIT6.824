package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskState int

const (
	IDLE TaskState = iota
	IN_PROGRESS
	COMPLETED
)

type Coordinator struct {
	files []string

	MapTask    []TaskState
	ReduceTask []TaskState

	MapStartTimeStamp    []time.Time
	ReduceStartTimeStamp []time.Time

	M    int // total map tasks
	R    int // total reduce tasks
	Mcnt int // completed map tasks
	Rcnt int // completed reduce tasks

	State TaskType
	lock  sync.RWMutex
}

func (m *Coordinator) Arrange(message *Args, reply *Reply) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	if m.Rcnt == m.R { // all tasks completed
		reply.Over = true
		return nil
	}
	if m.State == MAP {
		for i := 0; i < m.M; i++ {
			// the task_i is as-yet-unstarted or time-out
			if m.MapTask[i] == IDLE || (m.MapTask[i] == IN_PROGRESS && time.Since(m.MapStartTimeStamp[i]) > 10*time.Second) {
				*reply = Reply{
					Task:          MAP,
					Wait:          false,
					Filename:      m.files[i],
					R:             m.R,
					MapTaskNumber: i,
					TimeStamp:     time.Now(),
				}

				m.MapStartTimeStamp[i] = reply.TimeStamp
				m.MapTask[i] = IN_PROGRESS
				return nil
			}
		}
	} else if m.State == REDUCE {
		for i := 0; i < m.R; i++ {
			// the task_i is as-yet-unstarted or time-out
			if m.ReduceTask[i] == IDLE || (m.ReduceTask[i] == IN_PROGRESS && time.Since(m.ReduceStartTimeStamp[i]) > 10*time.Second) {
				*reply = Reply{
					Task:             REDUCE,
					M:                m.M,
					ReduceTaskNumber: i,
					TimeStamp:        time.Now(),
				}

				m.ReduceStartTimeStamp[i] = reply.TimeStamp
				m.ReduceTask[i] = IN_PROGRESS
				return nil
			}
		}
	}

	// no more as-yet-unstarted tasks
	reply.Wait = true
	return nil
}

// an RPC handler to tell the Coordinator that the worker finishes the task
func (m *Coordinator) Finished(message *Args, reply *Reply) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	if message.Finished == MAP {
		if m.MapStartTimeStamp[message.MapTaskNumber].Sub(message.TimeStamp) != 0 {
			reply.Wait = true
			return nil
		}
		m.MapTask[message.MapTaskNumber] = COMPLETED
		m.Mcnt++
		if m.Mcnt == m.M {
			m.State = REDUCE
		}
	} else {
		if m.ReduceStartTimeStamp[message.ReduceTaskNumber].Sub(message.TimeStamp) != 0 {
			reply.Wait = true
			return nil
		}
		m.ReduceTask[message.ReduceTaskNumber] = COMPLETED
		m.Rcnt++
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (m *Coordinator) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	os.Remove("mr-socket")
	l, e := net.Listen("unix", "mr-socket")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrCoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Coordinator) Done() bool {
	m.lock.RLock()
	defer m.lock.RUnlock()
	return m.Rcnt == m.R
}

// create a Coordinator.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	m := &Coordinator{
		files: files,
		M:     len(files),
		R:     nReduce,
		Mcnt:  0,
		Rcnt:  0,
		State: MAP,
	}
	m.MapTask = make([]TaskState, m.M)
	m.ReduceTask = make([]TaskState, m.R)

	m.MapStartTimeStamp = make([]time.Time, m.M)
	m.ReduceStartTimeStamp = make([]time.Time, m.R)

	m.server()
	return m
}
