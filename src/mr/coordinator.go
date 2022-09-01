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
	DOING
	DONE
)

type Coordinator struct {
	// Your definitions here.
	files []string

	MapTask      []TaskState
	MapTaskIndex int

	ReduceTask      []TaskState
	ReduceTaskIndex int

	MapStartTimeStamp    []time.Time
	ReduceStartTimeStamp []time.Time

	nMap    int
	nReduce int
	Mcnt    int
	Rcnt    int
	State   TaskType
	lock    sync.RWMutex
}

func (m *Coordinator) Arrange(message *Args, reply *Reply) error {
	// case 1 : there are still some as-yet-uncompleted map tasks
	m.lock.Lock()
	// fmt.Println("Coordinator arrange")
	if m.Rcnt == m.nReduce {
		reply.Over = true
		m.lock.Unlock()
		return nil
	}
	if m.State == MAP {
		for i := 0; i < m.nMap; i++ {
			// the task_i is as-yet-unstarted or time-out
			if m.MapTask[i] == IDLE || (m.MapTask[i] == DOING && time.Since(m.MapStartTimeStamp[i]) > 10*time.Second) {
				// fmt.Printf("map task %d arrange\n", i)
				reply.Task = MAP
				reply.Wait = false
				reply.Filename = m.files[i]
				reply.R = m.nReduce
				reply.MapTaskNumber = i
				reply.TimeStamp = time.Now()

				m.MapStartTimeStamp[i] = reply.TimeStamp
				m.MapTask[i] = DOING
				m.lock.Unlock()
				return nil
			}
		}
		// fmt.Println("wait")
		reply.Wait = true
		m.lock.Unlock()
		return nil
	}

	for i := 0; i < m.nReduce; i++ {
		// the task_i is time-out or as-yet-unstarted
		if m.ReduceTask[i] == IDLE || (m.ReduceTask[i] == DOING && time.Since(m.ReduceStartTimeStamp[i]) > 10*time.Second) {
			// fmt.Printf("reduce task %d arrange\n", i)
			reply.Task = REDUCE
			reply.M = m.nMap
			reply.ReduceTaskNumber = i
			reply.TimeStamp = time.Now()
			m.ReduceStartTimeStamp[i] = reply.TimeStamp
			m.ReduceTask[i] = DOING
			m.lock.Unlock()
			return nil
		}
	}

	// case 2.2 : no more as-yet-unstarted reduce tasks
	reply.Wait = true
	m.lock.Unlock()
	return nil
}

//
// an RPC handler to tell the Coordinator that the worker finishes the task
//
func (m *Coordinator) Finished(message *Args, reply *Reply) error {
	m.lock.Lock()

	if message.Finished == MAP {
		if m.MapStartTimeStamp[message.MapTaskNumber].Sub(message.TimeStamp) != 0 {
			reply.Wait = true
			m.lock.Unlock()
			return nil
		}
		m.MapTask[message.MapTaskNumber] = DONE
		m.Mcnt++
		if m.Mcnt == m.nMap {
			m.State = REDUCE
		}
	} else {
		if m.ReduceStartTimeStamp[message.ReduceTaskNumber].Sub(message.TimeStamp) != 0 {
			reply.Wait = true
			m.lock.Unlock()
			return nil
		}
		m.ReduceTask[message.ReduceTaskNumber] = DONE
		m.Rcnt++
	}

	m.lock.Unlock()
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Coordinator) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	os.Remove("mr-socket")
	l, e := net.Listen("unix", "mr-socket")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrCoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Coordinator) Done() bool {
	// Your code here.
	m.lock.RLock()

	ret := m.Rcnt == m.nReduce

	m.lock.RUnlock()
	return ret
}

//
// create a Coordinator.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	m := Coordinator{
		files:   files,
		nMap:    len(files),
		nReduce: nReduce,
		Mcnt:    0,
		Rcnt:    0,
		State:   MAP,
	}
	m.MapTask = make([]TaskState, m.nMap)
	m.ReduceTask = make([]TaskState, m.nReduce)

	m.MapStartTimeStamp = make([]time.Time, m.nMap)
	m.ReduceStartTimeStamp = make([]time.Time, m.nReduce)

	m.server()
	// Your code here.
	return &m
}
