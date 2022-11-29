package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

const WorkerWaitTime = 5 * time.Second

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type TaskType int

const (
	NONE TaskType = iota
	MAP
	REDUCE
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// One way to get started is to modify mr/worker.go's Worker()
	// to send an RPC to the coordinator asking for a task.
	for {
		reply := AskForTask()
		if reply.Over {
			return
		}

		if reply.Wait {
			time.Sleep(WorkerWaitTime)
		} else if reply.Task == MAP {
			// map phase
			mapTaskNumber := reply.MapTaskNumber

			// read file
			file, err := os.Open(reply.Filename)
			if err != nil {
				log.Fatalf("cannot open %v", reply.Filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", reply.Filename)
			}
			file.Close()
			kvs := mapf(reply.Filename, string(content))

			// write kv into file buckets
			// key -> filename: "ihash(key)"
			temp := make([][]KeyValue, reply.R)
			for i := range temp {
				temp[i] = make([]KeyValue, 0)
			}

			for _, kv := range kvs {
				hash := ihash(kv.Key) % reply.R
				temp[hash] = append(temp[hash], kv)
			}

			for i := 0; i < len(temp); i++ {
				ofile, _ := ioutil.TempFile("./mr/mapfile", fmt.Sprintf("%d", i))
				enc := json.NewEncoder(ofile)
				for _, kv := range temp[i] {
					enc.Encode(&kv)
				}

				old_path := ofile.Name()
				new_path := fmt.Sprintf("../main/mr-tmp/mr-%d-%d", mapTaskNumber, i)

				os.Rename(old_path, new_path)
				ofile.Close()
			}

			// tell the master that the map job is done
			CallFinish(MAP, reply.TimeStamp, mapTaskNumber, 0)
		} else {
			// reduce phase
			reduceTaskNumber := reply.ReduceTaskNumber

			// read file
			ofile, _ := ioutil.TempFile("./mr/reducefile", fmt.Sprintf("%d", reduceTaskNumber))
			var kva []KeyValue
			for i := 0; i < reply.M; i++ {
				iFilename := fmt.Sprintf("../main/mr-tmp/mr-%d-%d", i, reduceTaskNumber)
				iFile, err := os.Open(iFilename)
				if err == nil {
					dec := json.NewDecoder(iFile)
					for {
						var kv KeyValue
						if err := dec.Decode(&kv); err != nil {
							break
						}
						kva = append(kva, kv)
					}
				} else {
					log.Fatal(err)
				}
			}
			sort.Sort(ByKey(kva))
			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

				i = j
			}

			old_path := ofile.Name()
			new_path := fmt.Sprintf("../main/mr-tmp/mr-out-%d", reduceTaskNumber)

			// no reduce task finished yet before
			if _, err := os.Open(new_path); err != nil {
				os.Rename(old_path, new_path)
			}
			ofile.Close()

			// tell the master that the reduce job is done
			CallFinish(REDUCE, reply.TimeStamp, 0, reduceTaskNumber)
		}
	}
}

// RPC call to the master.
func AskForTask() Reply {
	message := new(Args)
	reply := new(Reply)
	call("Master.Arrange", message, reply)

	return *reply
}

func CallFinish(finish TaskType, timestamp time.Time, m int, r int) {
	message := &Args{
		Finished:         finish,
		TimeStamp:        timestamp,
		MapTaskNumber:    m,
		ReduceTaskNumber: r,
	}
	reply := new(Reply)

	// send the RPC request, wait for the reply.
	call("Master.Finished", message, reply)
}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, message interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("unix", "mr-socket")
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, message, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
