package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

var ErrRPC = errors.New("rpc error")

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.

	// Retrieve Task
	for {
		task, err := GetTask()
		if err == ErrRPC {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		// Do Task
		switch task.Type {
		case "M":
			imedFile, err := doMTask(task.MRID, task.ID, task.Filename, task.NReduce, mapf)
			if err != nil {
				log.Fatal(err)
			}
			if err := AckMTask(task.MRID, task.ID, imedFile); err != nil {
				log.Fatal(err)
			}
		case "R":
			if err := doRTask(task.MRID, task.ID, task.Filename, reducef); err != nil {
				log.Fatal(err)
			}
			if err := AckRTask(task.MRID, task.ID); err != nil {
				log.Fatal(err)
			}
		}
	}
}

func doMTask(MRID string, taskID int, files []string, nReduce int,
	mapf func(string, string) []KeyValue) ([]string, error) {
	tmpFiles := make([]*os.File, 0)
	imeds := make([]*json.Encoder, 0)
	for i := 0; i < nReduce; i++ {
		tmpFile, err := ioutil.TempFile(".", "imd-*")
		if err != nil {
			return nil, err
		}
		tmpFiles = append(tmpFiles, tmpFile)
		imeds = append(imeds, json.NewEncoder(tmpFile))
	}
	immediates := make([][]KeyValue, nReduce)
	for _, filename := range files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		for _, kv := range kva {
			immediates[ihash(kv.Key)%nReduce] = append(immediates[ihash(kv.Key)%nReduce], kv)
		}
	}
	res := []string{}
	for i, tmpFile := range tmpFiles {
		sort.Sort(ByKey(immediates[i]))
		for _, kv := range immediates[i] {
			imeds[i].Encode(kv)
		}
		err := os.Rename(tmpFile.Name(), "mr-"+GetTID(MRID, taskID)+"-"+strconv.Itoa(i))
		res = append(res, "mr-"+GetTID(MRID, taskID)+"-"+strconv.Itoa(i))
		if err != nil {
			return nil, err
		}
		if err := tmpFile.Close(); err != nil {
			return nil, err
		}
	}
	return res, nil
}

func doRTask(MRID string, taskID int, files []string,
	reducef func(string, []string) string) error {
	immediate := []KeyValue{}
	for _, filename := range files {
		f, err := os.Open(filename)
		if err != nil {
			log.Fatal(err)
		}
		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			immediate = append(immediate, kv)
		}
		f.Close()
	}

	sort.Sort(ByKey(immediate))

	oname := "mr-out-" + strconv.Itoa(taskID)
	ofile, _ := ioutil.TempFile(".", "imd-*")
	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(immediate) {
		j := i + 1
		for j < len(immediate) && immediate[j].Key == immediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, immediate[k].Value)
		}
		output := reducef(immediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", immediate[i].Key, output)

		i = j
	}
	os.Rename(ofile.Name(), oname)
	ofile.Close()
	return nil
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func GetTask() (task *GetTaskReply, err error) {
	args := GetTaskArgs{}
	reply := GetTaskReply{}

	for {
		if !call("Coordinator.GetTask", &args, &reply) {
			return nil, ErrRPC
		}
		if reply.MRID != "" {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	fmt.Printf("reply %v\n", reply)
	return &reply, nil
}

func AckMTask(MRID string, taskID int, files []string) error {
	args := AckTaskArgs{
		MRID:  MRID,
		ID:    taskID,
		Type:  "M",
		Files: files,
	}
	reply := AckTaskReply{}
	if !call("Coordinator.AckTask", &args, &reply) {
		return errors.New("rpc error")
	}
	return nil
}

func AckRTask(MRID string, taskID int) error {
	args := AckTaskArgs{
		MRID: MRID,
		ID:   taskID,
		Type: "R",
	}
	reply := AckTaskReply{}
	if !call("Coordinator.AckTask", &args, &reply) {
		return errors.New("rpc error")
	}
	return nil
}

func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	return err == nil
}
