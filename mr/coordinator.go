package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
)

const (
	Waiting  int = 0
	Pending  int = 1
	Excuting int = 2
	Complete int = 3
)

const (
	StageMap    int = 1
	StageReduce int = 2
)

type MR struct {
	ID        string
	MTask     []*Task
	RTask     []*Task
	ImedFiles map[int][]string
	Stage     int
}

type Task struct {
	MRID      string
	ID        int
	Files     []string
	Status    int
	Timestamp time.Time
	Type      string
	mu        sync.Mutex
}

func (t *Task) GetTID() string {
	return t.MRID + "_" + strconv.Itoa(t.ID)
}

func GetTID(MRID string, taskID int) string {
	return MRID + "_" + strconv.Itoa(taskID)
}

type Coordinator struct {
	// Your definitions here.
	mrs       map[string]*MR
	mTaskChan chan *Task
	rTaskChan chan *Task
	mTasks    sync.Map
	rTasks    sync.Map
	nReduce   int
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) addMR(files []string) {
	// Create Map Task
	i := 0
	counter := 0
	mr := &MR{
		ID:    uuid.NewString(),
		MTask: make([]*Task, 0),
		RTask: make([]*Task, 0),
		Stage: StageMap,
	}
	for i < len(files) {
		task := &Task{
			MRID:   mr.ID,
			ID:     counter,
			Files:  []string{},
			Status: Pending,
			mu:     sync.Mutex{},
			Type:   "M",
		}
		j := i + 2
		for ; i < j && i < len(files); i++ {
			task.Files = append(task.Files, files[i])
		}
		c.mTasks.Store(task.GetTID(), task)
		c.mTaskChan <- task
		mr.MTask = append(mr.MTask, task)
		i = j
		counter++
	}
	for i := 0; i < c.nReduce; i++ {
		task := &Task{
			MRID:   mr.ID,
			ID:     i,
			Files:  []string{},
			Status: Waiting,
			mu:     sync.Mutex{},
			Type:   "R",
		}
		c.rTasks.Store(task.GetTID(), task)
		mr.RTask = append(mr.RTask, task)
	}
	c.mrs[mr.ID] = mr
}

func (c *Coordinator) corn() {
	go func() {
		for range time.NewTicker(1 * time.Second).C {
			// Re-execute map task
			c.mTasks.Range(func(key, value interface{}) bool {
				task := value.(*Task)
				task.mu.Lock()
				defer task.mu.Unlock()
				if task.Status == Excuting && task.Timestamp.Add(10*time.Second).Before(time.Now()) {
					fmt.Printf("map task %s_%d retry\n", task.MRID, task.ID)
					task.Status = Pending
					c.mTaskChan <- task
				}
				return true
			})
			// Re-execute reduce task
			c.rTasks.Range(func(key, value interface{}) bool {
				task := value.(*Task)
				task.mu.Lock()
				defer task.mu.Unlock()
				if task.Status == Excuting && task.Timestamp.Add(10*time.Second).Before(time.Now()) {
					fmt.Printf("reduce task %s_%d retry\n", task.MRID, task.ID)
					task.Status = Pending
					c.rTaskChan <- task
				}
				return true
			})
			// Check whether all map task complete
			for _, mr := range c.mrs {
				if mr.Stage == StageReduce {
					continue
				}
				allComplete := true
				for _, mtask := range mr.MTask {
					mtask.mu.Lock()
					if mtask.Status != Complete {
						mtask.mu.Unlock()
						allComplete = false
						break
					}
					mtask.mu.Unlock()
				}
				if allComplete {
					fmt.Printf("mr %s get into reduce phase\n", mr.ID)
					for _, rtask := range mr.RTask {
						rtask.mu.Lock()
						rtask.Status = Pending
						rtask.mu.Unlock()
						c.rTaskChan <- rtask
					}
					mr.Stage = StageReduce
				}
			}
		}
	}()
}

func (c *Coordinator) start() {
	c.server()
	c.corn()
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetTask(_ *GetTaskArgs, reply *GetTaskReply) error {
	select {
	case <-time.After(10 * time.Second):
		return nil
	case task := <-c.mTaskChan:
		reply.Type = "M"
		reply.MRID = task.MRID
		reply.ID = task.ID
		reply.Filename = task.Files
		reply.NReduce = c.nReduce
		task.mu.Lock()
		defer task.mu.Unlock()
		task.Timestamp = time.Now()
		task.Status = Excuting
	case task := <-c.rTaskChan:
		reply.Type = "R"
		reply.MRID = task.MRID
		reply.ID = task.ID
		reply.Filename = task.Files
		reply.NReduce = c.nReduce
		task.mu.Lock()
		defer task.mu.Unlock()
		task.Timestamp = time.Now()
		task.Status = Excuting
	}
	return nil
}

func (c *Coordinator) AckTask(args *AckTaskArgs, _ *AckTaskReply) error {
	switch args.Type {
	case "M":
		val, ok := c.mTasks.Load(GetTID(args.MRID, args.ID))
		if !ok {
			return nil
		}
		task := val.(*Task)
		task.mu.Lock()
		defer task.mu.Unlock()
		task.Status = Complete
		mr := c.mrs[args.MRID]
		for i, file := range args.Files {
			mr.RTask[i].mu.Lock()
			mr.RTask[i].Files = append(mr.RTask[i].Files, file)
			mr.RTask[i].mu.Unlock()
		}
		fmt.Printf("mtask %s complete\n", task.GetTID())
	case "R":
		val, ok := c.rTasks.Load(GetTID(args.MRID, args.ID))
		if !ok {
			return nil
		}
		task := val.(*Task)
		task.mu.Lock()
		defer task.mu.Unlock()
		task.Status = Complete
		fmt.Printf("rtask %s complete\n", task.GetTID())
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	for _, mr := range c.mrs {
		for _, mtask := range mr.MTask {
			mtask.mu.Lock()
			if mtask.Status != Complete {
				mtask.mu.Unlock()
				return false
			}
			mtask.mu.Unlock()
		}
		for _, rtask := range mr.RTask {
			rtask.mu.Lock()
			if rtask.Status != Complete {
				rtask.mu.Unlock()
				return false
			}
			rtask.mu.Unlock()
		}
	}
	// Your code here.
	return true
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce:   nReduce,
		mTaskChan: make(chan *Task, 100),
		mTasks:    sync.Map{},
		rTasks:    sync.Map{},
		rTaskChan: make(chan *Task, 100),
		mrs:       map[string]*MR{},
	}

	// Your code here.
	c.addMR(files)
	c.start()
	return &c
}
