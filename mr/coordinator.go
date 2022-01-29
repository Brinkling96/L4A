package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
	//my imports
)

//lkl
type MyError struct{}

type Done struct{}

func (m *Done) Error() string {
	return "done"
}

func (m *MyError) Error() string {
	return "boom"
}


type Coordinator struct {
	// Your definitions here.
	mapState    bool
	reduceState bool
	taskUid     int

	mapTasks          []MapReduceTask
	finishedMapTasks  int
	invalidMapTaskUid []int

	reduceTasks         []MapReduceTask
	finishedReduceTasks int
	nReduce             int

	muxtex sync.Mutex
	cond   *sync.Cond
}

type MapReduceTask struct {
	Uid       int
	Pos       int
	Status    string
	Inputfile []string
}

const ( // for status
	Unassigned = "unassigned"
	InProgress = "in_progress"
	Completed  = "completed"

	Map_task    = "map"
	Reduce_task = "reduce"

	Timeout = 10
)

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
<<<<<<< HEAD:src/mr/master.go
func (master *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
=======
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
>>>>>>> official/master:src/mr/coordinator.go
	reply.Y = args.X + 1
	return nil
}

<<<<<<< HEAD:src/mr/master.go
func (master *Master) HandleTaskComplete(request *TaskDoneArgs, reply *TaskDoneReply) error {
	master.cond.L.Lock()
	log.Println("task returned complete\n")
	switch request.TaskType {
	case Map_task:
		mapTask := master.findTask(Map_task, request.TaskNum)
		if mapTask != nil {
			mapTask.Status = Completed
			master.mapState = master.ismapTasksDone()
			master.addTempMapFilesToReduceTasks(request.FileNames)
			log.Println("Mt %v complete with %v", request.TaskNum, request.FileNames)
			master.finishedMapTasks = master.finishedMapTasks + 1
			if master.finishedMapTasks == master.numberOfMapTasks() {
				master.mapState = true
			}
		} else {
			log.Println("Assumed dead Mt %v just returned with %v", request.TaskNum, request.FileNames)
		}
		master.cond.L.Unlock()
		master.cond.Signal()
		return nil
	case Reduce_task:
		reduceTask := master.findTask(Reduce_task, request.TaskNum)
		if reduceTask != nil {
			reduceTask.Status = Completed
			log.Println("Rt %v complete with %v", request.TaskNum, request.FileNames)
			master.finishedReduceTasks = master.finishedReduceTasks + 1
			if master.finishedReduceTasks == master.nReduce {
				log.Println("DONE!!!!")
				master.reduceState = true
			}
		} else {
			log.Println("Assumed dead Rt %v just returned with %v", request.TaskNum, request.FileNames)
		}
		master.cond.L.Unlock()
		master.cond.Signal()
		return nil
	}

	master.cond.L.Unlock()
	master.cond.Signal()

	return &MyError{}

}

func (master *Master) HandleTaskRequest(request *RequestTaskArgs, reply *RequestTaskReply) error {
	log.Println("A worker just requested a task")
	master.cond.L.Lock()
	log.Println("Got the lock")
	if master.mapState {
		reduceTask, ok := master.findUnassignedReduceTask()
		if ok {
			log.Println("R t assigned")
			reduceTask.Status = InProgress
			reply.FileName = reduceTask.Inputfile
			reply.Tasknum = reduceTask.Uid
			reply.TaskPos = reduceTask.Pos
			reply.TaskType = Reduce_task
		} else {
			if master.reduceState {
				log.Println("A worker told to stop working cuz we done bois")
				reply.TaskType = Completed
			} else {
				master.cond.L.Unlock()
				master.cond.Signal()
				return &MyError{}
			}

		}
		timer2 := time.NewTimer(time.Second * Timeout)
		go func(tasknum int, taskpos int) {
			<-timer2.C
			master.cond.L.Lock()
			if master.reduceTasks[taskpos].Status != Completed {
				master.reduceTasks[taskpos].Status = Unassigned
				master.taskUid = master.taskUid + 1
				master.mapTasks[taskpos].Uid = master.taskUid
				log.Println("Lost Reduce Worker ", tasknum, "Assigned map task uid ", master.taskUid)
			}
			master.cond.L.Unlock()
			master.cond.Signal()
		}(reply.Tasknum, reply.TaskPos)
		reply.NReduce = master.nReduce
		master.cond.L.Unlock()
		return nil

	} else {
		mapTask, ok := master.findUnassignedMapTask()
		if ok {
			mapTask.Status = InProgress
			reply.FileName = mapTask.Inputfile
			reply.Tasknum = mapTask.Uid
			reply.TaskPos = mapTask.Pos
			reply.TaskType = Map_task
			log.Printf("M t assigned %v, %s", mapTask.Uid, mapTask.Inputfile)
		} else {
			master.cond.L.Unlock()
			master.cond.Signal()
			return &MyError{}
		}
		timer1 := time.NewTimer(time.Second * Timeout)
		go func(tasknum int, taskpos int) {
			<-timer1.C
			master.cond.L.Lock()
			if master.mapTasks[taskpos].Status != Completed {
				master.mapTasks[taskpos].Status = Unassigned
				master.taskUid = master.taskUid + 1
				master.mapTasks[taskpos].Uid = master.taskUid
				log.Println("Lost Mapper Worker ", tasknum, "Assigned map task uid ", master.taskUid)

			}
			master.cond.L.Unlock()
			master.cond.Signal()
		}(reply.Tasknum, reply.TaskPos)

		reply.NReduce = master.nReduce
		master.cond.L.Unlock()
		master.cond.Signal()
		return nil
	}

	master.cond.L.Unlock()

	return nil
}

func (master *Master) findUnassignedMapTask() (*MapReduceTask, bool) {
	for i := range master.mapTasks {
		v := &master.mapTasks[i]
		if v.isUnassigned() {
			return v, true
		}
	}
	return &MapReduceTask{}, false
}

func (master *Master) findUnassignedReduceTask() (*MapReduceTask, bool) {
	for i := range master.reduceTasks {
		v := &master.reduceTasks[i]
		if v.isUnassigned() {
			return v, true
		}
	}
	return &MapReduceTask{}, false
}

func (master *Master) ismapTasksDone() bool {
	for i := range master.mapTasks {
		v := &master.mapTasks[i]
		if v.isUnassigned() || v.isInProgress() {
			return false
		}
	}
	return true
}

func (master *Master) isreduceTasksDone() bool {
	for i := range master.reduceTasks {
		v := &master.reduceTasks[i]
		if v.isUnassigned() || v.isInProgress() {
			return false
		}
	}
	return true
}

func (task *MapReduceTask) isUnassigned() bool {
	if task.Status == Unassigned {
		return true
	}
	return false
}

func (master *Master) addTempMapFilesToReduceTasks(filenames []string) {
	for i := range filenames {
		filename := filenames[i]
		filesplit := strings.Split(filename, "-")
		reduceTaskNum, err := strconv.Atoi(filesplit[len(filesplit)-1])
		if err == nil {
			if reduceTaskNum < master.nReduce {
				master.reduceTasks[reduceTaskNum].Inputfile =
					append(master.reduceTasks[reduceTaskNum].Inputfile, filename)
			}
		}
	}
}

func (master *Master) findTask(taskType string, tasknum int) *MapReduceTask {
	switch taskType {
	case Map_task:
		for i := range master.mapTasks {
			if master.mapTasks[i].Uid == tasknum {
				return &master.mapTasks[i]
			}
		}
		return nil
	case Reduce_task:
		for i := range master.reduceTasks {
			if master.reduceTasks[i].Uid == tasknum {
				return &master.reduceTasks[i]
			}
		}
		return nil
	}
	return nil
}

func (task *MapReduceTask) isInProgress() bool {
	if task.Status == InProgress {
		return true
	}
	return false
}

func (task *MapReduceTask) isCompleted() bool {
	if task.Status == Completed {
		return true
	}
	return false
}

func (master *Master) numberOfMapTasks() int {
	return len(master.mapTasks)
}
=======
>>>>>>> official/master:src/mr/coordinator.go

//
// start a thread that listens for RPCs from worker.go
//
<<<<<<< HEAD:src/mr/master.go
func (master *Master) server() {
	rpc.Register(master)
=======
func (c *Coordinator) server() {
	rpc.Register(c)
>>>>>>> official/master:src/mr/coordinator.go
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
<<<<<<< HEAD:src/mr/master.go
func (master *Master) Done() bool {

=======
func (c *Coordinator) Done() bool {
>>>>>>> official/master:src/mr/coordinator.go
	ret := false

	master.cond.L.Lock()
	if master.isreduceTasksDone() {
		master.cond.Broadcast()
		time.Sleep(time.Second)
		ret = true
	}
	master.cond.L.Unlock()

	// Your code here.


	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
<<<<<<< HEAD:src/mr/master.go
func MakeMaster(files []string, nReduce int) *Master {
	mapTasks := make([]MapReduceTask, 0)
	reduceTasks := make([]MapReduceTask, 0)
	temp_taskUid := 0
	for i, v := range files {
		mapTasks = append(mapTasks,
			MapReduceTask{Uid: i, Pos: i, Status: Unassigned, Inputfile: []string{v}})
		temp_taskUid++
	}
	for i := 0; i < nReduce; i++ {
		reduceTasks = append(reduceTasks,
			MapReduceTask{Uid: i, Pos: i, Status: Unassigned})
		temp_taskUid++
	}

	master := Master{mapState: false, reduceState: false, taskUid: temp_taskUid,
		mapTasks: mapTasks, finishedMapTasks: 0,
		reduceTasks: reduceTasks, finishedReduceTasks: 0, nReduce: nReduce}

	master.cond = sync.NewCond(&master.muxtex)

	// Your code here.

	master.server()
	return &master
=======
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.


	c.server()
	return &c
>>>>>>> official/master:src/mr/coordinator.go
}
