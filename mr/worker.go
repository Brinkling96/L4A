package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"time"
)


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

const (
	outputPrefix = "mr-out"
	tempPrefix   = "mr-ini"
)

//Intermediate file name format is mr_x_y
//x is task num
//y is module

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

<<<<<<< HEAD
	// uncomment to send the Example RPC to the master.
=======
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
>>>>>>> official/master

	for true {
		log.Println("Looking for work")
		fileNames, nReduce, taskNum, taskType, ok := CallforWork()
		if taskType == Completed {
			log.Println("Worker told job is done")
			break
		}
		if ok {
			switch taskType {
			case Map_task:
				log.Println("Started A Map Task")
				intermediate := make([]ByKey, nReduce)
				for i := range fileNames {
					fileName := fileNames[i]
					log.Printf("Reading: %s", fileName)
					file, err := os.Open(fileName)
					if err != nil {
						log.Fatalf("cannot open %v", fileName)
					}

					content, err := ioutil.ReadAll(file)
					if err != nil {
						log.Fatalf("cannot read %v", fileName)
					}
					file.Close()

					kva := mapf(fileName, string(content))

					for j := range kva {
						hash := ihash(kva[j].Key) % nReduce
						intermediate[hash] = append(intermediate[hash], kva[j])
					}

				}

				var outputfilenames []string
				for i := range intermediate {
					oname := fmt.Sprintf("%s-%v-%v", tempPrefix, taskNum, i)
					ofile, _ := os.Create(oname)
					sort.Sort(ByKey(intermediate[i]))
					for j := 0; j < len(intermediate[i]); j++ {
						fmt.Fprintf(ofile, "%v %v\n", intermediate[i][j].Key, intermediate[i][j].Value)
					}
					ofile.Close()
					outputfilenames = append(outputfilenames, oname)
				}

				callDone(taskNum, Map_task, outputfilenames)
				log.Printf("M task: %v done \n", taskNum)
			case Reduce_task:
				log.Printf("Started A Reduce Task with %s\n", fileNames)
				kva := make([]KeyValue, 0)
				for i := range fileNames { //TODO: need to add loop for nMapTasks
					fileName := fileNames[i]
					file, err := os.Open(fileName)
					if err != nil {
						log.Fatalf("cannot open %s", fileName)
					}
					content, err := ioutil.ReadAll(file)
					if err != nil {
						log.Fatalf("cannot read %s", fileName)
					}
					file.Close()
					stringSlice := strings.Split(string(content), "\n")
					for k := range stringSlice {
						kvSplit := strings.Split(stringSlice[k], " ")
						if len(kvSplit) != 1 {
							kva = append(kva, KeyValue{Key: kvSplit[0], Value: kvSplit[1]})
						}
					}
				}
				sort.Sort(ByKey(kva))

				oname := fmt.Sprintf("%s-%v", outputPrefix, taskNum)
				ofile, _ := os.Create(oname)
				for i := 0; i < len(kva); {
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
				ofile.Close()
				fileNames := []string{oname}
				callDone(taskNum, Reduce_task, fileNames)
				log.Printf("R task: %v done \n", taskNum)
			}
		} else {
			time.Sleep(time.Second)
		}
	}
	log.Println("I got here somehow")
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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

func CallforWork() ([]string, int, int, string, bool) {

	args := RequestTaskArgs{}
	reply := RequestTaskReply{}

	ok := call("Master.HandleTaskRequest", &args, &reply)

	return reply.FileName, reply.NReduce, reply.Tasknum, reply.TaskType, ok
	/*FileName string
	NReduce  int
	Tasknum  int
	WorkerNum int
	TaskType string
	*/
}

func callDone(taskNum int, taskType string, filenames []string) (r *TaskDoneReply) {
	args := TaskDoneArgs{TaskNum: taskNum, TaskType: taskType, FileNames: filenames}
	reply := TaskDoneReply{}

	call("Master.HandleTaskComplete", &args, &reply)

	return &reply
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
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
