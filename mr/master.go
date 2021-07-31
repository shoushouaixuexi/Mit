package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

/*
import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)
*/
//继承TaskStatInterface

type TaskStat struct {
	beginTime time.Time //开始时间
	fileName  string    //文件名
	fileIndex int       //map任务序号FileIndex
	partIndex int       //reduce任务序号PartIndex
	nReduce   int       //reduce任务个数NReduce
	nFiles    int       //文件数量
}

//TaskStat接口

type TaskStatInterface interface {
	GenerateTaskInfo() TaskInfo //获得任务信息结构体
	OutOfTime() bool            //判断是否超时
	GetFileIndex() int          //获得map任务序号
	GetPartIndex() int          //获得reduce任务序号
	SetNow()                    //设置开始时间
}

//获得map任务信息结构体

func (this *MapTaskStat) GenerateTaskInfo() TaskInfo {
	return TaskInfo{
		State:     TaskMap,
		FileName:  this.fileName,
		FileIndex: this.fileIndex,
		PartIndex: this.partIndex,
		NReduce:   this.nReduce,
		NFiles:    this.nFiles,
	}
}

//获得reduce任务信息结构体

func (this *ReduceTaskStat) GenerateTaskInfo() TaskInfo {
	return TaskInfo{
		State:     TaskReduce,
		FileName:  this.fileName,
		FileIndex: this.fileIndex,
		PartIndex: this.partIndex,
		NReduce:   this.nReduce,
		NFiles:    this.nFiles,
	}
}

//实现接口的OutOfTime()

func (this *TaskStat) OutOfTime() bool {
	return time.Now().Sub(this.beginTime) > time.Duration(time.Second*60)
}

//实现接口的SetNow()

func (this *TaskStat) SetNow() {
	this.beginTime = time.Now()
}

//实现接口的GetFileIndex()

func (this *TaskStat) GetFileIndex() int {
	return this.fileIndex
}

//实现接口的GetPartIndex()

func (this *TaskStat) GetPartIndex() int {
	return this.partIndex
}

//map继承TaskStat

type MapTaskStat struct {
	TaskStat
}

//reduce继承TaskStat

type ReduceTaskStat struct {
	TaskStat
}

//任务队列类 用来管理任务（本质上是一个栈）

type TaskStatQueue struct {
	taskArray []TaskStatInterface
	mutex     sync.Mutex
}

//任务队列api 加锁
func (this *TaskStatQueue) lock() {
	this.mutex.Lock()
}

//任务队列api 解锁
func (this *TaskStatQueue) unlock() {
	this.mutex.Unlock()
}

// 任务队列api 获得任务队列大小

func (this *TaskStatQueue) Size() int {
	return len(this.taskArray)
}

//任务队列api 尾部出队列

func (this *TaskStatQueue) Pop() TaskStatInterface {
	//加锁
	this.lock()
	//获取当前队列长度
	arrayLength := len(this.taskArray)
	//队列为空
	if arrayLength == 0 {
		this.unlock()
		return nil
	}
	//获取队列最后的任务
	ret := this.taskArray[arrayLength-1]
	//队列长度减1
	this.taskArray = this.taskArray[:arrayLength-1]
	//解锁
	this.unlock()
	return ret
}

// 任务队列api 尾部入队列

func (this *TaskStatQueue) Push(taskStat TaskStatInterface) {
	this.lock()
	//任务为空
	if taskStat == nil {
		this.unlock()
		return
	}
	//将任务加到末尾
	this.taskArray = append(this.taskArray, taskStat)
	this.unlock()
}


func (this *TaskStatQueue) TimeOutQueue() []TaskStatInterface {
	//创建超时队列 初始大小为0
	outArray := make([]TaskStatInterface, 0)
	this.lock()
	for taskIndex := 0; taskIndex < len(this.taskArray); {
		//对每一个任务
		taskStat := this.taskArray[taskIndex]
		//如果超时
		if (taskStat).OutOfTime() {
			//放入超时队列尾部
			outArray = append(outArray, taskStat)
			//删除超时任务
			this.taskArray = append(this.taskArray[:taskIndex], this.taskArray[taskIndex+1:]...)
			// must resume at this index next time
		} else {
			taskIndex++
		}
	}
	this.unlock()
	//返回超时任务队列
	return outArray
}

//
func (this *TaskStatQueue) MoveAppend(rhs []TaskStatInterface) {
	this.lock()
	//将超时任务加入任务队列
	this.taskArray = append(this.taskArray, rhs...)
	rhs = make([]TaskStatInterface, 0)
	this.unlock()
}

func (this *TaskStatQueue) RemoveTask(fileIndex int, partIndex int) {
	this.lock()
	for index := 0; index < len(this.taskArray); {
		task := this.taskArray[index]
		if fileIndex == task.GetFileIndex() && partIndex == task.GetPartIndex() {
			this.taskArray = append(this.taskArray[:index], this.taskArray[index+1:]...)
		} else {
			index++
		}
	}
	this.unlock()
}

type Master struct {
	// Your definitions here.
	//所有的输入文件
	filenames []string
	// reduce task queue
	reduceTaskWaiting TaskStatQueue
	reduceTaskRunning TaskStatQueue

	// map task statistics
	mapTaskWaiting TaskStatQueue
	mapTaskRunning TaskStatQueue

	// machine state
	isDone  bool
	nReduce int
}

//Your code here -- RPC handlers for the worker to call.
//RPC用来接收work节点任务获取请求

func (this *Master) AskTask(args *ExampleArgs, reply *TaskInfo) error {
	if this.isDone {
		reply.State = TaskEnd
		return nil
	}

	// check for reduce tasks
	reduceTask := this.reduceTaskWaiting.Pop()
	if reduceTask != nil {
		// an available reduce task
		// record task begin time
		reduceTask.SetNow()
		// note task is running
		this.reduceTaskRunning.Push(reduceTask)
		// setup a reply
		*reply = reduceTask.GenerateTaskInfo()
		fmt.Printf("Distributing reduce task on part %v %vth file %v\n", reply.PartIndex, reply.FileIndex, reply.FileName)
		return nil
	}

	// check for map tasks
	mapTask := this.mapTaskWaiting.Pop()
	if mapTask != nil {
		// an available map task
		// record task begin time
		mapTask.SetNow()
		// note task is running
		this.mapTaskRunning.Push(mapTask)
		// setup a reply
		*reply = mapTask.GenerateTaskInfo()
		fmt.Printf("Distributing map task on %vth file %v\n", reply.FileIndex, reply.FileName)
		return nil
	}

	// all tasks distributed
	if this.mapTaskRunning.Size() > 0 || this.reduceTaskRunning.Size() > 0 {
		// must wait for new tasks
		reply.State = TaskWait
		return nil
	}
	// all tasks complete
	reply.State = TaskEnd
	this.isDone = true
	return nil
}

//分发reduce任务（前提：map任务都完成）
func (this *Master) distributeReduce() {
	//定义reduce任务队列结构体
	reduceTask := ReduceTaskStat{
		TaskStat{
			fileIndex: 0,
			partIndex: 0,
			nReduce:   this.nReduce,
			nFiles:    len(this.filenames),
		},
	}
	for reduceIndex := 0; reduceIndex < this.nReduce; reduceIndex++ {
		//获取任务
		task := reduceTask
		//编号
		task.partIndex = reduceIndex
		//将所有reduce任务放入等待队列
		this.reduceTaskWaiting.Push(&task)
	}
}

func (this *Master) TaskDone(args *TaskInfo, reply *ExampleReply) error {
	switch args.State {
	case TaskMap:
		fmt.Printf("Map task on %vth file %v complete\n", args.FileIndex, args.FileName)
		this.mapTaskRunning.RemoveTask(args.FileIndex, args.PartIndex)
		if this.mapTaskRunning.Size() == 0 && this.mapTaskWaiting.Size() == 0 {
			// all map tasks done
			// can distribute reduce tasks
			this.distributeReduce()
		}
		break
	case TaskReduce:
		fmt.Printf("Reduce task on %vth part complete\n", args.PartIndex)
		this.reduceTaskRunning.RemoveTask(args.FileIndex, args.PartIndex)
		break
	default:
		panic("Task Done error")
	}
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.

func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go

func (m *Master) server() {
	//注册RPC服务
	rpc.Register(m)
	//采用http协议作为rpc载体
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	//调用RPC.go下的masterSock()函数获取一个unix套接字
	sockname := masterSock()
	os.Remove(sockname)
	//监听套接字
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil) //启动
}

/*
main/mrmaster.go calls Done() periodically to find out
if the entire job has finished.
*/

func (this *Master) Done() bool {
	//ret := false

	// Your code here.

	return this.isDone
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.

func MakeMaster(files []string, nReduce int) *Master {
	//uangjianyigejiegouti
	mapArray := make([]TaskStatInterface, 0)
	//对于所有的任务，生成相应的map任务并且存放到maparray
	for fileIndex, filename := range files {

		mapTask := MapTaskStat{
			TaskStat{
				fileName:  filename,
				fileIndex: fileIndex,
				partIndex: 0,
				nReduce:   nReduce,
				nFiles:    len(files),
			},
		}
		mapArray = append(mapArray, &mapTask)
	}
	m := Master{
		mapTaskWaiting: TaskStatQueue{taskArray: mapArray},
		nReduce:        nReduce,
		filenames:      files,
	}

	// Your code here.
	// create tmp directory if not exists
	if _, err := os.Stat("mr-tmp"); os.IsNotExist(err) {
		err = os.Mkdir("mr-tmp", os.ModePerm)
		if err != nil {
			fmt.Print("Create tmp directory failed... Error: %v\n", err)
			panic("Create tmp directory failed...")
		}
	}
	// begin a thread to collect tasks out of time
	go m.collectOutOfTime()

	m.server()
	return &m
}

//收集超时任务
func (this *Master) lectOutOfTime() {
	for {
		time.Sleep(time.Duration(time.Second * 5))
		//reduce超时任务队列
		timeouts := this.reduceTaskRunning.TimeOutQueue()
		//存在wait任务
		if len(timeouts) > 0 {
			//将超时任务加入到相应等待任务队列
			this.reduceTaskWaiting.MoveAppend(timeouts)
		}
		//map超时任务队列
		timeouts = this.mapTaskRunning.TimeOutQueue()
		//存在running 任务
		if len(timeouts) > 0 {
			//将超时任务加入相应等待任务队列
			this.mapTaskWaiting.MoveAppend(timeouts)
		}
	}
}
