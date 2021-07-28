package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.824/src/labrpc"
)

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes4 aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool

	Command      interface{}
	CommandIndex int
	CommandTerm  int
}

type LogEntry struct {
	Command interface{} //用于状态机的命令
	Term    int         //领导者接收到该条目时的任期
}

type RaftState int

const (
	Follower = iota
	Candidate
	Leader
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 服务器的状态
	state             RaftState // Raft server state
	leaderId          int       // leader id
	lastReceiveTime   time.Time // last time receive message(上次接收信息的时间)
	lastBroadcastTime time.Time // 作为领导 上次广播的时间

	// 所有服务器上的持久性状态 (在响应RPC请求之前 已经更新到了稳定的存储设备)
	currentTerm int        // latest term server has seen(服务器已知最新的任期)
	votedFor    int        // candidateId that received vote in current term(当前任期内收到选票的候选者id 如果没有投给任何候选者 则为空)
	log         []LogEntry // log entries(日志条目;每个条目包含了用于状态机的命令，以及领导者接收到该条目时的任期)

	// 所有服务器，易失状态

	commitIndex int // 已知的最大已提交索引(初始值为0，单调递增)
	lastApplied int // 当前应用到状态机的索引(初始值为0，单调递增)

	// 仅Leader，易失状态(成为leader时重置)

	nextIndex  []int //	每个follower的log同步起点索引(初始为leader log的最后一项)
	matchIndex []int // 每个follower的log同步进度(初始为0)，和nextIndex强关联
	// 应用层提交队列
	applyCh chan ApplyMsg // 应用层的提交队列
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int        // 紧邻新日志条目之前的那个日志条目的索引
	PrevLogTerm  int        // 紧邻新日志条目之前的那个日志条目的任期
	Entries      []LogEntry // 需要被保存的日志条目
	LeaderCommit int        // 领导者的已知已提交的最高的日志条目的索引
}

type AppendEntriesReply struct {
	Term    int  // 当前任期,对于领导者而言 它会更新自己的任期
	Success bool // 结果为真 如果跟随者所含有的条目和prevLogIndex以及prevLogTerm匹配上了
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	if rf.state == Leader {
		isleader = true
	} else {
		isleader = false
	}
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int //候选人的任期号
	CandidateId  int //请求选票的候选人的 Id
	LastLogIndex int //候选人的最后日志条目的索引值
	LastLogTerm  int //候选人最后日志条目的任期号
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  //当前任期号，以便于候选人去更新自己的任期号
	VoteGranted bool //候选人赢得了此张选票时为真
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//操作先加锁
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//填充响应信息
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	// 如果term < currentTerm返回 false (5.2 节)
	if args.Term < rf.currentTerm {
		return
	}
	// 候选者任期大于当前任期 修正任期 转换为追随者
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.leaderId = -1
	}
	// 如果 votedFor 为空或者为 candidateId，并且候选人的日志至少和自己一样新，那么就投票给他(5.2 节，5.4 节)
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {

		lastLogTerm := 0
		if len(rf.log) != 0 {
			lastLogTerm = rf.log[len(rf.log)-1].Term
		}
		if args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= len(rf.log)) {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.lastReceiveTime = time.Now()
		}
	}
	//持久化
	rf.persist()
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	// 2B
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 不是leader 返回
	if rf.state != Leader {
		return -1, -1, false
	}
	LogEntry := LogEntry{
		Command: command,
		Term:    rf.currentTerm,
	}
	rf.log = append(rf.log, LogEntry)
	index = len(rf.log)
	term = rf.currentTerm
	rf.persist()
	// 2B end
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = Follower
	rf.lastReceiveTime = time.Now()
	rf.votedFor = -1
	rf.leaderId = -1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// 2A
	go rf.electionLoop()      //选举
	go rf.appendEntriesLoop() //日志同步以及心跳包
	// 2B
	go rf.applyLogLoop(applyCh)
	return rf
}

// 并发RPC请求vote
type VoteResult struct {
	peerId int               //
	resp   *RequestVoteReply //收到的响应
}

// 并发RPC心跳
type AppendResult struct {
	peerId int
	resp   *AppendEntriesReply
}

// leader选举函数
func (rf *Raft) electionLoop() {
	for !rf.killed() {
		//
		time.Sleep(5 * time.Millisecond)

		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			now := time.Now()
			timeout := time.Duration(200+rand.Int31n(150)) * time.Millisecond // 超时随机化 200-350ms之间
			//fmt.Println(timeout)
			timeDifference := now.Sub(rf.lastReceiveTime) //计算从上次接收信息到现在时间差
			// Follower -> Candidate
			if rf.state == Follower {
				// 如果在超过选举超时时间的情况之前没有收到当前领导人(即该领导人的任期需与这个跟随者的当前任期相同)的心跳/附加日志，或者是给某个候选人投了票，就自己变成候选人
				if timeDifference >= timeout {
					rf.state = Candidate
					//fmt.Println("state : Follower -> Candidate")
					DPrintf("RaftNode[%d] state : Follower -> Candidate", rf.me)
				}
			}
			// 	候选者开始选举
			if rf.state == Candidate {
				/*
					自增当前的任期号(currentTerm)
					给自己投票
					重置选举超时计时器
					发送请求投票的 RPC 给其他所有服务器
				*/
				rf.lastReceiveTime = now // 重置下次选举时间
				rf.currentTerm += 1      // 发起新任期
				rf.votedFor = rf.me      // 该任期投了自己
				rf.persist()

				// 请求投票req
				args := RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateId:  rf.me,
					LastLogIndex: len(rf.log),
				}
				if len(rf.log) != 0 {
					args.LastLogTerm = rf.log[len(rf.log)-1].Term
				}

				rf.mu.Unlock()

				DPrintf("RaftNode[%d] RequestVote starts, Term[%d] LastLogIndex[%d] LastLogTerm[%d]", rf.me, args.Term,
					args.LastLogIndex, args.LastLogTerm)

				voteCount := 1   // 收到投票个数(先给自己投1票)
				finishCount := 1 // 收到应答个数
				// 创造一个len(rf.peers))长度的chan
				voteResultChan := make(chan *VoteResult, len(rf.peers))
				for peerId := 0; peerId < len(rf.peers); peerId++ {
					// 将收到的响应和peerid构造VoteResult对象交给select处理
					go func(id int) {
						if id == rf.me {
							return
						}
						resp := RequestVoteReply{}
						if ok := rf.sendRequestVote(id, &args, &resp); ok {
							voteResultChan <- &VoteResult{peerId: id, resp: &resp}
						} else {
							voteResultChan <- &VoteResult{peerId: id, resp: nil}
						}
					}(peerId)
				}
				maxTerm := 0
				for {
					select {
					case voteResult := <-voteResultChan:
						finishCount += 1
						if voteResult.resp != nil {
							if voteResult.resp.VoteGranted {
								voteCount += 1
							}
							if voteResult.resp.Term > maxTerm {
								maxTerm = voteResult.resp.Term
							}
						}
						// 如果接收到大多数服务器的选票，那么就变成领导人
						if finishCount == len(rf.peers) || voteCount > len(rf.peers)/2 {
							goto VOTE_END
						}
					}
				}
			VOTE_END:
				rf.mu.Lock()
				defer func() {
					DPrintf("RaftNode[%d] RequestVote ends, finishCount[%d] voteCount[%d] State[%d] maxTerm[%d] currentTerm[%d]", rf.me, finishCount, voteCount,
						rf.state, maxTerm, rf.currentTerm)
				}()
				// 如果角色改变了，则忽略本轮投票结果
				if rf.state != Candidate {
					return
				}
				// 发现了更高的任期，切回follower(如果接收到来自新的领导人的附加日志 RPC，转变成跟随者)
				if maxTerm > rf.currentTerm {
					rf.state = Follower
					rf.leaderId = -1
					rf.currentTerm = maxTerm
					rf.votedFor = -1
					rf.persist()
					return
				}
				// 赢得大多数选票，则成为leader
				if voteCount > len(rf.peers)/2 {
					rf.state = Leader
					rf.leaderId = rf.me
					rf.nextIndex = make([]int, len(rf.peers))
					for i := 0; i < len(rf.nextIndex); i++ {
						//初始值为领导者最后的日志条目的索引+1
						rf.nextIndex[i] = len(rf.log) + 1
					}
					rf.matchIndex = make([]int, len(rf.peers))
					for i := 0; i < len(rf.peers); i++ {
						rf.matchIndex[i] = 0
					}
					rf.lastBroadcastTime = time.Unix(0, 0) // 令appendEntries广播立即执行
					return
				}
			}
		}()
	}
}

// 处理接收到的心跳包 sendAppendEntries对应的rpc处理函数
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false
	// 返回假 如果领导者的任期 小于 接收者的当前任期(译者注：这里的接收者是指跟随者或者候选者)
	if args.Term < rf.currentTerm {
		return
	}
	// 如果接收到的 RPC 请求或响应中，任期号T > currentTerm，那么就令 currentTerm 等于 T，并切换状态为跟随者(5.1 节)
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.persist()
	}
	// 认识新的leader
	rf.leaderId = args.LeaderId
	// 刷新活跃时间 防止开始选举
	rf.lastReceiveTime = time.Now()

	// 返回假 本地没有前一个日志
	if len(rf.log) < args.PrevLogIndex {
		return
	}
	// 返回假 如果接收者日志中没有包含这样一个条目 即该条目的任期在prevLogIndex上能和prevLogTerm匹配上
	if args.PrevLogIndex > 0 && rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm {
		return
	}
	// 2B 更新日志(使用leader发的日志替换本地日志)
	for i, logEntry := range args.Entries {

		index := args.PrevLogIndex + i + 1
		if index > len(rf.log) {
			rf.log = append(rf.log, logEntry)
		} else {
			if rf.log[index-1].Term != logEntry.Term {
				//删除重叠日志
				rf.log = rf.log[:index-1]
				//推入leader发来的新日志
				rf.log = append(rf.log, logEntry)
			}
		}
	}
	rf.persist()
	/*
		如果领导者的已知已经提交的最高的日志条目的索引leaderCommit 大于 接收者的已知已经提交的最高的日志条目的索引commitIndex
		则把 接收者的已知已经提交的最高的日志条目的索引commitIndex 重置为领导者的已知已经提交的最高的日志条目的索引leaderCommit
		或者是 上一个新条目的索引 取两者的最小值
	*/
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
		if len(rf.log) < rf.commitIndex {
			rf.commitIndex = len(rf.log)
		}
	}
	reply.Success = true
	// 2B end
}

// 发送心跳包rpc
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// lab-2A做心跳包 lab-2B考虑log同步
func (rf *Raft) appendEntriesLoop() {
	for !rf.killed() {
		time.Sleep(5 * time.Millisecond)

		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			// 只有leader才向外广播心跳
			if rf.state != Leader {
				return
			}
			// 50ms广播1次(广播周期过长会导致不能通过测试，过短会导致占用cpu过高)
			now := time.Now()
			if now.Sub(rf.lastBroadcastTime) < 50*time.Millisecond {
				return
			}
			rf.lastBroadcastTime = time.Now()
			// 给所有peer发送心跳包(附带日志)
			for peerId := 0; peerId < len(rf.peers); peerId++ {
				if peerId == rf.me {
					continue
				}
				args := AppendEntriesArgs{}
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				// 2B 填充日志同步需要的参数
				args.LeaderCommit = rf.commitIndex
				args.Entries = make([]LogEntry, 0)
				args.PrevLogIndex = rf.nextIndex[peerId] - 1
				if args.PrevLogIndex > 0 {
					args.PrevLogTerm = rf.log[args.PrevLogIndex-1].Term
				}
				args.Entries = append(args.Entries, rf.log[rf.nextIndex[peerId]-1:]...)
				//2B end

				go func(id int, args1 *AppendEntriesArgs) {
					reply := AppendEntriesReply{}
					if ok := rf.sendAppendEntries(id, args1, &reply); ok {
						rf.mu.Lock()
						defer rf.mu.Unlock()

						// 如果rpc前不是leader状态了，直接返回
						if rf.currentTerm != args1.Term {
							return
						}
						// 如果接收到的 RPC 请求或响应中，任期号T > currentTerm，那么就令 currentTerm 等于 T，并切换状态为跟随者(5.1 节)
						if reply.Term > rf.currentTerm { // 变成follower
							rf.state = Follower
							rf.leaderId = -1
							rf.currentTerm = reply.Term
							rf.votedFor = -1
							rf.persist()
							return
						}
						// 2B
						if reply.Success {
							// 更新相应peer同步状态
							rf.nextIndex[id] += len(args1.Entries)
							rf.matchIndex[id] = rf.nextIndex[id] - 1

							sortedMatchIndex := make([]int, 0)
							sortedMatchIndex = append(sortedMatchIndex, len(rf.log))
							for i := 0; i < len(rf.peers); i++ {
								if i == rf.me {
									continue
								}
								sortedMatchIndex = append(sortedMatchIndex, rf.matchIndex[i])
							}
							sort.Ints(sortedMatchIndex)
							newCommitIndex := sortedMatchIndex[len(rf.peers)/2]
							// 假设存在大于 commitIndex 的 N，使得大多数的 matchIndex[i] ≥ N 成立，且 log[N].term == currentTerm 成立，
							// 则令 commitIndex 等于 N
							//fmt.Println(len(rf.log), newCommitIndex)
							if newCommitIndex > rf.commitIndex && rf.log[newCommitIndex-1].Term == rf.currentTerm {
								rf.commitIndex = newCommitIndex
							}
						} else {
							//日志不匹配 同步起点索引后退
							rf.nextIndex[id] -= 1
							if rf.nextIndex[id] < 1 {
								rf.nextIndex[id] = 1
							}
						}
					}
				}(peerId, &args)
			}
		}()
	}
}

//日志提交协程
func (rf *Raft) applyLogLoop(applyCh chan ApplyMsg) {
	for !rf.killed() {

		time.Sleep(5 * time.Millisecond)
		var appliedMsgs = make([]ApplyMsg, 0)
		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			// 如果commitIndex > lastApplied，那么就 lastApplied 加一，并把log[lastApplied]应用到状态机中(5.3 节)
			for rf.commitIndex > rf.lastApplied {
				rf.lastApplied += 1
				appliedMsgs = append(appliedMsgs, ApplyMsg{
					CommandValid: true,
					Command:      rf.log[rf.lastApplied-1].Command,
					CommandIndex: rf.lastApplied,
					CommandTerm:  rf.log[rf.lastApplied-1].Term,
				})
			}
		}()
		// 留言(msg)锁外提交给应用层 应用层追加日志
		for _, msg := range appliedMsgs {
			applyCh <- msg
		}
	}
}
