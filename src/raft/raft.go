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
	"fmt"
	"math/rand"
	"sync"
	"time"
)
import "labrpc"

// import "bytes"
// import "labgob"



//
// as each Raft peer becomes aware that successive log entries are
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
}
type State int

const (
	Follower State=iota
	Candidate
	Leader
)
var statemap=make(map[State]string,3)
//
//statemap[Follower]="follower"
//statemap[Candidate]="candidate"
//statemap[Leader]="leader"

//var ElecttionTime=time.Duration(rand.Intn(100)+350)*time.Millisecond  //选举超时
const HeartBeatTime=time.Duration(100)*time.Millisecond *4  //心跳时间
//定义状态机日志
type LogEntry struct{
	Command      interface{}
	Term int

}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	//2A
	currentTerm int   //当前任期
	votedFor    int  //投票节点id
	log    []LogEntry      //日志包含一条command和任期

	//所有服务器上不稳定的状态
	commitIndex  int     //已经提交的日志索引，从0开始
	lastApplied   int   //被状态机执行的最大的日志索引
	//领导者的不稳定状态
	nextIndex[]   int    //发送给follwer的下一条日志的索引，初始是leader的最大索引+1
	matchIndex[]   int     //记录已经复制到每个节点的最高日之所因

	state State
	voteCount    int   //选票计数
	applych chan ApplyMsg  //将已提交日志应用到状态机的通信chanel

	//这两个通道是收到并处理相应RPC后，通知主活动routine打破select阻塞，根据通道信号开始动作（重置选举时间）
	votech   chan struct{}  //这是个通信同步机制，节点每次收到请求投票RPC请求(或者回复），处理完成之后发送信号到自身的此通道，然后在节点活动主routine（（Make（）里）中重置本节点选举超时
	appendLogch chan struct{}  //这是个通信同步机制，节点每次收到leader心跳RPC请求（或者回复），处理完成之后发送信号到自身此通道，然后在节点活动主routine（（Make（）里）中重置本节点选举超时

	// timer
	timer *time.Timer  //定时器
	electTimeout time.Duration  //每个节点有固定的选举超时
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
//给config.go使用
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here (2A).
	term=rf.currentTerm
	if rf.state==Leader{
		isleader=true
	}else{
		isleader=false
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
	Term   int//candidate任期
	CandidateId  int  //
	LastLogIndex   int//候选人最后一条日志的索引
	LastLogTerm   int//最后日志任期
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term    int//其他节点回复candidate自己的当前任期
	VoteGranted  bool //是否投票
}

//
// example RequestVote RPC handler.
//6 节点接受投票RPC
//  若申请者任期更高，接收者变follower
//	若接收者任期高，回复任期和false，并且直接return，不重置时间
//	若接收者任期内未投票，则投票
//  通过votech通知主活动进程重置选举时间  见2

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2A, 2B).

	fmt.Printf("节点raft%v[%v]  term[%v]收到候选节点·[%v]投票请求，其任期[%v]\n", rf.me, statemap[rf.state],rf.currentTerm, args.CandidateId,args.Term)

	if args.Term<rf.currentTerm{   //如果candidate任期小，返回false
		reply.Term=rf.currentTerm
		reply.VoteGranted=false
		return
	}


	//所有rpc的规则。若rpc的请求或相应参数的任期更高，则收到消息的节点变为follower
	reply.VoteGranted=false   //投票默认结果false
	if args.Term>rf.currentTerm{
		fmt.Printf("节点raft%v[%v]  term[%v]比候选节点·[%v]的任期[%v]小\n", rf.me, statemap[rf.state],rf.currentTerm, args.CandidateId,args.Term)
		rf.currentTerm=args.Term
		rf.becomeNewFollower()
		//rf.votech<- struct{}{}   //通知主routine重置选举超时
	}


	if rf.votedFor==-1||rf.votedFor==args.CandidateId{  //如果candidate任期大，且没有给别人投票

		//rf.state=Follower   //如果给别人投票，承认自己是follower
		reply.Term=rf.currentTerm
		reply.VoteGranted=true
		rf.votedFor=args.CandidateId
		fmt.Printf("节点raft[%v] trem[%v]给节点raft[%v]投票，回复%v,%v\n",rf.me,rf.currentTerm,args.CandidateId,reply.Term,reply.VoteGranted)
	}
	reply.Term=rf.currentTerm
	fmt.Printf("%v---",time.Now().Format("2006-01-02 15:04:05.000"))
	fmt.Printf("节点raft[%v] trem[%v]给节点raft[%v]投票结束，回复%v,%v\n",rf.me,rf.currentTerm,args.CandidateId,reply.Term,reply.VoteGranted)

	go func(){   //通知主活动进程收到RPC，重置时间
		fmt.Printf("%v---",time.Now().Format("2006-01-02 15:04:05.000"))
		fmt.Printf("节点raft[%v] trem[%v]收到节点raft[%v]RPC请求,重置选举时间\n",rf.me,rf.currentTerm,args.CandidateId)
		rf.votech<-struct{}{}
	}()
}

//完善节点接受心跳并处理的RPC服务
//请求添加日志的参数（为空是心跳）
type RequestAppendEntriesArgs struct{

	Term int//领导者的term
	LeaderId int //用于follwer转发客户端请求
	PrevLogIndex int//领导者最新的日志之前的哪个日志索引

	PrevLogTerm int //领导者最新的日志之前的哪个日志的任期号
	Entries[] LogEntry //follower要添加的日志，可能是多条，如果为空就是心跳
	LeaderCommit int// 领导者提交的索引

}

type RequestAppendEntriesReply struct{

	Term int //follower回复leader自己当前任期，leader可用于更新任期
	Success bool //true如果follower包含PrevLogIndex和PrevLogTerm

}
//12 节点收到AppendEntries RPC请求的处理逻辑
//   若领导任期高，节点变大任期，变成follower
//	若接收者任期高，返回false，直接return，不重置时间
//   通过appendlogch通知主活动进程重置时间
func (rf *Raft) AppendEntries(args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply ) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//这个判断必须放在最前
	if args.Term<rf.currentTerm{   //如果leader任期比此节点还小
		reply.Term=rf.currentTerm
		reply.Success=false
		return
	}/*else if !rf.containLog(args.PrevLogIndex,args.PrevLogTerm){
		*reply=RequestAppendEntriesReply{rf.currentTerm,false}
	}*/
	reply.Success=true   //默认回复true
	//所有rpc的规则。若rpc的请求或相应参数的任期更高，则收到消息的节点变为follower
	if args.Term>=rf.currentTerm{
		rf.currentTerm=args.Term
			rf.becomeNewFollower()
	}


	reply.Term=rf.currentTerm
	go func (){
		fmt.Printf("%v---",time.Now().Format("2006-01-02 15:04:05.000"))
		fmt.Printf("raft%v[%v]  term[%v]收到 leader raft%v心跳RPV，重置选举时间\n", rf.me, statemap[rf.state],rf.currentTerm, args.LeaderId)
		rf.appendLogch<-struct{}{}
	}()

}
func (rf *Raft)containLog(PrevLogIndex int,PrevLogTerm int)bool{
	return true
}

//11	 领导广播发心跳
//
//	根据回复做动作
//		若发现任期更高的,交换任期,变成follower,这里不要通知主活动进程重置选举时间吗
func (rf *Raft) startAppendLog(){
	//rf.mu.Lock()
	//req:=RequestAppendEntriesArgs{Term:rf.currentTerm,LeaderId:rf.me,Entries:nil}
	//rf.mu.Unlock()
	for i:=0;i<len(rf.peers);i++{
		if i == rf.me {
			continue
		}
		go func(serverid int){
			//rf.mu.Lock()      //RPC这里不能用粗力度锁
			//defer rf.mu.Unlock()
			rf.mu.Lock()
			req:=RequestAppendEntriesArgs{Term:rf.currentTerm,LeaderId:rf.me,Entries:nil}
			rf.mu.Unlock()

			reply:=RequestAppendEntriesReply{}
			fmt.Printf("%v---",time.Now().Format("2006-01-02 15:04:05.000"))
			fmt.Printf("raft%v[%v]  term[%v]发送心跳 to raft%v\n", rf.me, statemap[rf.state],rf.currentTerm, serverid)
			ret:=rf.sendAppendEntries(serverid,&req,&reply)
			fmt.Printf("%v---",time.Now().Format("2006-01-02 15:04:05.000"))
			fmt.Printf("raft%v[%v]  term[%v]收到raft%v 心跳回复，reply是term[%v] success[%v]\n", rf.me, statemap[rf.state],rf.currentTerm, serverid,reply.Term,reply.Success)
			if rf.state!=Leader{
				fmt.Printf("我不是leader---")
				return
			}

			if ret{
				rf.mu.Lock()
				if reply.Term>rf.currentTerm{   //如果收到心跳节点的任期比leader更高
					fmt.Printf("我变了---")
					rf.currentTerm=reply.Term
					rf.becomeNewFollower()

					//rf.appendLogch<- struct{}{}  //通知主活动routine重置选举时间
				}
				rf.mu.Unlock()
			}
		}(i)
	}
	time.Sleep(HeartBeatTime)

}
//leader向某个server发送日志RPC
func (rf *Raft) sendAppendEntries(server int, args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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


	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
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
	//1 raft结构体初始化
	rf.state=Follower
	rf.currentTerm=0
	rf.votedFor=-1
	rf.voteCount=0
	rf.log=make([]LogEntry,100)
	rf.commitIndex=0
	rf.lastApplied=0

	rf.applych=applyCh
	//初始化同步通信无缓冲通道

	rf.votech=make(chan struct{})
	rf.appendLogch=make(chan struct{})
	//rf.nextIndex=make([]int,len(peers))
	//rf.matchIndex=make([]int,len(peers))
	electtime:=300+rand.Intn(100)
	electtime*=4
	rf.electTimeout=time.Duration(electtime)*time.Millisecond
	rf.timer=time.NewTimer(rf.electTimeout)   //初始化定时器
	fmt.Printf("%v---",time.Now().Format("2006-01-02 15:04:05.000"))
	fmt.Printf("create raft%v...", me)
	fmt.Printf("raft%v's term[%v] election timeout is:%v\n", rf.me,rf.currentTerm, rf.electTimeout)

	//节点开始运行，因为make要立即返回，开一个goroutine
	go func(){
		//defer rf.timer.Stop()
		//重新设置时间timeout
		//	2 节点运行,根据状态运动
		for{
			rf.mu.Lock()
			state:=rf.state
			rf.mu.Unlock()
			electtime:=300+rand.Intn(100)
			electtime*=4
			rf.electTimeout=time.Duration(electtime)*time.Millisecond

			switch state {
				case Follower:
					select {
						case <-rf.appendLogch:  //收到心跳
							rf.timer.Reset(rf.electTimeout)
						case <-rf.votech:  //收到投票
							rf.timer.Reset(rf.electTimeout)
							// 	3开始都是follower
						case<-rf.timer.C:

							fmt.Printf("%v---",time.Now().Format("2006-01-02 15:04:05.000"))
							fmt.Printf("raft%v[%v]  term[%v]收到定时器通知\n", rf.me, statemap[rf.state],rf.currentTerm,)
							//定时器如果不reset，只会发一条消息，reset之后才会再次发消息,下次发消息的时间间隔是reset传入的时间
							//rf.timer.Reset(rf.electTimeout)
							//4    某个节点超时,切换成申请者,并且开始选举
							rf.mu.Lock()
							rf.becomeCandidate()
							rf.mu.Unlock()
							rf.startElection()
					}
				case Candidate:

					select {
						case <-rf.appendLogch:  //收到心跳
							rf.timer.Reset(rf.electTimeout)
							//rf.becomeNewFollower()
						case <-rf.votech:  //收到投票
							rf.timer.Reset(rf.electTimeout)
						case<-rf.timer.C:
							fmt.Printf("%v---",time.Now().Format("2006-01-02 15:04:05.000"))
							fmt.Printf("raft%v[%v]  term[%v]收到定时消息\n", rf.me, statemap[rf.state],rf.currentTerm,)

							//rf.timer.Reset(rf.electTimeout)
							rf.startElection()
						default:  //8收到过半投票，变成leader
						//fmt.Printf("%v\n",rf.voteCount)
						//time.Sleep(100)
							rf.mu.Lock()
							if rf.voteCount>len(rf.peers)/2{
								rf.becomeLeader()
							}
							rf.mu.Unlock()
					}
				case Leader://10开始发心跳
					fmt.Printf("%v---",time.Now().Format("2006-01-02 15:04:05.000"))
					fmt.Printf("raft%v[%v]  term[%v]发送心跳之前\n", rf.me, statemap[rf.state],rf.currentTerm,)
					rf.startAppendLog()  //发心跳或appendentry
					//time.Sleep(HeartBeatTime)

			}
		}
	}()


	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	return rf
}
//4 变成申请者，并且开始选举(发广播投票）
func (rf *Raft)becomeCandidate(){

	fmt.Printf("raft%v[%v] become candidate in term:%v\n", rf.me, statemap[rf.state],rf.currentTerm)

	//状态参数开便
	//rf.mu.Lock()
	rf.state=Candidate

	//rf.mu.Unlock()

}
//5广播投票,并且根据投票回复做动作
	//7若回复任期高，此申请者变回follower
	//若收到投票，票数+1
func (rf *Raft)startElection(){
	rf.timer.Reset(rf.electTimeout)
	rf.mu.Lock()

	rf.currentTerm+=1
	rf.votedFor=rf.me
	rf.voteCount=1

	fmt.Printf("raft%v[%v]  term:%v    重置定时器开始选举\n", rf.me, statemap[rf.state],rf.currentTerm)
	//req:=RequestVoteArgs{rf.currentTerm,rf.me,len(rf.log)-1,rf.log[len(rf.log)-1].term}
	rf.mu.Unlock()

	for i:=0;i<len(rf.peers);i++{
		if i==rf.me{
			continue
		}
		go func(serverid int){
			rf.mu.Lock()

			req:=RequestVoteArgs{rf.currentTerm,rf.me,len(rf.log)-1,rf.log[len(rf.log)-1].Term}
			rf.mu.Unlock()
			if rf.state!=Candidate||rf.voteCount>len(rf.peers)/2{
				return
			}
			reply:=RequestVoteReply{}
			//发送投票rpc，根据返回结果作相应操作
			fmt.Printf("%v---",time.Now().Format("2006-01-02 15:04:05.000"))
			fmt.Printf("raft%v[%v]  term[%v]is sending RequestVote RPC to raft%v\n", rf.me, statemap[rf.state],rf.currentTerm, serverid)
			ret:=rf.sendRequestVote(serverid,&req,&reply)   //注意这不要用goroutine调用，否则ret没有意义 走到6
			fmt.Printf("raft%v[%v] term[%v]收到%v节点   投票RPC调用ret=%v\n", rf.me, statemap[rf.state],rf.currentTerm, serverid,ret)
			if(ret){
				fmt.Printf("%v---",time.Now().Format("2006-01-02 15:04:05.000"))
				fmt.Printf("raft%v[%v] term[%v]收到%v节点投票回复\n", rf.me, statemap[rf.state],rf.currentTerm, serverid)
				fmt.Printf("%v节点回复是,任期%v,投票%v\n", serverid,reply.Term,reply.VoteGranted)
				rf.mu.Lock()
				if reply.Term>rf.currentTerm{
					rf.currentTerm=reply.Term
					rf.becomeNewFollower()
					//rf.votech<-struct{}{}   //给主活动routine信号重置本candidate选举时间
					return
				}
				//if rf.state!=Candidate||rf.voteCount>len(rf.peers)/2{  //这个判断必须在发出RPC之前判断
				//	return
				//}
				if reply.VoteGranted{   //节点主活动进程会判断票数,当票数过半会变成leader
					rf.voteCount+=1
					fmt.Printf("raft%v[%v] term[%v]收到节点[%v]投票，当前票数[%v]\n", rf.me, statemap[rf.state],rf.currentTerm ,serverid, rf.voteCount)
				}
				rf.mu.Unlock()
			}

		}(i)

	}

}
//candidate变成follower，该状态，任期，投票votefor（-1）
func (rf *Raft)becomeNewFollower(){
	//rf.mu.Lock()
	fmt.Printf("raft%v become follower in term:%v\n", rf.me, rf.currentTerm)

	rf.state=Follower
	rf.votedFor=-1

	//rf.mu.Unlock()
}

//9获得过半投票，成为leader

func (rf *Raft)becomeLeader(){
	//rf.mu.Lock()   外层加索，里层再加锁造成死锁
	fmt.Printf("raft%v become leader in term:%v\n", rf.me, rf.currentTerm)
	rf.state=Leader

	//与leader的字段初始化放这比较好
	rf.nextIndex=make([]int,len(rf.peers))
	//初始化nextIndex
	//for i:=0;i<len(rf.peers);i++{
	//	rf.nextIndex[i]=rf.getLastLogIndex()
	//}


	rf.matchIndex=make([]int,len(rf.peers))
	//rf.timer.Reset(ElecttionTime)
	//rf.mu.Unlock()
}
//获取节点最后条日志索引
func (rf *Raft)getLastLogIndex()int{
	return len(rf.log)-1
}
//获取节点最后条日志任期
func (rf *Raft)getLastLogTerm()int{
	if len(rf.log)==0{return -1}
	index:=rf.getLastLogIndex()
	return rf.log[index].Term
}