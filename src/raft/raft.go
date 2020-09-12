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
	"math"
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
	Follower State = iota
	Candidate
	Leader
)

var statemap = make(map[State]string, 3)

//
//statemap[Follower]="follower"
//statemap[Candidate]="candidate"
//statemap[Leader]="leader"

//var ElecttionTime=time.Duration(rand.Intn(100)+350)*time.Millisecond  //选举超时
const HeartBeatTime = time.Duration(100) * time.Millisecond //心跳时间
//定义状态机日志
type LogEntry struct {
	Command interface{}
	Term    int
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
	currentTerm int        //当前任期
	votedFor    int        //投票节点id
	log         []LogEntry //日志包含一条command和任期,从1开始

	//所有服务器上不稳定的状态
	commitIndex int //已经提交的日志索引，从0开始
	lastApplied int //被状态机执行的最大的日志索引
	//领导者的不稳定状态
	nextIndex  []int //发送给follwer的下一条日志的索引，初始是leader的最大索引+1
	matchIndex []int //记录已经复制到每个节点的最高日志索引

	state     State
	voteCount int           //选票计数
	applych   chan ApplyMsg //将已提交日志应用到状态机的通信chanel,只要把msg发到这个channel就表示应用到状态机

	//这两个通道是收到并处理相应RPC后，通知主活动routine打破select阻塞，根据通道信号开始动作（重置选举时间）
	votech      chan struct{} //这是个通信同步机制，节点每次收到请求投票RPC请求(或者回复），处理完成之后发送信号到自身的此通道，然后在节点活动主routine（（Make（）里）中重置本节点选举超时
	appendLogch chan struct{} //这是个通信同步机制，节点每次收到leader心跳RPC请求（或者回复），处理完成之后发送信号到自身此通道，然后在节点活动主routine（（Make（）里）中重置本节点选举超时

	// timer
	timer        *time.Timer   //定时器
	electTimeout time.Duration //每个节点有固定的选举超时
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
	Term         int //candidate任期
	CandidateId  int //
	LastLogIndex int //候选人最后一条日志的索引
	LastLogTerm  int //最后日志任期
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  //其他节点回复candidate自己的当前任期
	VoteGranted bool //是否投票
}

//
// example RequestVote RPC handler.
//6 节点接受投票RPC
//  若申请者任期更高，接收者变follower
//	若接收者任期高，回复任期和false，并且直接return，不重置时间
//	若接收者任期内未投票，
//		且申请者日志更新
//		则投票
//  通过votech通知主活动进程重置选举时间  见2

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2A, 2B).

	fmt.Printf("节点raft%v[%v]  term[%v]收到候选节点·[%v]投票请求，其任期[%v]\n", rf.me, statemap[rf.state], rf.currentTerm, args.CandidateId, args.Term)

	if args.Term < rf.currentTerm { //如果candidate任期小，返回false
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	//所有rpc的规则。若rpc的请求或相应参数的任期更高，则收到消息的节点变为follower
	reply.VoteGranted = false //投票默认结果false
	if args.Term > rf.currentTerm {
		fmt.Printf("节点raft%v[%v]  term[%v]比候选节点·[%v]的任期[%v]小\n", rf.me, statemap[rf.state], rf.currentTerm, args.CandidateId, args.Term)
		rf.currentTerm = args.Term
		rf.becomeNewFollower()
		//rf.votech<- struct{}{}   //通知主routine重置选举超时
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId { //如果candidate任期大，且没有给别人投票
		//B1 选举限制，确保申请者的日志是更新的
		if (args.LastLogTerm > rf.getLastLogTerm()) || (args.LastLogTerm == rf.getLastLogTerm() && args.LastLogIndex >= rf.getLastLogIndex()) {
			//rf.state=Follower   //如果给别人投票，承认自己是follower
			reply.Term = rf.currentTerm
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			fmt.Printf("节点raft[%v] trem[%v]给节点raft[%v]投票，回复%v,%v\n", rf.me, rf.currentTerm, args.CandidateId, reply.Term, reply.VoteGranted)
			//只有把票投出去才能重置选举时间，而不是收到投票请求就重置选举时间
			go func() { //通知主活动进程收到RPC，重置时间
				fmt.Printf("%v---", time.Now().Format("2006-01-02 15:04:05.000"))
				fmt.Printf("节点raft[%v] trem[%v]收到节点raft[%v]RPC请求,重置选举时间\n", rf.me, rf.currentTerm, args.CandidateId)
				rf.votech <- struct{}{}
			}()
		}
	}
	reply.Term = rf.currentTerm
	fmt.Printf("%v---", time.Now().Format("2006-01-02 15:04:05.000"))
	fmt.Printf("节点raft[%v] trem[%v]给节点raft[%v]投票结束，回复%v,%v\n", rf.me, rf.currentTerm, args.CandidateId, reply.Term, reply.VoteGranted)

}

//完善节点接受心跳并处理的RPC服务
//请求添加日志的参数（为空是心跳）
type RequestAppendEntriesArgs struct {
	Term         int //领导者的term
	LeaderId     int //用于follwer转发客户端请求
	PrevLogIndex int //领导者最新的日志之前的哪个日志索引

	PrevLogTerm  int        //领导者最新的日志之前的哪个日志的任期号
	Entries      []LogEntry //follower要添加的日志，可能是多条，如果为空就是心跳
	LeaderCommit int        // 领导者提交的索引

}

type RequestAppendEntriesReply struct {
	Term    int  //follower回复leader自己当前任期，leader可用于更新任期
	Success bool //true如果follower包含PrevLogIndex和PrevLogTerm

}

//12 节点收到AppendEntries RPC请求的处理逻辑
//   若领导任期高，节点变大任期，变成follower
//	若接收者任期高，返回false，直接return，不重置时间
//   通过appendlogch通知主活动进程重置时间
/*
1所有rpc规则，如果rpc发起者任期高，本节点变follower
2如果leader任期更小直接回复false
2如果本节点log不包含prelogindex和prelogterm,回复false
3如果追加的日志与节点存在的日志相冲突,删除这条日志及其后的所有日志
4将leader发送的新日志追加
5如果leadercommit>本节点的commitindex,将commitindex设为leadercommit和lastlogindex的较小值

*/
func (rf *Raft) AppendEntries(args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//所有rpc的规则。若rpc的请求或相应参数的任期更高，则收到消息的节点变为follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.becomeNewFollower()
	}

	//reply.Success=true   //默认回复true
	//这个判断必须放在最前
	fmt.Printf("raft%v[%v]  term[%v]收到 leader raft%v appendentry请求 \n", rf.me, statemap[rf.state], rf.currentTerm, args.LeaderId)
	fmt.Printf("请求参数prelogindex %v prelogterm %v entrys %v leadercommit %v\n", args.PrevLogIndex, args.PrevLogTerm, args.Entries, args.LeaderCommit)
	if args.Term < rf.currentTerm { //如果leader任期比此节点还小
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	//必须在此重置选举时间
	go func() {
		fmt.Printf("%v---", time.Now().Format("2006-01-02 15:04:05.000"))
		fmt.Printf("raft%v[%v]  term[%v]收到 leader raft%v心跳RPV，重置选举时间\n", rf.me, statemap[rf.state], rf.currentTerm, args.LeaderId)
		rf.appendLogch <- struct{}{}
	}()

	prelogInedxTerm := -1
	if args.PrevLogIndex >= 0 && args.PrevLogIndex < len(rf.log) {
		prelogInedxTerm = rf.log[args.PrevLogIndex].Term
	}
	if prelogInedxTerm != args.PrevLogTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	//如果追加的日志与本节点冲突，删除冲突点之后的日志
	//并且将日志追加上去
	index := args.PrevLogIndex
	for i := 0; i < len(args.Entries); i++ {
		index++
		if index >= len(rf.log) {
			rf.log = append(rf.log, args.Entries[i:]...)
			break
		}
		if rf.log[index].Term != args.Entries[i].Term {
			rf.log = rf.log[:index]
			rf.log = append(rf.log, args.Entries[i:]...)
			break
		}

	}
	//if args.Entries!=nil&&!rf.containLog(args.PrevLogIndex,args.PrevLogTerm) {
	//	//B2 如果不包含追加日志的前一条日志
	//	*reply = RequestAppendEntriesReply{rf.currentTerm, false}
	//	fmt.Printf("不含前一条日志\n")
	//	return
	//	//B2 如果追加的第一条日志与节点存在的日志任期冲突，则删除这条日志及其之后的所有日志
	//}
	//if ((len(rf.log)>0)&&(len(rf.log)-1>args.PrevLogIndex)&&(rf.log[args.PrevLogIndex+1].Term!=args.Entries[0].Term)){
	//	rf.deleteLogAfterIndex(args.PrevLogIndex)
	//}

	////B2 将收到的日志加到此节点
	//for i:=0;i<len(args.Entries);i++{
	//	rf.log[args.PrevLogIndex+1+i]=args.Entries[i]
	//}
	//B2 根据领导者提交的索引更新本节点日志索引,取领导者提交索引和本节点最大索引的较小值
	fmt.Printf("raft[%v]的commintindex %v  leadercommit %v\n", rf.me, rf.commitIndex, args.LeaderCommit)
	if args.LeaderCommit > rf.commitIndex {
		newcommitIndex := int(math.Min(float64(args.LeaderCommit), float64(len(rf.log)-1)))
		fmt.Printf("raft[%v]的newcommintindex %v  leadercommit %v\n", rf.me, newcommitIndex, args.LeaderCommit)
		rf.setNewcommitIndexAndApplyLog(newcommitIndex)
	}
	reply.Term = rf.currentTerm
	reply.Success = true
	//比较commitindex和lsatapplied，将已经提交但还未应用到状态机的日志应用
	//rf.applyLogfromIndex()

}
func (rf *Raft) containLog(PrevLogIndex int, PrevLogTerm int) bool {
	if PrevLogIndex == -1 {
		return true
	}
	if len(rf.log)-1 >= PrevLogIndex && rf.log[PrevLogIndex].Term == PrevLogTerm {
		return true
	}
	return false
}
func (rf *Raft) deleteLogAfterIndex(PrevLogIndex int) {

	for i := PrevLogIndex + 1; i < len(rf.log); i++ {
		rf.log[i] = LogEntry{
			Command: nil,
			Term:    0,
		}
	}
}

//11	 领导广播发心跳
//
//	根据回复做动作
//		若发现任期更高的,交换任期,变成follower,这里不要通知主活动进程重置选举时间吗
//
//开始复制日志

//B3 从心跳功能改编成日志复制
/*
1如果不是leader,直接返回
2 如果回复成功
	更新 nextindex和matchindex
	并且更新leader的commitindex
否则
	如果回复者term更高,此leader变为follower
其他情况则需将nextindex递减
*/
func (rf *Raft) startAppendLog() {
	//rf.mu.Lock()
	//req:=RequestAppendEntriesArgs{Term:rf.currentTerm,LeaderId:rf.me,Entries:nil}
	//rf.mu.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(serverid int) {
			//rf.mu.Lock()      //RPC这里不能用粗力度锁
			//defer rf.mu.Unlock()
			rf.mu.Lock()
			req := RequestAppendEntriesArgs{Term: rf.currentTerm,
				LeaderId:     rf.me,
				Entries:      nil,
				LeaderCommit: rf.commitIndex,
				PrevLogIndex: rf.nextIndex[serverid] - 1, //PrevLogIndex用于follwer验证收到的追加日志的前一条日志是否与领导者匹配
				PrevLogTerm:  rf.log[rf.nextIndex[serverid]-1].Term,
			}
			//B3
			//规则5.3如果leader的lastindex>= 某个follower的nextindex，那么通过appendRPC将nextindex之后的所有日志发出去
			//如果成功，更新follower的nextindex和matchindex+-
			//如果因为一致性检查失败，减少nextindex并重试
			fmt.Printf("lastlogindex%v nextindex[%v] %v\n", rf.getLastLogIndex(), serverid, rf.nextIndex[serverid])
			if rf.getLastLogIndex() >= rf.nextIndex[serverid] {

				req = RequestAppendEntriesArgs{Term: rf.currentTerm,
					LeaderId: rf.me,
					Entries:  append([]LogEntry{}, rf.log[rf.nextIndex[serverid]:]...),

					PrevLogIndex: rf.nextIndex[serverid] - 1, //PrevLogIndex用于follwer验证收到的追加日志的前一条日志是否与领导者匹配
					PrevLogTerm:  rf.log[rf.nextIndex[serverid]-1].Term,
					LeaderCommit: rf.commitIndex,
				}
			}
			rf.mu.Unlock()

			reply := RequestAppendEntriesReply{}
			fmt.Printf("%v---", time.Now().Format("2006-01-02 15:04:05.000"))
			fmt.Printf("raft%v[%v]  term[%v]发送日志 to raft%v ，参数args：%v \n", rf.me, statemap[rf.state], rf.currentTerm, serverid, req)

			ret := rf.sendAppendEntries(serverid, &req, &reply)
			fmt.Printf("%v---", time.Now().Format("2006-01-02 15:04:05.000"))
			fmt.Printf("raft%v[%v]  term[%v]收到raft%v 日志回复，reply是%v\n", rf.me, statemap[rf.state], rf.currentTerm, serverid, reply)
			if rf.state != Leader {
				fmt.Printf("我不是leader---\n")
				return
			}
			// If last log index ≥ nextIndex for a follower: send
			// AppendEntries RPC with log entries starting at nextIndex
			//如果leader的最后一条日志大于等于一个跟随者的nextindex，则把nextindex之后的所有日志发送给跟随者

			// • If successful: update nextIndex and matchIndex for
			// follower (§5.3)
			//如果follwer返回成功，更新这个follower的matchIndex和nextIndex
			// • If AppendEntries fails because of log inconsistency:
			// decrement nextIndex and retry (§5.3)
			//如果因为日志一致性检查导致follower返回一个失败的reply，那么减小nextindex并且重试
			if ret {
				rf.mu.Lock()
				//parta部分
				//if reply.Term>rf.currentTerm{   //如果收到心跳节点的任期比leader更高
				//	fmt.Printf("我变了---")
				//	rf.currentTerm=reply.Term
				//	rf.becomeNewFollower()
				//
				//	//rf.appendLogch<- struct{}{}  //通知主活动routine重置选举时间
				//}
				if reply.Success { //sucess可能是追加日志成功，或日志为空

					rf.matchIndex[serverid] = rf.matchIndex[serverid] + len(req.Entries)
					//rf.matchIndex[serverid]=req.PrevLogIndex+len(req.Entries)
					rf.nextIndex[serverid] = rf.matchIndex[serverid] + 1
					fmt.Printf("nextindex更新:%v\n", rf.nextIndex)
					//如果追加日志成功，可以数数是否将日志复制到过半节点，然后可以更新leader的commitindex，并且可以apply部分日志，更新applyindex
					// If there exists an N such that N > commitIndex, a majority
					// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
					// set commitIndex = N (§5.3, §5.4).
					//这里是leader更新自己的commitindex
					//从最后一条日志到现在的commitindex之间找到一个点N，这个点出的日志存在与过半节点（即matchindex>=N）
					//那么这个点可以作为leader新的commitindex

					//更新leader自己的matchindex，自己与自己永远是match的
					rf.matchIndex[rf.me] = len(rf.log) - 1
					for N := len(rf.log) - 1; N > rf.commitIndex; N-- {
						count := 0 //记录多少节点复制了N索引的日志
						for i := 0; i < len(rf.matchIndex); i++ {
							if rf.matchIndex[i] >= N {
								count++
							}
						}
						if count > len(rf.peers)/2 {
							fmt.Printf("领导者commitindex%v\n", N)
							rf.setNewcommitIndexAndApplyLog(N)
							break
						}
					}
				} else {
					//失败的情况：1 follower任期更高 2一致性检查失败：PrevLogIndex不匹配，或者发送的Entries与follower冲突
					if reply.Term > rf.currentTerm { //如果收到心跳节点的任期比leader更高
						fmt.Printf("follower任期更高，本节点raft[%v]，变为follower---\n", rf.me)
						rf.currentTerm = reply.Term
						rf.becomeNewFollower()

						//rf.appendLogch<- struct{}{}  //通知主活动routine重置选举时间
					} else {
						//nextIndex递减，并在下一次心跳间隔重新发送
						rf.nextIndex[serverid]--
						fmt.Printf("nextindex %v递减，下次心跳发送---\n", rf.nextIndex)
						if rf.nextIndex[serverid] < 0 {
							rf.nextIndex[serverid] = 0
						}
					}

				}
				rf.mu.Unlock()
			}
		}(i)
	}
	time.Sleep(HeartBeatTime)

}

//设置新的提交索引，并且将一些日志应用到状态机
func (rf *Raft) setNewcommitIndexAndApplyLog(commitIndex int) {

	rf.commitIndex = commitIndex

	rf.applyLogfromIndex()
}

//B2根据提交索引commitindex和已经应用索引applyindex来将部分日志应用到状态机
func (rf *Raft) applyLogfromIndex() {
	if rf.commitIndex > rf.lastApplied {
		//fmt.Printf("raft%v[%v]  term[%v]开始将日志%v到%v应用到状态机\n", rf.me, statemap[rf.state],rf.currentTerm,rf.lastApplied+1,rf.commitIndex)
		entrysToApply := append([]LogEntry{}, rf.log[rf.lastApplied+1:rf.commitIndex+1]...) //apend一个slice加。。。，表示追加这个slice所有元素
		fmt.Printf("entrysToApply %v\n", entrysToApply)
		//开启一个goroutine将日志应用到状态机
		go func(startIndex int, entrysToApply []LogEntry) {
			msg := ApplyMsg{}
			for idx, entry := range entrysToApply {

				msg.CommandValid = true
				msg.Command = entry.Command
				msg.CommandIndex = rf.lastApplied + 1 + idx
				//发送到通道，就算应用到状态极乐
				fmt.Printf("raft[%v]发送到状态机%v\n", rf.me, msg)
				rf.applych <- msg
			}
			//应用到状态机制后修改lastapplied
			//rf.mu.Lock()
			if rf.lastApplied < msg.CommandIndex {
				rf.lastApplied = msg.CommandIndex
			}
			//rf.mu.Unlock()

		}(rf.lastApplied+1, entrysToApply)
	}

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
//参数是command
//返回值是command在日志中的索引,当前任期,以及此server是否是leader
//此函数用于给server发送command,如果server不是leader返回false
//否则开始共识并立即返回,start并不保证这个command会提交到server的日志
//B2
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := rf.currentTerm
	isLeader := true

	// Your code here (2B).

	if rf.state != Leader {
		return index, term, false
	} else {
		//否则将命令添加到这个节点日志

		rf.log = append(rf.log, LogEntry{Command: command, Term: rf.currentTerm})
		index = len(rf.log) - 1
		rf.matchIndex[rf.me] = index
		rf.nextIndex[rf.me] = index + 1
		fmt.Printf("raft[%v]添加了一套条令，cmd=%v,,log=%v", rf.me, command, rf.log)
	}

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
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.voteCount = 0
	rf.log = make([]LogEntry, 1) //开始index是1
	//fmt.Printf("日志初始化%v",len(rf.log))
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.applych = applyCh
	//初始化同步通信无缓冲通道

	rf.votech = make(chan struct{})
	rf.appendLogch = make(chan struct{})
	//rf.nextIndex=make([]int,len(peers))
	rf.matchIndex = make([]int, len(peers))
	electtime := 300 + rand.Intn(100)
	//electtime*=4
	rf.electTimeout = time.Duration(electtime) * time.Millisecond
	rf.timer = time.NewTimer(rf.electTimeout) //初始化定时器
	fmt.Printf("%v---", time.Now().Format("2006-01-02 15:04:05.000"))
	fmt.Printf("create raft%v...", me)
	fmt.Printf("raft%v's term[%v] election timeout is:%v\n", rf.me, rf.currentTerm, rf.electTimeout)

	//节点开始运行，因为make要立即返回，开一个goroutine
	go func() {
		defer rf.timer.Stop()
		//重新设置时间timeout
		//	2 节点运行,根据状态运动
		for {
			rf.mu.Lock()
			state := rf.state
			rf.mu.Unlock()
			//electtime:=300+rand.Intn(100)
			//electtime*=4
			//rf.electTimeout=time.Duration(electtime)*time.Millisecond

			switch state {
			case Follower:
				select {
				case <-rf.appendLogch: //收到心跳
					rf.timer.Reset(rf.electTimeout)
				case <-rf.votech: //收到投票
					rf.timer.Reset(rf.electTimeout)
					// 	3开始都是follower
				case <-rf.timer.C:

					fmt.Printf("%v---", time.Now().Format("2006-01-02 15:04:05.000"))
					fmt.Printf("raft%v[%v]  term[%v]收到定时器通知\n", rf.me, statemap[rf.state], rf.currentTerm)
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
				case <-rf.appendLogch: //收到心跳
					rf.timer.Reset(rf.electTimeout)
					//rf.becomeNewFollower()
				case <-rf.votech: //收到投票
					rf.timer.Reset(rf.electTimeout)
				case <-rf.timer.C:
					fmt.Printf("%v---", time.Now().Format("2006-01-02 15:04:05.000"))
					fmt.Printf("raft%v[%v]  term[%v]收到定时消息\n", rf.me, statemap[rf.state], rf.currentTerm)

					//rf.timer.Reset(rf.electTimeout)
					rf.startElection()
				default: //8收到过半投票，变成leader
					//fmt.Printf("%v\n",rf.voteCount)
					//time.Sleep(100)
					rf.mu.Lock()
					if rf.voteCount > len(rf.peers)/2 {
						rf.becomeLeader()
					}
					rf.mu.Unlock()
				}
			case Leader: //10开始发心跳
				fmt.Printf("%v---", time.Now().Format("2006-01-02 15:04:05.000"))
				fmt.Printf("raft%v[%v]  term[%v]发送心跳之前\n", rf.me, statemap[rf.state], rf.currentTerm)
				fmt.Printf("leader raft%v[%v]  term[%v]的日志长度%v 日志内容 %v\n", rf.me, statemap[rf.state], rf.currentTerm, len(rf.log), rf.log)

				rf.startAppendLog() //发心跳或appendentry
				//time.Sleep(HeartBeatTime)

			}
		}
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	return rf
}

//4 变成申请者，并且开始选举(发广播投票）
func (rf *Raft) becomeCandidate() {

	fmt.Printf("raft%v[%v] become candidate in term:%v\n", rf.me, statemap[rf.state], rf.currentTerm)

	//状态参数开便
	//rf.mu.Lock()
	rf.state = Candidate

	//rf.mu.Unlock()

}

//5广播投票,并且根据投票回复做动作
//7若回复任期高，此申请者变回follower
//若收到投票，票数+1
func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.timer.Reset(rf.electTimeout)

	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.voteCount = 1

	fmt.Printf("raft%v[%v]  term:%v    重置定时器开始选举\n", rf.me, statemap[rf.state], rf.currentTerm)
	//req:=RequestVoteArgs{rf.currentTerm,rf.me,len(rf.log)-1,rf.log[len(rf.log)-1].term}
	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(serverid int) {
			rf.mu.Lock()
			var req RequestVoteArgs
			if len(rf.log) > 0 {
				req = RequestVoteArgs{rf.currentTerm, rf.me, len(rf.log) - 1, rf.log[len(rf.log)-1].Term}
			} else {
				req = RequestVoteArgs{rf.currentTerm, rf.me, -1, -1}
			}
			rf.mu.Unlock()
			if rf.state != Candidate || rf.voteCount > len(rf.peers)/2 {
				return
			}
			reply := RequestVoteReply{}
			//发送投票rpc，根据返回结果作相应操作
			fmt.Printf("%v---", time.Now().Format("2006-01-02 15:04:05.000"))
			fmt.Printf("raft%v[%v]  term[%v]is sending RequestVote RPC to raft%v\n", rf.me, statemap[rf.state], rf.currentTerm, serverid)
			ret := rf.sendRequestVote(serverid, &req, &reply) //注意这不要用goroutine调用，否则ret没有意义 走到6
			fmt.Printf("raft%v[%v] term[%v]收到%v节点   投票RPC调用ret=%v\n", rf.me, statemap[rf.state], rf.currentTerm, serverid, ret)
			if ret {
				fmt.Printf("%v---", time.Now().Format("2006-01-02 15:04:05.000"))
				fmt.Printf("raft%v[%v] term[%v]收到%v节点投票回复\n", rf.me, statemap[rf.state], rf.currentTerm, serverid)
				fmt.Printf("%v节点回复是,任期%v,投票%v\n", serverid, reply.Term, reply.VoteGranted)
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.becomeNewFollower()
					//rf.votech<-struct{}{}   //给主活动routine信号重置本candidate选举时间
					rf.mu.Unlock()
					return
				}
				//if rf.state!=Candidate||rf.voteCount>len(rf.peers)/2{  //这个判断必须在发出RPC之前判断
				//	return
				//}
				if reply.VoteGranted { //节点主活动进程会判断票数,当票数过半会变成leader
					rf.voteCount += 1
					fmt.Printf("raft%v[%v] term[%v]收到节点[%v]投票，当前票数[%v]\n", rf.me, statemap[rf.state], rf.currentTerm, serverid, rf.voteCount)
				}
				rf.mu.Unlock()
			}

		}(i)

	}

}

//candidate变成follower，该状态，任期，投票votefor（-1）
func (rf *Raft) becomeNewFollower() {
	//rf.mu.Lock()
	fmt.Printf("raft%v become follower in term:%v\n", rf.me, rf.currentTerm)

	rf.state = Follower
	rf.votedFor = -1

	//rf.mu.Unlock()
}

//9获得过半投票，成为leader

func (rf *Raft) becomeLeader() {
	//rf.mu.Lock()   外层加索，里层再加锁造成死锁
	fmt.Printf("raft%v become leader in term:%v\n", rf.me, rf.currentTerm)
	rf.state = Leader

	//与leader的字段初始化放这比较好
	rf.nextIndex = make([]int, len(rf.peers))
	//初始化nextIndex,领导者最后一条日志索引+1
	for i := 0; i < len(rf.peers); i++ {
		//rf.nextIndex[i]=rf.getLastLogIndex()+1
		rf.nextIndex[i] = rf.getLastLogIndex() + 1
	}
	fmt.Printf("nextindex初始化 %v \n", rf.nextIndex)

	////初始化matchindex，开始是0,不能在此处初始化
	//rf.matchIndex=make([]int,len(rf.peers))
	//rf.timer.Reset(ElecttionTime)
	//rf.mu.Unlock()
}

//获取节点最后条日志索引
func (rf *Raft) getLastLogIndex() int {
	return len(rf.log) - 1
}

//获取节点最后条日志任期
func (rf *Raft) getLastLogTerm() int {
	if len(rf.log) == 0 {
		return -1
	}
	index := rf.getLastLogIndex()
	return rf.log[index].Term
}
