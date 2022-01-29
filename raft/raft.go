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
	"sync/atomic"
	"time"

	"fmt"

	"6.824/labrpc"
)

//
// A Go object implementing a single Raft peer.
//

const (
	low  = 650
	high = 1200
)

type Raft struct {
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A,
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	term  int
	state State

	votedFor int
	votes    int

	votesFrom []bool

	requestStateChan chan chan *Mode

	requestVoteChan   chan *RequestVoteChan
	AppendEntriesChan chan *AppendEntriesChan

	requestVoteReply   chan *RequestVote
	AppendEntriesReply chan *AppendEntries

	//2B
	log []LogEntry

	CommitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	requestStartCh chan *StartEntryMsg

	publishCh chan ApplyMsg

	applych chan ApplyMsg

	//2C

	//2D
	SnapShotIndex int
	SnapShotTerm  int
	SnapShot      []byte
	SnapShotChan  chan SnapShot

	InstallSnapShotChan chan *InstallSnapShotMsg

	InstallSnapShotEndChan chan *InstallSnapShot
}

func (rf *Raft) getEntry(index int) *LogEntry {
	return &rf.log[rf.getModIndex(index)]
}
func (rf *Raft) getTermAt(index int) int {
	if rf.SnapShotIndex > index {
		return -1
	} else if rf.SnapShotIndex == index {
		return rf.SnapShotTerm
	} else {
		return rf.log[rf.getModIndex(index)].Term
	}
}

// this function helps modify index to the proper position in log after snapshoting
//for example Index 530 might get converted to 10 since the last snapshot index was 520
// (TODO or 519, forget which)
func (rf *Raft) getModIndex(index int) int {
	return index - (rf.SnapShotIndex + 1)
}

func (rf *Raft) getLastestLogIndex() int {
	return len(rf.log) + rf.SnapShotIndex
}

func (rf *Raft) truncateLogAt(index int) { // This removes ends
	rf.log = rf.log[:rf.getModIndex(index)]
}

func (rf *Raft) grabLogFrom(index int) []LogEntry { // This removes heads
	return rf.log[rf.getModIndex(index):]
}
func (rf *Raft) beheadLogAt(index int) {
	rf.log = rf.log[rf.getModIndex(index):]
}
func (rf *Raft) printLog() {
	fmt.Printf("Len(log):%v, snapIndex: %v, snapTermL %v\n", len(rf.log), rf.SnapShotIndex, rf.SnapShotTerm)
	for i := range rf.log {
		fmt.Printf("{Index:%v , Term %v, ModCommand:%v}", i+(rf.SnapShotIndex+1), rf.log[i].Term, rf.log[i].Command)
	}
}

func (rf *Raft) applyChThread() {
	msgBuffer := make([]ApplyMsg, 0)
	for rf.killed() == false {
		if len(msgBuffer) == 0 {
			select {
			case m := <-rf.publishCh:
				msgBuffer = append(msgBuffer, m)
			case <-time.After(time.Millisecond * 5):
			}
		} else {
			select {
			case m := <-rf.publishCh:
				msgBuffer = append(msgBuffer, m)
			case rf.applych <- msgBuffer[0]:
				msgBuffer = msgBuffer[1:]
			case <-time.After(time.Millisecond * 5):
			}
		}
	}
}

func (rf *Raft) broadcast() {
	rf.state.handleBroadcast(rf)
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

func timeoutReset() time.Duration {
	timeoutint := rand.Intn(high-low) + low
	timeout := time.Duration(timeoutint) * time.Millisecond
	return timeout
}

func (rf *Raft) nodeTimeout() {
	rf.state.handleNodeTimeout(rf)
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		timeout := time.After(timeoutReset())
		timeouted := false
		rf.broadcast()
		for !timeouted && !rf.killed() {
			select {
			case stateChan := <-rf.requestStateChan:
				stateChan <- rf.stateRequest()
			case <-timeout:
				rf.nodeTimeout()
				timeouted = true
			case reqVotechan := <-rf.requestVoteChan:
				timeouted = rf.RequestVoteRequest(reqVotechan.ReqVote)
				reqVotechan.ReplyCh <- reqVotechan.ReqVote
			case appEndChan := <-rf.AppendEntriesChan:
				timeouted = rf.AppendEntriesRequest(appEndChan.AppEntries)
				appEndChan.ReplyCh <- appEndChan.AppEntries
			case <-time.After(time.Millisecond * 100):
				rf.broadcast()
			case requestVoteReply := <-rf.requestVoteReply:
				temp := rf.state
				rf.requestVoteReplyF(requestVoteReply.Reply)
				if temp != rf.state {
					switch rf.state.(type) {
					case Leader:
						timeouted = true
					default:
					}
				}
			case appendEntryReply := <-rf.AppendEntriesReply:
				rf.appendEntryEnd(appendEntryReply)
			case startEntrymsg := <-rf.requestStartCh:
				reply := &StartEntryReply{}
				reply.Mode = &Mode{}
				reply.Index, reply.Mode.Term, reply.Mode.IsLeader = rf.startRequest(startEntrymsg.Command)
				startEntrymsg.ReplyCh <- reply
				if reply.Mode.IsLeader {
					timeouted = true
				}
			case snapShotMsg := <-rf.SnapShotChan:
				rf.updateSnapShot(snapShotMsg)
			case snapShotRPC := <-rf.InstallSnapShotChan:
				timeouted = rf.state.handleInstallSnapshot(rf, snapShotRPC.SnapShotRPC.Args, snapShotRPC.SnapShotRPC.Reply)
				snapShotRPC.ReplyCh <- true
			case snapShotEnd := <-rf.InstallSnapShotEndChan:
				rf.state.handleInstallSnapshotEnd(rf, snapShotEnd)
			}
		}
	}
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

	rf.state = Follower{}

	// initialize from state persisted before a crash
	//rf.votedFor = -1 //2c
	//rf.log = make([]LogEntry, 1) /////2c
	//rf.term = 0 //2c
	rf.readPersist(persister.ReadRaftState(), persister.ReadSnapshot())

	rf.votes = 0
	rf.votesFrom = make([]bool, len(peers))

	rf.requestStateChan = make(chan chan *Mode)

	rf.requestVoteChan = make(chan *RequestVoteChan, len(peers))
	rf.AppendEntriesChan = make(chan *AppendEntriesChan, len(peers))

	rf.requestVoteReply = make(chan *RequestVote, len(peers))
	rf.AppendEntriesReply = make(chan *AppendEntries, len(peers))

	//2b

	rf.CommitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.requestStartCh = make(chan *StartEntryMsg)

	rf.applych = applyCh

	rf.publishCh = make(chan ApplyMsg)
	go rf.applyChThread()

	if rf.SnapShotTerm == -1 {
		clientMsg := &ApplyMsg{}
		clientMsg.CommandIndex = rf.lastApplied
		clientMsg.Command = rf.getEntry(clientMsg.CommandIndex).Command
		clientMsg.CommandValid = true
		clientMsg.SnapshotValid = false
		rf.publishCh <- *clientMsg
	} else {
		clientMsg := &ApplyMsg{}
		clientMsg.SnapshotValid = true
		clientMsg.SnapshotIndex = rf.SnapShotIndex
		clientMsg.SnapshotTerm = rf.SnapShotTerm
		clientMsg.Snapshot = rf.SnapShot
		rf.lastApplied = rf.SnapShotIndex
		rf.publishCh <- *clientMsg
	}

	//2d
	rf.SnapShotChan = make(chan SnapShot)
	rf.InstallSnapShotChan = make(chan *InstallSnapShotMsg)
	rf.InstallSnapShotEndChan = make(chan *InstallSnapShot)
	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
