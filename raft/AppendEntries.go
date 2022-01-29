package raft

import (
	"fmt"
	"reflect"
	"time"
)

//Useful Structs from passing command request from client
//RPC call from start to main loop and back again to start
type StartEntryMsg struct {
	Command interface{}
	ReplyCh chan *StartEntryReply
}
type StartEntryReply struct {
	Index int
	Mode  *Mode
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

//All Append Entries (besides my base one to avoid empty log cases), start here.
//This function sends a request to the main loop to append an entry to the log
//See mit commet above for more details on what the tester expects this to do.
//index,term,isleader
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	if !rf.killed() {
		startEntryMsg := &StartEntryMsg{}
		startEntryMsg.ReplyCh = make(chan *StartEntryReply)
		startEntryMsg.Command = command
		rf.requestStartCh <- startEntryMsg
		x := <-startEntryMsg.ReplyCh
		index = x.Index
		term = x.Mode.Term
		isLeader = x.Mode.IsLeader
	}
	return index, term, isLeader
}

//This is what is stored at each index of the log
type LogEntry struct {
	Term    int
	Command interface{}
}

//This is the fucntion usesd by the main func to handle start requests
//from the Start RPC
func (rf *Raft) startRequest(command interface{}) (int, int, bool) {
	index := -1
	mode := rf.stateRequest()
	if mode.IsLeader {
		index = rf.getLastestLogIndex() + 1
		entry := &LogEntry{}
		entry.Command = command
		entry.Term = mode.Term
		entriesSlice := make([]LogEntry, 0)
		entriesSlice = append(entriesSlice, *entry)
		rf.appendLog(entriesSlice)
	}
	return index, mode.Term, mode.IsLeader
}

//This is the function that every node calls when appending an entry to its log
//Leaders had to do more work in updating thier volitile state than Followers
//Since this function modifies a persistant variable, it must call persist()
func (rf *Raft) appendLog(entries []LogEntry) {
	fmt.Printf("")
	rf.log = append(rf.log, entries...)
	rf.persistState()

	if reflect.TypeOf(rf.state).Name() == "Leader" {
		rf.matchIndex[rf.me] = rf.getLastestLogIndex()
		rf.nextIndex[rf.me] = rf.getLastestLogIndex() + 1
	}
}

//Useful structs for passing data around from the main loop to RPCS and back
type AppendEntriesChan struct {
	AppEntries *AppendEntries
	ReplyCh    chan *AppendEntries
}
type AppendEntries struct {
	Args     *AppendEntriesArgs
	Reply    *AppendEntriesReply
	Server   int
	Recieved bool
}

//Structs actually sent over the wire
type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	CommitIndex  int
	//
	//
	//
}

//I added the additonal terms, LastLogTerm and LastLogIndex to the reply
//This helps the leader search its own log for a potential matching Index
//in much fewer messages than the original design, good for part 2c during
//constant leader failures halting progress.
type AppendEntriesReply struct {
	Term         int
	Sucess       bool
	LastLogTerm  int
	LastLogIndex int
}

//This is the function that leaders call to append entries to individual servers in a broadcast
func (rf *Raft) SendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) {
	ok := rf.peers[server].Call("Raft.AppendEntries", &args, reply)
	responseWrap := &AppendEntries{}
	responseWrap.Args = &args
	responseWrap.Reply = reply
	responseWrap.Server = server
	responseWrap.Recieved = ok
	rf.AppendEntriesReply <- responseWrap
}

//This is the RPC uses to handle Leaders trying to append to this node, sends a request to
//the main loop to process the data.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	ae := &AppendEntries{}
	ae.Args = args
	ae.Reply = reply

	aec := &AppendEntriesChan{}
	aec.AppEntries = ae
	aec.ReplyCh = make(chan *AppendEntries)

	rf.AppendEntriesChan <- aec
	<-aec.ReplyCh
	close(aec.ReplyCh)
}

//This is the function the main loop uses to calculate a response to the append
//Entry request
func (rf *Raft) AppendEntriesRequest(appEntries *AppendEntries) bool {
	return rf.state.handleAppendEntriesRequest(rf, appEntries)
}

//This is the function Leader uses to respone to append entry replies
//Cadidates and followers just return quickly
func (rf *Raft) appendEntryEnd(appendEntrys *AppendEntries) {
	rf.state.handleAppendEntryEnd(rf, appendEntrys)
}

//This function is uses to minimize messages between leader and followers who are trying to get in sync.
//Both leaders and followers will use this helper to determine the next best index to try to match on
func (rf *Raft) FindBestMatchIndex(inclusiveUpperBound int, inclusiveLowerBound int, termToLookFor int) int {
	for k := inclusiveUpperBound; k > inclusiveLowerBound-1; k-- {
		if rf.getTermAt(k) <= termToLookFor {
			return k
		}
	}
	return -1
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

func (rf *Raft) CommitEntries() {
	if rf.lastApplied < rf.SnapShotIndex {
		return
	}
	for rf.CommitIndex > rf.lastApplied {
		clientMsg := &ApplyMsg{}
		clientMsg.CommandIndex = rf.lastApplied + 1
		clientMsg.Command = rf.getEntry(clientMsg.CommandIndex).Command
		clientMsg.CommandValid = true
		select {
		case rf.publishCh <- *clientMsg:
		case <-time.After(time.Millisecond * 200):
			if !rf.killed() {
				panic("pro gamer move")
			}
		}
		rf.lastApplied++
	}

}
