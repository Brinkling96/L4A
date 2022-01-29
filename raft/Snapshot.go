package raft

import (
	"fmt"
	"time"
)

type SnapShot struct {
	Index   int
	Data    []byte
	replych chan bool
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true

	//if client is sending me an old snapshot;  { return false}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshotData []byte) {
	// Your code here (2D).
	fmt.Printf("")
	snapShotMsg := &SnapShot{
		Index:   index,
		Data:    snapshotData,
		replych: make(chan bool),
	}
	unset := true
	for unset {
		select {
		case rf.SnapShotChan <- *snapShotMsg:
			unset = false
		case <-time.After(time.Millisecond * 100):
			if rf.killed() {
				unset = false
			}
		}
	}
	<-snapShotMsg.replych
}

func (rf *Raft) updateSnapShot(snapshot SnapShot) {
	if snapshot.Index > rf.CommitIndex {
		fmt.Printf("A raft %v logsize, commit, ssi, %v, %v, %v", rf.me, rf.getLastestLogIndex(), rf.CommitIndex, snapshot.Index)
		panic("It can happen")
	} else if snapshot.Index < rf.SnapShotIndex {
		fmt.Printf("B raft %v logsize, commit, ssi, %v, %v, %v", rf.me, rf.getLastestLogIndex(), rf.CommitIndex, snapshot.Index)
		return
	} else {
		rf.SnapShotTerm = rf.getTermAt(snapshot.Index)
		rf.beheadLogAt(snapshot.Index + 1)
		rf.SnapShot = snapshot.Data
		rf.SnapShotIndex = snapshot.Index
		if rf.lastApplied < rf.SnapShotIndex {
			rf.lastApplied = rf.SnapShotIndex
		}
		rf.persistSnapshotandState()
		snapshot.replych <- true
	}
}

type InstallSnapShotMsg struct {
	SnapShotRPC *InstallSnapShot
	ReplyCh     chan bool
}

type InstallSnapShot struct {
	Args     *InstallSnapShotArgs
	Reply    *InstallSnapShotReply
	Server   int
	Recieved bool
}

type InstallSnapShotArgs struct {
	Term     int
	LeaderID int
	Data     []byte
	Index    int
	STerm    int
}
type InstallSnapShotReply struct {
	Term  int
	Index int
}

//sendSnapShot

func (rf *Raft) SendInstallSnapShot(server int, args *InstallSnapShotArgs, reply *InstallSnapShotReply) {

	ok := rf.peers[server].Call("Raft.InstallSnapShot", args, reply)
	responseWrap := &InstallSnapShot{}
	responseWrap.Args = args
	responseWrap.Reply = reply
	responseWrap.Server = server
	responseWrap.Recieved = ok

	rf.InstallSnapShotEndChan <- responseWrap

}

//The InstallSnapshot handler can use the applyCh to send the snapshot to the service, by putting the snapshot in ApplyMsg. The service reads from applyCh, and invokes CondInstallSnapshot with the snapshot to tell Raft that the service is switching to the passed-in snapshot state, and that Raft should update its log at the same time. (See applierSnap() in config.go to see how the tester service does this)
//InstallSnapShot

func (rf *Raft) InstallSnapShot(args *InstallSnapShotArgs, reply *InstallSnapShotReply) {
	eventloopmsg := &InstallSnapShotMsg{}
	eventloopmsg.SnapShotRPC = &InstallSnapShot{}
	eventloopmsg.SnapShotRPC.Args = args
	eventloopmsg.SnapShotRPC.Reply = reply
	eventloopmsg.ReplyCh = make(chan bool)

	rf.InstallSnapShotChan <- eventloopmsg
	<-eventloopmsg.ReplyCh
	close(eventloopmsg.ReplyCh)
}

//sendSnapShotEnd
