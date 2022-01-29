package raft

import (
	"bytes"

	"6.824/labgob"
)

//these fucntison ensure that any changes to votes or terms are persisted
func (rf *Raft) vote(server int) {
	rf.votedFor = server
	rf.persistState()
}
func (rf *Raft) changeTerm(term int) {
	rf.term = term
	rf.persistState()
}

//This function is use to update the persist object pass to raft nodes
//during creation. THis object will be uses to restart the state after a node is reborn
//by the tester or config file (not sure which does this right now)
func (rf *Raft) getPersistData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.term)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.SnapShotIndex)
	e.Encode(rf.SnapShotTerm)
	data := w.Bytes()
	return data
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persistState() {
	data := rf.getPersistData()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) persistSnapshotandState() {
	data := rf.getPersistData()
	rf.persister.SaveStateAndSnapshot(data, rf.SnapShot)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte, ss []byte) {
	if data == nil || len(data) < 1 {
		rf.term = 0
		rf.log = make([]LogEntry, 1)
		rf.log[0] = LogEntry{
			Term:    0,
			Command: 1003,
		}
		rf.votedFor = -1
		rf.SnapShotIndex = -1
		rf.SnapShotTerm = -1
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var term int
	var votedFor int
	var logs []LogEntry
	var SnapShotIndex int
	var SnapShotTerm int

	if d.Decode(&term) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&SnapShotIndex) != nil ||
		d.Decode(&SnapShotTerm) != nil {
		panic("rf read persist err")
	} else {
		rf.term = term
		rf.votedFor = votedFor
		rf.log = logs
		rf.SnapShotIndex = SnapShotIndex
		rf.SnapShotTerm = SnapShotTerm
	}
	rf.SnapShot = ss
}

func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
}
