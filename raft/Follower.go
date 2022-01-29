package raft

import "fmt"

type Follower struct{}

func (state Follower) handleAppendEntriesRequest(rf *Raft, appEntries *AppendEntries) bool {
	args := appEntries.Args
	reply := appEntries.Reply
	temp := len(args.Entries) + args.PrevLogIndex
	temp += 1
	temp -= 1
	if args.Term < rf.term {
		reply.Sucess = false
		reply.Term = rf.term
		return false

	} else if args.Term > rf.term {
		rf.changeTerm(args.Term)
		rf.vote(-1)
	} else if args.PrevLogIndex < rf.CommitIndex {
		reply.Sucess = false
		reply.Term = rf.term
		reply.LastLogIndex = -1
		reply.LastLogTerm = -1

	}

	nodePrevLogIndex := rf.getLastestLogIndex()
	nodePrevLogTerm := rf.getTermAt(nodePrevLogIndex)
	if args.PrevLogIndex > nodePrevLogIndex {
		reply.Sucess = false
		reply.Term = rf.term
		if nodePrevLogTerm > args.PrevLogTerm {
			if rf.CommitIndex <= rf.SnapShotIndex {
				reply.LastLogIndex = rf.FindBestMatchIndex(nodePrevLogIndex-1, rf.SnapShotIndex+1, args.PrevLogTerm)
				if reply.LastLogIndex < rf.SnapShotIndex {
					reply.LastLogTerm = -1
				} else if reply.LastLogIndex == rf.SnapShotIndex {
					reply.LastLogTerm = rf.SnapShotTerm
				} else {
					reply.LastLogTerm = rf.getTermAt(reply.LastLogIndex)
				}
			} else {
				reply.LastLogIndex = rf.FindBestMatchIndex(nodePrevLogIndex-1, rf.CommitIndex, args.PrevLogTerm)
				reply.LastLogTerm = rf.getTermAt(reply.LastLogIndex)
			}
		} else {
			reply.LastLogIndex = nodePrevLogIndex
			reply.LastLogTerm = rf.getTermAt(nodePrevLogIndex)

		}
		return true
	} else if args.PrevLogIndex < nodePrevLogIndex {
		if (args.PrevLogIndex < rf.SnapShotIndex) && (rf.SnapShotTerm != -1) {
			if rf.CommitIndex > rf.SnapShotIndex {
				reply.LastLogIndex = rf.CommitIndex
				reply.LastLogTerm = rf.getTermAt(rf.CommitIndex)
			} else {
				reply.LastLogIndex = rf.SnapShotIndex
				reply.LastLogTerm = rf.getTermAt(rf.SnapShotIndex)
			}
			return true

		} else if args.PrevLogTerm == rf.getTermAt(args.PrevLogIndex) {
			rf.truncateLogAt(args.PrevLogIndex + 1)
			followerAppend(rf, appEntries)
			return true
		} else {
			reply.Sucess = false
			reply.Term = rf.term
			if args.PrevLogTerm > rf.getTermAt(args.PrevLogIndex) {
				reply.LastLogIndex = args.PrevLogIndex - 1
				reply.LastLogTerm = rf.getTermAt(reply.LastLogIndex)
				return true
			} else {
				reply.LastLogIndex = rf.FindBestMatchIndex(args.PrevLogIndex-1, rf.CommitIndex, args.PrevLogTerm)
				reply.LastLogTerm = rf.getTermAt(reply.LastLogIndex)
				return true
			}
		}
	} else {
		if args.PrevLogTerm == nodePrevLogTerm {
			followerAppend(rf, appEntries)
			return true
		} else {
			reply.Sucess = false
			reply.Term = rf.term
			if args.PrevLogTerm > rf.getTermAt(args.PrevLogIndex) {
				reply.LastLogIndex = args.PrevLogIndex - 1
				reply.LastLogTerm = rf.getTermAt(reply.LastLogIndex)

			} else {
				reply.LastLogIndex = rf.FindBestMatchIndex(args.PrevLogIndex-1, rf.CommitIndex, args.PrevLogTerm)
				reply.LastLogTerm = rf.getTermAt(reply.LastLogIndex)
			}
			return true
		}
	}
}

func followerAppend(rf *Raft, appEntries *AppendEntries) {
	args := appEntries.Args
	reply := appEntries.Reply

	reply.Sucess = true
	reply.Term = rf.term
	reply.LastLogTerm = -1
	reply.LastLogIndex = -1
	rf.appendLog(args.Entries)
	fmt.Printf("")
	if rf.CommitIndex < args.CommitIndex {
		rf.CommitIndex = args.CommitIndex
		rf.CommitEntries()
	}
}

func (state Follower) handleBroadcast(rf *Raft) {
}

func (state Follower) handleNodeTimeout(rf *Raft) {
	rf.ChangeRole(Candidate{})

}

func (state Follower) handleRequestVoteReplyF(rf *Raft, reply *RequestVoteReply) {

}

func (state Follower) handleAppendEntryEnd(rf *Raft, appendEntrys *AppendEntries) {
}

func (state Follower) handleRequestVoteRequest(rf *Raft, requestVote *RequestVote) bool {
	args := requestVote.Args
	reply := requestVote.Reply
	if args.Term > rf.term {
		rf.changeTerm(args.Term)
		if rf.CheckLogForVote(args) {
			reply.VoteGranted = true
			rf.vote(args.CandidateID)
		} else {
			reply.VoteGranted = false
		}
	} else if args.Term == rf.term {
		if rf.votedFor == args.CandidateID {
			reply.VoteGranted = true
		} else if rf.votedFor == -1 {
			if rf.CheckLogForVote(args) {
				reply.VoteGranted = true
				rf.vote(args.CandidateID)
			} else {
				reply.VoteGranted = false
			}
		} else {
			reply.VoteGranted = false
		}
	} else {
		reply.VoteGranted = false
	}
	reply.Term = rf.term
	reply.ID = rf.me
	return reply.VoteGranted
}

func (state Follower) handleInstallSnapshot(rf *Raft, args *InstallSnapShotArgs, reply *InstallSnapShotReply) bool {
	reply.Index = -100
	if args.Term < rf.term {
		reply.Term = rf.term
		return false
	} else if args.Term > rf.term {
		rf.changeTerm(args.Term)
		rf.vote(-1)
	}
	if rf.SnapShotIndex > args.Index {
		reply.Term = rf.SnapShotTerm
		reply.Index = rf.SnapShotIndex
		return true
	} else if rf.SnapShotIndex == args.Index {
		reply.Term = rf.term
		return true
	} else {
		old := args.Index <= rf.getLastestLogIndex()
		rf.SnapShotIndex = args.Index
		rf.SnapShotTerm = args.STerm
		rf.SnapShot = args.Data
		if rf.lastApplied < rf.SnapShotIndex {
			rf.lastApplied = rf.SnapShotIndex
		}

		if !old {
			clientMsg := &ApplyMsg{}
			clientMsg.SnapshotValid = true
			clientMsg.SnapshotIndex = rf.SnapShotIndex
			clientMsg.SnapshotTerm = rf.SnapShotTerm
			clientMsg.Snapshot = rf.SnapShot
			rf.log = make([]LogEntry, 0)
			rf.publishCh <- *clientMsg
			rf.lastApplied = rf.SnapShotIndex

		}
		rf.persistSnapshotandState()
		return true
	}

}

func (state Follower) handleInstallSnapshotEnd(rf *Raft, snapshotEnd *InstallSnapShot) {
	if snapshotEnd.Reply.Term > rf.term {
		rf.changeTerm(snapshotEnd.Reply.Term)
	}
}
