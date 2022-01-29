package raft

import (
	"fmt"
	"sort"
)

type Leader struct{}

func (state Leader) handleAppendEntriesRequest(rf *Raft, appEntries *AppendEntries) bool {
	args := appEntries.Args
	reply := appEntries.Reply
	fmt.Printf("")
	if args.Term > rf.term {
		rf.changeTerm(args.Term)
		rf.ChangeRole(Follower{})
		return rf.state.handleAppendEntriesRequest(rf, appEntries)
	} else {
		reply.Term = rf.term
		reply.Sucess = false
	}

	return reply.Sucess
}

func (state Leader) handleBroadcast(rf *Raft) {
	args := &AppendEntriesArgs{}
	args.LeaderID = rf.me
	args.Term = rf.term
	args.CommitIndex = rf.CommitIndex

	for i := range rf.peers {
		args.Entries = make([]LogEntry, 0)
		if i != rf.me {
			args.PrevLogIndex = rf.nextIndex[i] - 1
			if args.PrevLogIndex < rf.SnapShotIndex {
				sargs := &InstallSnapShotArgs{}
				sargs.LeaderID = rf.me
				sargs.Data = rf.SnapShot
				sargs.Index = rf.SnapShotIndex
				sargs.STerm = rf.SnapShotTerm
				sargs.Term = rf.term
				go rf.SendInstallSnapShot(i, sargs, &InstallSnapShotReply{})
			} else {
				if args.PrevLogIndex == rf.SnapShotIndex {
					args.PrevLogTerm = rf.SnapShotTerm
				} else {
					args.PrevLogTerm = rf.getTermAt(args.PrevLogIndex)
				}
				if rf.nextIndex[i] < rf.getLastestLogIndex()+1 {
					args.Entries = rf.grabLogFrom(rf.nextIndex[i])
				}
				go rf.SendAppendEntries(i, *args, &AppendEntriesReply{})
			}
		}
	}
}

func (state Leader) handleNodeTimeout(rf *Raft) {
}

func (state Leader) handleRequestVoteReplyF(rf *Raft, reply *RequestVoteReply) {
}

func (state Leader) handleAppendEntryEnd(rf *Raft, appendEntrys *AppendEntries) {
	args := appendEntrys.Args
	reply := appendEntrys.Reply
	if appendEntrys.Recieved {
		if reply.Sucess {
			if len(args.Entries) != 0 {
				rf.nextIndex[appendEntrys.Server] = args.PrevLogIndex + len(args.Entries) + 1
			}
			rf.matchIndex[appendEntrys.Server] = rf.nextIndex[appendEntrys.Server] - 1
			rf.blah()
		} else {
			if reply.Term > rf.term {
				rf.changeTerm(args.Term)
				rf.ChangeRole(Follower{})
				return
			} else if reply.LastLogIndex == -1 {
				//panic("It can happen 2")
			} else if rf.matchIndex[appendEntrys.Server] > reply.LastLogIndex {
				return
			} else {
				if rf.matchIndex[appendEntrys.Server] > rf.SnapShotIndex+1 {
					rf.nextIndex[appendEntrys.Server] = rf.FindBestMatchIndex(reply.LastLogIndex, rf.matchIndex[appendEntrys.Server], reply.LastLogTerm) + 1
				} else {
					rf.nextIndex[appendEntrys.Server] = rf.FindBestMatchIndex(reply.LastLogIndex, rf.SnapShotIndex+1, reply.LastLogTerm) + 1
				}
			}
		}
	}
}

func (rf *Raft) blah() {
	matchIndexSort := make([]int, len(rf.matchIndex))
	copy(matchIndexSort, rf.matchIndex)
	sort.Ints(matchIndexSort)

	var majority int
	if len(matchIndexSort)%2 == 1 {
		majority = matchIndexSort[len(matchIndexSort)/2]

	} else {
		//majority = matchIndexSort[len(matchIndexSort)/2 -1]
		panic("dfasd")
	}

	if majority > rf.CommitIndex && rf.getTermAt(majority) == rf.term { //only update commitIndex if log[pos].term is equal to term
		rf.CommitIndex = matchIndexSort[len(matchIndexSort)/2]
	} else if matchIndexSort[0] > rf.CommitIndex { //or if every server has the log entry
		rf.CommitIndex = matchIndexSort[0]

	}

	if matchIndexSort[0] > rf.CommitIndex {
		panic("fasdf")
	}
	rf.CommitEntries()

}

func (state Leader) handleRequestVoteRequest(rf *Raft, requestVote *RequestVote) bool {
	args := requestVote.Args
	reply := requestVote.Reply

	if args.Term > rf.term {
		rf.ChangeRole(Follower{})
		rf.state.handleRequestVoteRequest(rf, requestVote)
	} else {
		reply.VoteGranted = false
	}
	reply.Term = rf.term
	reply.ID = rf.me
	return reply.VoteGranted
}

func (state Leader) handleInstallSnapshot(rf *Raft, args *InstallSnapShotArgs, reply *InstallSnapShotReply) bool {
	if args.Term < rf.term {
		reply.Term = rf.term
		return false
	} else if args.Term == rf.term {
		panic("mutiple leaders")
	} else {
		rf.changeTerm(args.Term)
		rf.vote(-1)
		rf.ChangeRole(Follower{})
		return rf.state.handleInstallSnapshot(rf, args, reply)
	}
}

func (state Leader) handleInstallSnapshotEnd(rf *Raft, snapshotEnd *InstallSnapShot) {
	if snapshotEnd.Recieved {
		if snapshotEnd.Reply.Term > rf.term {
			rf.changeTerm(snapshotEnd.Reply.Term)
			rf.ChangeRole(Follower{})
		} else {
			if snapshotEnd.Reply.Index > rf.SnapShotIndex {
				//panic("WTF")
			} else if rf.matchIndex[snapshotEnd.Server] < snapshotEnd.Reply.Index {
				rf.matchIndex[snapshotEnd.Server] = snapshotEnd.Reply.Index
				rf.nextIndex[snapshotEnd.Server] = snapshotEnd.Reply.Index + 1
				rf.blah()
			} else if rf.matchIndex[snapshotEnd.Server] > snapshotEnd.Args.Index {
				//panic("leader MI is > SSAI")
			} else {
				rf.nextIndex[snapshotEnd.Server] = snapshotEnd.Args.Index + 1
				rf.matchIndex[snapshotEnd.Server] = snapshotEnd.Args.Index
				rf.blah()
			}
		}
	}

}
