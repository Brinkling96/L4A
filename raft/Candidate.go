package raft

import "fmt"

type Candidate struct{}

func (state Candidate) handleAppendEntriesRequest(rf *Raft, appEntries *AppendEntries) bool {
	args := appEntries.Args
	reply := appEntries.Reply
	fmt.Printf("")

	if args.Term >= rf.term {
		rf.ChangeRole(Follower{})
		rf.changeTerm(reply.Term)
		return rf.state.handleAppendEntriesRequest(rf, appEntries)
	} else {
		reply.Term = rf.term
		reply.Sucess = false
	}
	return reply.Sucess
}

func (state Candidate) handleBroadcast(rf *Raft) {

	args := &RequestVoteArgs{}
	args.Term = rf.term
	args.CandidateID = rf.me
	args.LastLogIndex = rf.getLastestLogIndex()
	args.LastLogTerm = rf.getTermAt(args.LastLogIndex)

	for i := range rf.peers {
		if i != rf.me {
			go rf.sendRequestVote(i, args, &RequestVoteReply{})
		}
	}
}

func (state Candidate) handleNodeTimeout(rf *Raft) {
	rf.ChangeRole(Candidate{})
}

func (state Candidate) handleRequestVoteReplyF(rf *Raft, reply *RequestVoteReply) {
	if rf.term < reply.Term {
		rf.changeTerm(reply.Term)
		rf.ChangeRole(Follower{})
	} else if reply.VoteGranted {
		vote := rf.votesFrom[reply.ID]
		if !vote {
			rf.votesFrom[reply.ID] = true
			rf.votes = rf.votes + 1
			if rf.votes > len(rf.peers)/2 { /// this might not work for even amount of servers in a config
				rf.ChangeRole(Leader{})
			}
		}
	}

}

func (state Candidate) handleAppendEntryEnd(rf *Raft, appendEntrys *AppendEntries) {
}

func (state Candidate) handleRequestVoteRequest(rf *Raft, requestVote *RequestVote) bool {
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

func (state Candidate) handleInstallSnapshot(rf *Raft, args *InstallSnapShotArgs, reply *InstallSnapShotReply) bool {
	if args.Term < rf.term {
		reply.Term = rf.term
		return false
	} else {
		rf.changeTerm(args.Term)
		rf.vote(-1)
		rf.ChangeRole(Follower{})
		return rf.state.handleInstallSnapshot(rf, args, reply)
	}
}

func (state Candidate) handleInstallSnapshotEnd(rf *Raft, snapshotEnd *InstallSnapShot) {
	if snapshotEnd.Reply.Term > rf.term {
		rf.changeTerm(snapshotEnd.Reply.Term)
		rf.ChangeRole(Follower{})
	}
}
