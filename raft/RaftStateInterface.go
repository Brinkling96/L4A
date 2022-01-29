package raft

type State interface {
	handleAppendEntriesRequest(rf *Raft, appEntries *AppendEntries) bool

	handleBroadcast(rf *Raft)

	handleNodeTimeout(rf *Raft)

	handleRequestVoteReplyF(rf *Raft, reply *RequestVoteReply)

	handleAppendEntryEnd(rf *Raft, appendEntrys *AppendEntries)

	handleRequestVoteRequest(rf *Raft, requestVote *RequestVote) bool

	handleInstallSnapshot(rf *Raft, args *InstallSnapShotArgs, reply *InstallSnapShotReply) bool

	handleInstallSnapshotEnd(rf *Raft, snapshotEnd *InstallSnapShot)
}
