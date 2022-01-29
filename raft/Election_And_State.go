package raft

import (
	"reflect"
)

//Useful Struct for sending rf.term and rf.State around in channel
type Mode struct {
	Term     int
	IsLeader bool
}

func (rf *Raft) stateRequest() *Mode {

	mode := &Mode{}
	mode.Term = rf.term
	if reflect.TypeOf(rf.state).Name() != "Leader" {
		mode.IsLeader = false
	} else {
		mode.IsLeader = true
	}

	return mode
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	c := make(chan *Mode)
	rf.requestStateChan <- c
	mode := <-c
	close(c)
	return mode.Term, mode.IsLeader
}

//Encapsulates changing Roles/State
func (rf *Raft) ChangeRole(state State) {
	rf.state = state
	switch state {
	case Follower{}:
		rf.votes = 0
		rf.vote(-1)
	case Leader{}:
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		rf.startRequest(1003)
		next := rf.getLastestLogIndex() + 1
		for i := range rf.nextIndex {
			rf.nextIndex[i] = next
		}
		rf.matchIndex[rf.me] = next - 1
	case Candidate{}:
		rf.changeTerm(rf.term + 1)
		rf.votes = 1
		rf.vote(rf.me)
		rf.votesFrom = make([]bool, len(rf.peers))
		rf.votesFrom[rf.me] = true
	}
}

//Useful Struct form passing both Args and Reply to channels.
//RPC functions like RequestVote use this Struct to request a particular vote to be processed by the
//main loop.
//The attached channel is used to send the main loop response back to the RPC after it has been processed
type RequestVoteChan struct {
	ReqVote *RequestVote
	ReplyCh chan *RequestVote
}
type RequestVote struct {
	Args  *RequestVoteArgs
	Reply *RequestVoteReply
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//These fields are taken straight from figure 2 in the paper
type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
	ID          int //TODO fix, should be requestors job to keep track of sever number
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

//This fucntion is used by Candidates in thier broadcasts. Reponsese are sent to main loop to be processed,
//Whatever state the node who originally called this is in
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok { //if we couldnt reach the node, we didnt get the vote
		reply.VoteGranted = ok
	}
	responseWrap := &RequestVote{}
	responseWrap.Args = args
	responseWrap.Reply = reply
	rf.requestVoteReply <- responseWrap
}

//
// example RequestVote RPC handler.
// Request vote takes the pointers given to it by the remote call and send
//them to the main loop to be processed.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	rv := &RequestVote{}
	rv.Args = args
	rv.Reply = reply

	rvc := &RequestVoteChan{}
	rvc.ReqVote = rv
	rvc.ReplyCh = make(chan *RequestVote)

	rf.requestVoteChan <- rvc
	<-rvc.ReplyCh
	close(rvc.ReplyCh)

}

//This function is used by all roles to process a request vote request.
//It uses requestVote.args to determine its requestVote.reply to the requestor
func (rf *Raft) RequestVoteRequest(requestVote *RequestVote) bool {
	return rf.state.handleRequestVoteRequest(rf, requestVote)
}

//Helper function for RequestVoteRequest
//Uses log from additions in part 2b to determine if log of requestor is up to date enough to grant vote
func (rf *Raft) CheckLogForVote(args *RequestVoteArgs) bool {
	lastestIndex := rf.getLastestLogIndex()
	lastestTerm := rf.getTermAt(lastestIndex)
	if args.LastLogTerm > lastestTerm {
		return true
	} else if args.LastLogTerm == lastestTerm && args.LastLogIndex >= lastestIndex {
		return true
	} else {
		return false
	}
}

//This is the function used by Candidate to processes vote responses
//Followers or Leaders who recieve vote responses just quickly return
func (rf *Raft) requestVoteReplyF(reply *RequestVoteReply) {
	rf.state.handleRequestVoteReplyF(rf, reply)
}
