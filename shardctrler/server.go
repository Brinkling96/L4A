package shardctrler

import (
	"fmt"
	"reflect"
	"sort"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

type GroupAss struct {
	Server int
	Shards []int
}

type Op struct {
	Type     interface{}
	ClientID int64
	SqNum    int
}
type RPCMsg struct {
	op      Op
	replych chan *RPCreply
}

type RPCreply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}

type Join struct {
	Servers map[int][]string
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	clientOpRequest := &Op{Join{Servers: args.Servers}, args.ClientID, args.SqNum}
	replych := make(chan *RPCreply)
	clientmsg := &RPCMsg{*clientOpRequest, replych}
	sc.clientRequestCh <- clientmsg
	r := <-replych
	reply.WrongLeader = r.WrongLeader
	reply.Err = r.Err
}

type Leave struct {
	GIDs []int
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	clientOpRequest := &Op{Leave{GIDs: args.GIDs}, args.ClientID, args.SqNum}
	replych := make(chan *RPCreply)
	clientmsg := &RPCMsg{*clientOpRequest, replych}
	sc.clientRequestCh <- clientmsg
	r := <-replych
	reply.WrongLeader = r.WrongLeader
	reply.Err = r.Err
}

type Move struct {
	Shard int
	GID   int
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	clientOpRequest := &Op{Move{Shard: args.Shard, GID: args.GID}, args.ClientID, args.SqNum}
	replych := make(chan *RPCreply)
	clientmsg := &RPCMsg{*clientOpRequest, replych}
	sc.clientRequestCh <- clientmsg
	r := <-replych
	reply.WrongLeader = r.WrongLeader
	reply.Err = r.Err
}

type Query struct {
	Num    int              // desired config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	clientOpRequest := &Op{Query{Num: args.Num}, args.ClientID, args.SqNum}
	replych := make(chan *RPCreply)
	clientmsg := &RPCMsg{*clientOpRequest, replych}
	sc.clientRequestCh <- clientmsg
	r := <-replych
	reply.WrongLeader = r.WrongLeader
	reply.Err = r.Err
	reply.Config = r.Config
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	sc.killed = true
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

type PendingRequest struct {
	op      Op
	index   int
	replyCh chan *RPCreply
}

func (sc *ShardCtrler) mainLoop() {
	for !sc.killed {
		select {
		case clientMsg := <-sc.clientRequestCh:
			RequestNum := sc.findRequestIndex(clientMsg)
			if RequestNum == -1 {
				if clientMsg.op.SqNum < sc.clientAppliedMap[clientMsg.op.ClientID].SqNum {
					clientMsg.replych <- &RPCreply{false, ErrOldRPC, Config{}}
				} else if clientMsg.op.SqNum == sc.clientAppliedMap[clientMsg.op.ClientID].SqNum {
					temp := sc.clientAppliedMap[clientMsg.op.ClientID]
					switch temp.Type.(type) {
					case Query:
						qy := temp.Type.(Query)
						clientMsg.replych <- &RPCreply{false, OK, Config{Num: qy.Num,
							Shards: qy.Shards,
							Groups: qy.Groups}}
					default:
						clientMsg.replych <- &RPCreply{false, OK, Config{}}
					}
				} else if clientMsg.op.SqNum == sc.clientAppliedMap[clientMsg.op.ClientID].SqNum+1 {
					index, _, isLeader := sc.rf.Start(clientMsg.op)
					if !isLeader {
						clientMsg.replych <- &RPCreply{true, "", Config{}}
						sc.cancelRequests()
					} else {
						pr := PendingRequest{clientMsg.op, index, clientMsg.replych}
						sc.requests = append(sc.requests, pr)
					}
				} else {
					clientMsg.replych <- &RPCreply{true, "", Config{}}
				}
			} else {
				sc.requests[RequestNum].replyCh <- &RPCreply{false, ErrOldRPC, Config{}}
				sc.requests[RequestNum].replyCh = clientMsg.replych
			}
		case m := <-sc.applyCh:
			if m.CommandValid && reflect.TypeOf(m.Command).Name() == "Op" {
				//fmt.Printf("%v,%v\n", sc.me, m.CommandIndex)
				op := m.Command.(Op)
				if sc.clientAppliedMap[op.ClientID].SqNum == op.SqNum-1 {
					switch op.Type.(type) {
					case Join:
						jn := op.Type.(Join)

						lastestconfig := sc.configs[len(sc.configs)-1]
						newGroups := map[int][]string{}
						for i := range lastestconfig.Groups {
							newGroups[i] = lastestconfig.Groups[i]
						}
						for i := range jn.Servers {
							if i == 0 {
								panic("Zero is not a  valid group")
							} else {
								newGroup := jn.Servers[i]
								newGroups[i] = newGroup
							}
						}
						servers := make([]int, 0)
						for i := range newGroups {
							servers = append(servers, i)
						}
						sort.Ints(servers)

						shards := newjoin(lastestconfig.Shards, servers)
						newConfig := Config{
							Num:    lastestconfig.Num + 1,
							Shards: shards,
							Groups: newGroups,
						}
						sc.configs = append(sc.configs, newConfig)
					case Leave:
						lv := op.Type.(Leave)
						lastestconfig := sc.configs[len(sc.configs)-1]
						newGroups := map[int][]string{}
						for i := range lastestconfig.Groups {
							newGroups[i] = lastestconfig.Groups[i]
						}
						for _, v := range lv.GIDs {
							delete(newGroups, v)
						}

						servers := make([]int, 0)
						for i := range newGroups {
							servers = append(servers, i)
						}
						sort.Ints(servers)

						shards := newleave(lastestconfig.Shards, servers)
						newConfig := Config{
							Num:    lastestconfig.Num + 1,
							Shards: shards,
							Groups: newGroups,
						}
						sc.configs = append(sc.configs, newConfig)
					case Move:
						mv := op.Type.(Move)
						lastestconfig := sc.configs[len(sc.configs)-1]
						shards := lastestconfig.Shards
						shards[mv.Shard] = mv.GID
						newConfig := Config{
							Num:    lastestconfig.Num + 1,
							Shards: shards,
							Groups: lastestconfig.Groups,
						}
						sc.configs = append(sc.configs, newConfig)
					case Query:
						qy := op.Type.(Query)
						if qy.Num != -1 {
							//qy.Num = sc.configs[qy.Num].Num
							qy.Shards = sc.configs[qy.Num].Shards
							newGroups := map[int][]string{}
							for i := range sc.configs[qy.Num].Groups {
								newGroups[i] = sc.configs[qy.Num].Groups[i]
							}
							qy.Groups = newGroups
							op.Type = qy
						} else {
							lastestconfig := sc.configs[len(sc.configs)-1]
							newGroups := map[int][]string{}
							for i := range lastestconfig.Groups {
								newGroups[i] = lastestconfig.Groups[i]
							}
							qy.Num = lastestconfig.Num
							qy.Shards = lastestconfig.Shards
							qy.Groups = newGroups
							op.Type = qy
						}
					default:
						fmt.Println(op.Type)
						panic("unknown type")
					}
					sc.clientAppliedMap[op.ClientID] = op
				}
				if len(sc.requests) > 0 {
					pr := sc.requests[0]
					if pr.index > m.CommandIndex {
						if pr.op.ClientID == op.ClientID && pr.op.SqNum == op.SqNum {
							switch op.Type.(type) {
							case Query:
								qy := op.Type.(Query)
								pr.replyCh <- &RPCreply{false, OK, Config{Num: qy.Num,
									Shards: qy.Shards,
									Groups: qy.Groups}}
							default:
								pr.replyCh <- &RPCreply{false, OK, Config{}}
							}
							sc.requests = sc.requests[1:]
						}
					} else if pr.index == m.CommandIndex {
						if pr.op.ClientID == op.ClientID && pr.op.SqNum == op.SqNum {
							switch op.Type.(type) {
							case Query:
								qy := op.Type.(Query)
								pr.replyCh <- &RPCreply{false, OK, Config{Num: qy.Num,
									Shards: qy.Shards,
									Groups: qy.Groups}}
							default:
								pr.replyCh <- &RPCreply{false, OK, Config{}}
							}
						} else {
							pr.replyCh <- &RPCreply{true, OK, Config{}}
						}
						sc.requests = sc.requests[1:]
					}
				}
			}
		case <-time.After(time.Millisecond * 250):
			_, isLeader := sc.rf.GetState()
			if !isLeader {
				sc.cancelRequests()
			}
		}
	}
}

func (sc *ShardCtrler) findRequestIndex(clientMsg *RPCMsg) int {
	for i := range sc.requests {
		pr := sc.requests[i]
		if clientMsg.op.ClientID == pr.op.ClientID && clientMsg.op.SqNum == pr.op.SqNum {
			return i
		}
	}
	return -1
}

func (sc *ShardCtrler) cancelRequests() {
	if len(sc.requests) > 0 {
		for i := range sc.requests {
			pr := sc.requests[i]
			pr.replyCh <- &RPCreply{true, ErrOldRPC, Config{}}
		}
	}
	sc.requests = make([]PendingRequest, 0)
}

type ShardCtrler struct {
	me              int
	rf              *raft.Raft
	applyCh         chan raft.ApplyMsg
	clientRequestCh chan *RPCMsg
	killed          bool

	// Your data here.

	configs          []Config // indexed by config num
	requests         []PendingRequest
	clientAppliedMap map[int64]Op
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	labgob.Register(Query{})
	labgob.Register(Join{})
	labgob.Register(Move{})
	labgob.Register(Leave{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	sc.clientRequestCh = make(chan *RPCMsg)
	sc.killed = false
	sc.clientAppliedMap = map[int64]Op{}

	// Your code here.
	go sc.mainLoop()
	return sc
}

func newjoin(curr [NShards]int, servers []int) [NShards]int {
	currList := makeList(curr, servers)
	currList = sortGroupListOnShards(currList)
	newAss := generateOptDistribution(len(currList))

	pos := 0
	for pos < len(currList) && len(currList[pos].Shards) == newAss[pos] {
		pos++
	}
	for i := 0; i < len(currList); i++ {
		for len(currList[i].Shards) < newAss[i] {
			if len(currList[pos].Shards) > newAss[pos] {
				relocatedShard := currList[pos].Shards[0]
				temp := currList[pos].Shards[1:]
				currList[pos].Shards = temp
				currList[i].Shards = append(currList[i].Shards, relocatedShard)
			}
			if len(currList[pos].Shards) == newAss[pos] {
				pos++
			}
		}
	}
	shards := makeShardList(currList)
	return shards
}

func newleave(curr [NShards]int, servers []int) [NShards]int {
	if len(servers) == 0 {
		var shards [NShards]int
		for i := range shards {
			shards[i] = 0
		}
		return shards
	} else {
		currList := makeList(curr, servers)

		newAss := generateOptDistribution(len(servers))
		for len(newAss) < len(currList) {
			newAss = append(newAss, 0)
		}
		pos := 0
		for pos < len(currList) && len(currList[pos].Shards) == newAss[pos] {
			pos++
		}
		for i := 0; i < len(currList); i++ {
			for len(currList[i].Shards) > newAss[i] && len(currList[pos].Shards) < newAss[pos] {
				relocatedShard := currList[i].Shards[0]
				temp := currList[i].Shards[1:]
				currList[i].Shards = temp
				currList[pos].Shards = append(currList[pos].Shards, relocatedShard)
				if len(currList[pos].Shards) == newAss[pos] {
					pos++
				}
			}
		}
		shards := makeShardList(currList)
		return shards
	}
}

func makeList(curr [NShards]int, servers []int) []GroupAss {
	temp := curr
	sort.Ints(temp[:])

	currList := make([]GroupAss, 0)

	for i, k := range servers {
		temp := make([]int, 0)
		for j, v := range curr {
			if v == k {
				temp = append(temp, j)
			} else if v == 0 {
				temp = append(temp, j)
				curr[j] = servers[i]
			}
		}
		currList = append(currList, GroupAss{
			Server: servers[i],
			Shards: temp,
		})
	}

	temp2 := map[int][]int{}
	for i, k := range curr {
		leaver := true
		for _, v := range servers {
			if k == v {
				leaver = false
				break
			}
		}
		if leaver {
			temp2[k] = append(temp2[k], i)
		}
	}
	leavers := make([]int, 0)
	for i := range temp2 {
		leavers = append(leavers, i)
	}
	sort.Ints(leavers)

	for _, v := range leavers {
		ss := temp2[v]
		sort.Ints(ss)
		currList = append(currList, GroupAss{
			Server: v,
			Shards: ss,
		})
	}

	return currList
}

func makeShardList(currList []GroupAss) [NShards]int {
	var shards [NShards]int
	for i := range currList {
		for _, v := range currList[i].Shards {
			shards[v] = currList[i].Server
		}
	}
	return shards
}

func generateOptDistribution(newGroupNumber int) []int {
	newAssignments := make([]int, newGroupNumber)
	count := 0
	if newGroupNumber == 0 {
		return []int{0}
	}
	for count < NShards {
		for i := range newAssignments {
			newAssignments[i] += 1
			count += 1
			if count == NShards {
				break
			}
		}
	}
	return newAssignments
}

func sortGroupListOnShards(currList []GroupAss) []GroupAss {
	for i := 0; i < len(currList)-1; i++ {
		for j := 0; j < len(currList)-i-1; j++ {
			if len(currList[j].Shards) < len(currList[j+1].Shards) {
				temp := currList[j+1]
				currList[j+1] = currList[j]
				currList[j] = temp
			}
		}
	}
	return currList
}
