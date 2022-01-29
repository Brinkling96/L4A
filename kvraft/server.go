package kvraft

import (
	"bytes"
	"fmt"
	"log"
	"reflect"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type     string
	Key      string
	Value    string
	ClientID int
	SqNum    int
}

type RPCMsg struct {
	op      Op
	replych chan *RPCreply
}

type RPCreply struct {
	value string
	err   Err
}

type PendingRequest struct {
	op      Op
	index   int
	replyCh chan *RPCreply
}

type KVServer struct {
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data            map[string]string
	clientRequestCh chan *RPCMsg

	clientAppliedMap map[int]Op //ClientID to OP

	requests []PendingRequest
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	clientOpRequest := &Op{"Get", args.Key, "", args.ClientID, args.MsgID}
	replych := make(chan *RPCreply)
	clientmsg := &RPCMsg{*clientOpRequest, replych}
	kv.clientRequestCh <- clientmsg
	r := <-replych
	reply.Err = r.err
	reply.Value = r.value
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	clientOpRequest := &Op{args.Op, args.Key, args.Value, args.ClientID, args.MsgID}
	replych := make(chan *RPCreply)
	clientmsg := &RPCMsg{*clientOpRequest, replych}
	kv.clientRequestCh <- clientmsg
	r := <-replych
	reply.Err = r.err
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) mainLoop() {
	for !kv.killed() {
		select {
		case clientMsg := <-kv.clientRequestCh:
			RequestNum := kv.findRequestIndex(clientMsg)
			if RequestNum == -1 {
				if clientMsg.op.SqNum < kv.clientAppliedMap[clientMsg.op.ClientID].SqNum {
					clientMsg.replych <- &RPCreply{"", ErrOldRPC}
				} else if clientMsg.op.SqNum == kv.clientAppliedMap[clientMsg.op.ClientID].SqNum {
					clientMsg.replych <- &RPCreply{kv.clientAppliedMap[clientMsg.op.ClientID].Value, OK}
				} else if clientMsg.op.SqNum == kv.clientAppliedMap[clientMsg.op.ClientID].SqNum+1 {
					index, _, isLeader := kv.rf.Start(clientMsg.op)
					if !isLeader {
						clientMsg.replych <- &RPCreply{"", ErrWrongLeader}
						kv.cancelRequests()
					} else {
						pr := PendingRequest{clientMsg.op, index, clientMsg.replych}
						kv.requests = append(kv.requests, pr)
					}
				} else {
					clientMsg.replych <- &RPCreply{"", ErrWrongLeader}
				}
			} else {
				kv.requests[RequestNum].replyCh <- &RPCreply{"", ErrOldRPC}
				kv.requests[RequestNum].replyCh = clientMsg.replych
			}
		case m := <-kv.applyCh:
			if m.SnapshotValid {
				kv.readPersist(m.Snapshot)
				kv.cancelRequests()
			} else if m.CommandValid && reflect.TypeOf(m.Command).Name() == "Op" {
				op := m.Command.(Op)
				if kv.clientAppliedMap[op.ClientID].SqNum == op.SqNum-1 {
					switch op.Type {
					case "Get":
						op.Value = kv.data[op.Key]
					case "Put":
						kv.data[op.Key] = op.Value
					case "Append":
						curr := kv.data[op.Key]
						kv.data[op.Key] = curr + op.Value
					default:
						fmt.Println(op.Type)
						panic("unknown type")
					}
					kv.clientAppliedMap[op.ClientID] = op
				}
				if len(kv.requests) > 0 {
					pr := kv.requests[0]
					if pr.index == m.CommandIndex {
						if pr.op.ClientID == op.ClientID && pr.op.SqNum == op.SqNum {
							pr.replyCh <- &RPCreply{kv.clientAppliedMap[op.ClientID].Value, OK}
						} else {
							pr.replyCh <- &RPCreply{kv.clientAppliedMap[op.ClientID].Value, ErrWrongLeader}
						}
						kv.requests = kv.requests[1:]
					}
				}
				kv.snapshot(m.CommandIndex)
			}
		case <-time.After(time.Millisecond * 250):
			_, isLeader := kv.rf.GetState()
			if !isLeader {
				kv.cancelRequests()
			}
		}
	}
}

func (kv *KVServer) findRequestIndex(clientMsg *RPCMsg) int {
	for i := range kv.requests {
		pr := kv.requests[i]
		if clientMsg.op.ClientID == pr.op.ClientID && clientMsg.op.SqNum == pr.op.SqNum {
			return i
		}
	}
	return -1
}

func (kv *KVServer) cancelRequests() {
	if len(kv.requests) > 0 {
		for i := range kv.requests {
			pr := kv.requests[i]
			pr.replyCh <- &RPCreply{"", ErrWrongLeader}
		}
	}
	kv.requests = make([]PendingRequest, 0)
}

func (kv *KVServer) snapshot(index int) {
	if kv.rf.GetRaftStateSize() >= ((kv.maxraftstate*9)/10) && kv.maxraftstate != -1 {
		buffer := new(bytes.Buffer)
		encoder := labgob.NewEncoder(buffer)

		encoder.Encode(kv.data)
		encoder.Encode(kv.clientAppliedMap)

		thing := buffer.Bytes()

		kv.rf.Snapshot(index, thing)
	}
}

func (kv *KVServer) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		kv.data = map[string]string{}
		kv.clientAppliedMap = map[int]Op{}
	} else {
		buffer := bytes.NewBuffer(data)
		d := labgob.NewDecoder(buffer)

		var kvsdata map[string]string
		var applied map[int]Op

		if d.Decode(&kvsdata) != nil || d.Decode(&applied) != nil {
			panic("decoder err")
		} else {
			kv.data = kvsdata
			kv.clientAppliedMap = applied
		}
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	kv.readPersist(persister.ReadSnapshot())

	kv.clientRequestCh = make(chan *RPCMsg)
	kv.requests = make([]PendingRequest, 0)
	go kv.mainLoop()
	return kv
}
