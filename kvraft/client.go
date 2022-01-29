package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	me int //unique ID for the client. Due to the nature of the Constructor, I will randomly generate it, with little risk
	//for collisions of the same ID during debuging
	msgNum   int //This is a logicial clock for msgs created by this client
	leaderID int //This is the ID of the raft server which this Clerk believes is the leader
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.me = (int)(nrand())
	ck.msgNum = 0
	ck.leaderID = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	ck.msgNum += 1
	args := &GetArgs{key, ck.me, ck.msgNum}
	value := ""
	success := false
	for !success {
		//fmt.Printf(".")
		reply := &GetReply{}
		ok := ck.servers[ck.leaderID].Call("KVServer.Get", args, reply)
		if ok {
			switch reply.Err {
			case OK:
				value = reply.Value
				success = true
			case ErrNoKey:
				success = true
			case ErrWrongLeader:
				if ck.leaderID < len(ck.servers)-1 {
					ck.leaderID = ck.leaderID + 1
				} else {
					ck.leaderID = 0
				}
			case ErrOldRPC:
				//panic("tst")
			}
		} else {
			if ck.leaderID < len(ck.servers)-1 {
				ck.leaderID = ck.leaderID + 1
			} else {
				ck.leaderID = 0
			}
		}

	}
	return value
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.msgNum += 1
	args := &PutAppendArgs{key, value, op, ck.me, ck.msgNum}
	success := false
	for !success {
		//fmt.Printf(".")
		reply := &PutAppendReply{}
		ok := ck.servers[ck.leaderID].Call("KVServer.PutAppend", args, reply)
		if ok {
			switch reply.Err {
			case OK:
				success = true
			case ErrNoKey:
				panic("dsf")
			case ErrWrongLeader:
				if ck.leaderID < len(ck.servers)-1 {
					ck.leaderID = ck.leaderID + 1
				} else {
					ck.leaderID = 0
				}
			case ErrOldRPC:
				//panic("tst")
			}

		} else {
			if ck.leaderID < len(ck.servers)-1 {
				ck.leaderID = ck.leaderID + 1
			} else {
				ck.leaderID = 0
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
