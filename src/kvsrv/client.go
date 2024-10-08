package kvsrv

import (
	"6.5840/labrpc"
	"crypto/rand"
	"github.com/sasha-s/go-deadlock"
	"math/big"
	"time"
)

type Clerk struct {
	server *labrpc.ClientEnd
	mu     deadlock.Mutex
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	// You'll have to add code here.
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.

	args := GetArgs{Key: key}
	reply := GetReply{}

	for {
		ok := ck.server.Call("KVServer.Get", &args, &reply)
		if ok == true {
			return reply.Value
		}
		time.Sleep(time.Millisecond * 50)
	}

}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(args PutAppendArgs, op string) (reply PutAppendReply, ok bool) {
	// You will have to modify this function.

	if op == "Put" {
		ok = ck.server.Call("KVServer.Put", &args, &reply)
	} else {
		ok = ck.server.Call("KVServer.Append", &args, &reply)
	}
	return
}

func (ck *Clerk) Put(key string, value string) {

	ck.mu.Lock()
	defer ck.mu.Unlock()

	i := nrand()
	args := PutAppendArgs{Key: key, Value: value, Id: int(i)}

	for {
		_, ok := ck.PutAppend(args, "Put")
		if ok == true {
			return
		}
		time.Sleep(time.Millisecond * 50)
	}

}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {

	ck.mu.Lock()
	defer ck.mu.Unlock()

	i := nrand()
	args := PutAppendArgs{Key: key, Value: value, Id: int(i)}

	for {
		reply, ok := ck.PutAppend(args, "Append")
		if ok == true {
			DPrintf(" Reply:Value%v\n", reply.Value)
			return reply.Value
		}
		time.Sleep(time.Millisecond * 50)
	}

}
