package kvsrv

import (
	"github.com/sasha-s/go-deadlock"
	"log"
	"strings"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu             deadlock.Mutex
	data           map[string]string
	executedTaskId map[string]int

	// Your definitions here.
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	value := kv.data[args.Key]

	reply.Value = value
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.executedTaskId[args.Key] != args.Id {
		kv.data[args.Key] = args.Value
		reply.Value = kv.data[args.Key]
	}

}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.executedTaskId[args.Key] == args.Id {
		reply.Value = strings.TrimSuffix(kv.data[args.Key], args.Value)
		return
	}

	value := kv.data[args.Key]
	reply.Value = value

	kv.data[args.Key] += args.Value
	kv.executedTaskId[args.Key] = args.Id
	DPrintf(" Key:%v Vlue:%v\n", args.Key, args.Value)
	DPrintf(" Map:%v\n", kv.data)

}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	kv.data = make(map[string]string)
	kv.executedTaskId = make(map[string]int)

	return kv
}
