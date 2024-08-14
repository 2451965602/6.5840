package raft

import "log"

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func (rf *Raft) GetNextIndex(i int) int {
	if rf.lastIncludedIndex == -1 {
		return rf.nextIndex[i]
	}
	return rf.nextIndex[i] - rf.lastIncludedIndex - 1
}

func (rf *Raft) GetLogsLen() int {
	return rf.lastIncludedIndex + len(rf.logs)
}

func (rf *Raft) GetCommitIndex() int {
	return rf.lastIncludedIndex + len(rf.logs)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a < b {
		return b
	}
	return a
}
