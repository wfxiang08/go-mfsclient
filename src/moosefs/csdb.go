package moosefs

import (
    "sync"
)

var (
    stat   map[uint32]int
    smutex sync.Mutex
)

func init() {
    stat = make(map[uint32]int)
}

func getOpCnt(ip uint32) int {
    smutex.Lock()
    defer smutex.Unlock()

    cnt, ok := stat[ip]
    if !ok {
        cnt = 0
    }
    return cnt
}

func incOp(ip uint32) {
    smutex.Lock()
    defer smutex.Unlock()

    cnt, ok := stat[ip]
    if !ok {
        cnt = 0
    }
    stat[ip] = cnt + 1
}
