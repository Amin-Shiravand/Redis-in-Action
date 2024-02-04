package common

import "sync"

const (
	LOGIN     = "login:"
	RECENT    = "recent:"
	VIEWED    = "viewed:"
	DELAY     = "delay:"
	CART      = "cart:"
	SCHEDULE  = "schedule:"
	INVENTORY = "inv:"
	ITEM      = "item"
)

var mu sync.Mutex

var (
	QUIT        = false
	LIMIT int64 = 10000000
	FLAG  int32 = 1
)

func SetQuit(quit bool) {
	mu.Lock()
	defer mu.Unlock()
	QUIT = quit
}

func SetLimit(limit int64) {
	mu.Lock()
	defer mu.Unlock()
	LIMIT = limit
}

func SetFlag(flag int32) {
	mu.Lock()
	defer mu.Unlock()
	FLAG = flag
}
