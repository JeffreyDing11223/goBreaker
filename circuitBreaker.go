package goBreaker

import ()

// State changes between CLOSED, OPEN, HALFOPEN
// [CLOSED] -->- tripped ----> [OPEN]<-------+
//    ^                          |           ^
//    |                          v           |
//    |                          |      detect fail
//    |                          |           |
//    |                    cooling timeout   |
//    ^                          |           ^
//    |                          v           |
//    +--- detect succeed --<-[HALFOPEN]-->--+

type State int

const (
	OPEN State = iota
	HALFOPEN
	CLOSED
)

func (s State) String() string {
	switch s {
	case OPEN:
		return "OPEN"
	case HALFOPEN:
		return "HALFOPEN"
	case CLOSED:
		return "CLOSED"
	}
	return "INVALID"
}
