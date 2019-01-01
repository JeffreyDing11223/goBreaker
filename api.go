package goBreaker

import (
	"log"
	"sync"
	"time"
)

const (
	// MinQps determines the MinSamples when using AdjustBreakers func
	DEFAULT_BREAKER_MINQPS = 200
)

type CircuitBreaker struct {
	Breakers map[int32]*Breaker
	Mutex    sync.RWMutex
}

var BreakerWhitelist = map[int32]bool{}

func InitCircuitBreakers(cmds []int32, options Options) (cb CircuitBreaker) {
	cb.Breakers = map[int32]*Breaker{}
	for _, cmd := range cmds {
		cb.Breakers[cmd] = cb.GenBreaker(cmd, options)
	}
	return cb
}

func (b *CircuitBreaker) GetBreaker(cmd int32) *Breaker {
	b.Mutex.RLock()
	defer b.Mutex.RUnlock()
	cb := b.Breakers[cmd]
	if cb == nil {
		return b.GenBreaker(cmd, Options{})
	}
	return cb
}

func (b *CircuitBreaker) GetAllBreakers() map[int32]*Breaker {
	breakers := map[int32]*Breaker{}
	b.Mutex.RLock()
	defer b.Mutex.RUnlock()
	for cmd, breaker := range b.Breakers {
		breakers[cmd] = breaker
	}
	return breakers
}

// when instances >1, you can use AdjustBreakers
//count means how many instances you have
func (b *CircuitBreaker) AdjustBreakers(count int, options Options) {
	var preCount, breakerWindows int
	windowTime := options.BucketTime * time.Duration(options.BucketNums)
	breakerWindows = int(windowTime / 1000000000)

	if options.BreakerMinQPS <= 0 {
		options.BreakerMinQPS = DEFAULT_BREAKER_MINQPS
	}
	for {
		if count == preCount {
			time.Sleep(time.Minute)
			continue
		}
		preCount = count
		options.BreakerMinSamples = breakerWindows * options.BreakerMinQPS / count
		log.Printf("breaker min sample change, instances count: %v, sample: %v", count, options.BreakerMinSamples)

		b.Mutex.Lock()
		for cmd := range b.Breakers {
			b.Breakers[cmd] = b.GenBreaker(cmd, options)
		}
		b.Mutex.Unlock()

		time.Sleep(time.Minute)
	}
}

func (b *CircuitBreaker) GenBreaker(cmd int32, options Options) *Breaker {
	callback := func(oldState, newState State, m Container) {
		log.Printf("breaker state change, command %v: %s -> %s, (succ: %d, err: %d, timeout: %d, rate: %.2f)",
			cmd, oldState.String(), newState.String(),
			m.Successes(), m.Failures(), m.Timeouts(), m.ErrorRate())
	}
	if options.StateChangeHandler == nil {
		options.StateChangeHandler = callback
	}
	defaultBreaker, _ := NewBreaker(options)
	return defaultBreaker
}

func (b *CircuitBreaker) IsTriggerBreaker(cmd int32) bool {
	if BreakerWhitelist[cmd] {
		return false
	}
	breaker := b.GetBreaker(cmd)
	if !breaker.IsAllowed() {
		return true
	}
	return false
}
