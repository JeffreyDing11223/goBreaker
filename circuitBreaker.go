package goBreaker

import (
	"sync"
	"time"
)

// State changes between CLOSED, OPEN, HALFOPEN
// [CLOSED] -->- tripped ----> [OPEN]<-------+
//    ^                          |           ^
//    |                          v           |
//    |                          |      detect fail
//    |                          |           |
//    |                    cooling timeout   |
//    ^                          |           ^
//    |                          v           |
//    --<- detect succeed --<-[HALFOPEN]-->---

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

const (
	// COOLING_TIMEOUT is the time the breaker stay in OPEN before becoming HALFOPEN
	DEFAULT_COOLING_TIMEOUT = time.Second * 5

	// DETECT_TIMEOUT is the time interval between every detect in HALFOPEN
	DEFAULT_DETECT_TIMEOUT = time.Millisecond * 200

	// HALFOPEN_SUCCESSES is the threshold when the breaker is in HALFOPEN;
	// after secceeding consecutively this times, it will change its state from HALFOPEN to CLOSED;
	DEFAULT_HALFOPEN_SUCCESSES = 2
)

// TripFunc is a function called by a Breaker when error appears and
// determines whether the breaker should trip
type TripFunc func(Container) bool

// StateChangeHandler
type StateChangeHandler func(oldState, newState State, m Container)

type Breaker struct {
	Container // contains all success, error and timeout
	sync.RWMutex

	state           State
	openTime        time.Time // the time when the breaker become OPEN
	lastRetryTime   time.Time // last retry time when in HALFOPEN state
	halfopenSuccess int       // consecutive successes when HALFOPEN

	options Options

	now func() time.Time
}

// Options for Breaker
type Options struct {
	// parameters for container
	BucketTime time.Duration // the time each bucket holds
	BucketNums int           // the number of buckets the breaker have

	// parameters for breaker
	BreakerRate        float64
	BreakerMinQPS      int           // if qps is over this value, the breaker trip will work
	BreakerMinSamples  int           // for RateTrip callback
	CoolingTimeout     time.Duration // fixed when create
	DetectTimeout      time.Duration // fixed when create
	HalfOpenSuccess    int
	ShouldTrip         TripFunc // trip callback, default is RateTrip
	StateChangeHandler StateChangeHandler

	now func() time.Time
}

// NewBreaker creates a base breaker with a specified options
func NewBreaker(options Options) (*Breaker, error) {
	if options.now == nil {
		options.now = time.Now
	}

	if options.BucketTime <= 0 {
		options.BucketTime = DEFAULT_BUCKET_TIME
	}

	if options.BucketNums <= 0 {
		options.BucketNums = DEFAULT_BUCKET_NUMS
	}

	if options.CoolingTimeout <= 0 {
		options.CoolingTimeout = DEFAULT_COOLING_TIMEOUT
	}

	if options.DetectTimeout <= 0 {
		options.DetectTimeout = DEFAULT_DETECT_TIMEOUT
	}

	if options.HalfOpenSuccess <= 0 {
		options.HalfOpenSuccess = DEFAULT_HALFOPEN_SUCCESSES
	}

	if options.BreakerRate <= 0 {
		options.BreakerRate = DEFAULT_BREAKER_RATE
	}

	if options.BreakerMinSamples <= 0 {
		options.BreakerMinSamples = DEFAULT_BREAKER_MINSAMPLES
	}

	if options.ShouldTrip == nil {
		options.ShouldTrip = RateTripFunc(options.BreakerRate, int64(options.BreakerMinSamples))
	}

	container, err := NewWindowWithOptions(options.BucketTime, options.BucketNums)
	if err != nil {
		return nil, err
	}

	breaker := &Breaker{
		Container: container,
		now:       options.now,
		state:     CLOSED,
	}

	breaker.options = Options{
		BucketTime:         options.BucketTime,
		BucketNums:         options.BucketNums,
		BreakerRate:        options.BreakerRate,
		BreakerMinSamples:  options.BreakerMinSamples,
		CoolingTimeout:     options.CoolingTimeout,
		DetectTimeout:      options.DetectTimeout,
		HalfOpenSuccess:    options.HalfOpenSuccess,
		ShouldTrip:         options.ShouldTrip,
		StateChangeHandler: options.StateChangeHandler,
		now:                options.now,
	}

	return breaker, nil
}

// Succeed records a success and decreases the concurrency counter by one
func (b *Breaker) Succeed() {
	b.Lock()
	switch b.state {
	case OPEN: // do nothing
	case HALFOPEN:
		b.halfopenSuccess++
		if b.halfopenSuccess == b.options.HalfOpenSuccess {
			if b.options.StateChangeHandler != nil {
				b.options.StateChangeHandler(HALFOPEN, CLOSED, b.Container)
			}
			b.Container.Reset()
			b.state = CLOSED
		}
	case CLOSED:
		b.Container.Succeed()
	}
	b.Unlock() // don't use defer
}

func (b *Breaker) error(isTimeout bool, trip TripFunc) {
	b.Lock()
	if isTimeout {
		b.Container.Timeout()
	} else {
		b.Container.Fail()
	}

	switch b.state {
	case OPEN: // do nothing
	case HALFOPEN: // become OPEN
		if b.options.StateChangeHandler != nil {
			b.options.StateChangeHandler(HALFOPEN, OPEN, b.Container)
		}
		b.openTime = time.Now()
		b.state = OPEN
	case CLOSED: // call ShouldTrip
		if trip != nil && trip(b) {
			// become OPEN and set the open time
			if b.options.StateChangeHandler != nil {
				b.options.StateChangeHandler(CLOSED, OPEN, b.Container)
			}
			b.openTime = time.Now()
			b.state = OPEN
		}
	}
	b.Unlock() // don't use defer
}

func (b *Breaker) Fail() {
	b.error(false, b.options.ShouldTrip)
}

func (b *Breaker) FailWithTrip(trip TripFunc) {
	b.error(false, trip)
}

func (b *Breaker) Timeout() {
	b.error(true, b.options.ShouldTrip)
}

func (b *Breaker) TimeoutWithTrip(trip TripFunc) {
	b.error(true, trip)
}

func (b *Breaker) IsAllowed() bool {
	return b.isAllowed()
}

func (b *Breaker) isAllowed() bool {
	b.Lock()
	switch b.state {
	case OPEN:
		now := time.Now()
		if b.openTime.Add(b.options.CoolingTimeout).After(now) {
			b.Unlock()
			return false
		}
		// cooling timeout, then become HALFOPEN
		b.state = HALFOPEN
		b.halfopenSuccess = 0
		b.lastRetryTime = now
	case HALFOPEN:
		now := time.Now()
		if b.lastRetryTime.Add(b.options.DetectTimeout).After(now) {
			b.Unlock()
			return false
		}
		b.lastRetryTime = now
	case CLOSED:
	}

	b.Unlock()
	return true
}

// returns the breaker's state now
func (b *Breaker) State() State {
	b.Lock()
	state := b.state
	b.Unlock()
	return state
}

// resets this breaker
func (b *Breaker) Reset() {
	b.Lock()
	b.Container.Reset()
	b.state = CLOSED
	b.Unlock()
}

// ThresholdTripFunc
func ThresholdTripFunc(threshold int64) TripFunc {
	return func(m Container) bool {
		return m.Failures()+m.Timeouts() >= threshold
	}
}

// ConsecutiveTripFunc
func ConsecutiveTripFunc(threshold int64) TripFunc {
	return func(m Container) bool {
		return m.ConsecutiveErrors() >= threshold
	}
}

// RateTripFunc
func RateTripFunc(rate float64, minSamples int64) TripFunc {
	return func(m Container) bool {
		samples := m.Samples()
		return samples >= minSamples && m.ErrorRate() >= rate
	}
}
