package goBreaker

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// bucket time is the time each bucket holds
	DEFAULT_BUCKET_TIME = time.Millisecond * 100

	// bucket nums is the number of buckets the container has;
	DEFAULT_BUCKET_NUMS = 100

	// default window size is (DEFAULT_BUCKET_TIME * DEFAULT_BUCKET_NUMS)
	// it is 10 seconds
)

// Container contains errors, timeouts and successes
type Container interface {
	Fail()    // records a failure
	Succeed() // records a success
	Timeout() // records a timeout

	Failures() int64          // return the number of failures
	Successes() int64         // return the number of successes
	Timeouts() int64          // return the number of timeouts
	ConsecutiveErrors() int64 // return the consecutive errors recently
	ErrorRate() float64       // rate = (timeouts + failures) / (timeouts + failures + successes)
	Samples() int64           // (timeouts + failures + successes)
	Counts() (successes, failures, timeouts int64)

	Reset()
}

// bucket holds counts of failures and successes
type bucket struct {
	failure int64
	success int64
	timeout int64

	timeStamp int64 // unix nano time when the bucket was created
}

// Reset resets the counts to 0 and refreshes the time stamp
func (b *bucket) Reset() {
	atomic.StoreInt64(&b.failure, 0)
	atomic.StoreInt64(&b.success, 0)
	atomic.StoreInt64(&b.timeout, 0)
	atomic.StoreInt64(&b.timeStamp, time.Now().UnixNano())
}

func (b *bucket) Fail() {
	atomic.AddInt64(&b.failure, 1)
}

func (b *bucket) Succeed() {
	atomic.AddInt64(&b.success, 1)
}

func (b *bucket) Timeout() {
	atomic.AddInt64(&b.timeout, 1)
}

func (b *bucket) Failures() int64 {
	return atomic.LoadInt64(&b.failure)
}

func (b *bucket) Successes() int64 {
	return atomic.LoadInt64(&b.success)
}

func (b *bucket) Timeouts() int64 {
	return atomic.LoadInt64(&b.timeout)
}

func (b *bucket) TimeStamp() int64 {
	return atomic.LoadInt64(&b.timeStamp)
}

// window maintains a slice of buckets and increments the failure and success
// counts of the current bucket
type window struct {
	sync.Mutex
	oldest  int       // oldest bucket index
	latest  int       // latest bucket index
	buckets []*bucket // buckets this window has

	bucketTime time.Duration // time each bucket holds
	bucketNums int           // the largest number of buckets of window could have
	expireTime time.Duration // expire time of this window, equals to window size
	inWindow   int           // the number of buckets in the window currently

	conseErr int64 //consecutive errors
}

// NewWindowWithOptions creates a new window
func NewWindowWithOptions(bucketTime time.Duration, bucketNums int) (Container, error) {
	if bucketNums < 100 {
		return nil, fmt.Errorf("BucketNums can't be less than 100")
	}

	w := new(window)
	w.bucketNums = bucketNums
	w.bucketTime = bucketTime
	w.buckets = make([]*bucket, w.bucketNums)
	w.expireTime = bucketTime * time.Duration(bucketNums)
	for i := 0; i < w.bucketNums; i++ {
		w.buckets[i] = new(bucket)
	}

	w.Reset()
	return w, nil
}

// Fail records a failure in the current bucket
func (w *window) Fail() {
	w.Lock()
	b := w.latestBucket()
	w.conseErr++
	w.Unlock()
	b.Fail()
}

// Success records a success in the current bucket
func (w *window) Succeed() {
	w.Lock()
	b := w.latestBucket()
	w.conseErr = 0
	w.Unlock()
	b.Succeed()
}

// Timeout records a timeout in the current bucket
func (w *window) Timeout() {
	w.Lock()
	b := w.latestBucket()
	w.conseErr++
	w.Unlock()
	b.Timeout()
}

func (w *window) Counts() (successes, failures, timeouts int64) {
	w.Lock()
	oldest, remain := w.expire()
	w.Unlock()

	for remain > 0 {
		timeouts += w.buckets[oldest].Timeouts()
		successes += w.buckets[oldest].Successes()
		failures += w.buckets[oldest].Failures()
		oldest++
		if oldest >= w.bucketNums {
			oldest = 0
		}
		remain--
	}

	return
}

// Successes returns the total number of successes recorded in all buckets
func (w *window) Successes() int64 {
	successes, _, _ := w.Counts()
	return successes
}

// Failures returns the total number of failures recorded in all buckets
func (w *window) Failures() int64 {
	_, failures, _ := w.Counts()
	return failures
}

// Timeouts returns the total number of Timeout recorded in all buckets
func (w *window) Timeouts() int64 {
	_, _, timeouts := w.Counts()
	return timeouts
}

func (w *window) ConsecutiveErrors() int64 {
	w.Lock()
	c := w.conseErr
	w.Unlock()
	return c
}

// ErrorRate returns the error rate calculated over all buckets, expressed as
// a floating point number (ex: 0.9 for 90%)
func (w *window) ErrorRate() float64 {
	successes, failures, timeouts := w.Counts()

	if (successes + failures + timeouts) == 0 {
		return 0.0
	}

	return float64(failures+timeouts) / float64(successes+failures+timeouts)
}

func (w *window) Samples() int64 {
	successes, failures, timeouts := w.Counts()

	return successes + failures + timeouts
}

// Reset resets this window
func (w *window) Reset() {
	w.Lock()
	w.oldest = 0
	w.latest = 0
	w.inWindow = 1
	w.conseErr = 0
	w.buckets[w.latest].Reset()
	w.Unlock()
}

// expire removes expired buckets's affect
// lock should be obtained by the outside function who call it
func (w *window) expire() (oldest, inWindow int) {
	now := time.Now().UnixNano()
	for w.inWindow > 0 {
		oldestBucket := w.buckets[w.oldest]
		stamp := oldestBucket.TimeStamp()

		if stamp+int64(w.expireTime) <= now { // expired
			w.oldest++
			if w.oldest == w.bucketNums {
				w.oldest = 0
			}
			w.inWindow--
		} else {
			break
		}
	}
	return w.oldest, w.inWindow
}

// latestBucket returns the latest bucket;
// lock should be obtained by the outside function who call it
func (w *window) latestBucket() *bucket {
	// check or create the lastest bucket
	lastestBucket := w.buckets[w.latest]
	stamp := lastestBucket.TimeStamp()
	now := time.Now().UnixNano()

	if stamp+int64(w.bucketTime) <= now { // expired
		w.latest++
		if w.latest >= w.bucketNums {
			w.latest = 0
		}

		if w.inWindow == w.bucketNums {
			// the lastest covered the oldest(latest == oldest)
			w.oldest++
			if w.oldest >= w.bucketNums {
				w.oldest = 0
			}
		} else {
			w.inWindow++
		}
		w.buckets[w.latest].Reset()
	}
	b := w.buckets[w.latest]
	return b
}
