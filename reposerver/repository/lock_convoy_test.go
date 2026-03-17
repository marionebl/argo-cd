package repository

// These tests demonstrate the goroutine convoy deadlock in the repo-server revision lock.
//
// When a different revision is being processed, Lock() blocks indefinitely with no way
// for the caller to give up. This means goroutines accumulate without bound during rapid
// commit bursts, forming a convoy that the server never recovers from.
//
// See: https://github.com/marionebl/argo-cd/issues/1

import (
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	utilio "github.com/argoproj/argo-cd/v3/util/io"
)

// TestLock_WaiterForDifferentRevision_CannotBeUnblocked demonstrates the core issue:
// once a goroutine starts waiting for a different revision to complete, there is no way
// to unblock it other than the current revision finishing. In production, when the caller's
// gRPC deadline expires, the goroutine keeps waiting forever — leaked and holding resources.
func TestLock_WaiterForDifferentRevision_CannotBeUnblocked(t *testing.T) {
	lock := NewRepositoryLock()
	init := numberOfInits(new(int))

	// Acquire lock with revision "1"
	closer1, done := lockQuickly(func() (io.Closer, error) {
		return lock.Lock("myRepo", "1", true, init)
	})
	assert.True(t, done)

	// Start a goroutine that tries to acquire revision "2".
	// It will block at sync.Cond.Wait() indefinitely.
	waiterDone := make(chan struct{})
	go func() {
		defer close(waiterDone)
		lock.Lock("myRepo", "2", true, init)
	}()

	// Give the waiter 500ms — it should be able to give up by now in a healthy system,
	// but it can't because there is no cancellation mechanism.
	select {
	case <-waiterDone:
		// If we get here before releasing closer1, the lock has a cancellation mechanism.
		// That's the desired behavior — this test should start passing after the fix.
		t.Log("waiter was able to exit — cancellation works (test should now pass)")
	case <-time.After(500 * time.Millisecond):
		// This is the current (broken) behavior: the waiter is stuck forever.
		// We release the lock so the goroutine doesn't leak past test cleanup.
		utilio.Close(closer1)
		<-waiterDone // wait for cleanup
		t.Fatal("waiter for different revision blocked indefinitely with no way to cancel")
	}
}

// TestLock_ConvoyFormsUnderSequentialRevisions demonstrates the full convoy scenario:
// when revision "A" holds the lock, goroutines for revision "B" pile up and cannot
// be reclaimed, even though their callers have long since given up.
func TestLock_ConvoyFormsUnderSequentialRevisions(t *testing.T) {
	lock := NewRepositoryLock()
	init := func(_ bool) (io.Closer, error) {
		return utilio.NopCloser, nil
	}

	// Batch 1: hold the lock on revision "A"
	closer1, done := lockQuickly(func() (io.Closer, error) {
		return lock.Lock("myRepo", "A", true, init)
	})
	assert.True(t, done)

	// Batch 2: 10 goroutines try to acquire revision "B".
	// They all block on sync.Cond.Wait() and cannot be cancelled.
	const batchSize = 10
	var wg sync.WaitGroup
	var stuck int32

	for i := 0; i < batchSize; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			atomic.AddInt32(&stuck, 1)
			closer, _ := lock.Lock("myRepo", "B", true, init)
			atomic.AddInt32(&stuck, -1)
			utilio.Close(closer)
		}()
	}

	// Wait for all goroutines to enter the wait state
	time.Sleep(200 * time.Millisecond)
	stuckCount := atomic.LoadInt32(&stuck)
	assert.Equal(t, int32(batchSize), stuckCount,
		"all batch-2 goroutines should be stuck waiting")

	// In a healthy system, the caller would cancel these goroutines after a deadline.
	// But there is no cancellation path — we must release batch 1 to unblock them.
	allDone := make(chan struct{})
	go func() { wg.Wait(); close(allDone) }()

	// Verify they DON'T exit on their own within 500ms (proving the convoy)
	select {
	case <-allDone:
		t.Fatal("batch-2 goroutines exited without batch-1 releasing — unexpected")
	case <-time.After(500 * time.Millisecond):
		// Expected: they are stuck. This IS the bug.
	}

	// Release batch 1 to clean up
	utilio.Close(closer1)

	// Now they should all complete
	select {
	case <-allDone:
		// OK, cleaned up
	case <-time.After(2 * time.Second):
		t.Fatal("goroutines leaked even after releasing batch 1")
	}

	// The bug: all 10 goroutines were stuck for 500ms+ with no way to cancel.
	// In production with 167 apps and sequential commits, this grows without bound.
	t.Fatal("convoy formed: 10 goroutines were stuck with no cancellation mechanism")
}
