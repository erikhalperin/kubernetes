/*
Copyright 2016 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cacher

import (
	"sync"
	"time"

	"k8s.io/utils/clock"
)

const (
	refreshPerSecond = 50 * time.Millisecond
	maxBudget        = 100 * time.Millisecond
	maxEventTime     = 2 * time.Millisecond
)

// timeBudget implements a budget of time that you can use and is
// periodically being refreshed. The pattern to use it is:
//
//	budget := newTimeBudget(...)
//	...
//	timeout := budget.takeAvailable()
//	// Now you can spend at most timeout on doing stuff
//	...
//	// If you didn't use all timeout, return what you didn't use
//	budget.returnUnused(<unused part of timeout>)
//
// NOTE: It's not recommended to be used concurrently from multiple threads -
// if first user takes the whole timeout, the second one will get 0 timeout
// even though the first one may return something later.
type timeBudget interface {
	takeAvailable() time.Duration
	returnUnused(unused time.Duration)
}

type timeBudgetImpl struct {
	sync.Mutex
	clock     clock.Clock
	budget    time.Duration
	maxBudget time.Duration
	refresh   time.Duration
	// last store last access time
	last time.Time
}

func newTimeBudget() timeBudget {
	result := &timeBudgetImpl{
		clock:     clock.RealClock{},
		budget:    time.Duration(0),
		refresh:   refreshPerSecond,
		maxBudget: maxBudget,
	}
	result.last = result.clock.Now()
	return result
}

func (t *timeBudgetImpl) takeAvailable() time.Duration {
	t.Lock()
	defer t.Unlock()
	// budget accumulated since last access
	now := t.clock.Now()
	acc := now.Sub(t.last).Seconds() * t.refresh.Seconds()
	if acc < 0 {
		acc = 0
	}
	// update current budget and store the current time
	if t.budget = t.budget + time.Duration(acc*1e9); t.budget > t.maxBudget {
		t.budget = t.maxBudget
	}
	t.last = now
	result := t.budget
	t.budget = time.Duration(0)
	return result
}

func (t *timeBudgetImpl) returnUnused(unused time.Duration) {
	t.Lock()
	defer t.Unlock()
	if unused < 0 {
		// We used more than allowed.
		return
	}
	// add the unused time directly to the budget
	// takeAvailable() will take into account the elapsed time
	if t.budget = t.budget + unused; t.budget > t.maxBudget {
		t.budget = t.maxBudget
	}
}

// eventBudget implements a budget of time designed to track whether a client
// is keeping up with an expected throughput. Each event that takes longer than
// expected depletes the budget while events faster than expected restore
// the budget
type eventBudget struct {
	budget           time.Duration
	maxBudget        time.Duration
	expectedPerEvent time.Duration
}

func newEventBudget(maxBudget, expectedPerEvent time.Duration) *eventBudget {
	return &eventBudget{
		budget:           maxBudget,
		maxBudget:        maxBudget,
		expectedPerEvent: expectedPerEvent,
	}
}

// getTimeout returns how long the next event is allowed to take
// This is the expected duration plus any accumulated budget
func (b *eventBudget) getTimeout() time.Duration {
	return b.expectedPerEvent + b.budget
}

// reset restores the budget to its maximum value
func (b *eventBudget) reset() {
	b.budget = b.maxBudget
}

// updateBudget updates the budget based on how long the event took
func (b *eventBudget) updateBudget(actual time.Duration) {
	b.budget -= actual - b.expectedPerEvent
	if b.budget > b.maxBudget {
		b.budget = b.maxBudget
	}
}
