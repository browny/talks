// Package polling abstracts polling behavior
package polling

import (
	"errors"
	"time"

	"straas.io/base/backoff"

	"github.com/cihub/seelog"
)

// Pollable defines behaviors which the pollable things should have
type Pollable interface {
	// New takes all necessary arguments then returns a new pollable
	New(args ...interface{}) Pollable
	// Query gets the response from the polled target
	Query() (response interface{}, err error)
	// Derive translates response from Query to meaningful events with context.
	// Format as map[<context>]<event_id>.
	Derive(response interface{}) map[interface{}]int
	// EventHandler defines event handler
	EventHandler(eventID int, context interface{}) error
	// Completed returns if the polling is completed
	Completed() bool
	// Result returns the desired output and any error
	Result() (interface{}, error)
}

// act iterates all events and triggers different handling according to different event
func act(
	events map[interface{}]int,
	handler func(event int, context interface{}) error,
	logger seelog.LoggerInterface,
) {
	for k, v := range events {
		err := handler(v, k)
		if err != nil {
			logger.Warnf("err: %s", err)
		}
	}
}

// DefaultPoller is the basic implementation of Pollable interface.
// It polls until some condition is satisfied.
type DefaultPoller struct {
	Interval       time.Duration
	MaxElapsedTime time.Duration
	Pollable       Pollable

	Logger seelog.LoggerInterface
}

// Poll polls until some condition is satisfied, then take corresponding action
func (dp *DefaultPoller) Poll() int {
	pollable := dp.Pollable

	c := backoff.NewConstBackOff(dp.Interval, dp.MaxElapsedTime)

	numOfLoop := 0
	op := func() error {
		response, err := pollable.Query()
		if err != nil {
			dp.Logger.Warnf("err: %s", err)
			return err
		}
		events := pollable.Derive(response)
		act(events, pollable.EventHandler, dp.Logger)

		if !pollable.Completed() {
			numOfLoop++
			return errors.New("not yet completed")
		}

		return nil
	}

	backoff.Retry(op, c)

	dp.Logger.Debugf("Poll completed")
	return numOfLoop
}

// InfinitePoller polls forever
type InfinitePoller struct {
	Interval time.Duration
	Pollable Pollable

	Logger seelog.LoggerInterface
}

// Poll polls something, then take corresponding action
func (infp *InfinitePoller) Poll() {
	pollable := infp.Pollable

	c := backoff.NewConstBackOff(infp.Interval, 0)

	op := func() error {
		response, err := pollable.Query()
		if err != nil {
			infp.Logger.Warnf("err: %s", err)
			return err
		}
		events := pollable.Derive(response)
		act(events, pollable.EventHandler, infp.Logger)

		return errors.New("infinite poll")
	}

	backoff.Retry(op, c)
}
