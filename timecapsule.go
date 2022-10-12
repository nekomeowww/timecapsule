package timecapsule

import (
	"time"

	"github.com/sirupsen/logrus"
)

// TimeCapsuleLogger is the interface that wraps the basic Log method
type TimeCapsuleLogger interface {
	Debug(args ...interface{})
	Debugf(format string, args ...interface{})

	Warn(args ...interface{})
	Warnf(format string, args ...interface{})

	Error(args ...interface{})
	Errorf(format string, args ...interface{})
}

// TimeCapsuleDiggerOption is the option for TimeCapsuleDigger
type TimeCapsuleDiggerOption struct {
	RetryLimit    int
	RetryInterval time.Duration
	Logger        TimeCapsuleLogger
}

// DefaultTimeCapsuleDiggerOption returns the default option for TimeCapsuleDigger
func DefaultTimeCapsuleDiggerOption() TimeCapsuleDiggerOption {
	return TimeCapsuleDiggerOption{
		RetryLimit:    100,
		RetryInterval: 500 * time.Millisecond,
	}
}

// mergeTimeCapsuleDiggerOption merges the options
func mergeTimeCapsuleDiggerOption(original *TimeCapsuleDiggerOption, options ...TimeCapsuleDiggerOption) TimeCapsuleDiggerOption {
	if len(options) == 0 {
		return *original
	}

	option := options[0]
	if option.RetryLimit > 0 {
		original.RetryLimit = option.RetryLimit
	}
	if option.RetryInterval > 0 {
		original.RetryInterval = option.RetryInterval
	}
	if option.Logger != nil {
		original.Logger = option.Logger
	}

	return *original
}

// TimeCapsuleDigger will keep polling from TimeCapsuleDigger instance for new messages
// once TimeCapsuleDigger.Start() is called, and will stop once TimeCapsuleDigger.Stop()
// is called
type TimeCapsuleDigger[P any] struct {
	logger     TimeCapsuleLogger
	dataloader Dataloader[P]
	option     TimeCapsuleDiggerOption

	handlerFunc func(digger *TimeCapsuleDigger[P], capsule *TimeCapsule[P])

	// Digging ticker to notify the goroutine to dig a new capsule
	diggingTicker *time.Ticker

	// Meant to be used as a stop signal
	stopChan   chan struct{}
	shouldStop bool
}

// Digger creates a new TimeCapsuleDigger instance which derives from the TimeCapsule instance
//
// Params:
//
// topicKey is the key of the topic of time capsules, in most cases it will be the set key such
// as a Redis sorted set key or a Kafka topic
//	topicKey: string
// digInterval is the interval of digging a new capsule
//	digInterval: time.Duration
func NewDigger[P any](dataloader Dataloader[P], digInterval time.Duration, options ...TimeCapsuleDiggerOption) *TimeCapsuleDigger[P] {
	digger := &TimeCapsuleDigger[P]{
		logger:        logrus.New(),
		dataloader:    dataloader,
		option:        DefaultTimeCapsuleDiggerOption(),
		diggingTicker: time.NewTicker(digInterval),
		stopChan:      make(chan struct{}, 1),
	}

	mergeTimeCapsuleDiggerOption(&digger.option, options...)
	return digger
}

func (t *TimeCapsuleDigger[P]) SetHandler(handlerFunc func(digger *TimeCapsuleDigger[P], capsule *TimeCapsule[P])) {
	t.handlerFunc = handlerFunc
}

// Start starts the digger, which will keep polling the time capsule for new messages once the interval ticks
func (t *TimeCapsuleDigger[P]) Start() {
	for {
		if t.shouldStop {
			return
		}

		select {
		case <-t.diggingTicker.C:
			dugCapsule, err := t.dataloader.Dig()
			if err != nil {
				t.logger.Errorf("TimeCapsule: failed to dig time capsule from dataloder %v: %v", t.dataloader.Type(), err)
				continue
			}
			if dugCapsule == nil {
				continue
			}

			t.logger.Debugf("TimeCapsule: dug a new capsule from dataloder %v", t.dataloader.Type())
			if assertedCapsule, ok := any(dugCapsule).(*TimeCapsule[P]); t.handlerFunc != nil && ok && assertedCapsule != nil {
				t.handlerFunc(t, assertedCapsule)
			}
			if err := t.dataloader.Destroy(dugCapsule); err != nil {
				t.logger.Errorf("TimeCapsule: failed to burn time capsule: %v", err)
			} else {
				t.logger.Debugf("TimeCapsule: burned a capsule from dataloder %v", t.dataloader.Type())
			}
		case <-t.stopChan:
			return
		default:
			time.Sleep(100 * time.Millisecond) // prevent busy loop
		}
	}
}

// BuryFor bury a capsule for a specific time
func (t *TimeCapsuleDigger[P]) BuryFor(payload P, forTimeRange time.Duration) error {
	return t.dataloader.BuryFor(payload, forTimeRange)
}

// BuryUtil bury a capsule until a specific time
func (t *TimeCapsuleDigger[P]) BuryUtil(payload P, utilUnixMilliTimestamp int64) error {
	return t.dataloader.BuryUtil(payload, utilUnixMilliTimestamp)
}

// Stop stops the digger
func (t *TimeCapsuleDigger[P]) Stop() {
	if t.shouldStop {
		return
	}

	t.shouldStop = true
	t.diggingTicker.Stop()
	t.stopChan <- struct{}{}
	close(t.stopChan)
}
