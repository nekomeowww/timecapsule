package timecapsule

import (
	"time"
)

type Dataloader[C any] interface {
	Type() string

	BuryFor(payload C, forTimeRange time.Duration) error
	BuryUtil(payload C, utilUnixMilliTimestamp int64) error

	Dig() (capsules *TimeCapsule[C], err error)
	Destroy(capsule *TimeCapsule[C]) error
}
