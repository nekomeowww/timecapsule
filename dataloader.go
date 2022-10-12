package timecapsule

import (
	"time"
)

type Dataloader[P any] interface {
	Type() string

	BuryFor(payload P, forTimeRange time.Duration) error
	BuryUtil(payload P, utilUnixMilliTimestamp int64) error

	Dig() (capsules *TimeCapsule[P], err error)
	Destroy(capsule *TimeCapsule[P]) error
}
