package timecapsule

import (
	"time"

	"golang.org/x/net/context"
)

type Dataloader[P any] interface {
	Type() string

	BuryFor(ctx context.Context, payload P, forTimeRange time.Duration) error
	BuryUtil(ctx context.Context, payload P, utilUnixMilliTimestamp int64) error

	Dig(ctx context.Context) (capsules *TimeCapsule[P], err error)
	Destroy(ctx context.Context, capsule *TimeCapsule[P]) error
}
