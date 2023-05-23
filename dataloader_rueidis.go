package timecapsule

import (
	"strconv"
	"time"

	"github.com/redis/rueidis"
	"github.com/samber/lo"
	"golang.org/x/net/context"
)

// RedisDataloader is a dataloader that loads data from redis.
type RueidisDataloader[P any] struct {
	sortedSetKey  string
	rueidisClient rueidis.Client
}

var _ Dataloader[any] = (*RueidisDataloader[any])(nil)

// NewRueidisDataloader creates a new RueidisDataloader.
func NewRueidisDataloader[P any](sortedSetKey string, redisClient rueidis.Client) *RueidisDataloader[P] {
	return &RueidisDataloader[P]{
		sortedSetKey:  sortedSetKey,
		rueidisClient: redisClient,
	}
}

// Type returns the type of the dataloader.
func (r *RueidisDataloader[P]) Type() string {
	return "Rueidis"
}

// BuryFor buries the payload into the ground for the given duration
//
// Equivalent to redis command:
//
//	ZADD sortedSetKey <now timestamp + forTimeRange> <capsule base64 string>
func (r *RueidisDataloader[P]) BuryFor(ctx context.Context, payload P, forTimeRange time.Duration) error {
	utilUnixMilliTimestamp := time.Now().UTC().Add(forTimeRange).UnixMilli()
	return r.BuryUtil(ctx, payload, utilUnixMilliTimestamp)
}

// BuryUtil buries the payload into the ground util the given timestamp
//
// Equivalent to redis command:
//
//	ZADD sortedSetKey utilUnixMilliTimestamp <capsule base64 string>
func (r *RueidisDataloader[P]) BuryUtil(ctx context.Context, payload P, utilUnixMilliTimestamp int64) error {
	newCapsule := TimeCapsule[any]{Payload: payload}
	return r.bury(ctx, newCapsule.Base64String(), utilUnixMilliTimestamp)
}

func (r *RueidisDataloader[P]) bury(ctx context.Context, capsuleBase64String string, utilUnixMilliTimestamp int64) error {
	err := r.rueidisClient.Do(ctx, r.rueidisClient.B().Zadd().Key(r.sortedSetKey).ScoreMember().ScoreMember(float64(utilUnixMilliTimestamp), capsuleBase64String).Build()).Error()
	if err != nil {
		return err
	}

	return nil
}

// Dig digs the time capsule from the dataloader
//
// Equivalent to redis command flow:
//
//	     ZRANGEBYSCORE sortedSetKey 0 <now timestamp>
//	                            |
//	                      got elements?
//	                            |
//	                   -------------------
//	                   |                 |
//	        ZPOPMIN sortedSetKey 1     return
//	                   |
//	            dut to execute?
//	                   |
//	           -----------------
//	           |               |
//	return TimeCapsule     return
func (r *RueidisDataloader[P]) Dig(ctx context.Context) (*TimeCapsule[P], error) {
	now := time.Now().UTC()

	zrangebyscoreCmd := r.rueidisClient.
		B().
		Zrangebyscore().
		Key(r.sortedSetKey).
		Min("0").
		Max(strconv.FormatInt(now.UnixMilli(), 10)).
		Build()

	resp := r.rueidisClient.Do(ctx, zrangebyscoreCmd)

	err := resp.Error()
	if err != nil {
		if rueidis.IsRedisNil(err) {
			return nil, nil
		}

		return nil, err
	}

	members, err := resp.AsStrSlice()
	if err != nil {
		return nil, err
	}
	if len(members) == 0 {
		return nil, nil
	}

	zpopminCmd := r.rueidisClient.
		B().
		Zpopmin().
		Key(r.sortedSetKey).
		Count(1).
		Build()

	resp = r.rueidisClient.Do(ctx, zpopminCmd)

	err = resp.Error()
	if err != nil {
		if rueidis.IsRedisNil(err) {
			return nil, nil
		}

		return nil, err
	}

	capsulesList, err := resp.AsZScores()
	if err != nil {
		return nil, err
	}
	if len(capsulesList) == 0 {
		return nil, nil
	}

	headCapsule := capsulesList[0]

	capsuleOpeningTime := time.UnixMilli(int64(headCapsule.Score))
	if capsuleOpeningTime.After(now) {
		time.Sleep(10 * time.Millisecond)

		_, _, err := lo.AttemptWithDelay(100, 10*time.Millisecond, func(i int, d time.Duration) error {
			return r.bury(ctx, headCapsule.Member, capsuleOpeningTime.UnixMilli())
		})
		if err != nil {
			return nil, err
		}

		return nil, nil
	}

	capsule, err := NewTimeCapsuleFromBase64String[P](headCapsule.Member)
	if err != nil {
		return nil, err
	}

	capsule.DugOutAt = now.UnixMilli()

	return capsule, nil
}

// Destroy destroys the given capsule
//
// Equivalent to redis command:
//
//	ZREM sortedSetKey <capsule base64 string>
func (r *RueidisDataloader[P]) Destroy(ctx context.Context, capsule *TimeCapsule[P]) error {
	_, _, err := lo.AttemptWithDelay(100, 10*time.Millisecond, func(i int, d time.Duration) error {
		zremCmd := r.rueidisClient.
			B().
			Zrem().
			Key(r.sortedSetKey).
			Member(capsule.Base64String()).
			Build()

		resp := r.rueidisClient.Do(ctx, zremCmd)
		err := resp.Error()
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return err
	}

	return nil
}
