package timecapsule

import (
	"errors"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/samber/lo"
	"golang.org/x/net/context"
)

// RedisDataloader is a dataloader that loads data from redis.
type RedisDataloader[P any] struct {
	sortedSetKey string
	redisClient  *redis.Client
}

// static check implementation.
var _ Dataloader[any] = (*RedisDataloader[any])(nil)

// NewRedisDataloader creates a new RedisDataloader.
func NewRedisDataloader[P any](sortedSetKey string, redisClient *redis.Client) *RedisDataloader[P] {
	return &RedisDataloader[P]{
		sortedSetKey: sortedSetKey,
		redisClient:  redisClient,
	}
}

// Type returns the type of the dataloader.
func (r *RedisDataloader[P]) Type() string {
	return "Redis"
}

// BuryFor buries the payload into the ground for the given duration
//
// Equivalent to redis command:
//
//	ZADD sortedSetKey <now timestamp + forTimeRange> <capsule base64 string>
func (r *RedisDataloader[P]) BuryFor(ctx context.Context, payload P, forTimeRange time.Duration) error {
	utilUnixMilliTimestamp := time.Now().UTC().Add(forTimeRange).UnixMilli()
	return r.BuryUtil(ctx, payload, utilUnixMilliTimestamp)
}

// BuryUtil buries the payload into the ground util the given timestamp
//
// Equivalent to redis command:
//
//	ZADD sortedSetKey utilUnixMilliTimestamp <capsule base64 string>
func (r *RedisDataloader[P]) BuryUtil(ctx context.Context, payload P, utilUnixMilliTimestamp int64) error {
	newCapsule := TimeCapsule[any]{Payload: payload}
	return r.bury(ctx, newCapsule.Base64String(), utilUnixMilliTimestamp)
}

func (r *RedisDataloader[P]) bury(ctx context.Context, capsuleBase64String string, utilUnixMilliTimestamp int64) error {
	return invoke0(ctx, func() error {
		err := r.redisClient.ZAdd(ctx, r.sortedSetKey, redis.Z{Score: float64(utilUnixMilliTimestamp), Member: capsuleBase64String}).Err()
		if err != nil {
			return err
		}

		return nil
	})
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
func (r *RedisDataloader[P]) Dig(ctx context.Context) (*TimeCapsule[P], error) {
	now := time.Now().UTC()

	members, err := r.redisClient.ZRangeByScore(ctx, r.sortedSetKey, &redis.ZRangeBy{
		Min: "0",
		Max: strconv.FormatInt(now.UnixMilli(), 10),
	}).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}

		return nil, err
	}
	if len(members) == 0 {
		return nil, nil
	}

	capsulesList, err := r.redisClient.ZPopMin(ctx, r.sortedSetKey, 1).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}

		return nil, err
	}
	if len(capsulesList) == 0 {
		return nil, nil
	}

	capsuleOpeningTime := time.UnixMilli(int64(capsulesList[0].Score))
	if capsuleOpeningTime.After(now) {
		time.Sleep(10 * time.Millisecond)

		_, _, err := lo.AttemptWithDelay(100, 10*time.Millisecond, func(i int, d time.Duration) error {
			member, ok := capsulesList[0].Member.(string)
			if !ok {
				return errors.New("invalid capsule content")
			}

			return r.bury(ctx, member, capsuleOpeningTime.UnixMilli())
		})
		if err != nil {
			return nil, err
		}

		return nil, nil
	}

	capsuleContent, ok := capsulesList[0].Member.(string)
	if !ok {
		return nil, err
	}

	capsule, err := NewTimeCapsuleFromBase64String[P](capsuleContent)
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
func (r *RedisDataloader[P]) Destroy(ctx context.Context, capsule *TimeCapsule[P]) error {
	_, _, err := lo.AttemptWithDelay(100, 10*time.Millisecond, func(i int, d time.Duration) error {
		pipeline := r.redisClient.TxPipeline()
		err := pipeline.ZRem(ctx, r.sortedSetKey, capsule.Base64String()).Err()
		if err != nil {
			return err
		}

		_, err = pipeline.Exec(ctx)
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
