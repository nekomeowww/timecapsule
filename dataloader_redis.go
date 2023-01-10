package timecapsule

import (
	"strconv"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/samber/lo"
)

// RedisDataloader is a dataloader that loads data from redis
type RedisDataloader[P any] struct {
	sortedSetKey string
	redisClient  *redis.Client
}

// static check implementation
var _ Dataloader[any] = (*RedisDataloader[any])(nil)

// NewRedisDataloader creates a new RedisDataloader
func NewRedisDataloader[P any](sortedSetKey string, redisClient *redis.Client) *RedisDataloader[P] {
	return &RedisDataloader[P]{
		sortedSetKey: sortedSetKey,
		redisClient:  redisClient,
	}
}

// Type returns the type of the dataloader
func (r *RedisDataloader[P]) Type() string {
	return "Redis"
}

// BuryFor buries the payload into the ground for the given duration
//
// Equivalent to redis command:
//
//	ZADD sortedSetKey <now timestamp + forTimeRange> <capsule base64 string>
func (r *RedisDataloader[P]) BuryFor(payload P, forTimeRange time.Duration) error {
	utilUnixMilliTimestamp := time.Now().UTC().Add(forTimeRange).UnixMilli()
	return r.BuryUtil(payload, utilUnixMilliTimestamp)
}

// BuryUtil buries the payload into the ground util the given timestamp
//
// Equivalent to redis command:
//
//	ZADD sortedSetKey utilUnixMilliTimestamp <capsule base64 string>
func (r *RedisDataloader[P]) BuryUtil(payload P, utilUnixMilliTimestamp int64) error {
	now := time.Now().UTC().UnixMilli()
	newCapsule := TimeCapsule[any]{Payload: payload, BuriedAt: now}
	return r.bury(newCapsule.Base64String(), utilUnixMilliTimestamp)
}

func (r *RedisDataloader[P]) bury(capsuleBase64String string, utilUnixMilliTimestamp int64) error {
	err := r.redisClient.ZAdd(r.sortedSetKey, &redis.Z{Score: float64(utilUnixMilliTimestamp), Member: capsuleBase64String}).Err()
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
func (r *RedisDataloader[P]) Dig() (*TimeCapsule[P], error) {
	now := time.Now().UTC()
	members, err := r.redisClient.ZRangeByScore(r.sortedSetKey, &redis.ZRangeBy{
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

	capsulesList, err := r.redisClient.ZPopMin(r.sortedSetKey, 1).Result()
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
		time.Sleep(1 * time.Second)
		_, _, err := lo.AttemptWithDelay(100, 500*time.Millisecond, func(i int, d time.Duration) error {
			return r.bury(capsulesList[0].Member.(string), capsuleOpeningTime.UnixMilli())
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
func (r *RedisDataloader[P]) Destroy(capsule *TimeCapsule[P]) error {
	_, _, err := lo.AttemptWithDelay(100, 500*time.Millisecond, func(i int, d time.Duration) error {
		pipeline := r.redisClient.TxPipeline()
		err := pipeline.ZRem(r.sortedSetKey, capsule.Base64String()).Err()
		if err != nil {
			return err
		}

		_, err = pipeline.Exec()
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
