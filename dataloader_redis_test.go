package timecapsule

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/go-redis/redis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewRedisDataloader(t *testing.T) {
	sortedSetKey := "test/timecapsule/redis/zset"

	redisClient := redis.NewClient(&redis.Options{Addr: net.JoinHostPort("localhost", "6379"), Password: "123456"})
	err := redisClient.Ping().Err()
	require.NoError(t, err)

	require.NotPanics(t, func() {
		dataloader := NewRedisDataloader[any](sortedSetKey, redisClient)
		assert.Equal(t, sortedSetKey, dataloader.sortedSetKey)
	})
}

func TestRedisDataloaderType(t *testing.T) {
	sortedSetKey := "test/timecapsule/redis/zset"

	redisClient := redis.NewClient(&redis.Options{Addr: net.JoinHostPort("localhost", "6379"), Password: "123456"})
	err := redisClient.Ping().Err()
	require.NoError(t, err)

	dataloader := NewRedisDataloader[any](sortedSetKey, redisClient)
	assert.Equal(t, "Redis", dataloader.Type())
}

func TestRedisDataloaderBuryFor(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	randomSeed, err := rand.Int(rand.Reader, big.NewInt(100000))
	require.NoError(err)

	sortedSetKey := fmt.Sprintf("test/timecapsule/redis/zset/%d", randomSeed.Int64())
	redisClient := redis.NewClient(&redis.Options{Addr: net.JoinHostPort("localhost", "6379"), Password: "123456"})
	err = redisClient.Ping().Err()
	require.NoError(err)

	dataloader := NewRedisDataloader[any](sortedSetKey, redisClient)
	err = dataloader.BuryFor("test", time.Minute)
	require.NoError(err)
	defer func() {
		err = dataloader.redisClient.Del(dataloader.sortedSetKey).Err()
		assert.NoError(err)
	}()

	memsCount, err := dataloader.redisClient.ZCount(sortedSetKey, "-inf", "+inf").Result()
	require.NoError(err)
	assert.Equal(int64(1), memsCount)

	mems, err := dataloader.redisClient.ZRangeWithScores(sortedSetKey, 0, -1).Result()
	require.NoError(err)
	require.Len(mems, 1)

	capsule, err := NewTimeCapsuleFromBase64String[any](mems[0].Member.(string))
	require.NoError(err)

	now := time.Now().UTC()
	assert.GreaterOrEqual(now.Add(time.Minute).UnixMilli(), int64(mems[0].Score))
	assert.Equal("test", capsule.Payload)
	assert.GreaterOrEqual(now.UnixMilli(), capsule.BuriedAt)
}

func TestRedisDataloaderBuryUtil(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	randomSeed, err := rand.Int(rand.Reader, big.NewInt(100000))
	require.NoError(err)

	sortedSetKey := fmt.Sprintf("test/timecapsule/redis/zset/%d", randomSeed.Int64())
	redisClient := redis.NewClient(&redis.Options{Addr: net.JoinHostPort("localhost", "6379"), Password: "123456"})
	err = redisClient.Ping().Err()
	require.NoError(err)

	dataloader := NewRedisDataloader[any](sortedSetKey, redisClient)
	err = dataloader.BuryUtil("test", time.Now().UTC().Add(time.Hour).UnixMilli())
	require.NoError(err)
	defer func() {
		err = dataloader.redisClient.Del(dataloader.sortedSetKey).Err()
		assert.NoError(err)
	}()

	memsCount, err := dataloader.redisClient.ZCount(sortedSetKey, "-inf", "+inf").Result()
	require.NoError(err)
	assert.Equal(int64(1), memsCount)

	mems, err := dataloader.redisClient.ZRangeWithScores(sortedSetKey, 0, -1).Result()
	require.NoError(err)
	require.Len(mems, 1)

	capsule, err := NewTimeCapsuleFromBase64String[any](mems[0].Member.(string))
	require.NoError(err)

	now := time.Now().UTC()
	assert.GreaterOrEqual(now.Add(time.Hour).UnixMilli(), int64(mems[0].Score))
	assert.Equal("test", capsule.Payload)
	assert.GreaterOrEqual(now.UnixMilli(), capsule.BuriedAt)
}

func TestRedisDataloaderDig(t *testing.T) {
	t.Run("DugOutCorrectCapsule", func(t *testing.T) {
		assert := assert.New(t)
		require := require.New(t)

		randomSeed, err := rand.Int(rand.Reader, big.NewInt(100000))
		require.NoError(err)

		sortedSetKey := fmt.Sprintf("test/timecapsule/redis/zset/%d", randomSeed.Int64())
		redisClient := redis.NewClient(&redis.Options{Addr: net.JoinHostPort("localhost", "6379"), Password: "123456"})
		err = redisClient.Ping().Err()
		require.NoError(err)

		dataloader := NewRedisDataloader[any](sortedSetKey, redisClient)
		err = dataloader.BuryUtil("shouldBeDugOut", time.Now().UTC().Add(-5*time.Millisecond).UnixMilli())
		require.NoError(err)

		err = dataloader.BuryUtil("shouldNotBeDugOut", time.Now().UTC().Add(5*time.Millisecond).UnixMilli())
		require.NoError(err)
		defer func() {
			err = dataloader.redisClient.Del(dataloader.sortedSetKey).Err()
			assert.NoError(err)
		}()

		capsule, err := dataloader.Dig()
		require.NoError(err)
		require.NotNil(capsule)

		now := time.Now().UTC()
		assert.Equal("shouldBeDugOut", capsule.Payload)
		assert.GreaterOrEqual(now.UnixMilli(), capsule.BuriedAt)
		assert.GreaterOrEqual(now.UnixMilli(), capsule.DugOutAt)
	})

	t.Run("DugOutInCorrectOpeningTimeCapsule", func(t *testing.T) {
		assert := assert.New(t)
		require := require.New(t)

		randomSeed, err := rand.Int(rand.Reader, big.NewInt(100000))
		require.NoError(err)

		sortedSetKey := fmt.Sprintf("test/timecapsule/redis/zset/%d", randomSeed.Int64())
		redisClient := redis.NewClient(&redis.Options{Addr: net.JoinHostPort("localhost", "6379"), Password: "123456"})
		err = redisClient.Ping().Err()
		require.NoError(err)

		dataloader := NewRedisDataloader[any](sortedSetKey, redisClient)
		err = dataloader.BuryUtil("shouldNotBeDugOut", time.Now().UTC().Add(5*time.Millisecond).UnixMilli())
		require.NoError(err)
		defer func() {
			err = dataloader.redisClient.Del(dataloader.sortedSetKey).Err()
			assert.NoError(err)
		}()

		dugCapsule, err := dataloader.Dig()
		require.NoError(err)
		require.Nil(dugCapsule)

		memsCount, err := dataloader.redisClient.ZCount(sortedSetKey, "-inf", "+inf").Result()
		require.NoError(err)
		assert.Equal(int64(1), memsCount)

		mems, err := dataloader.redisClient.ZRangeWithScores(sortedSetKey, 0, -1).Result()
		require.NoError(err)
		require.Len(mems, 1)

		requeuedCapsule, err := NewTimeCapsuleFromBase64String[any](mems[0].Member.(string))
		require.NoError(err)

		now := time.Now().UTC()
		assert.Equal("shouldNotBeDugOut", requeuedCapsule.Payload)
		assert.GreaterOrEqual(now.UnixMilli(), requeuedCapsule.BuriedAt)
		assert.GreaterOrEqual(now.UnixMilli(), requeuedCapsule.DugOutAt)
	})
}

func TestRedisDataloaderDestroy(t *testing.T) {
	require := require.New(t)

	randomSeed, err := rand.Int(rand.Reader, big.NewInt(100000))
	require.NoError(err)

	sortedSetKey := fmt.Sprintf("test/timecapsule/redis/zset/%d", randomSeed.Int64())
	redisClient := redis.NewClient(&redis.Options{Addr: net.JoinHostPort("localhost", "6379"), Password: "123456"})
	err = redisClient.Ping().Err()
	require.NoError(err)

	dataloader := NewRedisDataloader[any](sortedSetKey, redisClient)
	err = dataloader.BuryUtil("shouldBeDugOut", time.Now().UTC().Add(-5*time.Millisecond).UnixMilli())
	require.NoError(err)

	capsule, err := dataloader.Dig()
	require.NoError(err)
	require.NotNil(capsule)

	err = dataloader.Destroy(capsule)
	require.NoError(err)

	mems, err := dataloader.redisClient.ZRangeWithScores(sortedSetKey, 0, -1).Result()
	require.NoError(err)
	require.Len(mems, 0)
}
