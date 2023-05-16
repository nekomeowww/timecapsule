package timecapsule

import (
	"context"
	"crypto/rand"
	"fmt"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/redis/rueidis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewRueidisDataloader(t *testing.T) {
	sortedSetKey := "test/timecapsule/redis/zset"

	redisClient, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{net.JoinHostPort("localhost", "6379")}})
	require.NoError(t, err)

	err = redisClient.Do(context.Background(), redisClient.B().Ping().Build()).Error()
	require.NoError(t, err)

	require.NotPanics(t, func() {
		dataloader := NewRueidisDataloader[any](sortedSetKey, redisClient)
		assert.Equal(t, sortedSetKey, dataloader.sortedSetKey)
	})
}

func TestRueidisDataloaderType(t *testing.T) {
	sortedSetKey := "test/timecapsule/redis/zset"

	redisClient, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{net.JoinHostPort("localhost", "6379")}})
	require.NoError(t, err)

	err = redisClient.Do(context.Background(), redisClient.B().Ping().Build()).Error()
	require.NoError(t, err)

	dataloader := NewRueidisDataloader[any](sortedSetKey, redisClient)
	assert.Equal(t, "Rueidis", dataloader.Type())
}

func TestRueidisDataloaderBuryFor(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	randomSeed, err := rand.Int(rand.Reader, big.NewInt(100000))
	require.NoError(err)

	sortedSetKey := fmt.Sprintf("test/timecapsule/redis/zset/%d", randomSeed.Int64())

	redisClient, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{net.JoinHostPort("localhost", "6379")}})
	require.NoError(err)

	err = redisClient.Do(context.Background(), redisClient.B().Ping().Build()).Error()
	require.NoError(err)

	dataloader := NewRueidisDataloader[any](sortedSetKey, redisClient)

	err = dataloader.BuryFor(context.Background(), "test", time.Minute)
	require.NoError(err)

	defer func() {
		err = dataloader.rueidisClient.Do(context.Background(), dataloader.rueidisClient.B().Del().Key(dataloader.sortedSetKey).Build()).Error()
		assert.NoError(err)
	}()

	zcountCmd := dataloader.rueidisClient.B().Zcount().Key(sortedSetKey).Min("-inf").Max("+inf").Build()
	resp := dataloader.rueidisClient.Do(context.Background(), zcountCmd)
	require.NoError(resp.Error())

	memsCount, err := resp.AsInt64()
	require.NoError(err)
	assert.Equal(int64(1), memsCount)

	zrangeWithScoresCmd := dataloader.rueidisClient.B().Zrange().Key(sortedSetKey).Min("0").Max("-1").Withscores().Build()
	resp = dataloader.rueidisClient.Do(context.Background(), zrangeWithScoresCmd)
	require.NoError(resp.Error())

	mems, err := resp.AsZScores()
	require.NoError(err)
	require.Len(mems, 1)

	capsule, err := NewTimeCapsuleFromBase64String[any](mems[0].Member)
	require.NoError(err)

	now := time.Now().UTC()
	assert.GreaterOrEqual(now.Add(time.Minute).UnixMilli(), int64(mems[0].Score))
	assert.Equal("test", capsule.Payload)
	assert.GreaterOrEqual(now.UnixMilli(), capsule.BuriedAt)
}

func TestRueidisDataloaderBuryUtil(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	randomSeed, err := rand.Int(rand.Reader, big.NewInt(100000))
	require.NoError(err)

	sortedSetKey := fmt.Sprintf("test/timecapsule/redis/zset/%d", randomSeed.Int64())

	redisClient, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{net.JoinHostPort("localhost", "6379")}})
	require.NoError(err)

	err = redisClient.Do(context.Background(), redisClient.B().Ping().Build()).Error()
	require.NoError(err)

	dataloader := NewRueidisDataloader[any](sortedSetKey, redisClient)

	err = dataloader.BuryUtil(context.Background(), "test", time.Now().UTC().Add(time.Hour).UnixMilli())
	require.NoError(err)

	defer func() {
		err = dataloader.rueidisClient.Do(context.Background(), dataloader.rueidisClient.B().Del().Key(dataloader.sortedSetKey).Build()).Error()
		assert.NoError(err)
	}()

	zcountCmd := dataloader.rueidisClient.B().Zcount().Key(sortedSetKey).Min("-inf").Max("+inf").Build()
	resp := dataloader.rueidisClient.Do(context.Background(), zcountCmd)
	require.NoError(resp.Error())

	memsCount, err := resp.AsInt64()
	require.NoError(err)
	assert.Equal(int64(1), memsCount)

	zrangeWithScoresCmd := dataloader.rueidisClient.B().Zrange().Key(sortedSetKey).Min("0").Max("-1").Withscores().Build()
	resp = dataloader.rueidisClient.Do(context.Background(), zrangeWithScoresCmd)
	require.NoError(resp.Error())

	mems, err := resp.AsZScores()
	require.NoError(err)
	require.Len(mems, 1)

	capsule, err := NewTimeCapsuleFromBase64String[any](mems[0].Member)
	require.NoError(err)

	now := time.Now().UTC()
	assert.GreaterOrEqual(now.Add(time.Hour).UnixMilli(), int64(mems[0].Score))
	assert.Equal("test", capsule.Payload)
	assert.GreaterOrEqual(now.UnixMilli(), capsule.BuriedAt)
}

func TestRueidisDataloaderDig(t *testing.T) {
	t.Run("DugOutCorrectCapsule", func(t *testing.T) {
		assert := assert.New(t)
		require := require.New(t)

		randomSeed, err := rand.Int(rand.Reader, big.NewInt(100000))
		require.NoError(err)

		sortedSetKey := fmt.Sprintf("test/timecapsule/redis/zset/%d", randomSeed.Int64())

		redisClient, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{net.JoinHostPort("localhost", "6379")}})
		require.NoError(err)

		err = redisClient.Do(context.Background(), redisClient.B().Ping().Build()).Error()
		require.NoError(err)

		dataloader := NewRueidisDataloader[any](sortedSetKey, redisClient)

		err = dataloader.BuryUtil(context.Background(), "shouldBeDugOut", time.Now().UTC().Add(-5*time.Millisecond).UnixMilli())
		require.NoError(err)

		err = dataloader.BuryUtil(context.Background(), "shouldNotBeDugOut", time.Now().UTC().Add(5*time.Millisecond).UnixMilli())
		require.NoError(err)
		defer func() {
			err = dataloader.rueidisClient.Do(context.Background(), dataloader.rueidisClient.B().Del().Key(dataloader.sortedSetKey).Build()).Error()
			assert.NoError(err)
		}()

		capsule, err := dataloader.Dig(context.Background())
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

		redisClient, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{net.JoinHostPort("localhost", "6379")}})
		require.NoError(err)

		err = redisClient.Do(context.Background(), redisClient.B().Ping().Build()).Error()
		require.NoError(err)

		dataloader := NewRueidisDataloader[any](sortedSetKey, redisClient)

		err = dataloader.BuryUtil(context.Background(), "shouldNotBeDugOut", time.Now().UTC().Add(5*time.Millisecond).UnixMilli())
		require.NoError(err)
		defer func() {
			err = dataloader.rueidisClient.Do(context.Background(), dataloader.rueidisClient.B().Del().Key(dataloader.sortedSetKey).Build()).Error()
			assert.NoError(err)
		}()

		dugCapsule, err := dataloader.Dig(context.Background())
		require.NoError(err)
		require.Nil(dugCapsule)

		zcountCmd := dataloader.rueidisClient.B().Zcount().Key(sortedSetKey).Min("-inf").Max("+inf").Build()
		resp := dataloader.rueidisClient.Do(context.Background(), zcountCmd)
		require.NoError(resp.Error())

		memsCount, err := resp.AsInt64()
		require.NoError(err)
		assert.Equal(int64(1), memsCount)

		zrangeWithScoresCmd := dataloader.rueidisClient.B().Zrange().Key(sortedSetKey).Min("0").Max("-1").Withscores().Build()
		resp = dataloader.rueidisClient.Do(context.Background(), zrangeWithScoresCmd)
		require.NoError(resp.Error())

		mems, err := resp.AsZScores()
		require.NoError(err)
		require.Len(mems, 1)

		requeuedCapsule, err := NewTimeCapsuleFromBase64String[any](mems[0].Member)
		require.NoError(err)

		now := time.Now().UTC()
		assert.Equal("shouldNotBeDugOut", requeuedCapsule.Payload)
		assert.GreaterOrEqual(now.UnixMilli(), requeuedCapsule.BuriedAt)
		assert.GreaterOrEqual(now.UnixMilli(), requeuedCapsule.DugOutAt)
	})
}

func TestRueidisDataloaderDestroy(t *testing.T) {
	require := require.New(t)

	randomSeed, err := rand.Int(rand.Reader, big.NewInt(100000))
	require.NoError(err)

	sortedSetKey := fmt.Sprintf("test/timecapsule/redis/zset/%d", randomSeed.Int64())

	redisClient, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{net.JoinHostPort("localhost", "6379")}})
	require.NoError(err)

	err = redisClient.Do(context.Background(), redisClient.B().Ping().Build()).Error()
	require.NoError(err)

	dataloader := NewRueidisDataloader[any](sortedSetKey, redisClient)

	err = dataloader.BuryUtil(context.Background(), "shouldBeDugOut", time.Now().UTC().Add(-5*time.Millisecond).UnixMilli())
	require.NoError(err)

	capsule, err := dataloader.Dig(context.Background())
	require.NoError(err)
	require.NotNil(capsule)

	err = dataloader.Destroy(context.Background(), capsule)
	require.NoError(err)

	zrangewithscoresCmd := dataloader.rueidisClient.B().Zrange().Key(sortedSetKey).Min("0").Max("-1").Withscores().Build()
	resp := dataloader.rueidisClient.Do(context.Background(), zrangewithscoresCmd)
	require.NoError(resp.Error())

	mems, err := resp.AsZScores()
	require.NoError(err)
	require.Len(mems, 0)
}
