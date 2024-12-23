package timecapsule

import (
	"context"
	"crypto/rand"
	"fmt"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	sortedSetKeyRedis = "test/timecapsule/redis/zset"
	redisv5Client     = redis.NewClient(&redis.Options{Addr: net.JoinHostPort("localhost", "6379")})
	redisv6Client     = redis.NewClient(&redis.Options{Addr: net.JoinHostPort("localhost", "6380")})
	redisv7Client     = redis.NewClient(&redis.Options{Addr: net.JoinHostPort("localhost", "6381")})
)

var redisDataloaders = map[string]*RedisDataloader[any]{
	"Redis/redis:5": NewRedisDataloader[any](sortedSetKeyRedis, redisv5Client),
	"Redis/redis:6": NewRedisDataloader[any](sortedSetKeyRedis, redisv6Client),
	"Redis/redis:7": NewRedisDataloader[any](sortedSetKeyRedis, redisv7Client),
}

func TestRedisDataloader(t *testing.T) {
	for k, d := range redisDataloaders {
		d := d

		t.Run(k, func(t *testing.T) {
			t.Run("Type", func(t *testing.T) {
				assert.Equal(t, "Redis", d.Type())
			})

			t.Run("BuryFor", func(t *testing.T) {
				assert := assert.New(t)
				require := require.New(t)

				randomSeed, err := rand.Int(rand.Reader, big.NewInt(100000))
				require.NoError(err)

				d.sortedSetKey = fmt.Sprintf("test/timecapsule/redis/zset/%d", randomSeed.Int64())

				err = d.BuryFor(context.Background(), "test", time.Minute)
				require.NoError(err)

				defer func() {
					err = d.redisClient.Del(context.Background(), d.sortedSetKey).Err()
					assert.NoError(err)
				}()

				memsCount, err := d.redisClient.ZCount(context.Background(), d.sortedSetKey, "-inf", "+inf").Result()
				require.NoError(err)
				assert.Equal(int64(1), memsCount)

				mems, err := d.redisClient.ZRangeWithScores(context.Background(), d.sortedSetKey, 0, -1).Result()
				require.NoError(err)
				require.Len(mems, 1)

				capsule, err := NewTimeCapsuleFromBase64String[any](mems[0].Member.(string))
				require.NoError(err)

				now := time.Now().UTC()
				assert.GreaterOrEqual(now.Add(time.Minute).UnixMilli(), int64(mems[0].Score))
				assert.Equal("test", capsule.Payload)
			})

			t.Run("BuryUtil", func(t *testing.T) {
				assert := assert.New(t)
				require := require.New(t)

				randomSeed, err := rand.Int(rand.Reader, big.NewInt(100000))
				require.NoError(err)

				d.sortedSetKey = fmt.Sprintf("test/timecapsule/redis/zset/%d", randomSeed.Int64())

				err = d.BuryUtil(context.Background(), "test", time.Now().UTC().Add(time.Hour).UnixMilli())
				require.NoError(err)

				defer func() {
					err = d.redisClient.Del(context.Background(), d.sortedSetKey).Err()
					assert.NoError(err)
				}()

				memsCount, err := d.redisClient.ZCount(context.Background(), d.sortedSetKey, "-inf", "+inf").Result()
				require.NoError(err)
				assert.Equal(int64(1), memsCount)

				mems, err := d.redisClient.ZRangeWithScores(context.Background(), d.sortedSetKey, 0, -1).Result()
				require.NoError(err)
				require.Len(mems, 1)

				capsule, err := NewTimeCapsuleFromBase64String[any](mems[0].Member.(string))
				require.NoError(err)

				now := time.Now().UTC()
				assert.GreaterOrEqual(now.Add(time.Hour).UnixMilli(), int64(mems[0].Score))
				assert.Equal("test", capsule.Payload)
			})

			t.Run("Dig", func(t *testing.T) {
				t.Run("DugOutCorrectCapsule", func(t *testing.T) {
					assert := assert.New(t)
					require := require.New(t)

					randomSeed, err := rand.Int(rand.Reader, big.NewInt(100000))
					require.NoError(err)

					d.sortedSetKey = fmt.Sprintf("test/timecapsule/redis/zset/%d", randomSeed.Int64())

					err = d.BuryUtil(context.Background(), "shouldBeDugOut", time.Now().UTC().Add(-5*time.Millisecond).UnixMilli())
					require.NoError(err)

					err = d.BuryUtil(context.Background(), "shouldNotBeDugOut", time.Now().UTC().Add(5*time.Millisecond).UnixMilli())
					require.NoError(err)

					defer func() {
						err = d.redisClient.Del(context.Background(), d.sortedSetKey).Err()
						assert.NoError(err)
					}()

					capsule, err := d.Dig(context.Background())
					require.NoError(err)
					require.NotNil(capsule)

					now := time.Now().UTC()

					assert.Equal("shouldBeDugOut", capsule.Payload)
					assert.GreaterOrEqual(now.UnixMilli(), capsule.DugOutAt)
				})

				t.Run("DugOutInCorrectOpeningTimeCapsule", func(t *testing.T) {
					assert := assert.New(t)
					require := require.New(t)

					randomSeed, err := rand.Int(rand.Reader, big.NewInt(100000))
					require.NoError(err)

					d.sortedSetKey = fmt.Sprintf("test/timecapsule/redis/zset/%d", randomSeed.Int64())

					err = d.BuryUtil(context.Background(), "shouldNotBeDugOut", time.Now().UTC().Add(5*time.Millisecond).UnixMilli())
					require.NoError(err)

					defer func() {
						err = d.redisClient.Del(context.Background(), d.sortedSetKey).Err()
						assert.NoError(err)
					}()

					dugCapsule, err := d.Dig(context.Background())
					require.NoError(err)
					require.Nil(dugCapsule)

					memsCount, err := d.redisClient.ZCount(context.Background(), d.sortedSetKey, "-inf", "+inf").Result()
					require.NoError(err)
					assert.Equal(int64(1), memsCount)

					mems, err := d.redisClient.ZRangeWithScores(context.Background(), d.sortedSetKey, 0, -1).Result()
					require.NoError(err)
					require.Len(mems, 1)

					requeuedCapsule, err := NewTimeCapsuleFromBase64String[any](mems[0].Member.(string))
					require.NoError(err)

					now := time.Now().UTC()

					assert.Equal("shouldNotBeDugOut", requeuedCapsule.Payload)
					assert.GreaterOrEqual(now.UnixMilli(), requeuedCapsule.DugOutAt)
				})
			})

			t.Run("Destroy", func(t *testing.T) {
				require := require.New(t)

				randomSeed, err := rand.Int(rand.Reader, big.NewInt(100000))
				require.NoError(err)

				d.sortedSetKey = fmt.Sprintf("test/timecapsule/redis/zset/%d", randomSeed.Int64())

				err = d.BuryUtil(context.Background(), "shouldBeDugOut", time.Now().UTC().Add(-5*time.Millisecond).UnixMilli())
				require.NoError(err)

				capsule, err := d.Dig(context.Background())
				require.NoError(err)
				require.NotNil(capsule)

				err = d.Destroy(context.Background(), capsule)
				require.NoError(err)

				mems, err := d.redisClient.ZRangeWithScores(context.Background(), d.sortedSetKey, 0, -1).Result()
				require.NoError(err)
				require.Len(mems, 0)
			})

			t.Run("DestroyAll", func(t *testing.T) {
				assert := assert.New(t)
				require := require.New(t)

				randomSeed, err := rand.Int(rand.Reader, big.NewInt(100000))
				require.NoError(err)

				d.sortedSetKey = fmt.Sprintf("test/timecapsule/redis/zset/%d", randomSeed.Int64())

				err = d.BuryUtil(context.Background(), "shouldBeDugOut", time.Now().UTC().Add(-5*time.Millisecond).UnixMilli())
				require.NoError(err)

				err = d.DestroyAll(context.Background())
				require.NoError(err)

				result, err := d.redisClient.ZCount(context.Background(), d.sortedSetKey, "-inf", "+inf").Result()
				require.NoError(err)
				assert.Zero(result)
			})
		})
	}
}
