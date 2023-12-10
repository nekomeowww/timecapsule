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
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	sortedSetKeyRueidis = "test/timecapsule/rueidis/zset"
	rueidisv5Client     = lo.Must(rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{net.JoinHostPort("localhost", "6379")}, DisableCache: true}))
	rueidisv6Client     = lo.Must(rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{net.JoinHostPort("localhost", "6380")}}))
	rueidisv7Client     = lo.Must(rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{net.JoinHostPort("localhost", "6381")}}))
)

var rueidisDataloaders = map[string]*RueidisDataloader[any]{
	"Rueidis/redis:5": NewRueidisDataloader[any](sortedSetKeyRueidis, rueidisv5Client),
	"Rueidis/redis:6": NewRueidisDataloader[any](sortedSetKeyRueidis, rueidisv6Client),
	"Rueidis/redis:7": NewRueidisDataloader[any](sortedSetKeyRueidis, rueidisv7Client),
}

func TestRueidisDataloder(t *testing.T) {
	for k, d := range rueidisDataloaders {
		d := d

		t.Run(k, func(t *testing.T) {
			t.Run("Type", func(t *testing.T) {
				assert.Equal(t, "Rueidis", d.Type())
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
					err = d.rueidisClient.Do(context.Background(), d.rueidisClient.B().Del().Key(d.sortedSetKey).Build()).Error()
					assert.NoError(err)
				}()

				zcountCmd := d.rueidisClient.B().Zcount().Key(d.sortedSetKey).Min("-inf").Max("+inf").Build()
				resp := d.rueidisClient.Do(context.Background(), zcountCmd)
				require.NoError(resp.Error())

				memsCount, err := resp.AsInt64()
				require.NoError(err)
				assert.Equal(int64(1), memsCount)

				zrangeWithScoresCmd := d.rueidisClient.B().Zrange().Key(d.sortedSetKey).Min("0").Max("-1").Withscores().Build()
				resp = d.rueidisClient.Do(context.Background(), zrangeWithScoresCmd)
				require.NoError(resp.Error())

				mems, err := resp.AsZScores()
				require.NoError(err)
				require.Len(mems, 1)

				capsule, err := NewTimeCapsuleFromBase64String[any](mems[0].Member)
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
					err = d.rueidisClient.Do(context.Background(), d.rueidisClient.B().Del().Key(d.sortedSetKey).Build()).Error()
					assert.NoError(err)
				}()

				zcountCmd := d.rueidisClient.B().Zcount().Key(d.sortedSetKey).Min("-inf").Max("+inf").Build()
				resp := d.rueidisClient.Do(context.Background(), zcountCmd)
				require.NoError(resp.Error())

				memsCount, err := resp.AsInt64()
				require.NoError(err)
				assert.Equal(int64(1), memsCount)

				zrangeWithScoresCmd := d.rueidisClient.B().Zrange().Key(d.sortedSetKey).Min("0").Max("-1").Withscores().Build()
				resp = d.rueidisClient.Do(context.Background(), zrangeWithScoresCmd)
				require.NoError(resp.Error())

				mems, err := resp.AsZScores()
				require.NoError(err)
				require.Len(mems, 1)

				capsule, err := NewTimeCapsuleFromBase64String[any](mems[0].Member)
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
						err = d.rueidisClient.Do(context.Background(), d.rueidisClient.B().Del().Key(d.sortedSetKey).Build()).Error()
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

					err = d.BuryUtil(context.Background(), "shouldNotBeDugOut", time.Now().UTC().Add(100*time.Millisecond).UnixMilli())
					require.NoError(err)
					defer func() {
						err = d.rueidisClient.Do(context.Background(), d.rueidisClient.B().Del().Key(d.sortedSetKey).Build()).Error()
						assert.NoError(err)
					}()

					dugCapsule, err := d.Dig(context.Background())
					require.NoError(err)
					require.Nil(dugCapsule)

					zcountCmd := d.rueidisClient.B().Zcount().Key(d.sortedSetKey).Min("-inf").Max("+inf").Build()
					resp := d.rueidisClient.Do(context.Background(), zcountCmd)
					require.NoError(resp.Error())

					memsCount, err := resp.AsInt64()
					require.NoError(err)
					assert.Equal(int64(1), memsCount)

					zrangeWithScoresCmd := d.rueidisClient.B().Zrange().Key(d.sortedSetKey).Min("0").Max("-1").Withscores().Build()
					resp = d.rueidisClient.Do(context.Background(), zrangeWithScoresCmd)
					require.NoError(resp.Error())

					mems, err := resp.AsZScores()
					require.NoError(err)
					require.Len(mems, 1)

					requeuedCapsule, err := NewTimeCapsuleFromBase64String[any](mems[0].Member)
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

				zrangewithscoresCmd := d.rueidisClient.B().Zrange().Key(d.sortedSetKey).Min("0").Max("-1").Withscores().Build()
				resp := d.rueidisClient.Do(context.Background(), zrangewithscoresCmd)
				require.NoError(resp.Error())

				mems, err := resp.AsZScores()
				require.NoError(err)
				require.Len(mems, 0)
			})

			t.Run("DestroyAll", func(t *testing.T) {
				assert := assert.New(t)
				require := require.New(t)

				randomSeed, err := rand.Int(rand.Reader, big.NewInt(100000))
				require.NoError(err)

				d.sortedSetKey = fmt.Sprintf("test/timecapsule/redis/zset/%d", randomSeed.Int64())

				err = d.BuryUtil(context.Background(), "shouldNotBeDugOut", time.Now().UTC().Add(5*time.Millisecond).UnixMilli())
				require.NoError(err)
				defer func() {
					err = d.rueidisClient.Do(context.Background(), d.rueidisClient.B().Del().Key(d.sortedSetKey).Build()).Error()
					assert.NoError(err)
				}()

				err = d.DestroyAll(context.Background())
				require.NoError(err)

				zcountCmd := d.rueidisClient.B().Zcount().Key(d.sortedSetKey).Min("-inf").Max("+inf").Build()
				resp := d.rueidisClient.Do(context.Background(), zcountCmd)
				require.NoError(resp.Error())

				memsCount, err := resp.AsInt64()
				require.NoError(err)
				assert.Zero(memsCount)
			})
		})
	}
}
