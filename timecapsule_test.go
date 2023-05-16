package timecapsule

import (
	"net"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/redis/rueidis"
	"github.com/samber/lo"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
)

var (
	sortedSetKeyRedis = "test/timecapsule/redis/zset"
	redisv5Client     = redis.NewClient(&redis.Options{Addr: net.JoinHostPort("localhost", "6379")})
	redisv6Client     = redis.NewClient(&redis.Options{Addr: net.JoinHostPort("localhost", "6380")})
	redisv7Client     = redis.NewClient(&redis.Options{Addr: net.JoinHostPort("localhost", "6381")})
)

var (
	sortedSetKeyRueidis = "test/timecapsule/rueidis/zset"
	rueidisv5Client     = lo.Must(rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{net.JoinHostPort("localhost", "6379")}, DisableCache: true}))
	rueidisv6Client     = lo.Must(rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{net.JoinHostPort("localhost", "6380")}}))
	rueidisv7Client     = lo.Must(rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{net.JoinHostPort("localhost", "6381")}}))
)

var dataloders = map[string]Dataloader[any]{
	"Redis/redis:5":   NewRedisDataloader[any](sortedSetKeyRedis, redisv5Client),
	"Redis/redis:6":   NewRedisDataloader[any](sortedSetKeyRedis, redisv6Client),
	"Redis/redis:7":   NewRedisDataloader[any](sortedSetKeyRedis, redisv7Client),
	"Rueidis/redis:5": NewRueidisDataloader[any](sortedSetKeyRueidis, rueidisv5Client),
	"Rueidis/redis:6": NewRueidisDataloader[any](sortedSetKeyRueidis, rueidisv6Client),
	"Rueidis/redis:7": NewRueidisDataloader[any](sortedSetKeyRueidis, rueidisv7Client),
}

func TestMain(m *testing.M) {
	lo.Must0(redisv5Client.Ping(context.Background()).Err())
	lo.Must0(redisv6Client.Ping(context.Background()).Err())
	lo.Must0(redisv7Client.Ping(context.Background()).Err())

	lo.Must0(rueidisv5Client.Do(context.Background(), rueidisv5Client.B().Ping().Build()).Error())
	lo.Must0(rueidisv6Client.Do(context.Background(), rueidisv6Client.B().Ping().Build()).Error())
	lo.Must0(rueidisv7Client.Do(context.Background(), rueidisv7Client.B().Ping().Build()).Error())

	os.Exit(m.Run())
}

func cleanupKey(t *testing.T, dataloder Dataloader[any]) {
	redisDataloader, ok := dataloder.(*RedisDataloader[any])
	if ok {
		err := redisDataloader.redisClient.Del(context.Background(), redisDataloader.sortedSetKey).Err()
		assert.NoError(t, err)
	}

	rueidisDataloader, ok := dataloder.(*RueidisDataloader[any])
	if ok {
		err := rueidisDataloader.rueidisClient.Do(context.Background(), rueidisDataloader.rueidisClient.B().Del().Key(rueidisDataloader.sortedSetKey).Build()).Error()
		assert.NoError(t, err)
	}
}

func TestTimeCapsule(t *testing.T) {
	for k, d := range dataloders {
		d := d

		t.Run(k, func(t *testing.T) {
			t.Run("NewDigger", func(t *testing.T) {
				assert := assert.New(t)
				require := require.New(t)

				digger := NewDigger(d, time.Second)
				require.NotNil(digger)
				assert.NotNil(digger.dataloader)
			})

			t.Run("NewDiggerWithOptions", func(t *testing.T) {
				assert := assert.New(t)
				require := require.New(t)

				digger := NewDigger(d, time.Second, TimeCapsuleDiggerOption{
					RetryLimit:    10,
					RetryInterval: 250 * time.Millisecond,
					Logger:        logrus.New(),
				})

				require.NotNil(digger)
				assert.NotNil(digger.dataloader)
				assert.Equal(10, digger.option.RetryLimit)
				assert.Equal(250*time.Millisecond, digger.option.RetryInterval)
			})

			t.Run("SetHandler", func(t *testing.T) {
				assert := assert.New(t)
				require := require.New(t)

				digger := NewDigger(d, 250*time.Millisecond)
				require.NotNil(digger)

				var handlerProceeded bool

				digger.SetHandler(func(digger *TimeCapsuleDigger[any], capsule *TimeCapsule[any]) {
					assert.Equal("hello", capsule.Payload)
					handlerProceeded = true
				})

				go digger.Start()
				defer digger.Stop()

				err := d.BuryFor(context.Background(), "hello", time.Second)
				assert.NoError(err)

				defer cleanupKey(t, d)

				time.Sleep(2 * time.Second)

				assert.True(handlerProceeded)
			})

			t.Run("Start", func(t *testing.T) {
				assert := assert.New(t)
				require := require.New(t)

				var mutex sync.Mutex
				handlerProceeded := make(map[string]int)

				diggerCloseFuncs := make([]func(), 0)

				for i := 0; i < 10; i++ {
					digger := NewDigger(d, 5*time.Millisecond, TimeCapsuleDiggerOption{Logger: logrus.New()})
					require.NotNil(digger)

					digger.SetHandler(func(digger *TimeCapsuleDigger[any], capsule *TimeCapsule[any]) {
						mutex.Lock()
						defer mutex.Unlock()

						payload, ok := capsule.Payload.(string)
						assert.True(ok)

						handlerProceeded[payload] = handlerProceeded[payload] + 1
					})

					go digger.Start()
					diggerCloseFuncs = append(diggerCloseFuncs, digger.Stop)
				}

				defer func() {
					for _, closeFun := range diggerCloseFuncs {
						closeFun()
					}
				}()

				var waitGroup sync.WaitGroup
				for i := 0; i < 1000; i++ {
					waitGroup.Add(1)
					iteration := strconv.FormatInt(int64(i), 10)

					go func() {
						err := d.BuryFor(context.Background(), iteration, time.Second)
						assert.NoError(err)
						waitGroup.Done()
					}()
				}

				defer cleanupKey(t, d)

				waitGroup.Wait()
				time.Sleep(15 * time.Second)
				require.Equal(1000, len(handlerProceeded))

				for _, h := range handlerProceeded {
					assert.Equal(1, h)
				}
			})

			t.Run("BuryFor", func(t *testing.T) {
				assert := assert.New(t)
				require := require.New(t)

				digger := NewDigger(d, 250*time.Millisecond)
				require.NotNil(digger)

				var handlerProceeded bool

				digger.SetHandler(func(digger *TimeCapsuleDigger[any], capsule *TimeCapsule[any]) {
					assert.Equal("hello", capsule.Payload)
					handlerProceeded = true
				})

				go digger.Start()
				defer digger.Stop()

				err := digger.BuryFor(context.Background(), "hello", time.Second)
				assert.NoError(err)

				defer cleanupKey(t, d)

				time.Sleep(2 * time.Second)

				assert.True(handlerProceeded)
			})

			t.Run("BuryUntil", func(t *testing.T) {
				assert := assert.New(t)
				require := require.New(t)

				digger := NewDigger(d, 250*time.Millisecond)
				require.NotNil(digger)

				var handlerProceeded bool

				digger.SetHandler(func(digger *TimeCapsuleDigger[any], capsule *TimeCapsule[any]) {
					assert.Equal("hello", capsule.Payload)
					handlerProceeded = true
				})

				go digger.Start()
				defer digger.Stop()

				err := digger.BuryUtil(context.Background(), "hello", time.Now().UTC().Add(time.Second).UnixMilli())
				assert.NoError(err)

				defer cleanupKey(t, d)

				time.Sleep(2 * time.Second)

				assert.True(handlerProceeded)
			})
		})
	}
}
