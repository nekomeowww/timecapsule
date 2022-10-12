package timecapsule

import (
	"log"
	"net"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/go-redis/redis"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var sortedSetKey = "test/timecapsule/redis/zset"
var redisClient = redis.NewClient(&redis.Options{Addr: net.JoinHostPort("localhost", "6379")})

func TestMain(m *testing.M) {
	err := redisClient.Ping().Err()
	if err != nil {
		log.Fatal(err)
	}

	os.Exit(m.Run())
}

func TestNewDigger(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	digger := NewDigger[any](NewRedisDataloader[any](sortedSetKey, redisClient), time.Second)
	require.NotNil(digger)
	assert.NotNil(digger.dataloader)
}

func TestNewDiggerWithOptions(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	digger := NewDigger[any](NewRedisDataloader[any](sortedSetKey, redisClient), time.Second, TimeCapsuleDiggerOption{
		RetryLimit:    10,
		RetryInterval: 250 * time.Millisecond,
		Logger:        logrus.New(),
	})

	require.NotNil(digger)
	assert.NotNil(digger.dataloader)
	assert.Equal(10, digger.option.RetryLimit)
	assert.Equal(250*time.Millisecond, digger.option.RetryInterval)
}

func TestSetHandler(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	redisDataloder := NewRedisDataloader[any](sortedSetKey, redisClient)

	digger := NewDigger[any](redisDataloder, 250*time.Millisecond)
	require.NotNil(digger)

	var handlerProceesed bool
	digger.SetHandler(func(digger *TimeCapsuleDigger[any], capsule *TimeCapsule[any]) {
		assert.Equal("hello", capsule.Payload)
		handlerProceesed = true
	})

	go digger.Start()
	defer digger.Stop()

	err := redisDataloder.BuryFor("hello", time.Second)
	assert.NoError(err)
	time.Sleep(2 * time.Second)

	assert.True(handlerProceesed)
}

func TestStart(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	redisDataloder := NewRedisDataloader[string](sortedSetKey, redisClient)

	var mutex sync.Mutex
	handlerProceesed := make(map[string]int)

	diggerCloseFuns := make([]func(), 0)
	for i := 0; i < 10; i++ {
		digger := NewDigger[string](redisDataloder, 5*time.Millisecond, TimeCapsuleDiggerOption{Logger: logrus.New()})
		require.NotNil(digger)

		digger.SetHandler(func(digger *TimeCapsuleDigger[string], capsule *TimeCapsule[string]) {
			mutex.Lock()
			defer mutex.Unlock()
			handlerProceesed[capsule.Payload] = handlerProceesed[capsule.Payload] + 1
		})

		go digger.Start()
		diggerCloseFuns = append(diggerCloseFuns, digger.Stop)
	}
	defer func() {
		for _, closeFun := range diggerCloseFuns {
			closeFun()
		}
	}()

	var waitGroup sync.WaitGroup
	for i := 0; i < 1000; i++ {
		waitGroup.Add(1)
		iteration := strconv.FormatInt(int64(i), 10)
		go func() {
			err := redisDataloder.BuryFor(iteration, time.Second)
			assert.NoError(err)
			waitGroup.Done()
		}()
	}

	waitGroup.Wait()
	time.Sleep(30 * time.Second)
	require.Equal(1000, len(handlerProceesed))
	for _, h := range handlerProceesed {
		assert.Equal(1, h)
	}
}

func TestBuryFor(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	redisDataloder := NewRedisDataloader[any](sortedSetKey, redisClient)

	digger := NewDigger[any](redisDataloder, 250*time.Millisecond)
	require.NotNil(digger)

	var handlerProceesed bool
	digger.SetHandler(func(digger *TimeCapsuleDigger[any], capsule *TimeCapsule[any]) {
		assert.Equal("hello", capsule.Payload)
		handlerProceesed = true
	})

	go digger.Start()
	defer digger.Stop()

	err := digger.BuryFor("hello", time.Second)
	assert.NoError(err)
	time.Sleep(2 * time.Second)

	assert.True(handlerProceesed)
}

func TestBuryUntil(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	redisDataloder := NewRedisDataloader[any](sortedSetKey, redisClient)

	digger := NewDigger[any](redisDataloder, 250*time.Millisecond)
	require.NotNil(digger)

	var handlerProceesed bool
	digger.SetHandler(func(digger *TimeCapsuleDigger[any], capsule *TimeCapsule[any]) {
		assert.Equal("hello", capsule.Payload)
		handlerProceesed = true
	})

	go digger.Start()
	defer digger.Stop()

	err := digger.BuryUtil("hello", time.Now().UTC().Add(time.Second).UnixMilli())
	assert.NoError(err)
	time.Sleep(2 * time.Second)

	assert.True(handlerProceesed)
}
