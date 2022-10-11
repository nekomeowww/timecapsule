package timecapsule

import (
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewTimeCapsuleFromBase64String(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	capsule := TimeCapsule[string]{
		Payload:  "hello",
		BuriedAt: time.Now().UnixMilli(),
		DugOutAt: time.Now().UnixMilli(),
	}

	jsonData, err := json.Marshal(capsule)
	require.NoError(err)

	base64Str := base64.StdEncoding.EncodeToString(jsonData)
	capsule.base64Str = base64Str

	decodedCapsule, err := NewTimeCapsuleFromBase64String[string](base64Str)
	require.NoError(err)

	assert.Equal(capsule, *decodedCapsule)
}

func TestBase64String(t *testing.T) {

}
