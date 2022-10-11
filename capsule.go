package timecapsule

import (
	"encoding/base64"
	"encoding/json"
)

type TimeCapsule[P any] struct {
	Payload  P     `json:"payload"`
	BuriedAt int64 `json:"buriedAt"`
	DugOutAt int64 `json:"dugOutAt"`

	base64Str string
}

func NewTimeCapsuleFromBase64String[P any](base64Str string) (*TimeCapsule[P], error) {
	decodedData, err := base64.StdEncoding.DecodeString(base64Str)
	if err != nil {
		return nil, err
	}

	var capsule TimeCapsule[P]
	err = json.Unmarshal(decodedData, &capsule)
	if err != nil {
		return nil, err
	}

	capsule.base64Str = base64Str
	return &capsule, nil
}

func (c *TimeCapsule[any]) Base64String() string {
	if c.base64Str != "" {
		return c.base64Str
	}

	encodedData, _ := json.Marshal(c)
	c.base64Str = base64.StdEncoding.EncodeToString(encodedData)
	return c.base64Str
}
