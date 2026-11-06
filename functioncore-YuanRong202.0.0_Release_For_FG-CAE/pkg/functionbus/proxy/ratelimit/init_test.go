package ratelimit

import (
	"os"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
)

func TestInitLimitConfig(t *testing.T) {
	convey.Convey("Test init without system configuration", t, func() {
		patch := gomonkey.ApplyFunc(os.Getenv, func(key string) string {
			return ""
		})
		defer patch.Reset()
		InitLimitConfig()
		assert.Equal(t, 1000, traceIDBucketSize)
		assert.Equal(t, float64(1), traceIDFrequency)
	})
	convey.Convey("Test init with system configuration", t, func() {
		patch := gomonkey.ApplyFunc(os.Getenv, func(key string) string {
			if key == "INVOKE_BUCKET_SIZE" {
				return "2000"
			}
			if key == "INVOKE_RATE" {
				return "1.7389"
			}
			return ""
		})
		defer patch.Reset()
		InitLimitConfig()
		assert.Equal(t, 2000, traceIDBucketSize)
		assert.Equal(t, 1.7389, traceIDFrequency)
	})

	convey.Convey("Test init with wrong configuration", t, func() {
		patch := gomonkey.ApplyFunc(os.Getenv, func(key string) string {
			if key == "INVOKE_BUCKET_SIZE" {
				return "xxx"
			}
			if key == "INVOKE_RATE" {
				return "yy"
			}
			return ""
		})
		defer patch.Reset()
		InitLimitConfig()
		assert.Equal(t, 1000, traceIDBucketSize)
		assert.Equal(t, float64(1), traceIDFrequency)
	})

	convey.Convey("Test init with negative configuration", t, func() {
		patch := gomonkey.ApplyFunc(os.Getenv, func(key string) string {
			if key == "INVOKE_BUCKET_SIZE" {
				return "-2"
			}
			if key == "INVOKE_RATE" {
				return "-1"
			}
			return ""
		})
		defer patch.Reset()
		InitLimitConfig()
		assert.Equal(t, 1000, traceIDBucketSize)
		assert.Equal(t, float64(1), traceIDFrequency)
	})
}
