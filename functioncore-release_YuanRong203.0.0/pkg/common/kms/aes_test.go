package kms

import (
	"encoding/hex"
	"strings"
	"testing"

	"github.com/smartystreets/goconvey/convey"
)

func Test_encrypt(t *testing.T) {
	data := "hello world"
	convey.Convey("invalid key", t, func() {
		key := "1234"
		decode, _ := hex.DecodeString(key)
		_, err := Encrypt(decode, data)
		convey.So(err.Error(), convey.ShouldContainSubstring, "invalid key size")
	})
	convey.Convey("ok", t, func() {
		key := strings.Repeat("a", 32)
		en, err := Encrypt([]byte(key), data)
		convey.So(err, convey.ShouldBeNil)
		convey.So(en, convey.ShouldEqual, "YWFhYWFhYWFhYWFhVpiS9plXWL08YBZSwpeSHEIUBVQdlePyYPMN")
	})
}

func Test_decrypt(t *testing.T) {
	key := strings.Repeat("a", 32)
	data := "aGVsbG8gd29ybGQK"
	convey.Convey("invalid base64 data", t, func() {
		_, err := Decrypt([]byte(key), "abc")
		convey.So(err.Error(), convey.ShouldContainSubstring, "0")
	})
	convey.Convey("hex decode error", t, func() {
		_, err := Decrypt([]byte("123"), data)
		convey.So(err.Error(), convey.ShouldContainSubstring, "3")
	})
	convey.Convey("invalid key", t, func() {
		_, err := Decrypt([]byte("1234"), data)
		convey.So(err.Error(), convey.ShouldContainSubstring, "invalid key size")
	})
	convey.Convey("ok", t, func() {
		d := "hello world"
		en, err := Encrypt([]byte(key), d)
		convey.So(err, convey.ShouldBeNil)
		s, err := Decrypt([]byte(key), en)
		convey.So(err, convey.ShouldBeNil)
		convey.So(s, convey.ShouldEqual, d)
	})
}
