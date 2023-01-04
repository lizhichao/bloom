package bloom

import (
	"crypto/rand"
	"encoding/hex"
	"strconv"
	"testing"
	"time"

	goredis "github.com/go-redis/redis"
)

func getRedisClient() *goredis.Client {
	return goredis.NewClient(&goredis.Options{
		Addr:        "192.168.116.130:6379",
		Password:    "",
		DB:          0,
		ReadTimeout: 1 * time.Second,
	})
}

func getRandStr(len int) string {
	var bt = make([]byte, len)
	n, err := rand.Read(bt)
	if n != len || err != nil {
		return ""
	}
	return hex.EncodeToString(bt[:])
}

func TestNew(t *testing.T) {
	l := 1000
	p := 0.01

	b := New("boo", uint(l), p, getRedisClient())

	ks := make([]string, l)
	for i := 0; i < l; i++ {
		v := getRandStr(16)
		if err := b.Set([]byte(v)); err != nil {
			t.Error(err)
			break
		}
		ks[i] = v
	}

	t.Log("布隆过滤器 不存在 检查 100%")
	for _, v := range ks {

		a, err := b.Exists([]byte(v))
		if err != nil {
			t.Error(err)
			break
		}

		if !a {
			t.Error("检查错误", v)
			break
		}
	}

	c := 0
	for i, v := range ks {
		a, err := b.Exists([]byte(v + strconv.Itoa(i)))
		if err != nil {
			t.Error(err)
			break
		}
		if a {
			c++
		}
	}

	p1 := float64(c) / float64(len(ks))

	t.Log("布隆过滤器 存在 检查 误差概率 : ", c, "/", len(ks), "=", p1)

	if p1 >= 2*p {
		t.Log("误差偏大")
	}

	b.Del()
}
