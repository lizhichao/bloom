package bloom

import (
	"github.com/go-redis/redis"
	"github.com/spaolacci/murmur3"
	"math"
	"strconv"
	"time"
)

// 布隆过滤器

const (
	setScript = `
for _, offset in ipairs(ARGV) do
	redis.call("setbit", KEYS[1], offset, 1)
end
`
	testScript = `
for _, offset in ipairs(ARGV) do
	if tonumber(redis.call("getbit", KEYS[1], offset)) == 0 then
		return false
	end
end
return true
`
)

type Filter struct {
	key   string // redis key
	bits  uint   // 空间
	c     uint8  // 迭代次数
	redis *redis.Client
}

// New redis key , 预计总数， 重复概率
func New(key string, count uint, p float64, redis *redis.Client) *Filter {
	bt := getBit(count, p)
	return &Filter{
		key:   key,
		bits:  bt,
		c:     getCount(bt, count),
		redis: redis,
	}
}

func getBit(n uint, p float64) uint {
	return uint(-(float64(n) * math.Log(p)) / (math.Log(2) * math.Log(2)))
}

func getCount(m, n uint) uint8 {
	return uint8(math.Round(math.Log(2) * float64(m) / float64(n)))
}

// Set 设置
func (f *Filter) Set(data []byte) error {
	_, err := f.redis.Eval(setScript, []string{f.key}, f.getLocations(data)).Result()
	if err == redis.Nil {
		return nil
	}
	return err
}

// Exists 检查是否存在
func (f *Filter) Exists(data []byte) (bool, error) {
	locations := f.getLocations(data)
	return f.check(locations)
}

func (f *Filter) getLocations(data []byte) []string {
	locations := make([]string, f.c)
	for i := uint8(0); i < f.c; i++ {
		hashValue := murmur3.Sum64(append(data, i))
		locations[i] = strconv.FormatUint(hashValue%uint64(f.bits), 10)
	}
	return locations
}

func (f *Filter) check(offsets []string) (bool, error) {
	resp, err := f.redis.Eval(testScript, []string{f.key}, offsets).Result()
	if err == redis.Nil {
		return false, nil
	} else if err != nil {
		return false, err
	}
	exists, ok := resp.(int64)
	if !ok {
		return false, nil
	}
	return exists == 1, nil
}

func (f *Filter) Del() error {
	return f.redis.Del(f.key).Err()
}

func (f *Filter) Expire(seconds int) error {
	return f.redis.Expire(f.key, time.Duration(seconds)*time.Second).Err()
}
