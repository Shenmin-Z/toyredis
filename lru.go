/*
Based on https://github.com/golang/groupcache/blob/master/lru/lru.go
*/

package toyredis

import (
	"container/list"
	"errors"
	"math/rand"
	"sync"
	"time"
)

const (
	KB          = 1 << 10
	MB          = 1 << 20
	GB          = 1 << 30
	Millisecond = 1
	Second      = 1000
	Minute      = 60000
)

type Cache struct {
	sizeLimit int
	size      int

	ll    *list.List
	cache map[string]*list.Element
	array []*list.Element

	mu   sync.Mutex
	quit chan interface{}
}

type entry struct {
	key    string
	value  interface{} // []byte or map[string][]byte
	expire time.Time

	pos int
}

func (kv *entry) hasExpired() bool {
	if kv.expire == nilTime {
		return false
	}
	return kv.expire.Before(time.Now())
}

func NewCache(sizeLimit int) *Cache {
	if sizeLimit <= 0 {
		panic("Size limit should be greater than 0.")
	}

	cache := &Cache{
		sizeLimit: sizeLimit,
		size:      0,
		ll:        list.New(),
		cache:     make(map[string]*list.Element),
		array:     make([]*list.Element, 0),
		quit:      make(chan interface{}),
	}

	ticker := time.NewTicker(100 * time.Millisecond)
	go func() {
		for {
			select {
			case <-cache.quit:
				ticker.Stop()
				return
			case <-ticker.C:
				cache.gc()
			}
		}
	}()

	return cache
}

var nilTime = time.Time{}

var (
	nilValue  = errors.New("nil value not allowed")
	wrongType = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
)

func (c *Cache) Set(key string, value []byte) (err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if value == nil {
		panic(nilValue)
	}

	if ee, ok := c.cache[key]; ok {
		switch oldValue := ee.Value.(*entry).value.(type) {
		case []byte:
			c.size += len(value)
			c.size -= len(oldValue)
			c.ll.MoveToFront(ee)
			ee.Value.(*entry).value = value
			ee.Value.(*entry).expire = nilTime
		case map[string][]byte:
			return wrongType
		}
	} else {
		c.size += len(key) + len(value)
		ele := c.ll.PushFront(&entry{key, value, nilTime, len(c.array)})
		c.cache[key] = ele
		c.array = append(c.array, ele)
	}

	for c.sizeLimit != 0 && c.size > c.sizeLimit {
		c.removeOldest()
	}
	return
}

func (c *Cache) HSet(key string, vk string, vv []byte) (err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if vv == nil {
		panic(nilValue)
	}

	if ee, ok := c.cache[key]; ok {
		switch oldValue := ee.Value.(*entry).value.(type) {
		case []byte:
			return wrongType
		case map[string][]byte:
			if oldVv, ok := oldValue[vk]; ok {
				c.size += len(vv)
				c.size -= len(oldVv)
			} else {
				c.size += len(vk) + len(vv)
			}
			oldValue[vk] = vv
			c.ll.MoveToFront(ee)
			ee.Value.(*entry).expire = nilTime
		}
	} else {
		c.size += len(vk) + len(vv)
		ele := c.ll.PushFront(&entry{key, map[string][]byte{vk: vv}, nilTime, len(c.array)})
		c.cache[key] = ele
		c.array = append(c.array, ele)
	}

	for c.sizeLimit != 0 && c.size > c.sizeLimit {
		c.removeOldest()
	}
	return
}

// ttl: milliseconds
func (c *Cache) Expire(key string, ttl int) int {
	c.mu.Lock()
	defer c.mu.Unlock()

	if ee, ok := c.cache[key]; ok {
		expireAt := nilTime
		if ttl > 0 {
			expireAt = time.Now().Add(time.Millisecond * time.Duration(ttl))
		}
		ee.Value.(*entry).expire = expireAt
		return 1
	} else {
		return 0
	}
}

func (c *Cache) Exists(key string) int {
	c.mu.Lock()
	defer c.mu.Unlock()

	if ele, hit := c.cache[key]; hit {
		kv := ele.Value.(*entry)
		if kv.hasExpired() {
			c.removeElement(ele)
			return 0
		}
		return 1
	}
	return 0
}

func (c *Cache) Get(key string) (value []byte, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if ele, hit := c.cache[key]; hit {
		kv := ele.Value.(*entry)
		if kv.hasExpired() {
			c.removeElement(ele)
			return
		}
		switch v := kv.value.(type) {
		case []byte:
			c.ll.MoveToFront(ele)
			return v, nil
		default:
			return nil, wrongType
		}
	}
	return
}

func (c *Cache) HGet(key string, vk string) (value []byte, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if ele, hit := c.cache[key]; hit {
		kv := ele.Value.(*entry)
		if kv.hasExpired() {
			c.removeElement(ele)
			return
		}
		switch v := kv.value.(type) {
		case map[string][]byte:
			c.ll.MoveToFront(ele)
			vv := v[vk]
			return vv, nil
		default:
			return nil, wrongType
		}
	}
	return
}

func (c *Cache) HGetAll(key string) (value [][]byte, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if ele, hit := c.cache[key]; hit {
		kv := ele.Value.(*entry)
		if kv.hasExpired() {
			c.removeElement(ele)
			return
		}
		switch v := kv.value.(type) {
		case map[string][]byte:
			c.ll.MoveToFront(ele)
			list := make([][]byte, len(v)*2)
			idx := 0
			for vk, vv := range v {
				list[idx] = []byte(vk)
				list[idx+1] = vv
				idx += 2
			}
			return list, nil
		default:
			return nil, wrongType
		}
	}
	return
}

func (c *Cache) HDel(key string, kk []string) (num int, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if ele, hit := c.cache[key]; hit {
		kv := ele.Value.(*entry)
		if kv.hasExpired() {
			c.removeElement(ele)
			return
		}
		switch v := kv.value.(type) {
		case map[string][]byte:
			for _, k := range kk {
				if _, ok := v[k]; ok {
					num++
					delete(v, k)
				}
			}
			return
		default:
			return 0, wrongType
		}
	}
	return
}

func (c *Cache) HExists(key, k string) (num int, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if ele, hit := c.cache[key]; hit {
		kv := ele.Value.(*entry)
		if kv.hasExpired() {
			c.removeElement(ele)
			return
		}
		switch v := kv.value.(type) {
		case map[string][]byte:
			if _, ok := v[k]; ok {
				return 1, nil
			}
		default:
			return 0, wrongType
		}
	}
	return
}

func (c *Cache) Remove(key []string) (num int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, k := range key {
		if ele, hit := c.cache[k]; hit {
			c.removeElement(ele)
			num++
		}
	}
	return
}

func (c *Cache) removeOldest() {
	ele := c.ll.Back()
	if ele != nil {
		c.removeElement(ele)
		return
	}
}

func (c *Cache) removeElement(e *list.Element) {
	c.ll.Remove(e)
	kv := e.Value.(*entry)
	delete(c.cache, kv.key)
	c.size -= len(kv.key)
	switch v := kv.value.(type) {
	case []byte:
		c.size -= len(v)
	case map[string][]byte:
		for vk, vv := range v {
			c.size -= len(vk) + len(vv)
		}
	}
	// move the last one to the deleted position
	// update position
	// shrink size by 1
	c.array[kv.pos] = c.array[len(c.array)-1]
	c.array[len(c.array)-1].Value.(*entry).pos = kv.pos
	c.array = c.array[:len(c.array)-1]
}

func (c *Cache) Flush() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.size = 0
	c.ll = list.New()
	c.cache = make(map[string]*list.Element)
	c.array = make([]*list.Element, 0)
}

func (c *Cache) Stop() {
	close(c.quit)
}

func (c *Cache) GetSize() int {
	return c.size
}

func (c *Cache) GetSizeLimit() int {
	return c.sizeLimit
}

func (c *Cache) SetSizeLimit(sizeLimit int) {
	c.sizeLimit = sizeLimit
}

// test 20 random entries and delete those have expired
// if more than 25% were expired, repeat
func (c *Cache) gc() {
	c.mu.Lock()

	total := 0
	expired := make([]*list.Element, 0, 20)
	rand.Seed(time.Now().UnixNano())
	p := rand.Perm(len(c.array))
	for i, x := range p {
		if i >= 20 {
			break
		}
		if kv := c.array[x].Value.(*entry); kv.hasExpired() {
			expired = append(expired, c.array[x])
		}
		total++
	}
	for _, i := range expired {
		c.removeElement(i)
	}

	c.mu.Unlock()

	if len(expired)*4 > total {
		c.gc()
	}
}
