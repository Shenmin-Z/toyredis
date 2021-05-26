package toyredis

import (
	"fmt"
	"testing"
	"time"
)

func TestGet(t *testing.T) {
	lru := NewCache(MB)

	lru.Set("aaa", []byte("1234"))
	val, _ := lru.Get("aaa")
	if string(val) != "1234" {
		t.Fatal("Set failed.")
	}

	val, _ = lru.Get("bbb")
	if val != nil {
		t.Fatal("Set failed.")
	}

	err := lru.HSet("aaa", "1", []byte("11"))
	if err == nil {
		t.Fatal("HSet failed.")
	}
	lru.HSet("bbb", "1", []byte("11"))
	val, _ = lru.HGet("bbb", "1")
	if string(val) != "11" {
		t.Fatal("HSet failed.")
	}
	val, err = lru.HGet("bbb", "2")
	if val != nil || err != nil {
		t.Fatal("HSet failed.")
	}
	lru.HSet("bbb", "1", []byte("101"))
	val, _ = lru.HGet("bbb", "1")
	if string(val) != "101" {
		t.Fatal("HSet failed.")
	}
	lru.HSet("bbb", "2", []byte("22"))
	val, _ = lru.HGet("bbb", "1")
	if string(val) != "101" {
		t.Fatal("HSet failed.")
	}
	val, _ = lru.HGet("bbb", "2")
	if string(val) != "22" {
		t.Fatal("HSet failed.")
	}

	lru.Stop()
}

func TestRemove(t *testing.T) {
	lru := NewCache(10)
	lru.Set("myKey", []byte("1234"))
	if val, _ := lru.Get("myKey"); val == nil {
		t.Fatal("TestRemove returned no match")
	} else if string(val) != "1234" {
		t.Fatalf("TestRemove failed.  Expected %d, got %v", 1234, val)
	}

	lru.Remove([]string{"myKey"})
	if val, _ := lru.Get("myKey"); val != nil {
		t.Fatal("TestRemove returned a removed entry")
	}
	lru.Stop()
}

func TestEvict(t *testing.T) {
	lru := NewCache(50)
	for i := 0; i < 10; i++ {
		lru.Set(fmt.Sprintf("%d", i), []byte("123456789"))
	}

	for i := 0; i < 5; i++ {
		if val, _ := lru.Get(fmt.Sprintf("%d", i)); val != nil {
			t.Fatalf("%d should have been evicted.", i)
		}
	}

	for i := 5; i < 10; i++ {
		if val, _ := lru.Get(fmt.Sprintf("%d", i)); val == nil {
			t.Fatalf("%d should be present.", i)
		}
	}
}

func getKey(i int) string {
	return fmt.Sprintf("myKey-%d", i)
}

func TestExpire(t *testing.T) {
	lru := NewCache(1 << 20)
	for i := 0; i < 200; i++ {
		expire := 100 * i
		if i > 100 {
			expire = 100
		}
		lru.Set(getKey(i), []byte{})
		lru.Expire(getKey(i), expire)
	}
	for i := 0; i < 200; i++ {
		val, _ := lru.Get(getKey(i))
		if val == nil {
			t.Fatalf("myKey-%d should be present.", i)
		}
	}

	time.Sleep(time.Second)

	for i := 1; i < 9; i++ {
		val, _ := lru.Get(getKey(i))
		if val != nil {
			t.Fatalf("myKey-%d should NOT be present.", i)
		}
	}
	for i := 50; i < 100; i++ {
		val, _ := lru.Get(getKey(i))
		if val == nil {
			t.Fatalf("myKey-%d should be present.", i)
		}
	}
	count := 0
	for i := 101; i < 200; i++ {
		if val, _ := lru.cache[getKey(i)]; val == nil {
			count++
		}
		if val, _ := lru.Get(getKey(i)); val != nil {
			t.Fatalf("myKey-%d should NOT be present.", i)
		}
	}
	if count < 75 {
		t.Fatalf("total expired: 100, garbage collected: %d", count)
	}

	lru.Stop()
}
