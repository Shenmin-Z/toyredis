package toyredis

import (
	"fmt"
	"log"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestWithRedisCli(t *testing.T) {
	port := "6789"
	server := NewServer(port, MB*20)
	defer server.Stop()

	runCli := func(cmd string) string {
		params := strings.Split(cmd, " ")
		params = append([]string{"-p", port, "--no-raw"}, params...)
		cli := exec.Command("redis-cli", params...)
		out, err := cli.CombinedOutput()
		if err != nil {
			log.Fatal(err)
		}
		for len(out) > 0 && out[len(out)-1] == '\n' {
			out = out[0 : len(out)-1]
		}
		//t.Log("--", string(out), "--")
		return string(out)
	}

	printError := func(expected, actual string) {
		t.Errorf("expected: %v, actual: %v\n", expected, actual)
	}

	compare := func(expected, actual string) {
		if expected != actual {
			printError(expected, actual)
		}
	}

	hasStatus := func(cmd, expected string) {
		compare(expected, runCli(cmd))
	}

	hasError := func(cmd, expected string) {
		actual := runCli(cmd)
		if !strings.HasPrefix(actual, expected) {
			printError(expected, actual)
		}
	}

	hasString := func(cmd, expected string) {
		actual := runCli(cmd)
		compare("\""+expected+"\"", actual)
	}

	hasInteger := func(cmd string, expected int) {
		actual := runCli(cmd)
		compare("(integer) "+strconv.Itoa(expected), actual)
	}

	hasStringArray := func(cmd string, expected []string) {
		sb := new(strings.Builder)
		for idx, i := range expected {
			if i == "" {
				sb.WriteString(fmt.Sprintf("%d) (nil)\n", idx+1))
			} else {
				sb.WriteString(fmt.Sprintf("%d) \"%s\"\n", idx+1, i))
			}
		}
		expectedString := sb.String()
		actual := runCli(cmd) + "\n"
		compare(expectedString, actual)
	}

	isNil := func(cmd string) {
		compare("(nil)", runCli(cmd))
	}

	//------------------------------------------------------------------------------

	// test set/get
	hasStatus("flushdb", "OK")
	hasStatus("set foo fooValue", "OK")
	hasError("set foo", "(error) ERR wrong number of arguments")
	hasString("get foo", "fooValue")
	hasInteger("exists foo", 1)
	isNil("get bar")
	hasInteger("del foo", 1)
	isNil("get foo")
	hasInteger("del foo", 0)
	isNil("get foo")
	hasInteger("exists foo", 0)

	// test mset/mget
	runCli("flushdb")
	hasStatus("mset foo fooValue bar barValue baz bazValue", "OK")
	hasError("mset foo fooValue bar", "(error) ERR wrong number of arguments")
	hasString("get foo", "fooValue")
	hasInteger("exists foo", 1)
	hasStringArray("mget foo baz none bar", []string{"fooValue", "bazValue", "", "barValue"})
	hasString("get baz", "bazValue")
	hasInteger("del foo", 1)
	hasInteger("del bar baz none", 2)
	isNil("get foo")
	hasInteger("exists foo", 0)
	isNil("get none")

	// test hset/hget
	runCli("flushdb")
	hasStatus("hset foo k1 v1", "OK")
	hasError("hset foo k1", "(error) ERR wrong number of arguments")
	hasString("hget foo k1", "v1")
	hasInteger("hexists foo k1", 1)
	isNil("hget foo k2")
	hasInteger("hdel foo k1", 1)
	isNil("hget foo k1")
	hasInteger("hexists foo k1", 0)

	// test hmset/hmget
	runCli("flushdb")
	hasStatus("hmset foo k1 v1 k2 v2 k3 v3", "OK")
	hasError("hmset foo k1", "(error) ERR wrong number of arguments")
	hasError("hmset foo k1 v1 k2", "(error) ERR wrong number of arguments")
	hasString("hget foo k1", "v1")
	hasInteger("hexists foo k1", 1)
	hasStringArray("hmget foo k1 k2 none k3", []string{"v1", "v2", "", "v3"})
	hasStringArray("hgetall foo", []string{"k1", "v1", "k2", "v2", "k3", "v3"})
	hasString("hget foo k2", "v2")
	hasInteger("hdel foo k1 k2", 2)
	hasInteger("hdel foo none k3", 1)
	isNil("hget foo k1")
	hasInteger("hexists foo k1", 0)
	isNil("hget foo none")

	// test expire
	runCli("flushdb")
	hasStatus("set foo fooValue", "OK")
	hasStatus("hset bar k1 v1", "OK")
	hasInteger("exists foo", 1)
	hasInteger("hexists bar k1", 1)
	hasInteger("pexpire foo 100", 1)
	hasInteger("pexpire bar 100", 1)
	hasInteger("pexpire baz 100", 0)
	hasError("pexpire baz", "(error) ERR wrong number of arguments")
	hasError("pexpire baz xyz", "(error) ERR value is not an integer or out of range")
	time.Sleep(time.Millisecond * 50)
	hasInteger("exists foo", 1)
	hasInteger("hexists bar k1", 1)
	time.Sleep(time.Millisecond * 60)
	hasInteger("exists foo", 0)
	hasInteger("hexists bar k1", 0)
}
