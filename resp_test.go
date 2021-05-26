package toyredis

import (
	"bytes"
	"io"
	"strings"
	"testing"
)

func TestWriter(t *testing.T) {
	var expected string
	buf := new(bytes.Buffer)
	wr := NewWriter(buf)
	wr.Status("OK")
	expected = "+OK\r\n"
	if string(buf.Bytes()) != expected {
		t.Fatalf("want %q, got %q", buf.Bytes(), expected)
	}

	io.ReadAll(buf)
	wr.Error("Error xx")
	expected = "-Error xx\r\n"
	if string(buf.Bytes()) != expected {
		t.Fatalf("want %q, got %q", buf.Bytes(), expected)
	}

	io.ReadAll(buf)
	wr.Int(10)
	expected = ":10\r\n"
	if string(buf.Bytes()) != expected {
		t.Fatalf("want %q, got %q", buf.Bytes(), expected)
	}

	io.ReadAll(buf)
	wr.String([]byte("foobar"))
	expected = "$6\r\nfoobar\r\n"
	if string(buf.Bytes()) != expected {
		t.Fatalf("want %q, got %q", buf.Bytes(), expected)
	}

	io.ReadAll(buf)
	wr.StringArray([][]byte{
		[]byte("foo"),
		[]byte("bar"),
		[]byte("baz"),
	})
	expected = "*3\r\n" +
		"$3\r\nfoo\r\n" +
		"$3\r\nbar\r\n" +
		"$3\r\nbaz\r\n"
	if string(buf.Bytes()) != expected {
		t.Fatalf("want %q, got %q", buf.Bytes(), expected)
	}
}

func TestReader(t *testing.T) {
	buf := new(bytes.Buffer)
	wr := NewWriter(buf)
	data := [][]byte{
		[]byte("foo"),
		[]byte("bar"),
		[]byte("baz"),
	}
	wr.StringArray(data)
	rd := NewReader(buf)
	ss, err := rd.ReadRequest()
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	expected := "foo,bar,baz"
	actual := strings.Join(ss, ",")
	if expected != actual {
		t.Fatalf("want %q, got %q", expected, actual)
	}
}
