package toyredis

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

type server struct {
	port         string
	listener     net.Listener
	quit         chan interface{}
	cache        *Cache
	clientsCount int
	mu           *sync.Mutex
}

func NewServer(port string, sizeLimit int) *server {
	s := &server{
		port:  port,
		quit:  make(chan interface{}),
		cache: NewCache(MB * sizeLimit),
		mu:    &sync.Mutex{},
	}
	l, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalln(err)
	}
	s.listener = l
	go s.serve()
	return s
}

func (s *server) Stop() {
	close(s.quit)
	s.listener.Close()
	s.cache.Stop()
}

func (s *server) serve() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.quit:
				return
			default:
				fmt.Println(err)
			}
		}
		go s.handleConnection(conn)
	}
}

//------------------------------------------------------------------------------

type Conn struct {
	netConn net.Conn

	rd *Reader
	bw *bufio.Writer
	wr *Writer
}

func NewConn(c net.Conn) *Conn {
	cn := &Conn{
		netConn: c,
		rd:      NewReader(c),
	}
	cn.bw = bufio.NewWriter(c)
	cn.wr = NewWriter(cn.bw)
	return cn
}

var (
	invalidRequest     = errors.New("ERR invalid request")
	unsupportedRequest = errors.New("ERR unsupported command")
	arityError         = errors.New("ERR wrong number of arguments")
	notIntError        = errors.New("ERR value is not an integer or out of range")
)

func (s *server) handleConnection(c net.Conn) {
	s.mu.Lock()
	s.clientsCount++
	s.mu.Unlock()
	cn := NewConn(c)
	var err error
	var ss []string
	defer func() {
		if err != io.EOF {
			cn.wr.Error(err.Error())
			cn.bw.Flush()
		}
		c.Close()
		s.mu.Lock()
		s.clientsCount--
		s.mu.Unlock()
	}()

	for {
		ss, err = cn.rd.ReadRequest()
		if err != nil {
			if err != io.EOF {
				err = invalidRequest
			}
			break
		}

		switch strings.ToLower(ss[0]) {
		case "flushdb":
			err = s.handleFlush(cn, ss[1:])
		case "expire": // seconds
			err = s.handleExpire(cn, ss[1:], 1000)
		case "pexpire": // milliseconds
			err = s.handleExpire(cn, ss[1:], 1)
		case "set":
			err = s.handleSet(cn, ss[1:])
		case "mset":
			err = s.handleMSet(cn, ss[1:])
		case "get":
			err = s.handleGet(cn, ss[1:])
		case "mget":
			err = s.handleMGet(cn, ss[1:])
		case "exists":
			err = s.handleExists(cn, ss[1:])
		case "hset":
			err = s.handleHSet(cn, ss[1:])
		case "hmset":
			err = s.handleHMSet(cn, ss[1:])
		case "hget":
			err = s.handleHGet(cn, ss[1:])
		case "hmget":
			err = s.handleHMGet(cn, ss[1:])
		case "hgetall":
			err = s.handleHGetAll(cn, ss[1:])
		case "hexists":
			err = s.handleHExists(cn, ss[1:])
		case "del":
			err = s.handleDel(cn, ss[1:])
		case "hdel":
			err = s.handleHDel(cn, ss[1:])
		case "info":
			err = s.handleInfo(cn, ss[1:])
		case "config":
			err = s.handleConfigSet(cn, ss[1:])
		default:
			err = unsupportedRequest
		}
		if err != nil {
			cn.wr.Error(err.Error())
		}
		cn.bw.Flush()
	}
}

func (s *server) handleFlush(cn *Conn, ss []string) (err error) {
	if len(ss) != 0 {
		err = arityError
	} else {
		s.cache.Flush()
		cn.wr.Status("OK")
	}
	return
}

func (s *server) handleExpire(cn *Conn, ss []string, unit int) (err error) {
	if len(ss) != 2 {
		err = arityError
	} else {
		i, e := strconv.Atoi(ss[1])
		if e != nil {
			err = notIntError
		} else {
			num := s.cache.Expire(ss[0], i*unit)
			cn.wr.Int(num)
		}
	}
	return
}

func (s *server) handleSet(cn *Conn, ss []string) (err error) {
	if len(ss) != 2 {
		err = arityError
	} else {
		s.cache.Set(ss[0], []byte(ss[1]))
		cn.wr.Status("OK")
	}
	return
}

func (s *server) handleMSet(cn *Conn, ss []string) (err error) {
	if len(ss) < 2 || len(ss)%2 != 0 {
		err = arityError
	} else {
		for i := 0; i < len(ss); i += 2 {
			s.cache.Set(ss[i], []byte(ss[i+1]))
		}
		cn.wr.Status("OK")
	}
	return
}

func (s *server) handleGet(cn *Conn, ss []string) (err error) {
	if len(ss) != 1 {
		err = arityError
	} else {
		d, err := s.cache.Get(ss[0])
		if err != nil {
			return err
		}
		cn.wr.String((d))
	}
	return
}

func (s *server) handleMGet(cn *Conn, ss []string) (err error) {
	if len(ss) < 1 {
		err = arityError
	} else {
		buf := make([][]byte, len(ss))
		for i, k := range ss {
			d, err := s.cache.Get(k)
			if err != nil {
				return err
			}
			buf[i] = d
		}
		cn.wr.StringArray(buf)
	}
	return
}

func (s *server) handleExists(cn *Conn, ss []string) (err error) {
	if len(ss) != 1 {
		err = arityError
	} else {
		num := s.cache.Exists(ss[0])
		cn.wr.Int(num)
	}
	return
}

func (s *server) handleHSet(cn *Conn, ss []string) (err error) {
	if len(ss) != 3 {
		err = arityError
	} else {
		s.cache.HSet(ss[0], ss[1], []byte(ss[2]))
		cn.wr.Status("OK")
	}
	return
}

func (s *server) handleHMSet(cn *Conn, ss []string) (err error) {
	if len(ss) < 3 || len(ss)%2 != 1 {
		err = arityError
	} else {
		for i := 1; i < len(ss); i += 2 {
			s.cache.HSet(ss[0], ss[i], []byte(ss[i+1]))
		}
		cn.wr.Status("OK")
	}
	return
}

func (s *server) handleHGet(cn *Conn, ss []string) (err error) {
	if len(ss) != 2 {
		err = arityError
	} else {
		d, err := s.cache.HGet(ss[0], ss[1])
		if err != nil {
			return err
		}
		cn.wr.String((d))
	}
	return
}

func (s *server) handleHGetAll(cn *Conn, ss []string) (err error) {
	if len(ss) != 1 {
		err = arityError
	} else {
		d, err := s.cache.HGetAll(ss[0])
		if err != nil {
			return err
		}
		cn.wr.StringArray(d)
	}
	return
}

func (s *server) handleHMGet(cn *Conn, ss []string) (err error) {
	if len(ss) < 2 {
		err = arityError
	} else {
		buf := make([][]byte, len(ss)-1)
		for i, k := range ss[1:] {
			d, err := s.cache.HGet(ss[0], k)
			if err != nil {
				return err
			}
			buf[i] = d
		}
		cn.wr.StringArray(buf)
	}
	return
}

func (s *server) handleDel(cn *Conn, ss []string) (err error) {
	if len(ss) < 1 {
		err = arityError
	} else {
		num := s.cache.Remove(ss)
		cn.wr.Int(num)
	}
	return
}

func (s *server) handleHDel(cn *Conn, ss []string) (err error) {
	if len(ss) < 2 {
		err = arityError
	} else {
		num, err := s.cache.HDel(ss[0], ss[1:])
		if err != nil {
			return err
		}
		cn.wr.Int(num)
	}
	return
}

func (s *server) handleHExists(cn *Conn, ss []string) (err error) {
	if len(ss) != 2 {
		err = arityError
	} else {
		num, err := s.cache.HExists(ss[0], ss[1])
		if err != nil {
			return err
		}
		cn.wr.Int(num)
	}
	return
}

func (s *server) handleInfo(cn *Conn, ss []string) (err error) {
	if len(ss) != 0 {
		err = arityError
	} else {
		pid := os.Getpid()

		info := []struct {
			groupName string
			kv        map[string]string
		}{
			{"Server", make(map[string]string)},
			{"Clients", make(map[string]string)},
			{"Memory", make(map[string]string)},
		}
		info[0].kv["process_id"] = strconv.Itoa(pid)
		info[0].kv["tcp_port"] = s.port
		info[1].kv["connected_clients"] = strconv.Itoa(s.clientsCount)
		info[2].kv["used_memory"] = strconv.Itoa(s.cache.GetSize())
		info[2].kv["maxmemory"] = strconv.Itoa(s.cache.GetSizeLimit())

		sb := new(strings.Builder)
		clrf := "\r\n"
		for idx, i := range info {
			sb.WriteString("# ")
			sb.WriteString(i.groupName)
			sb.WriteString(clrf)
			for k, v := range i.kv {
				sb.WriteString(k)
				sb.WriteString(":")
				sb.WriteString(v)
				sb.WriteString(clrf)
			}
			if idx != len(info)-1 {
				sb.WriteString(clrf)
			}
		}
		cn.wr.String([]byte(sb.String()))
	}
	return
}

func (s *server) handleConfigSet(cn *Conn, ss []string) (err error) {
	if len(ss) == 3 && ss[0] == "set" {
		if ss[1] == "maxmemory" {
			r := regexp.MustCompile(`^(\d+)mb$`)
			match := r.FindStringSubmatch(ss[2])
			if len(match) != 2 {
				return fmt.Errorf("invalid maxmemory: %s", ss[2])
			}
			sizeLimit, _ := strconv.Atoi(match[1])
			s.cache.SetSizeLimit(sizeLimit * 1024)
			cn.wr.Status("OK")
			return
		}
		if ss[1] == "maxmemory-policy" {
			cn.wr.Status("OK")
			return
		}
	}
	return unsupportedRequest
}
