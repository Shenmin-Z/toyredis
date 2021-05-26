package toyredis

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

const (
	ErrorReply  = '-'
	StatusReply = '+'
	IntReply    = ':'
	StringReply = '$'
	ArrayReply  = '*'
)

type Reader struct {
	rd *bufio.Reader
}

func NewReader(rd io.Reader) *Reader {
	return &Reader{
		rd: bufio.NewReader(rd),
	}
}

func (r *Reader) readline() ([]byte, error) {
	b, err := r.rd.ReadSlice('\n')
	if err != nil {
		if err != bufio.ErrBufferFull {
			return nil, err
		}

		full := make([]byte, len(b))
		copy(full, b)

		b, err := r.rd.ReadBytes('\n')
		if err != nil {
			return nil, err
		}

		full = append(full, b...)
		b = full
	}
	if len(b) <= 2 || b[len(b)-2] != '\r' {
		return nil, fmt.Errorf("invalid replat: %q", b)
	}
	return b[:len(b)-2], nil
}

func (r *Reader) ReadRequest() ([]string, error) {
	line, err := r.readline()
	if err != nil {
		return nil, err
	}
	if line[0] != ArrayReply {
		return nil, fmt.Errorf("expecting array, got %q", line[0])
	}
	replyLen, err := strconv.Atoi(string(line[1:]))
	if err != nil {
		return nil, err
	}

	b := make([]string, replyLen)
	for i := 0; i < replyLen; i++ {
		s, err := r.readStringReply()
		if err != nil {
			return nil, err
		}
		b[i] = s
	}
	return b, nil
}

func (r *Reader) readStringReply() (string, error) {
	line, err := r.readline()
	if err != nil {
		return "", err
	}
	if line[0] != StringReply {
		return "", fmt.Errorf("expecting string, got %q", line[0])
	}
	replyLen, err := strconv.Atoi(string(line[1:]))
	if err != nil {
		return "", err
	}

	b := make([]byte, replyLen+2)
	_, err = io.ReadFull(r.rd, b)
	if err != nil {
		return "", err
	}

	return string(b[:replyLen]), nil
}

//------------------------------------------------------------------------------
type writer interface {
	io.Writer
	io.ByteWriter
}

type Writer struct {
	writer
}

func NewWriter(w writer) *Writer {
	return &Writer{w}
}

func (w *Writer) clrf() error {
	if err := w.WriteByte('\r'); err != nil {
		return err
	}
	return w.WriteByte('\n')
}

func (w *Writer) simple(id byte, b []byte) error {
	if err := w.WriteByte(id); err != nil {
		return err
	}
	if _, err := w.Write(b); err != nil {
		return err
	}
	return w.clrf()
}

func (w *Writer) Status(s string) error {
	return w.simple(StatusReply, []byte(s))
}

func (w *Writer) Error(s string) error {
	return w.simple(ErrorReply, []byte(s))
}

func (w *Writer) Int(i int) error {
	return w.simple(IntReply, []byte(strconv.Itoa(i)))
}

func (w *Writer) String(b []byte) error {
	if b == nil {
		return w.nullString()
	}
	if err := w.simple(StringReply, []byte(strconv.Itoa(len(b)))); err != nil {
		return err
	}
	if _, err := w.Write(b); err != nil {
		return err
	}
	return w.clrf()
}

func (w *Writer) nullString() error {
	return w.simple(StringReply, []byte{'-', '1'})
}

func (w *Writer) StringArray(bs [][]byte) error {
	if err := w.simple(ArrayReply, []byte(strconv.Itoa(len(bs)))); err != nil {
		return err
	}
	for _, b := range bs {
		err := w.String(b)
		if err != nil {
			return err
		}
	}
	return nil
}

func (w *Writer) NullStringArray() error {
	return w.simple(ArrayReply, []byte{'-', '1'})
}
