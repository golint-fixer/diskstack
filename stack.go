// Copyright 2016 Chao Wang <hit9@icloud.com>

/*

Package diskstack implements on-disk stack.

Design

	+------------------+------+
	| [offset 8 bytes] | head |
	| [length 4 bytes] |  12  |
	+------------------+------+
	| [data   X bytes] |      |
	| [size   4 bytes] | body |
	| [data   X bytes] |  X   |
	| [size   4 bytes] |      |
	| ...              |      |

*/
package diskstack

import (
	"encoding/binary"
	"errors"
	"os"
	"sync"
)

// Size units
const (
	KB int64 = 1024
	MB int64 = 1024 * KB
	GB int64 = 1024 * MB
)

// Head size
const (
	offsetSize = 8
	lengthSize = 4
	headSize   = offsetSize + lengthSize
)

// Default options.
const (
	DefaultFragmentsThreshold int64 = 512 * MB
	DefaultSizeLimit          int64 = 16 * GB
)

// Errors
var (
	ErrSizeLimit   = errors.New("diskstack: size limit")
	ErrFileInvalid = errors.New("diskstack: invalid file")
)

// Options is the options to open Stack.
type Options struct {
	// FragmentsThreshold is the fragments size threshold to trigger the
	// file compaction.
	FragmentsThreshold int64
	// SizeLimit is the file size limitation, operation Put returns
	// ErrSizeLimit if the file size is greater than this value.
	// Negative number means no size limitation.
	SizeLimit int64
}

// Stack is the disk-based stack abstraction.
type Stack struct {
	file   *os.File     // os file handle
	offset int64        // top offset (real offset is 8+4+offset)
	frags  int64        // fragments size
	length int          // length of stack
	lock   sync.RWMutex // protects offset,frags,length
	opts   *Options
}

// Open opens or creates a Stack for given path, will create if not exist.
func Open(path string, opts *Options) (s *Stack, err error) {
	// Open or create the file.
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, os.FileMode(0644))
	if err != nil {
		return
	}
	// Create Stack.
	options := &Options{
		FragmentsThreshold: DefaultFragmentsThreshold,
		SizeLimit:          DefaultSizeLimit,
	}
	if opts != nil {
		if opts.FragmentsThreshold != 0 {
			options.FragmentsThreshold = opts.FragmentsThreshold
		}
		if opts.SizeLimit != 0 {
			options.SizeLimit = opts.SizeLimit
		}
	}
	s = &Stack{opts: options, file: file}
	// Get file size.
	info, err := file.Stat()
	if err != nil {
		return
	}
	fileSize := info.Size()
	if fileSize < headSize {
		if fileSize != 0 {
			err = ErrFileInvalid // invalid small file
			return
		}
		if err = s.file.Truncate(0); err != nil {
			// Force truncate the file to be empty.
			return
		}
		s.offset = headSize
		s.length = 0
		err = s.writeHead()
		return
	}
	// Read offset.
	b := make([]byte, offsetSize)
	if _, err = file.ReadAt(b, 0); err != nil {
		return
	}
	s.offset = int64(binary.BigEndian.Uint64(b))
	// Read length.
	b = make([]byte, 4)
	if _, err = file.ReadAt(b, offsetSize); err != nil {
		return
	}
	s.length = int(binary.BigEndian.Uint32(b))
	// Frags
	if err = s.truncate(); err != nil { // Remove the fragements
		return
	}
	s.frags = 0
	return s, nil
}

// Put an item onto the Stack.
func (s *Stack) Put(data []byte) (err error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.opts.SizeLimit > 0 && s.offset >= s.opts.SizeLimit {
		return ErrSizeLimit
	}
	buf := make([]byte, len(data)+4)
	copy(buf, data)                                                // data
	binary.BigEndian.PutUint32(buf[len(data):], uint32(len(data))) // size
	if _, err = s.file.WriteAt(buf, s.offset); err != nil {
		return
	}
	s.offset += int64(len(buf))
	if s.frags > int64(len(buf)) {
		s.frags -= int64(len(buf))
	}
	s.length++
	return s.writeHead()
}

// top returns the top item.
func (s *Stack) top() (data []byte, err error) {
	if s.offset < headSize+4 {
		return nil, nil
	}
	b := make([]byte, 4)
	if _, err = s.file.ReadAt(b, s.offset-4); err != nil { // size
		return
	}
	size := binary.BigEndian.Uint32(b)
	data = make([]byte, size)
	if _, err = s.file.ReadAt(data, s.offset-4-int64(size)); err != nil { // data
		return
	}
	return
}

// Top returns the top item on the Stack, returns nil on empty.
func (s *Stack) Top() (data []byte, err error) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.top()
}

// Len returns the stack length.
func (s *Stack) Len() int {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.length
}

// Pop an item from the Stack, returns nil on empty.
func (s *Stack) Pop() (data []byte, err error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if data, err = s.top(); err != nil {
		return
	}
	if data == nil {
		return // Do nothing on stack empty.
	}
	s.offset -= int64(len(data)) + 4
	s.length--
	s.frags += int64(len(data)) + 4
	if err = s.writeHead(); err != nil {
		return
	}
	if err = s.compact(); err != nil {
		return
	}
	return
}

// Clear the Stack.
func (s *Stack) Clear() (err error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.frags = s.offset - 8 - 4
	s.offset = 8 + 4
	s.length = 0
	if err = s.writeHead(); err != nil {
		return
	}
	return s.truncate()
}

// Close the Stack.
func (s *Stack) Close() (err error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if err = s.file.Close(); err != nil {
		return
	}
	return
}

// compact truncates the file if the fragments is greater than the threshold.
func (s *Stack) compact() (err error) {
	if s.frags >= s.opts.FragmentsThreshold {
		return s.truncate()
	}
	return nil
}

// truncate the file to size the offset.
func (s *Stack) truncate() (err error) {
	if s.opts.SizeLimit > 0 && s.offset >= s.opts.SizeLimit {
		// Important: truncate with large size cause unexcepted no-space left error!
		return ErrSizeLimit
	}
	return s.file.Truncate(s.offset)
}

// writeHead writes the head.
func (s *Stack) writeHead() (err error) {
	b := make([]byte, 8+4)
	binary.BigEndian.PutUint64(b, uint64(s.offset))
	binary.BigEndian.PutUint32(b[8:], uint32(s.length))
	if _, err = s.file.WriteAt(b, 0); err != nil {
		return err
	}
	return nil
}
