// Copyright 2016 Chao Wang <hit9@icloud.com>

/*

Package diskstack implements on-disk stack.

Design

	[data   X bytes] -+
	[size   4 bytes]  +-> bucket
	[offset 8 bytes] -+
	[data   X bytes]
	[size   4 bytes]
	[offset 8 bytes] <- offset
	....

*/
package diskstack

import (
	"encoding/binary"
	"os"
	"sync"
)

// Stack is the disk-based stack abstraction.
type Stack struct {
	file   *os.File
	offset int64
	lock   sync.RWMutex // protects offset
}

// Open opens or creates a Stack for given path,  will be created if not exist.
func Open(path string) (*Stack, error) {
	// Open or create the file.
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, os.FileMode(0644))
	if err != nil {
		return nil, err
	}
	// Seek to end.
	offset, err := file.Seek(0, os.SEEK_END)
	if err != nil {
		return nil, err
	}
	if offset >= 4+8 {
		// Read offset.
		b := make([]byte, 8)
		if _, err := file.ReadAt(b, offset-8); err != nil {
			return nil, err
		}
		offset = int64(binary.BigEndian.Uint64(b))
	}
	return &Stack{file: file, offset: offset}, nil
}

// Put an item onto the Stack.
func (s *Stack) Put(data []byte) (err error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	buf := make([]byte, len(data)+4+8)
	copy(buf, data)                                                                 // data
	binary.BigEndian.PutUint32(buf[len(data):], uint32(len(data)))                  // size
	binary.BigEndian.PutUint64(buf[len(data)+4:], uint64(s.offset+int64(len(buf)))) // offset
	if _, err = s.file.WriteAt(buf, s.offset); err != nil {
		return
	}
	s.offset += int64(len(buf))
	return
}

// Pop an item from the Stack, returns nil on empty.
func (s *Stack) Pop() (data []byte, err error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if data, err = s.top(); err != nil {
		return
	}
	s.offset -= int64(len(data)) + 4 + 8
	return
}

// Top returns the top item on the Stack, returns nil on empty.
func (s *Stack) Top() (data []byte, err error) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.top()
}

// top returns the top item.
func (s *Stack) top() (data []byte, err error) {
	if s.offset < 4+8 {
		return nil, nil
	}
	b := make([]byte, 4)
	if _, err = s.file.ReadAt(b, s.offset-8-4); err != nil { // size
		return
	}
	size := binary.BigEndian.Uint32(b)
	data = make([]byte, size)
	if _, err = s.file.ReadAt(data, s.offset-8-4-int64(size)); err != nil { // data
		return
	}
	return
}

// Close the stack.
func (s *Stack) Close() (err error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if err = s.file.Close(); err != nil {
		return
	}
	s.offset = 0
	return
}
