// Copyright 2016 Chao Wang <hit9@icloud.com>

package diskstack

import (
	"bytes"
	"os"
	"runtime"
	"testing"
)

// Must asserts the given value is True for testing.
func Must(t *testing.T, v bool) {
	if !v {
		_, fileName, line, _ := runtime.Caller(1)
		t.Errorf("\n unexcepted: %s:%d", fileName, line)
	}
}

func TestOpenEmpty(t *testing.T) {
	fileName := "stack.db"
	s, err := Open(fileName)
	// Must open without errors
	Must(t, err == nil)
	Must(t, s != nil)
	defer os.Remove(fileName)
	info, err := os.Stat(fileName)
	// Must be an empty file
	Must(t, err == nil && info.Size() == 0)
}

func TestReOpen(t *testing.T) {
	fileName := "stack.db"
	s, _ := Open(fileName)
	defer os.Remove(fileName)
	// Put one item.
	data := []byte{1, 2, 3}
	s.Put(data)
	// Close stack.
	s.Close()
	// Reopen.
	s, _ = Open(fileName)
	// Must offset be correct.
	Must(t, s.offset == int64(len(data))+4+8)
}

func TestTopEmpty(t *testing.T) {
	fileName := "stack.db"
	s, _ := Open(fileName)
	defer os.Remove(fileName)
	data, err := s.Top()
	// Must be nil,nil
	Must(t, data == nil && err == nil)
	data, err = s.Pop()
	// Must be nil,nil
	Must(t, data == nil && err == nil)
}

func TestOperations(t *testing.T) {
	fileName := "stack.db"
	s, _ := Open(fileName)
	defer os.Remove(fileName)
	data1 := []byte{1, 2, 3, 4}
	data2 := []byte{5, 6, 7, 8}
	data3 := []byte{9, 10, 11, 12}
	// Must put ok.
	Must(t, s.Put(data1) == nil)
	// Top should be data1.
	data, err := s.Top()
	Must(t, err == nil && bytes.Compare(data, data1) == 0)
	// Must put ok.
	Must(t, s.Put(data2) == nil)
	Must(t, s.Put(data3) == nil)
	// Pop should be data3
	data, err = s.Pop()
	Must(t, err == nil && bytes.Compare(data, data3) == 0)
	// Pop again should be data2
	data, err = s.Pop()
	Must(t, err == nil && bytes.Compare(data, data2) == 0)
	// Pop again should be data1
	data, err = s.Pop()
	Must(t, err == nil && bytes.Compare(data, data1) == 0)
	// Pop again should be nil
	data, err = s.Pop()
	Must(t, err == nil && bytes.Compare(data, nil) == 0)
}

func TestOperationsBetweenOpens(t *testing.T) {
	fileName := "stack.db"
	s, _ := Open(fileName)
	defer os.Remove(fileName)
	data1 := []byte{1, 2, 3, 4}
	data2 := []byte{5, 6, 7, 8}
	data3 := []byte{9, 10, 11, 12}
	// Must put ok.
	Must(t, s.Put(data1) == nil)
	Must(t, s.Put(data2) == nil)
	Must(t, s.Put(data3) == nil)
	// Close.
	s.Close()
	// Reopen.
	s, _ = Open(fileName)
	// Must offset be correct.
	Must(t, s.offset == 3*(int64(len(data1))+4+8))
	// Pops should be correct.
	data, err := s.Pop()
	Must(t, err == nil && bytes.Compare(data, data3) == 0)
	data, err = s.Pop()
	Must(t, err == nil && bytes.Compare(data, data2) == 0)
	data, err = s.Pop()
	Must(t, err == nil && bytes.Compare(data, data1) == 0)
	data, err = s.Pop()
	Must(t, err == nil && bytes.Compare(data, nil) == 0)
}

func BenchmarkPut(b *testing.B) {
	fileName := "stack.db"
	s, _ := Open(fileName)
	defer os.Remove(fileName)
	data := []byte("12345678910")
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		s.Put(data)
	}
}

func BenchmarkPutLargeItem(b *testing.B) {
	fileName := "stack.db"
	s, _ := Open(fileName)
	defer os.Remove(fileName)
	var data []byte
	for i := 0; i < 1024; i++ {
		data = append(data, 255)
	} // data length is now 1kb
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		s.Put(data)
	}
}

func BenchmarkPop(b *testing.B) {
	fileName := "stack.db"
	s, _ := Open(fileName)
	defer os.Remove(fileName)
	data := []byte("12345678910")
	for i := 0; i < b.N; i++ {
		s.Put(data)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		s.Pop()
	}
}

func BenchmarkPopLargeItem(b *testing.B) {
	fileName := "stack.db"
	s, _ := Open(fileName)
	defer os.Remove(fileName)
	var data []byte
	for i := 0; i < 1024; i++ {
		data = append(data, 255)
	} // data length is now 1kb
	for i := 0; i < b.N; i++ {
		s.Put(data)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		s.Pop()
	}
}
