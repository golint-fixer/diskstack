// Copyright 2016 Chao Wang <hit9@icloud.com>

// +build ignore

package main

import (
	"log"

	"github.com/hit9/diskstack"
)

func main() {
	// Open stack on disk.
	s, err := diskstack.Open("stack.db")
	if err != nil {
		log.Fatal(err)
	}
	defer s.Close()
	// Put items.
	data1 := []byte{'A', 'B', 'C'}
	data2 := []byte{'a', 'b', 'c'}
	log.Printf("Put %v\n", data1)
	s.Put(data1)
	log.Printf("Put %v\n", data2)
	s.Put(data2)
	// Pop items.
	for i := 0; i < 2; i++ {
		data, err := s.Pop()
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("Pop %v\n", data)
	}
}
