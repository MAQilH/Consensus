package main

import (
	"fmt"
	"os"

	"raft/raft1"
	"raft/tester1"
)

func main() {
	if err := tester.InitDaemon(os.Args[1:], raft.NewRfsrv); err != nil {
		fmt.Printf("%v: InitDaemon err %v", os.Args[0], err)
		os.Exit(1)
	}
}
