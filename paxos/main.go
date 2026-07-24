package main

import (
	"fmt"
	"paxos/node"
	"sync"
	// "time"
)

var (
	NodeCount = 5
)

func main() {
	nodes := make([]node.Node, 0)

	for id := range NodeCount {
		node := node.NewNode(id)
		nodes = append(nodes, node)
	}

	for _, node := range nodes {
		for _, adjNode := range nodes {
			if node.GetID() == adjNode.GetID() {
				continue
			}
			node.AppendNode(adjNode)
		}
	}

	var wg sync.WaitGroup
	for index := range NodeCount {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			nodes[index].Serve()
		}(index)
	}
	wg.Wait()
	fmt.Print("all the nodes have been stopped!")
}
