package node

import (
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

type PrepareResponse struct {
	Promise       bool
	Value         int
	MaxStamp      int
	PromisedStamp int
}

type AcceptResponse struct {
	accepted      bool
	PromisedStamp int
}

type Node interface {
	Serve() error
	PrepareStamp(nodeId int, stamp int) (PrepareResponse, error)
	AcceptStamp(nodeId int, stamp int, value int) (AcceptResponse, error)
	GetID() int
	AppendNode(node Node)
}

type node struct {
	nodes        []Node
	idle         bool
	value        int
	stamp        int
	nodeCount    int
	promiseStamp int
	id           int
}

type prepareState struct {
	mu             sync.Mutex
	promissedCount int
	value          int
	maxStamp       int
}

type acceptState struct {
	mu                sync.Mutex
	maxPromissedStamp int
}

var (
	MaxIdleSec = 4
)

func NewNode(id int) *node {
	return &node{
		nodes:        make([]Node, 0),
		nodeCount:    0,
		value:        0,
		stamp:        -1,
		promiseStamp: 0,
		id:           id,
		idle:         true,
	}
}

func (n *node) AppendNode(node Node) {
	n.nodes = append(n.nodes, node)
	n.nodeCount += 1
}

func (n *node) Serve() error {
	for {
		sleepDuration := time.Duration(rand.N(MaxIdleSec*1000)) * time.Millisecond
		logrus.Info(n.id, sleepDuration)
		time.Sleep(sleepDuration)

		n.idle = false

		if n.stamp < n.promiseStamp {
			err := n.sendPrepareRequest()
			if err != nil {
				logrus.Warn(err)
				continue
			}
			err = n.sendAcceptRequest()
			if err != nil {
				logrus.Warn(err)
				continue
			}
		}
	}
}

func (n *node) sendPrepareRequest() error {
	candidateStamp := (n.promiseStamp/n.nodeCount)*n.nodeCount + n.nodeCount + n.id

	state := &prepareState{
		maxStamp: -1,
	}

	var g errgroup.Group
	for _, node := range n.nodes {
		g.Go(func() error {
			res, err := node.PrepareStamp(n.id, candidateStamp)
			if err != nil {
				logrus.WithError(err).Warnf("node %v can't send prepare request to node %v", n.id, node.GetID())
				return nil
			}
			state.mu.Lock()
			if res.Promise {
				state.promissedCount += 1
				if state.maxStamp < res.MaxStamp {
					state.maxStamp = res.MaxStamp
					state.value = res.Value
				}
			} else {
				if n.promiseStamp < res.PromisedStamp {
					n.promiseStamp = res.PromisedStamp
				}
				return fmt.Errorf("prepare cancelled, there is a higher stamp")
			}
			state.mu.Unlock()
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}
	if state.promissedCount <= n.nodeCount/2 {
		return fmt.Errorf("majority of nodes was not available for node %v", n.id)
	}
	if n.promiseStamp > candidateStamp {
		return fmt.Errorf("node %v prepare request failed, because the promised stamp is greater than the candidate stamp", n.id)
	}

	var selectedValue int
	if state.maxStamp > -1 {
		selectedValue = state.value
	} else {
		selectedValue = n.id
	}

	n.stamp = candidateStamp
	n.value = selectedValue
	n.promiseStamp = candidateStamp

	return nil
}

func (n *node) sendAcceptRequest() error {
	var g errgroup.Group
	state := &acceptState{
		maxPromissedStamp: -1,
	}

	for _, node := range n.nodes {
		g.Go(func() error {
			res, err := node.AcceptStamp(n.id, n.stamp, n.value)
			if err != nil {
				logrus.WithError(err).Warnf("node %v can't send accept request to node %v", n.id, node.GetID())
				return nil
			}
			if !res.accepted {
				state.mu.Lock()
				if res.PromisedStamp > state.maxPromissedStamp {
					state.maxPromissedStamp = res.PromisedStamp
				}
				state.mu.Unlock()
				return fmt.Errorf("accept request for node %v failed, there is a higher promised stamp %v", n.id, res.PromisedStamp)
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		if state.maxPromissedStamp > n.stamp {
			n.promiseStamp = state.maxPromissedStamp
		}

		return fmt.Errorf("accept request for node %v failed: %w", n.id, err)
	}
	return nil
}

func (n *node) PrepareStamp(nodeId int, stamp int) (PrepareResponse, error) {
	if n.idle {
		return PrepareResponse{}, fmt.Errorf("node %v was not served yet", n.id)
	}
	res := PrepareResponse{
		Value:         n.value,
		MaxStamp:      n.stamp,
		PromisedStamp: n.promiseStamp,
	}
	if n.promiseStamp > stamp {
		res.Promise = false
		return res, nil
	}

	logrus.Infof("node %v has promised node %v with stamp %v", n.id, nodeId, stamp)

	res.Promise = true
	n.promiseStamp = stamp

	return res, nil
}

func (n *node) AcceptStamp(nodeId int, stamp int, value int) (AcceptResponse, error) {
	if n.idle {
		return AcceptResponse{}, fmt.Errorf("node %v was not served yet", n.id)
	}
	res := AcceptResponse{
		PromisedStamp: n.promiseStamp,
	}
	if n.promiseStamp > stamp {
		res.accepted = false
		return res, nil
	}

	logrus.Infof("node %v was accepted value: %v from node %v", n.id, value, nodeId)

	n.promiseStamp = stamp
	n.stamp = stamp
	n.value = value

	res.accepted = true
	return res, nil
}

func (n *node) GetID() int {
	return n.id
}
