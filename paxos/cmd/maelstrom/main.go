package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"paxos/node"

	"github.com/sirupsen/logrus"
)

type envelope struct {
	Src  string          `json:"src"`
	Dest string          `json:"dest"`
	Body json.RawMessage `json:"body"`
}

type baseBody struct {
	Type      string `json:"type"`
	MsgID     int    `json:"msg_id,omitempty"`
	InReplyTo int    `json:"in_reply_to,omitempty"`
}

type transport struct {
	selfStr string

	out    *bufio.Writer
	outMu  sync.Mutex
	nextID int64

	pendingMu sync.Mutex
	pending   map[int]chan json.RawMessage
}

func newTransport(selfStr string) *transport {
	return &transport{
		selfStr: selfStr,
		out:     bufio.NewWriter(os.Stdout),
		pending: make(map[int]chan json.RawMessage),
	}
}

func (t *transport) send(dest string, body map[string]any) {
	env := envelope{Src: t.selfStr, Dest: dest}
	b, err := json.Marshal(body)
	if err != nil {
		logrus.WithError(err).Error("failed to marshal body")
		return
	}
	env.Body = b
	line, err := json.Marshal(env)
	if err != nil {
		logrus.WithError(err).Error("failed to marshal envelope")
		return
	}
	t.outMu.Lock()
	defer t.outMu.Unlock()
	t.out.Write(line)
	t.out.WriteByte('\n')
	t.out.Flush()
}

func (t *transport) rpc(dest string, body map[string]any, timeout time.Duration) (json.RawMessage, error) {
	id := int(atomic.AddInt64(&t.nextID, 1))
	body["msg_id"] = id

	ch := make(chan json.RawMessage, 1)
	t.pendingMu.Lock()
	t.pending[id] = ch
	t.pendingMu.Unlock()
	defer func() {
		t.pendingMu.Lock()
		delete(t.pending, id)
		t.pendingMu.Unlock()
	}()

	t.send(dest, body)

	select {
	case raw := <-ch:
		return raw, nil
	case <-time.After(timeout):
		return nil, fmt.Errorf("rpc to %s timed out", dest)
	}
}

func (t *transport) reply(dest string, inReplyTo int, body map[string]any) {
	body["in_reply_to"] = inReplyTo
	t.send(dest, body)
}

type remoteNode struct {
	id      int
	destStr string
	tr      *transport
}

func (r *remoteNode) GetID() int              { return r.id }
func (r *remoteNode) Serve() error            { return nil }
func (r *remoteNode) AppendNode(node.Node)    {}
func (r *remoteNode) Propose(int)             {}
func (r *remoteNode) GetDecision() (int, int) { return -1, 0 }

func (r *remoteNode) PrepareStamp(nodeId int, stamp int) (node.PrepareResponse, error) {
	body := map[string]any{
		"type":      "prepare",
		"from_node": nodeId,
		"stamp":     stamp,
	}
	raw, err := r.tr.rpc(r.destStr, body, 5*time.Second)
	if err != nil {
		return node.PrepareResponse{}, err
	}
	var resp struct {
		Promise       bool `json:"promise"`
		Value         int  `json:"value"`
		MaxStamp      int  `json:"max_stamp"`
		PromisedStamp int  `json:"promised_stamp"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		return node.PrepareResponse{}, err
	}
	return node.PrepareResponse{
		Promise:       resp.Promise,
		Value:         resp.Value,
		MaxStamp:      resp.MaxStamp,
		PromisedStamp: resp.PromisedStamp,
	}, nil
}

func (r *remoteNode) AcceptStamp(nodeId int, stamp int, value int) (node.AcceptResponse, error) {
	body := map[string]any{
		"type":      "accept",
		"from_node": nodeId,
		"stamp":     stamp,
		"value":     value,
	}
	raw, err := r.tr.rpc(r.destStr, body, 5*time.Second)
	if err != nil {
		return node.AcceptResponse{}, err
	}
	var resp struct {
		Accepted      bool `json:"accepted"`
		PromisedStamp int  `json:"promised_stamp"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		return node.AcceptResponse{}, err
	}
	return node.AcceptResponse{
		Accepted:      resp.Accepted,
		PromisedStamp: resp.PromisedStamp,
	}, nil
}

func main() {
	logrus.SetOutput(os.Stderr)

	reader := bufio.NewReaderSize(os.Stdin, 1<<20)

	line, err := reader.ReadString('\n')
	if err != nil {
		logrus.WithError(err).Fatal("failed to read init message")
	}

	var initEnv envelope
	if err := json.Unmarshal([]byte(line), &initEnv); err != nil {
		logrus.WithError(err).Fatal("failed to parse init envelope")
	}
	var initBody struct {
		baseBody
		NodeID  string   `json:"node_id"`
		NodeIDs []string `json:"node_ids"`
	}
	if err := json.Unmarshal(initEnv.Body, &initBody); err != nil {
		logrus.WithError(err).Fatal("failed to parse init body")
	}

	sortedIDs := append([]string(nil), initBody.NodeIDs...)
	sort.Strings(sortedIDs)

	selfIdx := -1
	for i, id := range sortedIDs {
		if id == initBody.NodeID {
			selfIdx = i
			break
		}
	}
	if selfIdx == -1 {
		logrus.Fatalf("node id %s not found in node_ids", initBody.NodeID)
	}

	tr := newTransport(initBody.NodeID)

	localNode := node.NewNode(selfIdx)
	for i, id := range sortedIDs {
		if i == selfIdx {
			continue
		}
		localNode.AppendNode(&remoteNode{id: i, destStr: id, tr: tr})
	}

	tr.reply(initEnv.Src, initBody.MsgID, map[string]any{"type": "init_ok"})
	logrus.Infof("node %s (id %d) initialized with peers %v", initBody.NodeID, selfIdx, sortedIDs)

	go func() {
		if err := localNode.Serve(); err != nil {
			logrus.WithError(err).Error("serve loop exited")
		}
	}()

	go func() {
		for range time.Tick(time.Second) {
			stamp, value := localNode.GetDecision()
			logrus.Infof("DECISION node=%d stamp=%d value=%d", selfIdx, stamp, value)
		}
	}()

	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 1<<20), 1<<20)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var env envelope
		if err := json.Unmarshal(line, &env); err != nil {
			logrus.WithError(err).Warn("failed to parse envelope")
			continue
		}
		var base baseBody
		if err := json.Unmarshal(env.Body, &base); err != nil {
			logrus.WithError(err).Warn("failed to parse body")
			continue
		}

		if base.InReplyTo != 0 {
			tr.pendingMu.Lock()
			ch, ok := tr.pending[base.InReplyTo]
			tr.pendingMu.Unlock()
			if ok {
				ch <- env.Body
			}
			continue
		}

		switch base.Type {
		case "echo":
			var body struct {
				Echo any `json:"echo"`
			}
			json.Unmarshal(env.Body, &body)
			tr.reply(env.Src, base.MsgID, map[string]any{
				"type": "echo_ok",
				"echo": body.Echo,
			})
		case "prepare":
			var body struct {
				FromNode int `json:"from_node"`
				Stamp    int `json:"stamp"`
			}
			json.Unmarshal(env.Body, &body)
			res, err := localNode.PrepareStamp(body.FromNode, body.Stamp)
			if err != nil {
				res = node.PrepareResponse{Promise: false, PromisedStamp: -1}
			}
			tr.reply(env.Src, base.MsgID, map[string]any{
				"type":           "prepare_ok",
				"promise":        res.Promise,
				"value":          res.Value,
				"max_stamp":      res.MaxStamp,
				"promised_stamp": res.PromisedStamp,
			})
		case "accept":
			var body struct {
				FromNode int `json:"from_node"`
				Stamp    int `json:"stamp"`
				Value    int `json:"value"`
			}
			json.Unmarshal(env.Body, &body)
			res, err := localNode.AcceptStamp(body.FromNode, body.Stamp, body.Value)
			if err != nil {
				res = node.AcceptResponse{Accepted: false, PromisedStamp: -1}
			}
			tr.reply(env.Src, base.MsgID, map[string]any{
				"type":           "accept_ok",
				"accepted":       res.Accepted,
				"promised_stamp": res.PromisedStamp,
			})
		case "read":
			stamp, value := localNode.GetDecision()
			if stamp < 0 {
				tr.reply(env.Src, base.MsgID, map[string]any{
					"type": "error", "code": 20, "text": "key does not exist",
				})
				continue
			}
			tr.reply(env.Src, base.MsgID, map[string]any{
				"type": "read_ok", "value": value,
			})
		case "write":
			var body struct {
				Value int `json:"value"`
			}
			json.Unmarshal(env.Body, &body)
			go handleWrite(localNode, tr, env.Src, base.MsgID, body.Value)
		case "cas":
			var body struct {
				From int `json:"from"`
				To   int `json:"to"`
			}
			json.Unmarshal(env.Body, &body)
			stamp, value := localNode.GetDecision()
			switch {
			case stamp < 0:
				tr.reply(env.Src, base.MsgID, map[string]any{
					"type": "error", "code": 20, "text": "key does not exist",
				})
			case value != body.From:
				tr.reply(env.Src, base.MsgID, map[string]any{
					"type": "error", "code": 22, "text": "cas expected value does not match decided value",
				})
			case body.To == body.From:
				tr.reply(env.Src, base.MsgID, map[string]any{"type": "cas_ok"})
			default:
				tr.reply(env.Src, base.MsgID, map[string]any{
					"type": "error", "code": 10, "text": "decided value cannot be changed",
				})
			}
		default:
			logrus.Warnf("unhandled message type %q", base.Type)
		}
	}
	if err := scanner.Err(); err != nil {
		logrus.WithError(err).Error("stdin scanner error")
	}
}

func handleWrite(localNode node.Node, tr *transport, dest string, msgID int, value int) {
	localNode.Propose(value)

	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if stamp, decided := localNode.GetDecision(); stamp >= 0 {
			if decided == value {
				tr.reply(dest, msgID, map[string]any{"type": "write_ok"})
			} else {
				tr.reply(dest, msgID, map[string]any{
					"type": "error", "code": 22, "text": "another value was already decided",
				})
			}
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}
