package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"
)

// Node represents a node in a network with a node ID and a list of peer IDs.
type Node struct {
	ID                  string
	PeerIDs             []interface{}
	UnackedPeers        []interface{}
	Messages            []interface{}
	Callbacks           map[float64]func(map[string]interface{})
	CallbackMutex       sync.RWMutex
	MsgResponseChannels map[float64]chan map[string]interface{}
	StdinChan           chan map[string]interface{}
}

// Init initializes the node with a node ID and a list of peer IDs.
func (n *Node) Init(id string) {
	n.ID = id
	n.Callbacks = make(map[float64]func(map[string]interface{}))
	n.MsgResponseChannels = make(map[float64]chan map[string]interface{})
	n.StdinChan = make(chan map[string]interface{})
	// read continuously from stdinChan
	go func() {
		for {
			msg := <-n.StdinChan
			n.HandleStdinMessage(msg)
		}
	}()

}

// HandleStdinMessage handles a message from stdin.
func (n *Node) HandleStdinMessage(jsonMap map[string]interface{}) {
	if jsonMap["body"] != nil {
		body := jsonMap["body"].(map[string]interface{})
		if body["type"] != nil {
			switch body["type"] {
			case "echo":
				n.Echo(jsonMap)
			case "topology":
				n.Topology(jsonMap)
			case "broadcast_ok":
				if body["in_reply_to"] != nil {
					n.CallbackMutex.RLock()
					msg_id := body["in_reply_to"].(float64)
					n.MsgResponseChannels[msg_id] <- jsonMap
					n.CallbackMutex.RUnlock()
				}
			case "broadcast":
				n.Broadcast(jsonMap)
			case "read":
				n.Read(jsonMap)
			}

		}
	}
}

// InitReply fucntion for Node
func (n *Node) InitReply(req map[string]interface{}) {
	ok := map[string]interface{}{"type": "init_ok"}
	reply(req, ok)
}

// Echo sends an echo message to a specific peer.
func (n *Node) Echo(req map[string]interface{}) {
	echo := req["body"].(map[string]interface{})["echo"].(string)
	reply(req, map[string]interface{}{"type": "echo_ok", "echo": echo})
}

// Broadcast sends a broadcast message to all of the node's peers.
func (n *Node) Broadcast(req map[string]interface{}) {
	msg := req["body"].(map[string]interface{})["message"]
	// check if this msg is already in the list
	for _, m := range n.Messages {
		if m == msg {
			return
		}
	}
	// reply only if 'msg_id' is in 'body'
	if req["body"].(map[string]interface{})["msg_id"] != nil {
		reply(req, map[string]interface{}{"type": "broadcast_ok"})
	}

	// add to list of messages
	n.Messages = append(n.Messages, msg)

	retryDelay := 1 * time.Second
	for _, p := range n.PeerIDs {

		msgChannel := make(chan map[string]interface{})
		msg_id := rand.Float64()
		n.CallbackMutex.Lock()
		n.MsgResponseChannels[msg_id] = msgChannel
		n.CallbackMutex.Unlock()

		go func(p interface{}, msgChannel chan map[string]interface{}, msg_id float64) {
			n.Send(p, map[string]interface{}{"type": "broadcast", "message": msg, "msg_id": msg_id})
			for {
				select {
				case <-msgChannel:
					n.CallbackMutex.Lock()
					delete(n.MsgResponseChannels, msg_id)
					n.CallbackMutex.Unlock()
					return
				case <-time.After(retryDelay):
					n.Send(p, map[string]interface{}{"type": "broadcast", "message": msg, "msg_id": msg_id})
				}
			}
		}(p, msgChannel, msg_id)

	}
}

// Topology sets peers of the node.
func (n *Node) Topology(req map[string]interface{}) {
	topo := req["body"].(map[string]interface{})["topology"].(map[string]interface{})
	if topo[n.ID] != nil {
		n.PeerIDs = topo[n.ID].([]interface{})
	}
	reply(req, map[string]interface{}{"type": "topology_ok"})
}

// Read function reply messages
func (n *Node) Read(req map[string]interface{}) {
	reply(req, map[string]interface{}{"type": "read_ok", "messages": n.Messages})
}

// Send message to neighbors
func (n *Node) Send(dest interface{}, msg map[string]interface{}) {
	// create a map to hold the message
	msgMap := make(map[string]interface{})
	msgMap["src"] = n.ID
	msgMap["dest"] = dest
	msgMap["body"] = msg

	enc := json.NewEncoder(os.Stdout)
	err := enc.Encode(msgMap)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error encoding json: %v", err)
	}
}

// rpc function to add to callback and reply to messages
func (n *Node) Rpc(dest interface{}, req map[string]interface{}, callback func(map[string]interface{})) {
	n.CallbackMutex.Lock()
	n.Callbacks[req["body"].(map[string]interface{})["msg_id"].(float64)] = callback
	n.CallbackMutex.Unlock()
	msg_id := req["body"].(map[string]interface{})["msg_id"].(float64) + 1
	n.Send(dest, map[string]interface{}{"type": "broadcast", "message": req["body"].(map[string]interface{})["message"], "msg_id": msg_id})
}
