package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
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
	MsgResponseChannels map[string]chan map[string]interface{}
}

// Init initializes the node with a node ID and a list of peer IDs.
func (n *Node) Init(id string) {
	n.ID = id
	n.Callbacks = make(map[float64]func(map[string]interface{}))
	n.MsgResponseChannels = make(map[string]chan map[string]interface{})
}

// main function to read json from stdin and create a node for every 'node_id' field. and respond to commands based on the 'type' field.

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

	// create a list of unacknowledged peers
	// n.UnackedPeers = make([]interface{}, len(n.PeerIDs))
	// copy(n.UnackedPeers, n.PeerIDs)
	// // delete the sender from the list
	// for i, p := range n.UnackedPeers {
	// 	if p == req["src"] {
	// 		n.UnackedPeers = append(n.UnackedPeers[:i], n.UnackedPeers[i+1:]...)
	// 	}
	// }

	// use BroadcastResponseChannel to wait for all replies and keep retrying until all peers have acknowledged. also send the message to all peers
	//for len(n.UnackedPeers) > 0 {
	// make n.MsgResponseChannels

	retryDelay := 1 * time.Second
	for _, p := range n.PeerIDs {

		msgChannel := make(chan map[string]interface{})
		// make msd_id as a combination of msg_id and peer_id
		msg_id := strconv.FormatFloat(req["body"].(map[string]interface{})["msg_id"].(float64), 'E', -1, 32) + "_" + p.(string)
		n.CallbackMutex.Lock()
		n.MsgResponseChannels[msg_id] = msgChannel
		n.CallbackMutex.Unlock()

		go func(p interface{}, msgChannel chan map[string]interface{}, msg_id string) {
			// retry count for each peer
			//retryCount := 0
			n.Send(p, map[string]interface{}{"type": "broadcast", "message": msg, "msg_id": req["body"].(map[string]interface{})["msg_id"]})
			for {
				select {
				case jsonMap := <-msgChannel:
					fmt.Fprintf(os.Stderr, "received broadcast_ok from peer")
					fmt.Fprintf(os.Stderr, "jsonmap count: %v", jsonMap)
					n.CallbackMutex.Lock()
					delete(n.MsgResponseChannels, msg_id)
					n.CallbackMutex.Unlock()
					return
				case <-time.After(retryDelay):
					//if retryCount < 3 {
					//fmt.Fprintf(os.Stderr, "sending broadcast to peers")
					n.Send(p, map[string]interface{}{"type": "broadcast", "message": msg, "msg_id": req["body"].(map[string]interface{})["msg_id"]})
					//retryCount++
					//}
				}
			}
		}(p, msgChannel, msg_id)

	}
	// for len(n.UnackedPeers) > 0 {
	// 	select {
	// 	case resp := <-n.BroadcastResponseChannel:
	// 		for i, p := range n.UnackedPeers {
	// 			if p == resp["src"] {
	// 				n.UnackedPeers = append(n.UnackedPeers[:i], n.UnackedPeers[i+1:]...)
	// 			}
	// 		}
	// 	}
	// }
	//}
}

// n.BroadcastResponseChannel = make(chan map[string]interface{})
// go func() {
// 	for {
// 		select {
// 		case resp := <-n.BroadcastResponseChannel:
// 			// remove the peer from the list of unacknowledged peers
// 			for i, p := range n.UnackedPeers {
// 				if p == resp["src"] {
// 					n.UnackedPeers = append(n.UnackedPeers[:i], n.UnackedPeers[i+1:]...)
// 				}
// 			}
// 			// if all peers have acknowledged, return
// 			if len(n.UnackedPeers) == 0 {
// 				return
// 			}
// 		}
// 	}
// }()

// while len(peers) > 0

// send broadcast message to all peers
// fmt.Fprintln(os.Stderr, n.UnackedPeers)
// //for len(n.UnackedPeers) > 0 {
// //fmt.Fprintf(os.Stderr, "unacked peers: %v", n.UnackedPeers)
// for i, p := range n.UnackedPeers {

// 	n.Rpc(p, req, func(req map[string]interface{}) {
// 		if req["body"].(map[string]interface{})["type"] == "broadcast_ok" {
// 			unackedPeers := make([]interface{}, len(n.UnackedPeers))
// 			// Copy the elements of 'n.UnackedPeers' into the new slice, skipping
// 			// the element at index 'i'.
// 			copy(unackedPeers, n.UnackedPeers[:i])
// 			copy(unackedPeers[i:], n.UnackedPeers[i+1:])
// 			// Replace 'n.UnackedPeers' with the new slice that doesn't include
// 			// the element at index 'i'.
// 			n.UnackedPeers = unackedPeers
// 		}
// 	})
// 	// correct broadcast message send
// 	//n.Send(p, map[string]interface{}{"type": "broadcast", "message": msg})
// }
//}

//fmt.Fprintln(os.Stderr, n.Messages)
//}

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
