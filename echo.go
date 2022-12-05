package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
)

func reply(req map[string]interface{}, body map[string]interface{}) {
	reply := make(map[string]interface{})
	reply["src"] = req["dest"]
	reply["dest"] = req["src"]
	body["msg_id"] = req["body"].(map[string]interface{})["msg_id"].(float64) + 1
	body["in_reply_to"] = req["body"].(map[string]interface{})["msg_id"]
	reply["body"] = body

	enc := json.NewEncoder(os.Stdout)
	err := enc.Encode(reply)
	//fmt.Fprintln(os.Stderr, reply)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error encoding json: %v", err)
	}
}

func main() {

	scanner := bufio.NewScanner(os.Stdin)
	nodes := make(map[string]*Node)
	for scanner.Scan() {

		jsonMap := make(map[string]interface{})
		err := json.Unmarshal(scanner.Bytes(), &jsonMap)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error parsing json: %v", err)
			continue
		}

		// make this a goroutine

		if jsonMap["body"] != nil {
			body := jsonMap["body"].(map[string]interface{})
			if body["type"] != nil {
				switch body["type"] {
				case "echo":
					nodes[jsonMap["dest"].(string)].Echo(jsonMap)
				case "init":
					node := new(Node)
					node.Init(body["node_id"].(string))
					node.InitReply(jsonMap)
					nodes[body["node_id"].(string)] = node
				case "topology":
					nodes[jsonMap["dest"].(string)].Topology(jsonMap)
				case "broadcast_ok":
					if body["in_reply_to"] != nil {
						// fmt.Fprintf(os.Stderr, "broadcast: %v", body)
						// nodes[body["node_id"].(string)].CallbackMutex.RLock()
						// if nodes[body["node_id"].(string)].Callbacks[body["in_reply_to"].(float64)] != nil {
						// 	nodes[body["node_id"].(string)].Callbacks[body["in_reply_to"].(float64)](jsonMap)
						// }
						// nodes[body["node_id"].(string)].CallbackMutex.RUnlock()
						// send response to channel
						if nodes[jsonMap["dest"].(string)] != nil {
							// print msgresponsechannels
							nodes[jsonMap["dest"].(string)].CallbackMutex.RLock()
							msg_id := strconv.FormatFloat(body["in_reply_to"].(float64), 'E', -1, 32) + "_" + jsonMap["src"].(string)
							//fmt.Fprintf(os.Stderr, "msgresponsechannels: %v", nodes[jsonMap["dest"].(string)].MsgResponseChannels)
							// print msg_id
							//fmt.Fprintf(os.Stderr, "msg_id: %v", msg_id)
							nodes[jsonMap["dest"].(string)].MsgResponseChannels[msg_id] <- jsonMap
							nodes[jsonMap["dest"].(string)].CallbackMutex.RUnlock()
						}
					}
				case "broadcast":
					nodes[jsonMap["dest"].(string)].Broadcast(jsonMap)
				case "read":
					nodes[jsonMap["dest"].(string)].Read(jsonMap)
				}

			}
		}

	}
}
