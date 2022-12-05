package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
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
	nodes := make(map[string]chan map[string]interface{})
	for scanner.Scan() {

		jsonMap := make(map[string]interface{})
		err := json.Unmarshal(scanner.Bytes(), &jsonMap)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error parsing json: %v", err)
			continue
		}

		if jsonMap["body"] != nil {
			body := jsonMap["body"].(map[string]interface{})
			if body["type"] != nil {
				switch body["type"] {
				case "init":
					node := new(Node)
					node.Init(body["node_id"].(string))
					node.InitReply(jsonMap)
					nodes[body["node_id"].(string)] = node.StdinChan

				default:
					if nodes[jsonMap["dest"].(string)] != nil {
						nodes[jsonMap["dest"].(string)] <- jsonMap
					}
				}

			}
		}

	}
}
