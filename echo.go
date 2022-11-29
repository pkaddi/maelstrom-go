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
	fmt.Fprintln(os.Stderr, reply)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error encoding json: %v", err)
	}
}

func main() {
	// loops over lines from stdin, printing out each one to stderr as it's received
	// and then echoing it back to stdout.
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		// parse each line as json
		jsonMap := make(map[string]interface{})
		err := json.Unmarshal(scanner.Bytes(), &jsonMap)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error parsing json: %v", err)
			continue
		}

		// if json has a nested "type" field inside "body" field, switch based on that
		if jsonMap["body"] != nil {
			body := jsonMap["body"].(map[string]interface{})
			if body["type"] != nil {
				switch body["type"] {
				case "echo":
					//body["type"] = "echo_ok"
					reply(jsonMap, map[string]interface{}{"type": "echo_ok", "echo": body["echo"]})
				case "init":
					ok := map[string]interface{}{"type": "init_ok"}
					reply(jsonMap, ok)
				}
			}
		}
		//fmt.Fprintln(os.Stderr, jsonMap["node_id"])
		//fmt.Println(scanner.Text())
	}
}
