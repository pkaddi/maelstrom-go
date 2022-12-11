package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// create a GSet node to add and read elements from a set
type GSetServer struct {
	ID            string
	Crdt          GSet
	StdinChan     chan map[string]interface{}
	AllNodes      []interface{}
	PeriodicTasks []map[string]interface{}
}

// add task to periodic tasks
func (g *GSetServer) AddPeriodicTask(task map[string]interface{}) {
	g.PeriodicTasks = append(g.PeriodicTasks, task)
}

// run periodic tasks
func (g *GSetServer) RunPeriodicTasks() {
	// get seconds from task and run the function every 'seconds'
	for _, task := range g.PeriodicTasks {
		// convert interface{} to int
		seconds := task["seconds"].(int)
		go func(task map[string]interface{}) {
			for {
				time.Sleep(time.Duration(seconds) * time.Second)
				task["function"].(func())()
			}
		}(task)
	}
}

// Init initializes the GSet with a node.
func (g *GSetServer) Init(jsonMap map[string]interface{}) {
	body := jsonMap["body"].(map[string]interface{})
	g.ID = body["node_id"].(string)
	g.StdinChan = make(chan map[string]interface{})
	g.AllNodes = jsonMap["body"].(map[string]interface{})["node_ids"].([]interface{})
	g.Crdt.Init()
	go func() {
		for {
			msg := <-g.StdinChan
			g.HandleStdinMessage(msg)
		}
	}()

	g.AddPeriodicTask(map[string]interface{}{"seconds": 5, "function": func() {
		// send a read request to all nodes
		for _, node := range g.AllNodes {
			g.Send(node, map[string]interface{}{"type": "replicate", "value": g.Crdt.Read()})
		}
	}})
	g.RunPeriodicTasks()
}

// Send message to neighbors
func (g *GSetServer) Send(dest interface{}, msg map[string]interface{}) {
	// create a map to hold the message
	msgMap := make(map[string]interface{})
	msgMap["src"] = g.ID
	msgMap["dest"] = dest
	msgMap["body"] = msg

	enc := json.NewEncoder(os.Stdout)
	err := enc.Encode(msgMap)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error encoding json: %v", err)
	}
}

// HandleStdinMessage handles a message from stdin.
func (g *GSetServer) HandleStdinMessage(jsonMap map[string]interface{}) {
	if jsonMap["body"] != nil {
		body := jsonMap["body"].(map[string]interface{})
		if body["type"] != nil {
			switch body["type"] {
			case "read":
				g.Read(jsonMap)
			case "add":
				g.Add(jsonMap)
			case "replicate":
				g.Replicate(jsonMap)
			}

		}
	}
}

// Add adds an element to the set.
func (g *GSetServer) Add(jsonMap map[string]interface{}) {
	body := jsonMap["body"].(map[string]interface{})
	g.Crdt.Add(body["element"])
	reply(jsonMap, map[string]interface{}{"type": "add_ok"})
}

// Read returns the set of elements.
func (g *GSetServer) Read(jsonMap map[string]interface{}) {
	reply(jsonMap, map[string]interface{}{"type": "read_ok", "value": g.Crdt.Read()})
}

// InitReply fucntion for Node
func (g *GSetServer) InitReply(req map[string]interface{}) {
	ok := map[string]interface{}{"type": "init_ok"}
	reply(req, ok)
}

// replicate function
func (g *GSetServer) Replicate(jsonMap map[string]interface{}) {
	newCrdt := g.Crdt.FromJSON(jsonMap["body"].(map[string]interface{})["value"].([]interface{}))
	g.Crdt.Merge(newCrdt)
}

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
					gset := new(GSetServer)
					gset.Init(jsonMap)
					gset.InitReply(jsonMap)
					nodes[body["node_id"].(string)] = gset.StdinChan

				default:
					if nodes[jsonMap["dest"].(string)] != nil {
						nodes[jsonMap["dest"].(string)] <- jsonMap
					}
				}

			}
		}

	}
}
