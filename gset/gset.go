package main

import (
	"encoding/json"
	"fmt"
	"os"
)

type GSet struct {
	Set []interface{}
}

// init initializes the GSet with a node.
func (g *GSet) Init() {
	g.Set = make([]interface{}, 0)
}

// FromJSON converts a JSON string to a GSet. this should return a GSet
// and an error.
func (g *GSet) FromJSON(jsonString []interface{}) *GSet {
	newGSet := new(GSet)
	newGSet.Init()
	for _, v := range jsonString {
		newGSet.Add(v)
	}
	return newGSet
}

// ToJSON converts a GSet to a JSON string.
func (g *GSet) ToJSON() (string, error) {
	jsonString, err := json.Marshal(g)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error encoding json: %v", err)
		return "", err
	}
	return string(jsonString), nil
}

// Read returns the set of elements in the GSet.
func (g *GSet) Read() []interface{} {
	return g.Set
}

// Merge merges two GSets.
func (g *GSet) Merge(g2 *GSet) {
	for _, v := range g2.Set {
		found := false
		for _, w := range g.Set {
			if v == w {
				found = true
				break
			}
		}
		if !found {
			g.Set = append(g.Set, v)
		}
	}
}

// Add adds an element to the GSet.
func (g *GSet) Add(element interface{}) {
	for _, v := range g.Set {
		if v == element {
			return
		}
	}
	g.Set = append(g.Set, element)
}
