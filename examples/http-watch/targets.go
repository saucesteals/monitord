package main

import (
	"encoding/json"
	"fmt"
	"os"
)

type Target struct {
	Name string `json:"name"`
	URL  string `json:"url"`
}

func loadTargets(path string) ([]Target, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var targets []Target
	if err := json.Unmarshal(contents, &targets); err != nil {
		return nil, err
	}
	if len(targets) == 0 {
		return nil, fmt.Errorf("%s lists no targets", path)
	}

	return targets, nil
}
