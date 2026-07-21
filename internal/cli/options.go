package cli

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/saucesteals/monitord/internal/routes"
)

func readRouteOptions(values []string, files []string) (routes.Options, error) {
	options := routes.Options{}
	for _, raw := range values {
		key, value, err := splitRouteOption(raw)
		if err != nil {
			return nil, err
		}
		if _, exists := options[key]; exists {
			return nil, fmt.Errorf("route option %q was provided more than once", key)
		}
		options[key] = value
	}
	for _, raw := range files {
		key, path, err := splitRouteOption(raw)
		if err != nil {
			return nil, err
		}
		if _, exists := options[key]; exists {
			return nil, fmt.Errorf("route option %q was provided more than once", key)
		}
		contents, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read route option %s from %s: %w", key, path, err)
		}
		options[key] = strings.TrimSpace(string(contents))
	}

	return options, nil
}

func splitRouteOption(raw string) (string, string, error) {
	key, value, ok := strings.Cut(raw, "=")
	key = routes.NormalizeOptionKey(key)
	if !ok || key == "" {
		return "", "", errors.New("route options must use key=value")
	}

	return key, value, nil
}
