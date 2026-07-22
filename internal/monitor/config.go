package monitor

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/saucesteals/monitord/internal/model"
	"github.com/saucesteals/monitord/internal/routes"
	"go.yaml.in/yaml/v3"
)

// ConfigFileName is the authored configuration file inside each monitor.
const ConfigFileName = "monitor.yaml"

// Config is the validated runtime configuration loaded from monitor.yaml.
type Config struct {
	Description string
	Clients     int
	Every       time.Duration
	TTL         time.Duration
	Timeout     time.Duration
	Persistent  bool
	// MaxEvents caps how many events one tick may deliver. Zero means the
	// daemon default.
	MaxEvents  int
	ProxyPool  model.PoolName
	Deliveries []routes.Delivery
}

type fileConfig struct {
	Description string      `yaml:"description"`
	Clients     int         `yaml:"clients"`
	Every       string      `yaml:"every"`
	TTL         string      `yaml:"ttl"`
	Timeout     string      `yaml:"timeout"`
	Persistent  bool        `yaml:"persistent"`
	MaxEvents   int         `yaml:"max_events"`
	Proxies     string      `yaml:"proxies"`
	Routes      []fileRoute `yaml:"routes"`
}

type fileRoute struct {
	Route   string         `yaml:"route"`
	Options map[string]any `yaml:"options"`
}

// LoadConfig reads and validates one monitor's authored configuration.
func LoadConfig(dir string) (Config, error) {
	path := filepath.Join(dir, ConfigFileName)
	file, err := os.Open(path)
	if err != nil {
		return Config{}, fmt.Errorf("open %s: %w", path, err)
	}
	defer func() { _ = file.Close() }()

	decoder := yaml.NewDecoder(file)
	decoder.KnownFields(true)
	var raw fileConfig
	if err := decoder.Decode(&raw); err != nil {
		return Config{}, fmt.Errorf("parse %s: %w", path, err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return Config{}, fmt.Errorf("parse %s: multiple YAML documents are not supported", path)
		}

		return Config{}, fmt.Errorf("parse %s: %w", path, err)
	}

	config, err := raw.validate()
	if err != nil {
		return Config{}, fmt.Errorf("invalid %s: %w", path, err)
	}

	return config, nil
}

func (raw fileConfig) validate() (Config, error) {
	clients := raw.Clients
	if clients == 0 {
		clients = 1
	}
	if clients < 0 {
		return Config{}, errors.New("clients must be positive")
	}

	if raw.MaxEvents < 0 {
		return Config{}, errors.New("max_events cannot be negative")
	}

	every, err := requiredDuration("every", raw.Every)
	if err != nil {
		return Config{}, err
	}
	timeout := 30 * time.Second
	if strings.TrimSpace(raw.Timeout) != "" {
		timeout, err = requiredDuration("timeout", raw.Timeout)
		if err != nil {
			return Config{}, err
		}
	}

	var ttl time.Duration
	if raw.Persistent {
		if strings.TrimSpace(raw.TTL) != "" {
			return Config{}, errors.New("ttl cannot be combined with persistent: true")
		}
	} else {
		ttl, err = requiredDuration("ttl", raw.TTL)
		if err != nil {
			return Config{}, err
		}
	}

	proxyPool, err := model.ParsePoolName(strings.TrimSpace(raw.Proxies))
	if err != nil {
		return Config{}, err
	}
	if len(raw.Routes) == 0 {
		return Config{}, errors.New("at least one route is required")
	}

	deliveries := make([]routes.Delivery, 0, len(raw.Routes))
	seen := make(map[model.RouteName]struct{}, len(raw.Routes))
	for index, item := range raw.Routes {
		name, err := model.ParseRouteName(strings.TrimSpace(item.Route))
		if err != nil {
			return Config{}, fmt.Errorf("routes[%d]: %w", index, err)
		}
		if _, exists := seen[name]; exists {
			return Config{}, fmt.Errorf("route %s is listed more than once", name)
		}
		seen[name] = struct{}{}

		options, err := scalarOptions(item.Options)
		if err != nil {
			return Config{}, fmt.Errorf("route %s: %w", name, err)
		}
		deliveries = append(deliveries, routes.Delivery{Route: name, Options: options})
	}

	return Config{
		Description: strings.TrimSpace(raw.Description),
		Clients:     clients,
		Every:       every,
		TTL:         ttl,
		Timeout:     timeout,
		Persistent:  raw.Persistent,
		MaxEvents:   raw.MaxEvents,
		ProxyPool:   proxyPool,
		Deliveries:  deliveries,
	}, nil
}

func requiredDuration(field string, value string) (time.Duration, error) {
	if strings.TrimSpace(value) == "" {
		return 0, fmt.Errorf("%s is required", field)
	}
	duration, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", field, err)
	}
	if duration <= 0 {
		return 0, fmt.Errorf("%s must be positive", field)
	}

	return duration, nil
}

func scalarOptions(raw map[string]any) (routes.Options, error) {
	options := make(routes.Options, len(raw))
	for key, value := range raw {
		key = routes.NormalizeOptionKey(key)
		if key == "" {
			return nil, errors.New("route option names must not be empty")
		}

		switch typed := value.(type) {
		case string:
			options[key] = typed
		case bool:
			options[key] = strconv.FormatBool(typed)
		case int:
			options[key] = strconv.Itoa(typed)
		case int64:
			options[key] = strconv.FormatInt(typed, 10)
		case uint64:
			options[key] = strconv.FormatUint(typed, 10)
		case float64:
			options[key] = strconv.FormatFloat(typed, 'g', -1, 64)
		case nil:
			options[key] = ""
		default:
			return nil, fmt.Errorf("route option %q must be a scalar value", key)
		}
	}

	return options, nil
}
