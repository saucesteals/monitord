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

	monitord "github.com/saucesteals/monitord"
	"github.com/saucesteals/monitord/internal/model"
	"github.com/saucesteals/monitord/internal/routes"
	"go.yaml.in/yaml/v3"
)

// ConfigFileName is the authored configuration file inside each monitor.
const ConfigFileName = "monitor.yaml"

// Config is the validated runtime configuration loaded from monitor.yaml.
type Config struct {
	TTL        time.Duration
	Persistent bool
	Deliveries []routes.Delivery
	Policy     monitord.DeploymentPolicy
}

type fileConfig struct {
	TTL        string         `yaml:"ttl"`
	Persistent bool           `yaml:"persistent"`
	Deliveries []fileDelivery `yaml:"deliveries"`
	Routes     []fileRoute    `yaml:"routes"`
	Health     fileHealth     `yaml:"health"`
	Events     fileEvents     `yaml:"events"`
}

type fileHealth struct {
	FailureThreshold *int `yaml:"failure_threshold"`
}

type fileEvents struct {
	MaxPerTransaction *int   `yaml:"max_per_transaction"`
	Retention         string `yaml:"retention"`
}

type fileDelivery struct {
	Discord   *fileDiscord  `yaml:"discord"`
	RateLimit fileRateLimit `yaml:"rate_limit"`
}

type fileDiscord struct {
	Account    string `yaml:"account"`
	ChannelID  string `yaml:"channel_id"`
	ThreadID   string `yaml:"thread_id"`
	WebhookURL string `yaml:"webhook_url"`
	Mentions   string `yaml:"mentions"`
}

type fileRoute struct {
	Route     string         `yaml:"route"`
	Options   map[string]any `yaml:"options"`
	RateLimit fileRateLimit  `yaml:"rate_limit"`
}

type fileRateLimit struct {
	PerSecond float64 `yaml:"per_second"`
	Burst     int     `yaml:"burst"`
}

func (limit fileRateLimit) rateLimit() routes.RateLimit {
	return routes.RateLimit{PerSecond: limit.PerSecond, Burst: limit.Burst}
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
	policy := monitord.DeploymentPolicy{
		Health: monitord.HealthPolicy{FailureThreshold: 3},
		Events: monitord.EventPolicy{
			MaxPerTransaction: monitord.MaxEventsPerTransaction,
			Retention:         30 * 24 * time.Hour,
		},
	}
	if raw.Health.FailureThreshold != nil {
		policy.Health.FailureThreshold = *raw.Health.FailureThreshold
	}
	if raw.Events.MaxPerTransaction != nil {
		policy.Events.MaxPerTransaction = *raw.Events.MaxPerTransaction
	}
	if value := strings.TrimSpace(raw.Events.Retention); value != "" {
		retention, err := parseDuration(value)
		if err != nil {
			return Config{}, fmt.Errorf("events.retention: %w", err)
		}
		policy.Events.Retention = retention
	}
	if err := policy.Validate(); err != nil {
		return Config{}, err
	}

	var ttl time.Duration
	var err error
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

	deliveries := make([]routes.Delivery, 0, len(raw.Deliveries)+len(raw.Routes))
	for index, item := range raw.Deliveries {
		if item.Discord == nil {
			return Config{}, fmt.Errorf("deliveries[%d]: discord is required", index)
		}
		delivery := routes.Delivery{Discord: &routes.Discord{
			Account:    strings.TrimSpace(item.Discord.Account),
			ChannelID:  strings.TrimSpace(item.Discord.ChannelID),
			ThreadID:   strings.TrimSpace(item.Discord.ThreadID),
			WebhookURL: strings.TrimSpace(item.Discord.WebhookURL),
			Mentions:   strings.TrimSpace(item.Discord.Mentions),
		}, RateLimit: item.RateLimit.rateLimit()}
		if err := delivery.Validate(); err != nil {
			return Config{}, fmt.Errorf("deliveries[%d]: %w", index, err)
		}

		deliveries = append(deliveries, delivery)
	}
	for index, item := range raw.Routes {
		name, err := model.ParseRouteName(strings.TrimSpace(item.Route))
		if err != nil {
			return Config{}, fmt.Errorf("routes[%d]: %w", index, err)
		}
		options, err := scalarOptions(item.Options)
		if err != nil {
			return Config{}, fmt.Errorf("route %s: %w", name, err)
		}
		delivery := routes.Delivery{
			Route: name, Options: options, RateLimit: item.RateLimit.rateLimit(),
		}
		if err := delivery.Validate(); err != nil {
			return Config{}, fmt.Errorf("routes[%d]: %w", index, err)
		}
		deliveries = append(deliveries, delivery)
	}

	return Config{
		TTL: ttl, Persistent: raw.Persistent, Deliveries: deliveries, Policy: policy,
	}, nil
}

func parseDuration(value string) (time.Duration, error) {
	if strings.HasSuffix(value, "d") {
		days, err := strconv.ParseInt(strings.TrimSuffix(value, "d"), 10, 32)
		if err != nil || days <= 0 {
			return 0, fmt.Errorf("%q is not a positive duration", value)
		}
		return time.Duration(days) * 24 * time.Hour, nil
	}
	duration, err := time.ParseDuration(value)
	if err != nil {
		return 0, err
	}
	if duration <= 0 {
		return 0, errors.New("must be positive")
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
