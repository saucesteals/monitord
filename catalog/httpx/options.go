package httpx

import (
	"errors"
	"strconv"
	"strings"

	"github.com/saucesteals/mimic"
	"github.com/saucesteals/mimic/chromium"
)

// DefaultChromeVersion is the Chrome stable milestone used by new clients unless
// overridden with WithChromeVersion. Uses a reduced major.0.0.0 version.
// Milestone verified for macOS on 2026-10-03 against
// https://chromiumdash.appspot.com/fetch_releases?channel=Stable&platform=Mac&num=1.
// This is a pinned default; constructing a client never fetches release metadata.
const DefaultChromeVersion = "154.0.0.0"

// Option configures a direct client or every client in a proxy pool.
type Option func(*clientOptions) error

type clientOptions struct {
	chromeVersion string
	profile       *mimic.Profile
}

// WithChromeVersion sets the Chromium version supplied to Mimic. The version
// must contain four unsigned decimal components (for example, "147.0.0.0")
// and a major version of at least 107. Mimic rejects versions 115–119 and
// 124–130 because they require obsolete draft-Kyber. The last option wins.
// Mimic controls the available TLS profiles; a version override does not add a
// new TLS implementation or guarantee an exact fingerprint for that release.
func WithChromeVersion(version string) Option {
	return func(options *clientOptions) error {
		parts := strings.Split(version, ".")
		if len(parts) != 4 {
			return errors.New("chrome version must contain four unsigned decimal components")
		}
		for i, part := range parts {
			n, err := strconv.ParseUint(part, 10, 32)
			if err != nil {
				return errors.New("chrome version must contain four unsigned decimal components")
			}
			if i == 0 && n < 107 {
				return errors.New("chrome major version must be at least 107")
			}
		}
		options.chromeVersion = version

		return nil
	}
}

func resolveOptions(options []Option) (clientOptions, error) {
	config := clientOptions{chromeVersion: DefaultChromeVersion}
	for _, option := range options {
		if option == nil {
			return clientOptions{}, errors.New("httpx option must not be nil")
		}
		if err := option(&config); err != nil {
			return clientOptions{}, err
		}
	}

	profile, err := chromium.New(chromium.Config{
		Brand:    chromium.Chrome,
		Version:  config.chromeVersion,
		Platform: chromium.Mac,
	})
	if err != nil {
		return clientOptions{}, err
	}
	config.profile = profile

	return config, nil
}
