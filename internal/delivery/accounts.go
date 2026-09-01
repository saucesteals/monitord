package delivery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"slices"
	"strings"
)

const (
	accountRegistryService = "monitord.accounts"
	accountRegistryName    = "registry"
)

// Account identifies one named delivery credential. It never contains a token.
type Account struct {
	Kind string `json:"kind"`
	Name string `json:"name"`
}

// StoreAccountToken stores one delivery credential in the logged-in user's
// macOS Keychain. Tokens never enter monitor YAML or SQLite.
func StoreAccountToken(ctx context.Context, kind string, account string, token string) error {
	if err := validateAccount(kind, account); err != nil {
		return err
	}
	token = strings.TrimSpace(token)
	if token == "" {
		return fmt.Errorf("%s token for account %q is empty", kind, account)
	}

	if err := keychainSet(ctx, keychainService(kind), account, token); err != nil {
		return fmt.Errorf("store %s account %q in Keychain: %w", kind, account, err)
	}
	accounts, err := ListAccounts(ctx)
	if err != nil {
		return err
	}
	entry := Account{Kind: kind, Name: account}
	if !slices.Contains(accounts, entry) {
		accounts = append(accounts, entry)
	}
	if err := storeAccountRegistry(ctx, accounts); err != nil {
		return err
	}

	return nil
}

// AccountToken loads one delivery credential from the logged-in user's macOS
// Keychain. The returned token must never be logged or persisted.
func AccountToken(ctx context.Context, kind string, account string) (string, error) {
	if err := validateAccount(kind, account); err != nil {
		return "", err
	}

	token, err := keychainGet(ctx, keychainService(kind), account)
	if err != nil {
		return "", fmt.Errorf("load %s account %q from Keychain: %w", kind, account, err)
	}
	if token == "" {
		return "", fmt.Errorf("%s account %q has an empty Keychain token", kind, account)
	}

	return token, nil
}

// ListAccounts returns registered account names and kinds, never token values.
func ListAccounts(ctx context.Context) ([]Account, error) {
	raw, err := keychainGet(ctx, accountRegistryService, accountRegistryName)
	if err != nil {
		if isKeychainNotFound(err) {
			return nil, nil
		}

		return nil, fmt.Errorf("load account registry from Keychain: %w", err)
	}

	var accounts []Account
	if err := json.Unmarshal([]byte(raw), &accounts); err != nil {
		return nil, fmt.Errorf("decode account registry: %w", err)
	}
	for _, account := range accounts {
		if err := validateAccount(account.Kind, account.Name); err != nil {
			return nil, fmt.Errorf("invalid account registry: %w", err)
		}
	}
	slices.SortFunc(accounts, func(left Account, right Account) int {
		return strings.Compare(left.Kind+":"+left.Name, right.Kind+":"+right.Name)
	})

	return accounts, nil
}

// RemoveAccount deletes one exact credential and its non-secret registry entry.
func RemoveAccount(ctx context.Context, kind string, account string) error {
	if err := validateAccount(kind, account); err != nil {
		return err
	}
	if err := keychainDelete(ctx, keychainService(kind), account); err != nil {
		return fmt.Errorf("remove %s account %q from Keychain: %w", kind, account, err)
	}

	accounts, err := ListAccounts(ctx)
	if err != nil {
		return err
	}
	accounts = slices.DeleteFunc(accounts, func(item Account) bool {
		return item.Kind == kind && item.Name == account
	})

	return storeAccountRegistry(ctx, accounts)
}

func storeAccountRegistry(ctx context.Context, accounts []Account) error {
	raw, err := json.Marshal(accounts)
	if err != nil {
		return fmt.Errorf("encode account registry: %w", err)
	}
	if err := keychainSet(ctx, accountRegistryService, accountRegistryName, string(raw)); err != nil {
		return fmt.Errorf("store account registry in Keychain: %w", err)
	}

	return nil
}

func keychainSet(ctx context.Context, service string, account string, value string) error {
	cmd := exec.CommandContext(ctx, "security", "add-generic-password", "-U", "-s", service, "-a", account, "-w", value)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%w: %s", err, strings.TrimSpace(string(output)))
	}

	return nil
}

func keychainGet(ctx context.Context, service string, account string) (string, error) {
	cmd := exec.CommandContext(ctx, "security", "find-generic-password", "-s", service, "-a", account, "-w")
	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("%w: %s", err, strings.TrimSpace(string(output)))
	}

	return strings.TrimSpace(string(output)), nil
}

func keychainDelete(ctx context.Context, service string, account string) error {
	cmd := exec.CommandContext(ctx, "security", "delete-generic-password", "-s", service, "-a", account)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%w: %s", err, strings.TrimSpace(string(output)))
	}

	return nil
}

func keychainService(kind string) string { return "monitord.account." + kind }

func validateAccount(kind string, account string) error {
	if !isAccountKind(kind) {
		return fmt.Errorf("invalid account kind %q", kind)
	}
	if !isAccountName(account) {
		return fmt.Errorf("invalid %s account %q", kind, account)
	}

	return nil
}

func isKeychainNotFound(err error) bool {
	return errors.Is(err, exec.ErrNotFound) || strings.Contains(err.Error(), "could not be found")
}

func isAccountKind(kind string) bool {
	switch kind {
	case "discord", "openclaw":
		return true
	default:
		return false
	}
}
