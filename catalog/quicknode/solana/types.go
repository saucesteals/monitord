package solana

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/mr-tron/base58/base58"
)

type Commitment string

const (
	Processed Commitment = "processed"
	Confirmed Commitment = "confirmed"
	Finalized Commitment = "finalized"
)

func ParseCommitment(value string) (Commitment, error) {
	commitment := Commitment(value)
	if err := commitment.Validate(); err != nil {
		return "", err
	}
	return commitment, nil
}

func (c Commitment) Validate() error {
	switch c {
	case Processed, Confirmed, Finalized:
		return nil
	default:
		return fmt.Errorf("solana commitment %q is invalid", c)
	}
}

func (c *Commitment) UnmarshalJSON(data []byte) error {
	var value string
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	parsed, err := ParseCommitment(value)
	if err != nil {
		return err
	}
	*c = parsed
	return nil
}

type Slot uint64

func ParseSlot(value string) (Slot, error) {
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, errors.New("solana slot must be an unsigned decimal integer")
	}
	return Slot(parsed), nil
}

func (s Slot) Validate() error { return nil }

type PublicKey string
type Signature string
type GenesisHash string

const MainnetGenesisHash GenesisHash = "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d"

func ParsePublicKey(value string) (PublicKey, error) {
	if err := validateBase58Size(value, 32); err != nil {
		return "", fmt.Errorf("solana public key: %w", err)
	}
	return PublicKey(value), nil
}

func ParseSignature(value string) (Signature, error) {
	if err := validateBase58Size(value, 64); err != nil {
		return "", fmt.Errorf("solana signature: %w", err)
	}
	return Signature(value), nil
}

func ParseGenesisHash(value string) (GenesisHash, error) {
	if err := validateBase58Size(value, 32); err != nil {
		return "", fmt.Errorf("solana genesis hash: %w", err)
	}
	return GenesisHash(value), nil
}

func (p PublicKey) Validate() error {
	_, err := ParsePublicKey(string(p))
	return err
}

func (s Signature) Validate() error {
	_, err := ParseSignature(string(s))
	return err
}

func (h GenesisHash) Validate() error {
	_, err := ParseGenesisHash(string(h))
	return err
}

func (p *PublicKey) UnmarshalJSON(data []byte) error {
	var value string
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	parsed, err := ParsePublicKey(value)
	if err != nil {
		return err
	}
	*p = parsed
	return nil
}

func (s *Signature) UnmarshalJSON(data []byte) error {
	var value string
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	parsed, err := ParseSignature(value)
	if err != nil {
		return err
	}
	*s = parsed
	return nil
}

func (h *GenesisHash) UnmarshalJSON(data []byte) error {
	var value string
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	parsed, err := ParseGenesisHash(value)
	if err != nil {
		return err
	}
	*h = parsed
	return nil
}

func validateBase58Size(value string, size int) error {
	decoded, err := base58.Decode(value)
	if err != nil {
		return errors.New("contains invalid base58")
	}
	if len(decoded) != size {
		return fmt.Errorf("must encode exactly %d bytes", size)
	}
	return nil
}
