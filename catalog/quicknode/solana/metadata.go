package solana

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"filippo.io/edwards25519"
	"github.com/mr-tron/base58/base58"
)

const tokenMetadataProgram PublicKey = "metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s"

type TokenMetadata struct {
	Mint    PublicKey
	Account PublicKey
	Name    string
	Symbol  string
	URI     string
}

// TokenMetadata reads the Metaplex metadata account for mint. A mint without a
// metadata account returns its identity with empty metadata fields.
func (c *Client) TokenMetadata(ctx context.Context, mint PublicKey, commitment Commitment) (TokenMetadata, error) {
	result := TokenMetadata{Mint: mint}
	address, err := tokenMetadataAddress(mint)
	if err != nil {
		return result, err
	}
	result.Account = address
	account, err := c.GetAccountInfo(ctx, address, AccountOptions{Commitment: commitment, Encoding: Base64})
	if err != nil {
		return result, err
	}
	if bytes.Equal(bytes.TrimSpace(account.Value), []byte("null")) {
		return result, nil
	}
	var value struct {
		Data [2]string `json:"data"`
	}
	if err := json.Unmarshal(account.Value, &value); err != nil {
		return result, fmt.Errorf("decode metadata account for %s: %w", mint, err)
	}
	payload, err := base64.StdEncoding.DecodeString(value.Data[0])
	if err != nil {
		return result, fmt.Errorf("decode metadata account for %s: %w", mint, err)
	}
	result.Name, result.Symbol, result.URI, err = decodeTokenMetadata(payload, mint)
	if err != nil {
		return result, fmt.Errorf("decode metadata account for %s: %w", mint, err)
	}
	return result, nil
}

func tokenMetadataAddress(mint PublicKey) (PublicKey, error) {
	program, err := publicKeyBytes(tokenMetadataProgram)
	if err != nil {
		return "", err
	}
	mintBytes, err := publicKeyBytes(mint)
	if err != nil {
		return "", err
	}
	for bump := 255; bump >= 0; bump-- {
		hash := sha256.New()
		hash.Write([]byte("metadata"))
		hash.Write(program)
		hash.Write(mintBytes)
		hash.Write([]byte{byte(bump)})
		hash.Write(program)
		hash.Write([]byte("ProgramDerivedAddress"))
		candidate := hash.Sum(nil)
		if _, err := new(edwards25519.Point).SetBytes(candidate); err != nil {
			return PublicKey(base58.Encode(candidate)), nil
		}
	}
	return "", errors.New("derive Metaplex metadata address")
}

func publicKeyBytes(value PublicKey) ([]byte, error) {
	if err := value.Validate(); err != nil {
		return nil, err
	}
	decoded, _ := base58.Decode(string(value))
	return decoded, nil
}

func decodeTokenMetadata(payload []byte, mint PublicKey) (string, string, string, error) {
	const headerSize = 1 + 32 + 32
	if len(payload) < headerSize {
		return "", "", "", errors.New("account is truncated")
	}
	expectedMint, err := publicKeyBytes(mint)
	if err != nil {
		return "", "", "", err
	}
	if !bytes.Equal(payload[33:headerSize], expectedMint) {
		return "", "", "", errors.New("account belongs to another mint")
	}
	reader := bytes.NewReader(payload[headerSize:])
	name, err := readMetadataString(reader, 256)
	if err != nil {
		return "", "", "", err
	}
	symbol, err := readMetadataString(reader, 64)
	if err != nil {
		return "", "", "", err
	}
	uri, err := readMetadataString(reader, 1024)
	if err != nil {
		return "", "", "", err
	}
	return cleanTokenMetadata(name), cleanTokenMetadata(symbol), cleanTokenMetadata(uri), nil
}

func readMetadataString(reader *bytes.Reader, maximum uint32) (string, error) {
	var size uint32
	if err := binary.Read(reader, binary.LittleEndian, &size); err != nil {
		return "", errors.New("string length is truncated")
	}
	if size > maximum || uint64(size) > uint64(reader.Len()) {
		return "", errors.New("string length is invalid")
	}
	value := make([]byte, size)
	if _, err := reader.Read(value); err != nil {
		return "", err
	}
	if !utf8.Valid(value) {
		return "", errors.New("string is not UTF-8")
	}
	return string(value), nil
}

func cleanTokenMetadata(value string) string {
	return strings.TrimSpace(strings.TrimRight(value, "\x00"))
}
