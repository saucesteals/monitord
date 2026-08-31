package cli

import (
	"bytes"
	"encoding/json"
)

func indentJSON(raw json.RawMessage) string {
	if len(raw) == 0 {
		return "{}"
	}

	var out bytes.Buffer
	if err := json.Indent(&out, raw, "", "  "); err != nil {
		return string(raw)
	}

	return out.String()
}
