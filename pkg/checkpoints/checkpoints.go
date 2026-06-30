package checkpoints

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
)

type LedgerWatermark struct {
	NetworkPassphrase  string `json:"network_passphrase"`
	LastLedgerSequence uint32 `json:"last_ledger_sequence"`
}

func Load(path string) (LedgerWatermark, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return LedgerWatermark{}, nil
		}
		return LedgerWatermark{}, err
	}
	var watermark LedgerWatermark
	if err := json.Unmarshal(data, &watermark); err != nil {
		return LedgerWatermark{}, err
	}
	return watermark, nil
}

func Store(path string, watermark LedgerWatermark) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(watermark, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0o644)
}
