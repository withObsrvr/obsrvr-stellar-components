// Command ledger-fixture-recorder converts jsonl-sink output into a portable,
// hashed corpus of length-delimited LedgerBatch protobuf messages.
package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/withObsrvr/obsrvr-stellar-components/internal/ledgerfixture"
)

func main() {
	if err := run(os.Args[1:], os.Stdin, os.Stdout); err != nil {
		log.Fatal(err)
	}
}

func run(args []string, stdin io.Reader, stdout io.Writer) error {
	flags := flag.NewFlagSet("ledger-fixture-recorder", flag.ContinueOnError)
	flags.SetOutput(stdout)
	inputPath := flags.String("input", "-", "protobuf JSONL input from jsonl-sink, or - for stdin")
	manifestPath := flags.String("manifest", "", "output manifest path (required)")
	objectStoreURL := flags.String("object-url", "", "optional URL for the externally stored complete fixture corpus")
	batchesPerFile := flags.Int("batches-per-file", 100, "number of LedgerBatch messages per protobuf chunk")
	reorderWindow := flags.Int("reorder-window", 16, "maximum bounded input reordering to sort before recording; 0 requires strict input order")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected positional arguments: %v", flags.Args())
	}
	if *manifestPath == "" {
		return fmt.Errorf("--manifest is required")
	}

	input := stdin
	var file *os.File
	if *inputPath != "-" {
		var err error
		file, err = os.Open(*inputPath)
		if err != nil {
			return fmt.Errorf("open JSONL input: %w", err)
		}
		defer file.Close()
		input = file
	}
	manifest, err := ledgerfixture.RecordJSONL(input, ledgerfixture.RecordOptions{
		ManifestPath:   *manifestPath,
		ObjectStoreURL: *objectStoreURL,
		BatchesPerFile: *batchesPerFile,
		ReorderWindow:  *reorderWindow,
	})
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(stdout, "recorded %d ledgers (%d-%d) in %d files: %s\n",
		manifest.BatchCount,
		manifest.LedgerStart,
		manifest.LedgerEnd,
		len(manifest.Files),
		*manifestPath,
	)
	return err
}
