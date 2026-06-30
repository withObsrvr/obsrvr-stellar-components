package main

import (
	"log"

	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/withObsrvr/flowctl-sdk/pkg/stellar"
	"github.com/withObsrvr/obsrvr-stellar-components/pkg/contracts"
	"github.com/withObsrvr/obsrvr-stellar-components/pkg/normalize"
	"google.golang.org/protobuf/proto"
)

func main() {
	stellar.Run(stellar.ProcessorConfig{
		ProcessorName: "Stellar Ledger Processor",
		OutputType:    contracts.LedgerBatchEventType,
		ComponentID:   contracts.DefaultComponentID,
		ProcessLedger: func(passphrase string, ledger xdr.LedgerCloseMeta) (proto.Message, error) {
			batch, err := normalize.LedgerBatch(ledger, normalize.Options{
				NetworkPassphrase: passphrase,
			})
			if err != nil {
				return nil, err
			}
			log.Printf("emitting ledger batch ledger=%d txs=%d ops=%d", batch.LedgerSequence, len(batch.Transactions), len(batch.Operations))
			return batch, nil
		},
	})
}
