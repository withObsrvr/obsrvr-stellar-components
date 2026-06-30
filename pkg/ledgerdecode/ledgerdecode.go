package ledgerdecode

import (
	"fmt"

	"github.com/stellar/go-stellar-sdk/xdr"
	stellarv1 "github.com/withObsrvr/flow-proto/go/gen/stellar/v1"
	"google.golang.org/protobuf/proto"
)

func DecodeRawLedger(payload []byte) (*stellarv1.RawLedger, xdr.LedgerCloseMeta, error) {
	var raw stellarv1.RawLedger
	if err := proto.Unmarshal(payload, &raw); err != nil {
		return nil, xdr.LedgerCloseMeta{}, fmt.Errorf("unmarshal stellar.v1.RawLedger: %w", err)
	}

	var lcm xdr.LedgerCloseMeta
	if err := xdr.SafeUnmarshal(raw.LedgerCloseMetaXdr, &lcm); err != nil {
		return nil, xdr.LedgerCloseMeta{}, fmt.Errorf("unmarshal LedgerCloseMeta XDR: %w", err)
	}

	return &raw, lcm, nil
}
