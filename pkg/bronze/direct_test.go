package bronze

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	componentsv1 "github.com/withObsrvr/obsrvr-stellar-components/gen/go/stellar/components/v1"
	extract "github.com/withObsrvr/stellar-extract"
)

func TestProjectLedgerDataMatchesJSONBridge(t *testing.T) {
	createdAt := time.Unix(1_700_000_000, 0).UTC()
	transaction := extract.TransactionData{
		LedgerSequence:  123,
		TransactionHash: "abc",
		SourceAccount:   "GABC",
		Successful:      true,
		CreatedAt:       createdAt,
		LedgerRange:     0,
		TransactionID:   42,
	}
	data := &extract.LedgerData{Transactions: []extract.TransactionData{transaction}}
	overrides := TransactionOverrides{"abc": {
		"tx_envelope": "envelope-xdr",
		"tx_result":   "result-xdr",
		"tx_meta":     "meta-xdr",
	}}

	direct := ProjectLedgerData(data, overrides)
	if len(direct) != 1 || direct[0].Err != nil || !direct[0].OK {
		t.Fatalf("direct projection = %+v", direct)
	}

	rowJSON, err := json.Marshal(transaction)
	if err != nil {
		t.Fatal(err)
	}
	batch := &componentsv1.LedgerBatch{
		Transactions: []*componentsv1.TransactionRow{{
			LedgerSequence:  123,
			TransactionHash: "abc",
			EnvelopeXdr:     "envelope-xdr",
			ResultXdr:       "result-xdr",
			MetaXdr:         "meta-xdr",
		}},
	}
	jsonSpec, jsonValues, ok, err := TypedRowInsertValues(&componentsv1.BronzeRow{
		TableName:      "transactions_row_v2",
		LedgerSequence: 123,
		RowJson:        string(rowJSON),
	}, BuildTypedRowEnrichments(batch))
	if err != nil || !ok {
		t.Fatalf("JSON projection: ok=%v err=%v", ok, err)
	}
	if !reflect.DeepEqual(direct[0].Spec, jsonSpec) {
		t.Fatal("direct and JSON specs differ")
	}
	if !reflect.DeepEqual(direct[0].Values, jsonValues) {
		t.Fatalf("direct and JSON values differ:\ndirect=%#v\njson=%#v", direct[0].Values, jsonValues)
	}
	parallel := ProjectLedgerDataWithWorkers(data, overrides, 4)
	if !reflect.DeepEqual(direct, parallel) {
		t.Fatalf("serial and parallel projection differ:\nserial=%#v\nparallel=%#v", direct, parallel)
	}
}

func TestProjectLedgerDataCoversEveryTypedTable(t *testing.T) {
	data := &extract.LedgerData{}
	value := reflect.ValueOf(data).Elem()
	for index := 0; index < value.NumField(); index++ {
		field := value.Field(index)
		field.Set(reflect.Append(field, reflect.Zero(field.Type().Elem())))
	}

	rows := ProjectLedgerData(data, nil)
	if len(rows) != len(TypedTableSpecs) {
		t.Fatalf("projected rows = %d, typed tables = %d", len(rows), len(TypedTableSpecs))
	}
	seen := make(map[string]bool, len(rows))
	for _, row := range rows {
		if row.Err != nil || !row.OK {
			t.Fatalf("project %s: ok=%v err=%v", row.Spec.TableName, row.OK, row.Err)
		}
		if seen[row.Spec.TableName] {
			t.Fatalf("table %s projected more than once", row.Spec.TableName)
		}
		seen[row.Spec.TableName] = true
	}
	for tableName := range TypedTableSpecs {
		if !seen[tableName] {
			t.Errorf("typed table %s is not projected", tableName)
		}
	}
}
