package main

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/stellar/go/clients/horizonclient"
	"github.com/stellar/go/support/render/hal"
	"github.com/stellar/go/xdr"
)

// RPCClient interface defines methods for fetching Stellar data via RPC
type RPCClient interface {
	GetLedger(ctx context.Context, sequence uint32) ([]byte, error)
	GetLatestLedger(ctx context.Context) (uint32, error)
	Close()
}

// HorizonRPCClient implements RPCClient using Stellar Horizon API
type HorizonRPCClient struct {
	client          *horizonclient.Client
	networkPassphrase string
}

// NewRPCClient creates a new RPC client based on configuration
func NewRPCClient(config *Config) (RPCClient, error) {
	log.Info().
		Str("endpoint", config.RPCEndpoint).
		Str("network", config.NetworkPassphrase).
		Msg("Creating RPC client")

	// Create Horizon client
	client := &horizonclient.Client{
		HorizonURL: config.RPCEndpoint,
		HTTP: &http.Client{
			Timeout: 30 * time.Second,
		},
	}

	return &HorizonRPCClient{
		client:            client,
		networkPassphrase: config.NetworkPassphrase,
	}, nil
}

// GetLedger fetches a specific ledger by sequence number
func (c *HorizonRPCClient) GetLedger(ctx context.Context, sequence uint32) ([]byte, error) {
	log.Debug().
		Uint32("sequence", sequence).
		Msg("Fetching ledger from Horizon")

	// Request the ledger
	ledgerRequest := horizonclient.LedgerRequest{
		ForSequence: sequence,
	}

	ledger, err := c.client.LedgerDetail(ledgerRequest)
	if err != nil {
		return nil, fmt.Errorf("failed to get ledger %d: %w", sequence, err)
	}

	// Parse the XDR data
	var ledgerCloseMeta xdr.LedgerCloseMeta
	if err := xdr.SafeUnmarshalBase64(ledger.HeaderXdr, &ledgerCloseMeta); err != nil {
		return nil, fmt.Errorf("failed to unmarshal ledger XDR for sequence %d: %w", sequence, err)
	}

	// Marshal to binary for processing
	xdrData, err := ledgerCloseMeta.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal ledger XDR for sequence %d: %w", sequence, err)
	}

	log.Debug().
		Uint32("sequence", sequence).
		Int("xdr_size", len(xdrData)).
		Msg("Successfully fetched ledger")

	return xdrData, nil
}

// GetLatestLedger fetches the latest ledger sequence number
func (c *HorizonRPCClient) GetLatestLedger(ctx context.Context) (uint32, error) {
	log.Debug().Msg("Fetching latest ledger sequence")

	// Get the latest ledger
	ledgerRequest := horizonclient.LedgerRequest{
		Order: horizonclient.OrderDesc,
		Limit: 1,
	}

	ledgers, err := c.client.Ledgers(ledgerRequest)
	if err != nil {
		return 0, fmt.Errorf("failed to get latest ledger: %w", err)
	}

	if len(ledgers.Embedded.Records) == 0 {
		return 0, fmt.Errorf("no ledgers found")
	}

	latestSequence := ledgers.Embedded.Records[0].Sequence
	
	log.Debug().
		Uint32("sequence", latestSequence).
		Msg("Found latest ledger")

	return latestSequence, nil
}

// Close cleans up the RPC client
func (c *HorizonRPCClient) Close() {
	log.Debug().Msg("Closing RPC client")
	// HTTP client will be garbage collected
}

// StreamingRPCClient implements real-time streaming from Stellar
type StreamingRPCClient struct {
	client            *horizonclient.Client
	networkPassphrase string
	stopChan          chan struct{}
}

// NewStreamingRPCClient creates a streaming RPC client
func NewStreamingRPCClient(config *Config) (*StreamingRPCClient, error) {
	log.Info().
		Str("endpoint", config.RPCEndpoint).
		Msg("Creating streaming RPC client")

	client := &horizonclient.Client{
		HorizonURL: config.RPCEndpoint,
		HTTP: &http.Client{
			Timeout: 60 * time.Second, // Longer timeout for streaming
		},
	}

	return &StreamingRPCClient{
		client:            client,
		networkPassphrase: config.NetworkPassphrase,
		stopChan:          make(chan struct{}),
	}, nil
}

// StreamLedgers streams ledgers starting from a given sequence
func (c *StreamingRPCClient) StreamLedgers(ctx context.Context, startSequence uint32, ledgerChan chan<- []byte) error {
	log.Info().
		Uint32("start_sequence", startSequence).
		Msg("Starting ledger stream")

	// Create streaming request
	ledgerRequest := horizonclient.LedgerRequest{
		Cursor: fmt.Sprintf("%d", startSequence-1), // Cursor is exclusive
		Order:  horizonclient.OrderAsc,
	}

	// Start streaming
	ledgerHandler := func(ledger horizonclient.Ledger) {
		log.Debug().
			Uint32("sequence", ledger.Sequence).
			Msg("Received ledger from stream")

		// Parse XDR data
		var ledgerCloseMeta xdr.LedgerCloseMeta
		if err := xdr.SafeUnmarshalBase64(ledger.HeaderXdr, &ledgerCloseMeta); err != nil {
			log.Error().
				Err(err).
				Uint32("sequence", ledger.Sequence).
				Msg("Failed to unmarshal streamed ledger XDR")
			return
		}

		// Marshal to binary
		xdrData, err := ledgerCloseMeta.MarshalBinary()
		if err != nil {
			log.Error().
				Err(err).
				Uint32("sequence", ledger.Sequence).
				Msg("Failed to marshal streamed ledger XDR")
			return
		}

		// Send to channel
		select {
		case ledgerChan <- xdrData:
			// Successfully sent
		case <-ctx.Done():
			return
		case <-c.stopChan:
			return
		}
	}

	// Error handler
	errorHandler := func(err error) {
		log.Error().
			Err(err).
			Msg("Error in ledger stream")
	}

	// Start streaming (this blocks)
	err := c.client.StreamLedgers(ctx, ledgerRequest, ledgerHandler)
	if err != nil {
		return fmt.Errorf("ledger streaming failed: %w", err)
	}

	return nil
}

// GetLedger fetches a specific ledger (same as HorizonRPCClient)
func (c *StreamingRPCClient) GetLedger(ctx context.Context, sequence uint32) ([]byte, error) {
	return (&HorizonRPCClient{
		client:            c.client,
		networkPassphrase: c.networkPassphrase,
	}).GetLedger(ctx, sequence)
}

// GetLatestLedger fetches the latest ledger (same as HorizonRPCClient)
func (c *StreamingRPCClient) GetLatestLedger(ctx context.Context) (uint32, error) {
	return (&HorizonRPCClient{
		client:            c.client,
		networkPassphrase: c.networkPassphrase,
	}).GetLatestLedger(ctx)
}

// Close stops the streaming client
func (c *StreamingRPCClient) Close() {
	log.Debug().Msg("Closing streaming RPC client")
	close(c.stopChan)
}

// SorobanRPCClient implements RPC client for Soroban (smart contracts)
type SorobanRPCClient struct {
	endpoint          string
	networkPassphrase string
	httpClient        *http.Client
}

// NewSorobanRPCClient creates a Soroban RPC client
func NewSorobanRPCClient(endpoint, networkPassphrase string) *SorobanRPCClient {
	return &SorobanRPCClient{
		endpoint:          endpoint,
		networkPassphrase: networkPassphrase,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// GetLedger fetches ledger data from Soroban RPC
func (c *SorobanRPCClient) GetLedger(ctx context.Context, sequence uint32) ([]byte, error) {
	// TODO: Implement Soroban RPC ledger fetching
	// For now, we'll use Horizon as fallback
	log.Debug().
		Uint32("sequence", sequence).
		Msg("Soroban RPC ledger fetching not yet implemented, using Horizon")

	// Create temporary Horizon client
	horizonClient := &HorizonRPCClient{
		client: &horizonclient.Client{
			HorizonURL: "https://horizon.stellar.org", // Default to mainnet
			HTTP:       c.httpClient,
		},
		networkPassphrase: c.networkPassphrase,
	}

	return horizonClient.GetLedger(ctx, sequence)
}

// GetLatestLedger fetches the latest ledger from Soroban RPC
func (c *SorobanRPCClient) GetLatestLedger(ctx context.Context) (uint32, error) {
	// TODO: Implement Soroban RPC latest ledger fetching
	log.Debug().Msg("Soroban RPC latest ledger fetching not yet implemented, using Horizon")

	// Create temporary Horizon client  
	horizonClient := &HorizonRPCClient{
		client: &horizonclient.Client{
			HorizonURL: "https://horizon.stellar.org", // Default to mainnet
			HTTP:       c.httpClient,
		},
		networkPassphrase: c.networkPassphrase,
	}

	return horizonClient.GetLatestLedger(ctx)
}

// Close cleans up the Soroban RPC client
func (c *SorobanRPCClient) Close() {
	log.Debug().Msg("Closing Soroban RPC client")
	// HTTP client will be garbage collected
}