package main

import (
	"context"
	"fmt"
	"net"
	"sync"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/flight"
	"github.com/apache/arrow/go/v17/arrow/ipc"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/withobsrvr/obsrvr-stellar-components/schemas"
)

// FlightServer implements the Arrow Flight service for streaming Stellar ledger data
type FlightServer struct {
	flight.BaseFlightServer

	service *StellarSourceService
	mu      sync.RWMutex
	streams map[string]*StreamContext
	addr    string
}

// StreamContext holds the context for an active stream
type StreamContext struct {
	ticket     *flight.Ticket
	schema     *arrow.Schema
	recordChan <-chan arrow.Record
	errorChan  <-chan error
	cancel     context.CancelFunc
}

// NewFlightServer creates a new Flight server
func NewFlightServer(service *StellarSourceService) *FlightServer {
	addr := fmt.Sprintf(":%d", service.config.FlightPort)
	return &FlightServer{
		service: service,
		streams: make(map[string]*StreamContext),
		addr:    addr,
	}
}

// Addr returns the server address
func (s *FlightServer) Addr() net.Addr {
	addr, err := net.ResolveTCPAddr("tcp", s.addr)
	if err != nil {
		log.Error().Err(err).Str("addr", s.addr).Msg("Failed to resolve server address")
		return nil
	}
	return addr
}

// GetSchema returns the schema for a given descriptor
func (s *FlightServer) GetSchema(ctx context.Context, in *flight.FlightDescriptor) (*flight.SchemaResult, error) {
	log.Debug().
		Str("type", in.Type.String()).
		Strs("path", in.Path).
		Msg("GetSchema request")

	// We only support one stream type for now
	if len(in.Path) == 0 || in.Path[0] != "stellar_ledgers" {
		return nil, status.Error(codes.NotFound, "stream not found")
	}

	// Serialize the schema
	schema := schemas.StellarLedgerSchema
	buf := flight.SerializeSchema(schema, memory.NewGoAllocator())

	return &flight.SchemaResult{Schema: buf}, nil
}

// GetFlightInfo returns flight information for a given descriptor
func (s *FlightServer) GetFlightInfo(ctx context.Context, in *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	log.Debug().
		Str("type", in.Type.String()).
		Strs("path", in.Path).
		Msg("GetFlightInfo request")

	// We only support one stream type for now
	if len(in.Path) == 0 || in.Path[0] != "stellar_ledgers" {
		return nil, status.Error(codes.NotFound, "stream not found")
	}
	streamName := in.Path[0]

	// Create a ticket for this stream
	ticket := &flight.Ticket{
		Ticket: []byte(fmt.Sprintf("stellar_ledgers_%d", generateTicketID())),
	}

	// Serialize the schema
	schema := schemas.StellarLedgerSchema
	schemaBuf := flight.SerializeSchema(schema, memory.NewGoAllocator())

	// Create endpoint
	endpoint := &flight.FlightEndpoint{
		Ticket: ticket,
		Location: []*flight.Location{
			{Uri: fmt.Sprintf("grpc://localhost:%d", s.service.config.FlightPort)},
		},
	}

	// Create flight info
	flightInfo := &flight.FlightInfo{
		Schema:           schemaBuf,
		FlightDescriptor: in,
		Endpoint:         []*flight.FlightEndpoint{endpoint},
		TotalRecords:     -1, // Unknown/streaming
		TotalBytes:       -1, // Unknown/streaming
	}

	log.Info().
		Str("stream", streamName).
		Str("ticket", string(ticket.Ticket)).
		Msg("Created flight info")

	return flightInfo, nil
}

// DoGet retrieves data for a given ticket
func (s *FlightServer) DoGet(ticket *flight.Ticket, stream flight.FlightService_DoGetServer) error {
	ticketStr := string(ticket.Ticket)
	
	log.Info().
		Str("ticket", ticketStr).
		Msg("DoGet request started")

	// Check if this is a valid stellar_ledgers ticket
	if len(ticketStr) < 15 || ticketStr[:14] != "stellar_ledgers" {
		return status.Error(codes.InvalidArgument, "invalid ticket")
	}

	// Create stream context
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	streamCtx := &StreamContext{
		ticket:     ticket,
		schema:     schemas.StellarLedgerSchema,
		recordChan: s.service.GetRecordsChannel(),
		errorChan:  s.service.GetErrorChannel(),
		cancel:     cancel,
	}

	// Register stream
	s.mu.Lock()
	s.streams[ticketStr] = streamCtx
	s.mu.Unlock()

	// Clean up on exit
	defer func() {
		s.mu.Lock()
		delete(s.streams, ticketStr)
		s.mu.Unlock()
		log.Info().Str("ticket", ticketStr).Msg("DoGet request completed")
	}()

	// Create and send schema message
	writer := flight.NewRecordWriter(stream, ipc.WithSchema(streamCtx.schema))
	defer writer.Close()

	log.Debug().
		Str("ticket", ticketStr).
		Msg("Starting to stream records")

	recordCount := 0

	// Stream records
	for {
		select {
		case <-ctx.Done():
			log.Info().
				Str("ticket", ticketStr).
				Int("records_sent", recordCount).
				Msg("Stream cancelled")
			return ctx.Err()

		case record, ok := <-streamCtx.recordChan:
			if !ok {
				log.Info().
					Str("ticket", ticketStr).
					Int("records_sent", recordCount).
					Msg("Record channel closed, ending stream")
				return nil
			}

			// Write record to stream
			if err := writer.Write(record); err != nil {
				record.Release()
				log.Error().
					Err(err).
					Str("ticket", ticketStr).
					Msg("Failed to write record to stream")
				return status.Errorf(codes.Internal, "failed to write record: %v", err)
			}

			record.Release()
			recordCount++

			if recordCount%100 == 0 {
				log.Debug().
					Str("ticket", ticketStr).
					Int("records_sent", recordCount).
					Msg("Stream progress")
			}

		case err, ok := <-streamCtx.errorChan:
			if !ok {
				continue
			}
			log.Error().
				Err(err).
				Str("ticket", ticketStr).
				Msg("Error from source service")
			return status.Errorf(codes.Internal, "source error: %v", err)
		}
	}
}

// ListFlights lists available flights
func (s *FlightServer) ListFlights(in *flight.Criteria, stream flight.FlightService_ListFlightsServer) error {
	log.Debug().Msg("ListFlights request")

	// We only have one flight for now
	descriptor := &flight.FlightDescriptor{
		Type: flight.DescriptorPATH,
		Path: []string{"stellar_ledgers"},
	}

	// Get flight info
	flightInfo, err := s.GetFlightInfo(stream.Context(), descriptor)
	if err != nil {
		return err
	}

	// Send the flight info
	if err := stream.Send(flightInfo); err != nil {
		return status.Errorf(codes.Internal, "failed to send flight info: %v", err)
	}

	return nil
}

// DoPut is not implemented (read-only source)
func (s *FlightServer) DoPut(stream flight.FlightService_DoPutServer) error {
	return status.Error(codes.Unimplemented, "DoPut not supported for source components")
}

// DoExchange is not implemented
func (s *FlightServer) DoExchange(stream flight.FlightService_DoExchangeServer) error {
	return status.Error(codes.Unimplemented, "DoExchange not supported")
}

// DoAction handles custom actions
func (s *FlightServer) DoAction(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	log.Debug().
		Str("type", action.Type).
		Bytes("body", action.Body).
		Msg("DoAction request")

	var result *flight.Result
	var err error

	switch action.Type {
	case "get_status":
		result, err = s.handleGetStatus(stream.Context())
	case "get_current_ledger":
		result, err = s.handleGetCurrentLedger(stream.Context())
	default:
		return status.Errorf(codes.InvalidArgument, "unknown action: %s", action.Type)
	}

	if err != nil {
		return err
	}

	return stream.Send(result)
}

// ListActions lists available actions
func (s *FlightServer) ListActions(in *flight.Empty, stream flight.FlightService_ListActionsServer) error {
	log.Debug().Msg("ListActions request")

	// Send available actions
	actions := []*flight.ActionType{
		{
			Type:        "get_status",
			Description: "Get service status",
		},
		{
			Type:        "get_current_ledger",
			Description: "Get current ledger being processed",
		},
	}

	for _, action := range actions {
		if err := stream.Send(action); err != nil {
			return err
		}
	}

	return nil
}

// handleGetStatus returns the current service status
func (s *FlightServer) handleGetStatus(ctx context.Context) (*flight.Result, error) {
	status := map[string]interface{}{
		"component": ComponentName,
		"version":   ComponentVersion,
		"running":   s.service.IsRunning(),
		"source_type": s.service.config.SourceType,
		"current_ledger": s.service.GetCurrentLedger(),
	}

	// Convert to JSON (simplified)
	result := fmt.Sprintf(`{"component":"%s","version":"%s","running":%t,"source_type":"%s","current_ledger":%d}`,
		status["component"], status["version"], status["running"], status["source_type"], status["current_ledger"])

	return &flight.Result{
		Body: []byte(result),
	}, nil
}

// handleGetCurrentLedger returns the current ledger being processed
func (s *FlightServer) handleGetCurrentLedger(ctx context.Context) (*flight.Result, error) {
	currentLedger := s.service.GetCurrentLedger()
	result := fmt.Sprintf(`{"current_ledger":%d}`, currentLedger)

	return &flight.Result{
		Body: []byte(result),
	}, nil
}

// Init initializes the Flight server
func (s *FlightServer) Init(addr string) error {
	s.addr = addr
	log.Debug().Str("addr", addr).Msg("FlightServer initialized")
	return nil
}

// InitListener initializes the Flight server listener
func (s *FlightServer) InitListener(listener net.Listener) {
	log.Debug().Msg("FlightServer listener initialized")
}

// RegisterService registers the service with gRPC server
func (s *FlightServer) RegisterService(sd *grpc.ServiceDesc, ss any) {
	log.Debug().Msg("Registering service")
}

// RegisterFlightService registers the flight service with gRPC server
func (s *FlightServer) RegisterFlightService(server flight.FlightServer) {
	log.Debug().Msg("Registering Flight service")
}

// GetServiceInfo returns service information for this Flight server
func (s *FlightServer) GetServiceInfo() map[string]grpc.ServiceInfo {
	log.Debug().Msg("GetServiceInfo request")
	return make(map[string]grpc.ServiceInfo)
}

// Helper function to generate unique ticket IDs
var ticketCounter int64

func generateTicketID() int64 {
	ticketCounter++
	return ticketCounter
}