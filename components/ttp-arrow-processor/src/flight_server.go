package main

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

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

// TTPFlightServer implements the Arrow Flight service for streaming TTP events
type TTPFlightServer struct {
	flight.BaseFlightServer

	service *TTPProcessorService
	mu      sync.RWMutex
	streams map[string]*TTPStreamContext
	addr    string
}

// TTPStreamContext holds the context for an active TTP stream
type TTPStreamContext struct {
	ticket     *flight.Ticket
	schema     *arrow.Schema
	recordChan <-chan arrow.Record
	errorChan  <-chan error
	cancel     context.CancelFunc
}

// NewTTPFlightServer creates a new TTP Flight server
func NewTTPFlightServer(service *TTPProcessorService) *TTPFlightServer {
	return &TTPFlightServer{
		service: service,
		streams: make(map[string]*TTPStreamContext),
	}
}

// GetSchema returns the schema for a given descriptor
func (s *TTPFlightServer) GetSchema(ctx context.Context, in *flight.FlightDescriptor) (*flight.SchemaResult, error) {
	log.Debug().
		Str("type", in.Type.String()).
		Strs("path", in.Path).
		Msg("GetSchema request")

	streamName := ""
	if len(in.Path) > 0 {
		streamName = in.Path[0]
	}
	
	var schema *arrow.Schema
	switch streamName {
	case "ttp_events":
		schema = schemas.TTPEventSchema
	default:
		return nil, status.Error(codes.NotFound, "stream not found")
	}

	// Serialize the schema
	buf := flight.SerializeSchema(schema, memory.NewGoAllocator())

	return &flight.SchemaResult{Schema: buf}, nil
}

// GetFlightInfo returns flight information for a given descriptor
func (s *TTPFlightServer) GetFlightInfo(ctx context.Context, in *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	log.Debug().
		Str("type", in.Type.String()).
		Strs("path", in.Path).
		Msg("GetFlightInfo request")

	streamName := ""
	if len(in.Path) > 0 {
		streamName = in.Path[0]
	}
	
	var schema *arrow.Schema
	switch streamName {
	case "ttp_events":
		schema = schemas.TTPEventSchema
	default:
		return nil, status.Error(codes.NotFound, "stream not found")
	}

	// Create a ticket for this stream
	ticket := &flight.Ticket{
		Ticket: []byte(fmt.Sprintf("%s_%d", streamName, generateTTPTicketID())),
	}

	// Serialize the schema
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
		Msg("Created TTP flight info")

	return flightInfo, nil
}

// DoGet retrieves data for a given ticket
func (s *TTPFlightServer) DoGet(ticket *flight.Ticket, stream flight.FlightService_DoGetServer) error {
	ticketStr := string(ticket.Ticket)
	
	log.Info().
		Str("ticket", ticketStr).
		Msg("TTP DoGet request started")

	// Parse ticket to determine stream type
	var schema *arrow.Schema
	var recordChan <-chan arrow.Record
	
	if len(ticketStr) >= 10 && ticketStr[:10] == "ttp_events" {
		schema = schemas.TTPEventSchema
		recordChan = s.service.GetRecordsChannel()
	} else {
		return status.Error(codes.InvalidArgument, "invalid ticket")
	}

	// Create stream context
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	streamCtx := &TTPStreamContext{
		ticket:     ticket,
		schema:     schema,
		recordChan: recordChan,
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
		log.Info().Str("ticket", ticketStr).Msg("TTP DoGet request completed")
	}()

	// Create and send schema message
	writer := flight.NewRecordWriter(stream, ipc.WithSchema(streamCtx.schema))
	defer writer.Close()

	log.Debug().
		Str("ticket", ticketStr).
		Msg("Starting to stream TTP events")

	recordCount := 0

	// Stream records
	for {
		select {
		case <-ctx.Done():
			log.Info().
				Str("ticket", ticketStr).
				Int("records_sent", recordCount).
				Msg("TTP stream cancelled")
			return ctx.Err()

		case record, ok := <-streamCtx.recordChan:
			if !ok {
				log.Info().
					Str("ticket", ticketStr).
					Int("records_sent", recordCount).
					Msg("TTP record channel closed, ending stream")
				return nil
			}

			// Write record to stream
			if err := writer.Write(record); err != nil {
				record.Release()
				log.Error().
					Err(err).
					Str("ticket", ticketStr).
					Msg("Failed to write TTP record to stream")
				return status.Errorf(codes.Internal, "failed to write record: %v", err)
			}

			record.Release()
			recordCount++

			if recordCount%50 == 0 { // Log more frequently for events
				log.Debug().
					Str("ticket", ticketStr).
					Int("records_sent", recordCount).
					Msg("TTP stream progress")
			}

		case err, ok := <-streamCtx.errorChan:
			if !ok {
				continue
			}
			log.Error().
				Err(err).
				Str("ticket", ticketStr).
				Msg("Error from TTP processor service")
			return status.Errorf(codes.Internal, "processor error: %v", err)
		}
	}
}

// ListFlights lists available flights
func (s *TTPFlightServer) ListFlights(in *flight.Criteria, stream flight.FlightService_ListFlightsServer) error {
	log.Debug().Msg("TTP ListFlights request")

	// We have one main flight for TTP events
	descriptor := &flight.FlightDescriptor{
		Type: flight.DescriptorPATH,
		Path: []string{"ttp_events"},
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

// DoPut is not implemented (read-only processor output)
func (s *TTPFlightServer) DoPut(stream flight.FlightService_DoPutServer) error {
	return status.Error(codes.Unimplemented, "DoPut not supported for processor components")
}

// DoExchange is not implemented
func (s *TTPFlightServer) DoExchange(stream flight.FlightService_DoExchangeServer) error {
	return status.Error(codes.Unimplemented, "DoExchange not supported")
}

// DoAction handles custom actions
func (s *TTPFlightServer) DoAction(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	log.Debug().
		Str("type", action.Type).
		Bytes("body", action.Body).
		Msg("TTP DoAction request")

	var result *flight.Result
	var err error

	switch action.Type {
	case "get_status":
		result, err = s.handleGetStatus(stream.Context())
	case "get_current_ledger":
		result, err = s.handleGetCurrentLedger(stream.Context())
	case "get_stats":
		result, err = s.handleGetStats(stream.Context())
	case "get_config":
		result, err = s.handleGetConfig(stream.Context())
	default:
		return status.Errorf(codes.InvalidArgument, "unknown action: %s", action.Type)
	}

	if err != nil {
		return err
	}

	return stream.Send(result)
}

// ListActions lists available actions
func (s *TTPFlightServer) ListActions(in *flight.Empty, stream flight.FlightService_ListActionsServer) error {
	log.Debug().Msg("TTP ListActions request")

	// Send available actions
	actions := []*flight.ActionType{
		{
			Type:        "get_status",
			Description: "Get processor status",
		},
		{
			Type:        "get_current_ledger",
			Description: "Get current ledger being processed",
		},
		{
			Type:        "get_stats",
			Description: "Get processing statistics",
		},
		{
			Type:        "get_config",
			Description: "Get processor configuration",
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
func (s *TTPFlightServer) handleGetStatus(ctx context.Context) (*flight.Result, error) {
	stats := s.service.GetStats()
	
	result := fmt.Sprintf(`{
		"component": "%s",
		"version": "%s",
		"running": %t,
		"source_endpoint": "%s",
		"current_ledger": %d,
		"ledgers_processed": %d,
		"events_extracted": %d,
		"events_filtered": %d
	}`,
		ComponentName, ComponentVersion, s.service.IsRunning(),
		s.service.config.SourceEndpoint, s.service.GetCurrentLedger(),
		stats.LedgersProcessed, stats.EventsExtracted, stats.EventsFiltered)

	return &flight.Result{
		Body: []byte(result),
	}, nil
}

// handleGetCurrentLedger returns the current ledger being processed
func (s *TTPFlightServer) handleGetCurrentLedger(ctx context.Context) (*flight.Result, error) {
	currentLedger := s.service.GetCurrentLedger()
	result := fmt.Sprintf(`{"current_ledger": %d}`, currentLedger)

	return &flight.Result{
		Body: []byte(result),
	}, nil
}

// handleGetStats returns detailed processing statistics
func (s *TTPFlightServer) handleGetStats(ctx context.Context) (*flight.Result, error) {
	stats := s.service.GetStats()
	
	result := fmt.Sprintf(`{
		"ledgers_processed": %d,
		"transactions_processed": %d,
		"operations_processed": %d,
		"events_extracted": %d,
		"events_filtered": %d,
		"batches_generated": %d,
		"start_time": "%s",
		"last_processed_ledger": %d,
		"uptime_seconds": %.0f
	}`,
		stats.LedgersProcessed, stats.TransactionsProcessed, stats.OperationsProcessed,
		stats.EventsExtracted, stats.EventsFiltered, stats.BatchesGenerated,
		stats.StartTime.Format("2006-01-02T15:04:05Z07:00"), stats.LastProcessedLedger,
		time.Since(stats.StartTime).Seconds())

	return &flight.Result{
		Body: []byte(result),
	}, nil
}

// handleGetConfig returns the processor configuration
func (s *TTPFlightServer) handleGetConfig(ctx context.Context) (*flight.Result, error) {
	config := s.service.config
	
	// Convert event types to JSON array
	eventTypesJSON := "["
	for i, eventType := range config.EventTypes {
		if i > 0 {
			eventTypesJSON += ", "
		}
		eventTypesJSON += fmt.Sprintf(`"%s"`, eventType)
	}
	eventTypesJSON += "]"
	
	result := fmt.Sprintf(`{
		"source_endpoint": "%s",
		"event_types": %s,
		"processor_threads": %d,
		"batch_size": %d,
		"buffer_size": %d,
		"include_raw_xdr": %t,
		"include_transaction_details": %t,
		"deduplicate_events": %t,
		"flight_port": %d,
		"health_port": %d
	}`,
		config.SourceEndpoint, eventTypesJSON, config.ProcessorThreads,
		config.BatchSize, config.BufferSize, config.IncludeRawXDR,
		config.IncludeTransactionDetails, config.DeduplicateEvents,
		config.FlightPort, config.HealthPort)

	return &flight.Result{
		Body: []byte(result),
	}, nil
}

// Addr returns the server address
func (s *TTPFlightServer) Addr() net.Addr {
	addr, err := net.ResolveTCPAddr("tcp", s.addr)
	if err != nil {
		log.Error().Err(err).Str("addr", s.addr).Msg("Failed to resolve TTP server address")
		return nil
	}
	return addr
}

// Init initializes the TTP Flight server
func (s *TTPFlightServer) Init(addr string) error {
	s.addr = addr
	log.Debug().Str("addr", addr).Msg("TTPFlightServer initialized")
	return nil
}

// InitListener initializes the TTP Flight server listener
func (s *TTPFlightServer) InitListener(listener net.Listener) {
	log.Debug().Msg("TTPFlightServer listener initialized")
}

// RegisterService registers the service with gRPC server
func (s *TTPFlightServer) RegisterService(sd *grpc.ServiceDesc, ss any) {
	log.Debug().Msg("Registering TTP service")
}

// RegisterFlightService registers the flight service with gRPC server
func (s *TTPFlightServer) RegisterFlightService(server flight.FlightServer) {
	log.Debug().Msg("Registering TTP Flight service")
}

// GetServiceInfo returns service information for this TTP Flight server
func (s *TTPFlightServer) GetServiceInfo() map[string]grpc.ServiceInfo {
	log.Debug().Msg("TTP GetServiceInfo request")
	return make(map[string]grpc.ServiceInfo)
}

// Helper function to generate unique ticket IDs for TTP
var ttpTicketCounter int64

func generateTTPTicketID() int64 {
	ttpTicketCounter++
	return ttpTicketCounter
}