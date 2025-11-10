// Copyright (c) 2023 AccelByte Inc. All Rights Reserved.
// This is licensed software from AccelByte Inc, for limitations
// and restrictions contact your company contract manager.

package service

import (
	"context"
	statpb "extend-challenge-event-handler/pkg/pb/accelbyte-asyncapi/social/statistic/v1"
	"extend-challenge-event-handler/pkg/processor"

	"github.com/AccelByte/extend-challenge-common/pkg/cache"
	"github.com/AccelByte/extend-challenge-common/pkg/domain"

	"github.com/rs/zerolog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// StatisticHandler processes AGS Statistic update events and updates user goal progress.
//
// This handler:
//   - Receives statistic update events from AccelByte Extend platform (abstracted from Kafka)
//   - Identifies goals with event_source="statistic" tracking the updated stat_code
//   - Routes to BufferedRepository based on goal type
//   - Leverages buffering for 1,000,000x database load reduction
//
// NOTE: Reward granting is NOT handled here. That happens in the REST API service (Phase 7).
// When implementing reward grants in Phase 7, use:
//   - factory.NewPlatformClient(configRepo) for client creation
//   - platform.FulfillmentService for item/wallet grants
//   - See original template for reference implementation
type StatisticHandler struct {
	statpb.UnimplementedStatisticStatItemUpdatedServiceServer
	eventProcessor processor.Processor
	goalCache      cache.GoalCache
	namespace      string
	logger         zerolog.Logger
}

// NewStatisticHandler creates a new StatisticHandler with required dependencies.
//
// Parameters:
//   - eventProcessor: Processor interface for processing statistic events
//   - goalCache: In-memory cache for goal configuration lookups
//   - namespace: AccelByte namespace for this deployment
//   - logger: Structured logger for operational logging
//
// Returns:
//   - *StatisticHandler: Ready-to-use handler for statistic events
func NewStatisticHandler(
	eventProcessor processor.Processor,
	goalCache cache.GoalCache,
	namespace string,
	logger zerolog.Logger,
) *StatisticHandler {
	return &StatisticHandler{
		eventProcessor: eventProcessor,
		goalCache:      goalCache,
		namespace:      namespace,
		logger:         logger,
	}
}

// OnMessage processes a statistic update event and updates progress for all matching goals.
//
// Event Flow:
//  1. Validate message (nil check, empty userID check, empty stat_code check)
//  2. Extract stat_code and latest_value from event payload
//  3. Get goals tracking this stat_code from cache (O(1) lookup)
//  4. Filter goals where event_source="statistic"
//  5. Build stat updates map for the stat_code
//  6. Call EventProcessor.ProcessEvent with stat updates
//  7. Return error if processing fails (enables Extend platform retry)
//
// Value Type Conversion:
//   - Proto defines latest_value as double (e.g., 42.7)
//   - EventProcessor expects int
//   - We truncate (floor) to nearest integer: int(42.7) = 42
//
// Error Handling:
//   - Validation errors (nil message, empty userID, empty stat_code): Return InvalidArgument error immediately
//   - Processing errors (buffer full): Return Internal error for Extend platform retry
//   - Normal processing: Return success (&emptypb.Empty{}, nil)
//
// Parameters:
//   - ctx: Context for tracing and cancellation
//   - msg: StatItemUpdated event from Statistic service
//
// Returns:
//   - *emptypb.Empty: Success response (event consumed)
//   - error: Non-nil if event should be retried by Extend platform
func (h *StatisticHandler) OnMessage(ctx context.Context, msg *statpb.StatItemUpdated) (*emptypb.Empty, error) {
	// Validation: nil message
	if msg == nil {
		h.logger.Warn().Msg("Received nil statistic message")
		return nil, status.Error(codes.InvalidArgument, "message cannot be nil")
	}

	// Extract userID from message
	userID := msg.UserId
	if userID == "" {
		h.logger.Warn().Msg("Received statistic message with empty userId")
		return nil, status.Error(codes.InvalidArgument, "userId cannot be empty")
	}

	// Validation: payload must exist
	if msg.Payload == nil {
		h.logger.Warn().Msg("Received statistic message with nil payload")
		return nil, status.Error(codes.InvalidArgument, "payload cannot be nil")
	}

	// Extract stat_code from payload
	statCode := msg.Payload.StatCode
	if statCode == "" {
		h.logger.Warn().Msg("Received statistic message with empty statCode")
		return nil, status.Error(codes.InvalidArgument, "statCode cannot be empty")
	}

	// Validate namespace matches deployment namespace
	// Events should only be processed if they belong to this deployment's namespace
	if msg.Namespace != h.namespace {
		h.logger.Warn().
			Str("eventNamespace", msg.Namespace).
			Str("deploymentNamespace", h.namespace).
			Str("userID", userID).
			Str("statCode", statCode).
			Msg("Received statistic event for different namespace, skipping")
		return &emptypb.Empty{}, nil
	}

	// Extract latest_value and convert from double to int (truncate)
	// Q1 Decision: Use truncation (floor) for double→int conversion
	statValue := int(msg.Payload.LatestValue)

	// Get goals tracking this stat_code from cache (O(1) lookup)
	// Q3 Decision: Use GetGoalsByStatCode() for efficiency
	goals := h.goalCache.GetGoalsByStatCode(statCode)
	if goals == nil {
		h.logger.Debug().Msg("Goal cache returned nil for stat_code, skipping event processing")
		return &emptypb.Empty{}, nil
	}

	// Filter goals by event_source="statistic"
	// Q6 Decision: Apply both filters (stat_code + event_source)
	var statisticGoals []*domain.Goal
	for _, goal := range goals {
		if goal.EventSource == domain.EventSourceStatistic {
			statisticGoals = append(statisticGoals, goal)
		}
	}

	// If no statistic goals for this stat_code, return success (no-op)
	if len(statisticGoals) == 0 {
		h.logger.Debug().
			Str("statCode", statCode).
			Str("userID", userID).
			Msg("No statistic goals found for stat_code, skipping event processing")
		return &emptypb.Empty{}, nil
	}

	// Build stat updates map
	statUpdates := map[string]int{
		statCode: statValue,
	}

	// Process via EventProcessor (handles goal type routing, buffering, etc.)
	// Q2 Decision: Pass all values to EventProcessor, let it validate (including negative values)
	// Use msg.Namespace (already validated to match h.namespace above)
	err := h.eventProcessor.ProcessEvent(ctx, userID, msg.Namespace, statUpdates)
	if err != nil {
		// Buffer full or critical error - return error for Extend platform retry
		// Event will NOT be marked consumed, allowing retry later
		h.logger.Error().
			Err(err).
			Str("userID", userID).
			Str("statCode", statCode).
			Int("statValue", statValue).
			Msg("Failed to process statistic event, returning error for retry")
		return nil, status.Errorf(codes.Internal, "failed to buffer event: %v", err)
	}

	h.logger.Debug().
		Str("userID", userID).
		Str("statCode", statCode).
		Int("statValue", statValue).
		Int("goalCount", len(statisticGoals)).
		Msg("Statistic event processed successfully")

	return &emptypb.Empty{}, nil
}
