// Copyright (c) 2023 AccelByte Inc. All Rights Reserved.
// This is licensed software from AccelByte Inc, for limitations
// and restrictions contact your company contract manager.

package service

import (
	"context"
	pb "extend-challenge-event-handler/pkg/pb/accelbyte-asyncapi/iam/account/v1"
	"extend-challenge-event-handler/pkg/processor"

	"github.com/AccelByte/extend-challenge-common/pkg/cache"
	"github.com/AccelByte/extend-challenge-common/pkg/domain"

	"github.com/rs/zerolog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// LoginHandler processes IAM login events and updates user goal progress.
//
// This handler:
//   - Receives login events from AccelByte Extend platform (abstracted from Kafka)
//   - Identifies goals with event_source="login"
//   - Routes to BufferedRepository based on goal type
//   - Leverages buffering for 1,000,000x database load reduction
//
// NOTE: Reward granting is NOT handled here. That happens in the REST API service (Phase 7).
// When implementing reward grants in Phase 7, use:
//   - factory.NewPlatformClient(configRepo) for client creation
//   - platform.FulfillmentService for item/wallet grants
//   - See original template loginHandler.go for reference implementation
type LoginHandler struct {
	pb.UnimplementedUserAuthenticationUserLoggedInServiceServer
	eventProcessor processor.Processor
	goalCache      cache.GoalCache
	namespace      string
	logger         zerolog.Logger
}

// NewLoginHandler creates a new LoginHandler with required dependencies.
//
// Parameters:
//   - eventProcessor: Processor interface for processing login events
//   - goalCache: In-memory cache for goal configuration lookups
//   - namespace: AccelByte namespace for this deployment
//   - logger: Structured logger for operational logging
//
// Returns:
//   - *LoginHandler: Ready-to-use handler for login events
func NewLoginHandler(
	eventProcessor processor.Processor,
	goalCache cache.GoalCache,
	namespace string,
	logger zerolog.Logger,
) *LoginHandler {
	return &LoginHandler{
		eventProcessor: eventProcessor,
		goalCache:      goalCache,
		namespace:      namespace,
		logger:         logger,
	}
}

// OnMessage processes a login event and updates progress for all login-triggered goals.
//
// Event Flow:
//  1. Validate message (nil check, empty userID check)
//  2. Get all goals from cache
//  3. Filter goals where event_source="login"
//  4. Build stat updates map for all login stat_codes with value=1
//  5. Call EventProcessor.ProcessEvent with stat updates
//  6. Return error if processing fails (enables Extend platform retry)
//
// Error Handling:
//   - Validation errors (nil message, empty userID): Return InvalidArgument error immediately
//   - Processing errors (buffer full): Return Internal error for Extend platform retry
//   - Normal processing: Return success (&emptypb.Empty{}, nil)
//
// Parameters:
//   - ctx: Context for tracing and cancellation
//   - msg: UserLoggedIn event from IAM service
//
// Returns:
//   - *emptypb.Empty: Success response (event consumed)
//   - error: Non-nil if event should be retried by Extend platform
func (h *LoginHandler) OnMessage(ctx context.Context, msg *pb.UserLoggedIn) (*emptypb.Empty, error) {
	// Validation: nil message
	if msg == nil {
		h.logger.Warn().Msg("Received nil login message")
		return nil, status.Error(codes.InvalidArgument, "message cannot be nil")
	}

	// Extract userID from message
	userID := msg.UserId
	if userID == "" {
		h.logger.Warn().Msg("Received login message with empty userId")
		return nil, status.Error(codes.InvalidArgument, "userId cannot be empty")
	}

	// Validate namespace matches deployment namespace
	// Events should only be processed if they belong to this deployment's namespace
	if msg.Namespace != h.namespace {
		h.logger.Warn().
			Str("eventNamespace", msg.Namespace).
			Str("deploymentNamespace", h.namespace).
			Str("userID", userID).
			Msg("Received login event for different namespace, skipping")
		return &emptypb.Empty{}, nil
	}

	// Get all goals from cache
	goals := h.goalCache.GetAllGoals()
	if goals == nil {
		h.logger.Warn().Msg("Goal cache returned nil, skipping event processing")
		return &emptypb.Empty{}, nil
	}

	// Build stat updates map for all login goals (Decision Q1: filter by event_source)
	// Decision Q2: Always use statValue=1 for login events
	statUpdates := make(map[string]int)
	for _, goal := range goals {
		// Skip non-login goals
		if goal.EventSource != domain.EventSourceLogin {
			continue
		}

		// Add this goal's stat_code to the map with value=1
		statUpdates[goal.Requirement.StatCode] = 1
	}

	// If no login goals, return success (no-op)
	if len(statUpdates) == 0 {
		h.logger.Debug().Msg("No login goals found, skipping event processing")
		return &emptypb.Empty{}, nil
	}

	// Process via EventProcessor (handles goal type routing, buffering, etc.)
	// Use msg.Namespace (already validated to match h.namespace above)
	err := h.eventProcessor.ProcessEvent(ctx, userID, msg.Namespace, statUpdates)
	if err != nil {
		// Buffer full or critical error - return error for Extend platform retry (Decision Q4)
		// Event will NOT be marked consumed, allowing retry later
		h.logger.Error().
			Err(err).
			Str("userID", userID).
			Interface("statUpdates", statUpdates).
			Msg("Failed to process login event, returning error for retry")
		return nil, status.Errorf(codes.Internal, "failed to buffer event: %v", err)
	}

	h.logger.Debug().
		Str("userID", userID).
		Int("loginStatCodes", len(statUpdates)).
		Msg("Login event processed successfully")

	return &emptypb.Empty{}, nil
}
