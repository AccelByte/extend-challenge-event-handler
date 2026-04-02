package service

import (
	"context"
	"time"

	pb "extend-challenge-event-handler/pkg/pb/accelbyte-asyncapi/iam/account/v1"

	"github.com/AccelByte/extend-challenge-common/pkg/repository"

	"github.com/rs/zerolog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// GDPRDeletionHandler processes AGS GDPR deletion events and removes user goal progress data.
//
// This handler uses GoalRepository directly (not buffered) because GDPR deletions
// must be immediate and durable — they cannot be deferred or lost in a buffer.
type GDPRDeletionHandler struct {
	pb.UnimplementedDeletionAccountGdprGdprRequestDataDeletionResponseServiceServer
	repo      repository.GoalRepository
	namespace string
	logger    zerolog.Logger
}

// NewGDPRDeletionHandler creates a new GDPR deletion event handler.
func NewGDPRDeletionHandler(
	repo repository.GoalRepository,
	namespace string,
	logger zerolog.Logger,
) *GDPRDeletionHandler {
	return &GDPRDeletionHandler{
		repo:      repo,
		namespace: namespace,
		logger:    logger,
	}
}

// OnMessage processes a GDPR data deletion event.
func (h *GDPRDeletionHandler) OnMessage(ctx context.Context, msg *pb.GdprRequestDataDeletionResponse) (*emptypb.Empty, error) {
	if msg == nil {
		h.logger.Warn().Msg("Received nil GDPR deletion message")
		return nil, status.Error(codes.InvalidArgument, "message cannot be nil")
	}

	// Extract userID: prefer payload.deletion_gdpr.user_id, fallback to top-level user_id
	userID := ""
	if payload := msg.GetPayload(); payload != nil {
		if gdpr := payload.GetDeletionGdpr(); gdpr != nil {
			userID = gdpr.GetUserId()
		}
	}
	if userID == "" {
		userID = msg.GetUserId()
	}

	if userID == "" {
		h.logger.Warn().Msg("Received GDPR deletion message with empty userId")
		return nil, status.Error(codes.InvalidArgument, "userId cannot be empty")
	}

	// Validate namespace matches deployment namespace
	eventNamespace := msg.GetNamespace()
	if eventNamespace != h.namespace {
		h.logger.Debug().
			Str("eventNamespace", eventNamespace).
			Str("deploymentNamespace", h.namespace).
			Str("userID", userID).
			Msg("GDPR deletion event for different namespace, skipping")
		return &emptypb.Empty{}, nil
	}

	deleted, err := h.repo.DeleteUserData(ctx, h.namespace, userID)
	if err != nil {
		h.logger.Error().
			Err(err).
			Str("userID", userID).
			Str("namespace", h.namespace).
			Msg("GDPR deletion via event failed")
		return nil, status.Errorf(codes.Internal, "failed to delete user data: %v", err)
	}

	h.logger.Info().
		Str("userID", userID).
		Int64("rowsDeleted", deleted).
		Str("namespace", h.namespace).
		Bool("audit", true).
		Str("auditAction", "gdpr_event_user_data_deletion").
		Str("deletedAt", time.Now().UTC().Format(time.RFC3339)).
		Msg("GDPR deletion via event completed")

	return &emptypb.Empty{}, nil
}
