// Copyright (c) 2025 AccelByte Inc. All Rights Reserved.
// This is licensed software from AccelByte Inc, for limitations
// and restrictions contact your company contract manager.

package processor

import (
	"context"
	"sync"
	"time"

	"extend-challenge-event-handler/pkg/buffered"

	"github.com/AccelByte/extend-challenge-common/pkg/cache"
	"github.com/AccelByte/extend-challenge-common/pkg/domain"

	"github.com/rs/zerolog"
)

// EventProcessor handles challenge events and updates user goal progress.
//
// Key Features:
// - Per-user mutex: Prevents concurrent processing for same user (avoids race conditions)
// - Event-driven: Processes IAM login events and Statistic update events
// - Cache-based lookup: Fast O(1) goal lookups from in-memory cache
// - Buffered updates: Uses BufferedRepository for batch database writes
//
// Thread Safety:
// - userMutexes map is protected by mutexMapLock
// - Per-user mutexes ensure sequential processing per user
// - Multiple users can be processed concurrently
type EventProcessor struct {
	// bufferedRepo for batch database updates
	bufferedRepo buffered.Repository

	// cache for fast goal lookups
	cache cache.GoalCache

	// namespace for this deployment
	namespace string

	// userMutexes provides per-user locking
	userMutexes map[string]*sync.Mutex

	// mutexMapLock protects the userMutexes map
	mutexMapLock sync.RWMutex

	// logger for structured logging
	logger zerolog.Logger
}

// NewEventProcessor creates a new EventProcessor.
//
// Parameters:
// - bufferedRepo: BufferedRepository for database updates
// - cache: GoalCache for fast goal lookups
// - namespace: AccelByte namespace for this deployment
// - logger: Structured logger
func NewEventProcessor(
	bufferedRepo buffered.Repository,
	cache cache.GoalCache,
	namespace string,
	logger zerolog.Logger,
) *EventProcessor {
	return &EventProcessor{
		bufferedRepo: bufferedRepo,
		cache:        cache,
		namespace:    namespace,
		userMutexes:  make(map[string]*sync.Mutex),
		logger:       logger,
	}
}

// ProcessEvent processes events and routes to appropriate goal type handlers.
//
// Decision Q13: Single ProcessEvent() method with switch statement routing.
//
// This method:
// 1. Acquires per-user mutex (prevents race conditions)
// 2. Looks up goals tracking the specified stat code(s)
// 3. Routes each goal to appropriate handler based on goal type
// 4. Buffers updates via BufferedRepository
//
// Parameters:
// - ctx: Context for request cancellation
// - userID: User identifier
// - namespace: AccelByte namespace
// - statUpdates: Map of stat code to value (e.g., {"login_count": 1, "kills": 50})
//
// The method is non-blocking for the event handler (buffering is async).
func (p *EventProcessor) ProcessEvent(ctx context.Context, userID, namespace string, statUpdates map[string]int) error {
	// Get per-user mutex
	mu := p.getUserMutex(userID)
	mu.Lock()
	defer mu.Unlock()

	startTime := time.Now()
	goalsProcessed := 0

	p.logger.Debug().
		Str("user_id", userID).
		Str("namespace", namespace).
		Int("stats", len(statUpdates)).
		Msg("Processing event")

	// For each stat update
	for statCode, value := range statUpdates {
		// Look up goals tracking this stat code (O(1) cache lookup)
		goals := p.cache.GetGoalsByStatCode(statCode)

		if len(goals) == 0 {
			p.logger.Debug().Str("stat_code", statCode).Msg("No goals track this stat")
			continue
		}

		for _, goal := range goals {
			// Route based on goal type (Decision Q13: switch statement)
			switch goal.Type {
			case domain.GoalTypeAbsolute:
				// Absolute stat value (e.g., kills=100)
				// Decision Q17: Always replace with new stat value
				p.processAbsoluteGoal(ctx, userID, namespace, goal, value)
				goalsProcessed++

			case domain.GoalTypeIncrement:
				// Increment counter (e.g., login count, daily login days)
				// Decision Q14: Login events use IncrementProgress
				// Decision Q18: Daily flag affects BufferedRepository behavior
				p.processIncrementGoal(ctx, userID, namespace, goal)
				goalsProcessed++

			case domain.GoalTypeDaily:
				// Daily occurrence check (e.g., daily login bonus)
				// Decision Q18: Daily type is different from Increment with daily flag
				p.processDailyGoal(ctx, userID, namespace, goal)
				goalsProcessed++

			default:
				// Decision Q16: Graceful degradation for unknown types
				p.logger.Warn().
					Str("goal_id", goal.ID).
					Str("goal_type", string(goal.Type)).
					Str("user_id", userID).
					Str("stat_code", statCode).
					Msg("Unknown goal type, skipping goal update")
			}
		}
	}

	duration := time.Since(startTime)
	p.logger.Info().
		Str("user_id", userID).
		Str("namespace", namespace).
		Int("goals_processed", goalsProcessed).
		Int64("duration_ms", duration.Milliseconds()).
		Msg("Event processed")

	return nil
}

// processAbsoluteGoal handles stat-based goals with absolute values.
// Decision Q17: Always replace with new stat value (no comparison needed)
// Decision Q15: Add validation for negative values with graceful degradation
func (p *EventProcessor) processAbsoluteGoal(ctx context.Context, userID, namespace string, goal *domain.Goal, value int) {
	// Decision Q15: Add validation for negative values
	if value < 0 {
		p.logger.Warn().
			Str("user_id", userID).
			Str("goal_id", goal.ID).
			Str("stat_code", goal.Requirement.StatCode).
			Int("value", value).
			Msg("Negative stat value, skipping goal update")
		return // Graceful degradation: skip invalid values
	}

	// Calculate status based on progress vs target
	status := domain.GoalStatusInProgress
	var completedAt *time.Time
	if value >= goal.Requirement.TargetValue {
		status = domain.GoalStatusCompleted
		now := time.Now()
		completedAt = &now
	}

	// Update progress with absolute value
	update := &domain.UserGoalProgress{
		UserID:      userID,
		GoalID:      goal.ID,
		ChallengeID: goal.ChallengeID,
		Namespace:   namespace,
		Progress:    value, // Absolute value (replaces previous)
		Status:      status,
		CompletedAt: completedAt,
	}

	if err := p.bufferedRepo.UpdateProgress(ctx, update); err != nil {
		p.logger.Error().
			Err(err).
			Str("user_id", userID).
			Str("goal_id", goal.ID).
			Str("stat_code", goal.Requirement.StatCode).
			Msg("Failed to buffer absolute goal update")
	}
}

// processIncrementGoal handles counter-based goals (both regular and daily).
// Decision Q14: Login events use IncrementProgress (not UpdateProgress)
// Decision Q18: Daily flag affects BufferedRepository behavior (date checking)
func (p *EventProcessor) processIncrementGoal(ctx context.Context, userID, namespace string, goal *domain.Goal) {
	// BufferedRepository accumulates deltas before flush
	// For daily increments: BufferedRepository checks date before buffering
	// Multiple events: delta=1, delta=1, delta=1 → flush with delta=3 (regular)
	//                  delta=1, delta=SKIP, delta=SKIP → flush with delta=1 (daily)
	if err := p.bufferedRepo.IncrementProgress(
		ctx,
		userID,
		goal.ID,
		goal.ChallengeID,
		namespace,
		1, // Always 1 for login events
		goal.Requirement.TargetValue,
		goal.Daily, // Decision Q18: Pass daily flag to BufferedRepository
	); err != nil {
		p.logger.Error().
			Err(err).
			Str("user_id", userID).
			Str("goal_id", goal.ID).
			Msg("Failed to buffer increment goal update")
	}
}

// processDailyGoal handles binary daily check goals.
// Decision Q18: Daily type is different from Increment with daily flag
func (p *EventProcessor) processDailyGoal(ctx context.Context, userID, namespace string, goal *domain.Goal) {
	now := time.Now()

	// Daily goals always set progress=1 and completed_at=NOW()
	// Claim validation checks if completed_at is today (repeatable reward)
	update := &domain.UserGoalProgress{
		UserID:      userID,
		GoalID:      goal.ID,
		ChallengeID: goal.ChallengeID,
		Namespace:   namespace,
		Progress:    1, // Always 1 for daily (binary check)
		Status:      domain.GoalStatusCompleted,
		CompletedAt: &now, // Key: timestamp for daily check
	}

	if err := p.bufferedRepo.UpdateProgress(ctx, update); err != nil {
		p.logger.Error().
			Err(err).
			Str("user_id", userID).
			Str("goal_id", goal.ID).
			Msg("Failed to buffer daily goal update")
	}
}

// getUserMutex retrieves or creates a mutex for a specific user.
//
// This ensures that events for the same user are processed sequentially,
// while events for different users can be processed concurrently.
func (p *EventProcessor) getUserMutex(userID string) *sync.Mutex {
	// Try fast path with read lock
	p.mutexMapLock.RLock()
	mu, exists := p.userMutexes[userID]
	p.mutexMapLock.RUnlock()

	if exists {
		return mu
	}

	// Slow path: create new mutex with write lock
	p.mutexMapLock.Lock()
	defer p.mutexMapLock.Unlock()

	// Double-check (another goroutine might have created it)
	if mu, exists := p.userMutexes[userID]; exists {
		return mu
	}

	// Create new mutex for this user
	mu = &sync.Mutex{}
	p.userMutexes[userID] = mu
	return mu
}
