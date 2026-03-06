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
// - M5 Phase 2: Creates BufferedEvent instead of UserGoalProgress (SQL handles status)
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

// ProcessEvent processes events and routes to the unified goal handler.
// M5 Phase 2: Passes both progress *int and incValue int to processGoal.
func (p *EventProcessor) ProcessEvent(ctx context.Context, userID, namespace string, statUpdates map[string]domain.StatUpdate) error {
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

	for statCode, update := range statUpdates {
		goals := p.cache.GetGoalsByStatCode(statCode)

		if len(goals) == 0 {
			p.logger.Debug().Str("stat_code", statCode).Msg("No goals track this stat")
			continue
		}

		// Extract progress pointer and incValue from update
		var progress *int
		if update.Value != nil {
			progress = update.Value
		}
		incValue := update.Inc

		for _, goal := range goals {
			switch goal.Requirement.ProgressMode {
			case domain.ProgressModeAbsolute, domain.ProgressModeRelative:
				p.processGoal(ctx, userID, namespace, goal, progress, incValue)
				goalsProcessed++

			default:
				p.logger.Warn().
					Str("goal_id", goal.ID).
					Str("progress_mode", string(goal.Requirement.ProgressMode)).
					Str("user_id", userID).
					Str("stat_code", statCode).
					Msg("Unknown progress mode, skipping goal update")
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

// processGoal handles all goal types by creating a BufferedEvent.
// M5 Phase 2: No status pre-computation. SQL handles status in CASE expressions.
func (p *EventProcessor) processGoal(ctx context.Context, userID, namespace string, goal *domain.Goal, progress *int, incValue int) {
	// Negative value validation for absolute progress
	if progress != nil && *progress < 0 {
		p.logger.Warn().
			Str("user_id", userID).
			Str("goal_id", goal.ID).
			Str("stat_code", goal.Requirement.StatCode).
			Int("value", *progress).
			Msg("Negative stat value, skipping goal update")
		return
	}

	event := &domain.BufferedEvent{
		UserID:       userID,
		GoalID:       goal.ID,
		ChallengeID:  goal.ChallengeID,
		Namespace:    namespace,
		Progress:     progress,
		IncValue:     incValue,
		ProgressMode: goal.Requirement.ProgressMode,
	}

	if err := p.bufferedRepo.UpdateProgress(ctx, event); err != nil {
		p.logger.Error().
			Err(err).
			Str("user_id", userID).
			Str("goal_id", goal.ID).
			Str("stat_code", goal.Requirement.StatCode).
			Msg("Failed to buffer goal update")
	}
}

// getUserMutex retrieves or creates a mutex for a specific user.
func (p *EventProcessor) getUserMutex(userID string) *sync.Mutex {
	p.mutexMapLock.RLock()
	mu, exists := p.userMutexes[userID]
	p.mutexMapLock.RUnlock()

	if exists {
		return mu
	}

	p.mutexMapLock.Lock()
	defer p.mutexMapLock.Unlock()

	if mu, exists := p.userMutexes[userID]; exists {
		return mu
	}

	mu = &sync.Mutex{}
	p.userMutexes[userID] = mu
	return mu
}
