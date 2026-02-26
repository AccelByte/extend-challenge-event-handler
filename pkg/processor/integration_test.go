// Copyright (c) 2025 AccelByte Inc. All Rights Reserved.
// This is licensed software from AccelByte Inc, for limitations
// and restrictions contact your company contract manager.

package processor

import (
	"context"
	"database/sql"
	"extend-challenge-event-handler/pkg/buffered"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/AccelByte/extend-challenge-common/pkg/cache"
	"github.com/AccelByte/extend-challenge-common/pkg/config"
	"github.com/AccelByte/extend-challenge-common/pkg/domain"
	"github.com/AccelByte/extend-challenge-common/pkg/repository"

	_ "github.com/lib/pq"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
)

// Note: These integration tests require a PostgreSQL database.
// Run with: docker run -d --name test-postgres -p 5432:5432 -e POSTGRES_PASSWORD=test postgres:15
// Or use docker-compose with a test database

const integrationTestDSN = "postgres://postgres:test@localhost:5432/postgres?sslmode=disable"

// setupIntegrationTest creates a complete test environment with real components
func setupIntegrationTest(t *testing.T) (*sql.DB, repository.GoalRepository, *buffered.BufferedRepository, *EventProcessor, cache.GoalCache) {
	t.Helper()

	// Setup database
	db, err := sql.Open("postgres", integrationTestDSN)
	if err != nil {
		t.Skipf("Skipping integration test: cannot connect to database: %v", err)
		return nil, nil, nil, nil, nil
	}

	// Check if database is available
	if err := db.Ping(); err != nil {
		t.Skipf("Skipping integration test: database not available: %v", err)
		return nil, nil, nil, nil, nil
	}

	// Create table with M3+ schema (matches migrations)
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS user_goal_progress (
			user_id VARCHAR(100) NOT NULL,
			goal_id VARCHAR(100) NOT NULL,
			challenge_id VARCHAR(100) NOT NULL,
			namespace VARCHAR(100) NOT NULL,
			progress INT NOT NULL DEFAULT 0,
			status VARCHAR(20) NOT NULL DEFAULT 'not_started',
			completed_at TIMESTAMP NULL,
			claimed_at TIMESTAMP NULL,
			created_at TIMESTAMP NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMP NOT NULL DEFAULT NOW(),

			-- M3: User assignment control
			is_active BOOLEAN NOT NULL DEFAULT true,
			assigned_at TIMESTAMP NULL,

			-- M5: System rotation control (added now for forward compatibility)
			expires_at TIMESTAMP NULL,

			PRIMARY KEY (user_id, goal_id),
			CONSTRAINT check_status CHECK (status IN ('not_started', 'in_progress', 'completed', 'claimed')),
			CONSTRAINT check_progress_non_negative CHECK (progress >= 0),
			CONSTRAINT check_claimed_implies_completed CHECK (claimed_at IS NULL OR completed_at IS NOT NULL)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Clean up existing data
	_, err = db.Exec("TRUNCATE TABLE user_goal_progress")
	if err != nil {
		t.Fatalf("Failed to truncate table: %v", err)
	}

	// Create repository
	repo := repository.NewPostgresGoalRepository(db)

	// Create goal cache with test configuration
	testConfig := &config.Config{
		Challenges: []*domain.Challenge{
			{
				ID:          "challenge1",
				Name:        "Test Challenge",
				Description: "Test challenge for integration tests",
				Goals: []*domain.Goal{
					{
						ID:          "login_goal_absolute",
						ChallengeID: "challenge1",
						Name:        "Login 5 Times",
						Description: "Login to the game 5 times",
						EventSource: domain.EventSourceLogin,
						Requirement: domain.Requirement{
							StatCode:     "login_count",
							TargetValue:  5,
							ProgressMode: domain.ProgressModeAbsolute,
						},
						Reward: domain.Reward{
							Type:     "ITEM",
							RewardID: "reward1",
							Quantity: 1,
						},
					},
					{
						ID:          "kills_goal_absolute",
						ChallengeID: "challenge1",
						Name:        "Get 100 Kills",
						Description: "Kill 100 enemies",
						EventSource: domain.EventSourceStatistic,
						Requirement: domain.Requirement{
							StatCode:     "kills",
							TargetValue:  100,
							ProgressMode: domain.ProgressModeAbsolute,
						},
						Reward: domain.Reward{
							Type:     "ITEM",
							RewardID: "reward2",
							Quantity: 1,
						},
					},
					{
						ID:          "daily_quest",
						ChallengeID: "challenge1",
						Name:        "Complete Daily Quest",
						Description: "Complete a daily quest",
						EventSource: domain.EventSourceStatistic,
						Requirement: domain.Requirement{
							StatCode:     "daily_quest_completed",
							TargetValue:  1,
							ProgressMode: domain.ProgressModeAbsolute,
						},
						Reward: domain.Reward{
							Type:     "WALLET",
							RewardID: "GEMS",
							Quantity: 10,
						},
					},
				},
			},
		},
	}
	// Create logger (suppress output in tests)
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	// Create slog logger for cache (required, cannot be nil)
	slogLogger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelError, // Suppress logs in tests
	}))

	// Create goal cache
	goalCache := cache.NewInMemoryGoalCache(testConfig, "", slogLogger)

	// Create buffered repository (short flush interval for testing)
	bufRepo := buffered.NewBufferedRepository(repo, goalCache, "test-namespace", 100*time.Millisecond, 1000, logger)

	// Create event processor
	processor := NewEventProcessor(bufRepo, goalCache, "test-namespace", logger)

	return db, repo, bufRepo, processor, goalCache
}

// cleanupIntegrationTest cleans up test resources
func cleanupIntegrationTest(t *testing.T, db *sql.DB, bufRepo *buffered.BufferedRepository) {
	t.Helper()

	if bufRepo != nil {
		_ = bufRepo.Close()
	}

	if db != nil {
		_, _ = db.Exec("TRUNCATE TABLE user_goal_progress")
		_ = db.Close()
	}
}

// TestE2E_StatEvent_Absolute_Flush_DB tests the full flow for stat update events with absolute goals
func TestE2E_StatEvent_Absolute_Flush_DB(t *testing.T) {
	db, repo, bufRepo, processor, _ := setupIntegrationTest(t)
	if db == nil {
		return
	}
	defer cleanupIntegrationTest(t, db, bufRepo)

	ctx := context.Background()

	t.Run("absolute goal - full flow", func(t *testing.T) {
		userID := "test-user-3"
		goalID := "kills_goal_absolute"

		// Verify no progress exists initially
		progress, err := repo.GetProgress(ctx, userID, goalID)
		assert.NoError(t, err)
		assert.Nil(t, progress, "Should have no initial progress")

		// Initialize the goal (simulates goal assignment)
		err = repo.BulkInsert(ctx, []*domain.UserGoalProgress{
			{
				UserID:      userID,
				GoalID:      goalID,
				ChallengeID: "challenge1",
				Namespace:   "test-namespace",
				Progress:    0,
				Status:      domain.GoalStatusNotStarted,
				IsActive:    true,
			},
		})
		assert.NoError(t, err)

		// Simulate stat update: 50 kills
		statUpdates := map[string]domain.StatUpdate{"kills": statUpdate(50)}
		err = processor.ProcessEvent(ctx, userID, "test-namespace", statUpdates)
		assert.NoError(t, err)

		// Flush
		err = bufRepo.Flush(ctx)
		assert.NoError(t, err)

		// Verify progress in database
		progress, err = repo.GetProgress(ctx, userID, goalID)
		assert.NoError(t, err)
		assert.NotNil(t, progress)
		assert.Equal(t, 50, progress.Progress, "Progress should be 50 (absolute value)")
		assert.Equal(t, domain.GoalStatusInProgress, progress.Status)

		// Simulate stat update: 75 kills (replaces previous value)
		statUpdates = map[string]domain.StatUpdate{"kills": statUpdate(75)}
		err = processor.ProcessEvent(ctx, userID, "test-namespace", statUpdates)
		assert.NoError(t, err)

		// Flush
		err = bufRepo.Flush(ctx)
		assert.NoError(t, err)

		// Verify progress was replaced (not incremented)
		progress, err = repo.GetProgress(ctx, userID, goalID)
		assert.NoError(t, err)
		assert.Equal(t, 75, progress.Progress, "Progress should be 75 (replaced, not 125)")
		assert.Equal(t, domain.GoalStatusInProgress, progress.Status)

		// Simulate stat update: 100 kills (complete goal)
		statUpdates = map[string]domain.StatUpdate{"kills": statUpdate(100)}
		err = processor.ProcessEvent(ctx, userID, "test-namespace", statUpdates)
		assert.NoError(t, err)

		// Flush
		err = bufRepo.Flush(ctx)
		assert.NoError(t, err)

		// Verify goal is completed
		progress, err = repo.GetProgress(ctx, userID, goalID)
		assert.NoError(t, err)
		assert.Equal(t, 100, progress.Progress)
		assert.Equal(t, domain.GoalStatusCompleted, progress.Status)
		assert.NotNil(t, progress.CompletedAt, "CompletedAt should be set")
	})

	t.Run("absolute goal - exceeds target", func(t *testing.T) {
		userID := "test-user-4"
		goalID := "kills_goal_absolute"

		// Initialize the goal (simulates goal assignment)
		err := repo.BulkInsert(ctx, []*domain.UserGoalProgress{
			{
				UserID:      userID,
				GoalID:      goalID,
				ChallengeID: "challenge1",
				Namespace:   "test-namespace",
				Progress:    0,
				Status:      domain.GoalStatusNotStarted,
				IsActive:    true,
			},
		})
		assert.NoError(t, err)

		// Simulate stat update: 150 kills (exceeds target of 100)
		statUpdates := map[string]domain.StatUpdate{"kills": statUpdate(150)}
		err = processor.ProcessEvent(ctx, userID, "test-namespace", statUpdates)
		assert.NoError(t, err)

		// Flush
		err = bufRepo.Flush(ctx)
		assert.NoError(t, err)

		// Verify goal is completed with progress > target
		progress, err := repo.GetProgress(ctx, userID, goalID)
		assert.NoError(t, err)
		assert.Equal(t, 150, progress.Progress, "Progress can exceed target")
		assert.Equal(t, domain.GoalStatusCompleted, progress.Status)
		assert.NotNil(t, progress.CompletedAt)
	})

	t.Run("absolute goal - negative value skipped", func(t *testing.T) {
		userID := "test-user-5"
		goalID := "kills_goal_absolute"

		// Simulate stat update with negative value (should be skipped)
		statUpdates := map[string]domain.StatUpdate{"kills": statUpdate(-10)}
		err := processor.ProcessEvent(ctx, userID, "test-namespace", statUpdates)
		assert.NoError(t, err)

		// Flush
		err = bufRepo.Flush(ctx)
		assert.NoError(t, err)

		// Verify no progress was created (negative value skipped)
		progress, err := repo.GetProgress(ctx, userID, goalID)
		assert.NoError(t, err)
		assert.Nil(t, progress, "Negative values should be skipped")
	})
}

// TestE2E_DailyGoal tests the daily goal using absolute progress mode
func TestE2E_DailyGoal(t *testing.T) {
	db, repo, bufRepo, processor, _ := setupIntegrationTest(t)
	if db == nil {
		return
	}
	defer cleanupIntegrationTest(t, db, bufRepo)

	ctx := context.Background()

	t.Run("daily goal - immediate completion", func(t *testing.T) {
		userID := "test-user-6"
		goalID := "daily_quest"

		// Initialize the goal (simulates goal assignment)
		err := repo.BulkInsert(ctx, []*domain.UserGoalProgress{
			{
				UserID:      userID,
				GoalID:      goalID,
				ChallengeID: "challenge1",
				Namespace:   "test-namespace",
				Progress:    0,
				Status:      domain.GoalStatusNotStarted,
				IsActive:    true,
			},
		})
		assert.NoError(t, err)

		// Simulate daily quest completion
		statUpdates := map[string]domain.StatUpdate{"daily_quest_completed": statUpdate(1)}
		err = processor.ProcessEvent(ctx, userID, "test-namespace", statUpdates)
		assert.NoError(t, err)

		// Flush
		err = bufRepo.Flush(ctx)
		assert.NoError(t, err)

		// Verify goal is immediately completed
		progress, err := repo.GetProgress(ctx, userID, goalID)
		assert.NoError(t, err)
		assert.NotNil(t, progress)
		assert.Equal(t, 1, progress.Progress)
		assert.Equal(t, domain.GoalStatusCompleted, progress.Status, "Daily goals complete immediately")
		assert.NotNil(t, progress.CompletedAt)
	})
}

// TestE2E_LoginEvent_Absolute_Flush_DB tests login events with absolute progress mode
func TestE2E_LoginEvent_Absolute_Flush_DB(t *testing.T) {
	db, repo, bufRepo, processor, _ := setupIntegrationTest(t)
	if db == nil {
		return
	}
	defer cleanupIntegrationTest(t, db, bufRepo)

	ctx := context.Background()

	t.Run("login goal with absolute mode", func(t *testing.T) {
		userID := "test-user-1"
		goalID := "login_goal_absolute"

		// Initialize the goal
		err := repo.BulkInsert(ctx, []*domain.UserGoalProgress{
			{
				UserID:      userID,
				GoalID:      goalID,
				ChallengeID: "challenge1",
				Namespace:   "test-namespace",
				Progress:    0,
				Status:      domain.GoalStatusNotStarted,
				IsActive:    true,
			},
		})
		assert.NoError(t, err)

		// Simulate login event (Value=nil, Inc=1 → falls back to Inc)
		statUpdates := map[string]domain.StatUpdate{"login_count": loginStatUpdate()}
		err = processor.ProcessEvent(ctx, userID, "test-namespace", statUpdates)
		assert.NoError(t, err)

		// Flush
		err = bufRepo.Flush(ctx)
		assert.NoError(t, err)

		// Verify progress
		progress, err := repo.GetProgress(ctx, userID, goalID)
		assert.NoError(t, err)
		assert.NotNil(t, progress)
		assert.Equal(t, 1, progress.Progress, "Login event should use Inc=1 as fallback")
	})
}

// TestE2E_AutomaticTimeBasedFlush tests that the buffered repository automatically flushes after the time interval
func TestE2E_AutomaticTimeBasedFlush(t *testing.T) {
	db, repo, bufRepo, processor, _ := setupIntegrationTest(t)
	if db == nil {
		return
	}
	defer cleanupIntegrationTest(t, db, bufRepo)

	ctx := context.Background()

	t.Run("automatic flush after interval", func(t *testing.T) {
		userID := "test-user-8"
		goalID := "login_goal_absolute"

		// Initialize the goal (simulates goal assignment)
		err := repo.BulkInsert(ctx, []*domain.UserGoalProgress{
			{
				UserID:      userID,
				GoalID:      goalID,
				ChallengeID: "challenge1",
				Namespace:   "test-namespace",
				Progress:    0,
				Status:      domain.GoalStatusNotStarted,
				IsActive:    true,
			},
		})
		assert.NoError(t, err)

		// Process event (buffered, not flushed yet)
		statUpdates := map[string]domain.StatUpdate{"login_count": loginStatUpdate()}
		err = processor.ProcessEvent(ctx, userID, "test-namespace", statUpdates)
		assert.NoError(t, err)

		// Wait for automatic flush (100ms interval configured in setup)
		time.Sleep(200 * time.Millisecond)

		// Verify progress was automatically flushed to database
		progress, err := repo.GetProgress(ctx, userID, goalID)
		assert.NoError(t, err)
		assert.NotNil(t, progress, "Progress should be auto-flushed after interval")
		assert.Equal(t, 1, progress.Progress)
	})
}
