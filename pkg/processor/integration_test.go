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
						ID:          "login_goal_increment",
						ChallengeID: "challenge1",
						Name:        "Login 5 Times",
						Description: "Login to the game 5 times",
						Type:        domain.GoalTypeIncrement,
						EventSource: domain.EventSourceLogin,
						Daily:       false,
						Requirement: domain.Requirement{
							StatCode:    "login_count",
							TargetValue: 5,
						},
						Reward: domain.Reward{
							Type:     "ITEM",
							RewardID: "reward1",
							Quantity: 1,
						},
					},
					{
						ID:          "login_goal_daily",
						ChallengeID: "challenge1",
						Name:        "Login 7 Days",
						Description: "Login to the game for 7 days (daily increment)",
						Type:        domain.GoalTypeIncrement,
						EventSource: domain.EventSourceLogin,
						Daily:       true,
						Requirement: domain.Requirement{
							StatCode:    "login_count",
							TargetValue: 7,
						},
						Reward: domain.Reward{
							Type:     "WALLET",
							RewardID: "GOLD",
							Quantity: 100,
						},
					},
					{
						ID:          "kills_goal_absolute",
						ChallengeID: "challenge1",
						Name:        "Get 100 Kills",
						Description: "Kill 100 enemies",
						Type:        domain.GoalTypeAbsolute,
						EventSource: domain.EventSourceStatistic,
						Requirement: domain.Requirement{
							StatCode:    "kills",
							TargetValue: 100,
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
						Type:        domain.GoalTypeDaily,
						EventSource: domain.EventSourceStatistic,
						Requirement: domain.Requirement{
							StatCode:    "daily_quest_completed",
							TargetValue: 1,
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

// TestE2E_LoginEvent_Increment_Flush_DB tests the full flow for login events with increment goals
func TestE2E_LoginEvent_Increment_Flush_DB(t *testing.T) {
	db, repo, bufRepo, processor, _ := setupIntegrationTest(t)
	if db == nil {
		return
	}
	defer cleanupIntegrationTest(t, db, bufRepo)

	ctx := context.Background()

	t.Run("regular increment goal - full flow", func(t *testing.T) {
		userID := "test-user-1"
		goalID := "login_goal_increment"

		// Verify no progress exists initially
		progress, err := repo.GetProgress(ctx, userID, goalID)
		assert.NoError(t, err)
		assert.Nil(t, progress, "Should have no initial progress")

		// Initialize the goal (simulates goal assignment)
		// This creates the row that increments will update
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

		// Simulate 3 login events
		for i := 1; i <= 3; i++ {
			statUpdates := map[string]int{"login_count": 1}
			err = processor.ProcessEvent(ctx, userID, "test-namespace", statUpdates)
			assert.NoError(t, err)
		}

		// Manually flush buffer
		err = bufRepo.Flush(ctx)
		assert.NoError(t, err)

		// Verify progress in database
		progress, err = repo.GetProgress(ctx, userID, goalID)
		assert.NoError(t, err)
		assert.NotNil(t, progress, "Progress should exist after flush")
		assert.Equal(t, 3, progress.Progress, "Progress should be 3 (3 logins)")
		assert.Equal(t, domain.GoalStatusInProgress, progress.Status, "Status should be in_progress")

		// Simulate 2 more logins to complete goal (5 total)
		for i := 1; i <= 2; i++ {
			statUpdates := map[string]int{"login_count": 1}
			err = processor.ProcessEvent(ctx, userID, "test-namespace", statUpdates)
			assert.NoError(t, err)
		}

		// Flush again
		err = bufRepo.Flush(ctx)
		assert.NoError(t, err)

		// Verify goal is completed
		progress, err = repo.GetProgress(ctx, userID, goalID)
		assert.NoError(t, err)
		assert.Equal(t, 5, progress.Progress, "Progress should be 5 (completed)")
		assert.Equal(t, domain.GoalStatusCompleted, progress.Status, "Status should be completed")
		assert.NotNil(t, progress.CompletedAt, "CompletedAt should be set")
	})

	t.Run("daily increment goal - full flow", func(t *testing.T) {
		userID := "test-user-2"
		goalID := "login_goal_daily"

		// Initialize the goal (simulates goal assignment)
		// Set updated_at to 3 days ago so first event will increment
		oldTime := time.Now().UTC().Add(-72 * time.Hour)
		_, err := db.ExecContext(ctx, `
			INSERT INTO user_goal_progress (
				user_id, goal_id, challenge_id, namespace,
				progress, status, is_active,
				created_at, updated_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		`, userID, goalID, "challenge1", "test-namespace",
			0, domain.GoalStatusNotStarted, true, oldTime, oldTime)
		assert.NoError(t, err)

		// Simulate first day login
		statUpdates := map[string]int{"login_count": 1}
		err = processor.ProcessEvent(ctx, userID, "test-namespace", statUpdates)
		assert.NoError(t, err)

		// Flush
		err = bufRepo.Flush(ctx)
		assert.NoError(t, err)

		// Verify first day progress
		progress, err := repo.GetProgress(ctx, userID, goalID)
		assert.NoError(t, err)
		assert.NotNil(t, progress)
		assert.Equal(t, 1, progress.Progress, "Progress should be 1 after first day")

		// Simulate second login same day (should be no-op due to client-side check in BufferedRepository)
		err = processor.ProcessEvent(ctx, userID, "test-namespace", statUpdates)
		assert.NoError(t, err)

		// Flush
		err = bufRepo.Flush(ctx)
		assert.NoError(t, err)

		// Verify progress is still 1 (same day no-op handled by BufferedRepository)
		progress, err = repo.GetProgress(ctx, userID, goalID)
		assert.NoError(t, err)
		assert.Equal(t, 1, progress.Progress, "Progress should still be 1 (same day)")
	})

	t.Run("multiple users concurrent logins", func(t *testing.T) {
		goalID := "login_goal_increment"

		// Initialize goals for all users
		users := []string{"user-a", "user-b", "user-c"}
		var progresses []*domain.UserGoalProgress
		for _, userID := range users {
			progresses = append(progresses, &domain.UserGoalProgress{
				UserID:      userID,
				GoalID:      goalID,
				ChallengeID: "challenge1",
				Namespace:   "test-namespace",
				Progress:    0,
				Status:      domain.GoalStatusNotStarted,
				IsActive:    true,
			})
		}
		err := repo.BulkInsert(ctx, progresses)
		assert.NoError(t, err)

		// Simulate 3 different users logging in
		for _, userID := range users {
			statUpdates := map[string]int{"login_count": 1}
			err := processor.ProcessEvent(ctx, userID, "test-namespace", statUpdates)
			assert.NoError(t, err)
		}

		// Flush all at once
		err = bufRepo.Flush(ctx)
		assert.NoError(t, err)

		// Verify each user has progress
		for _, userID := range users {
			progress, err := repo.GetProgress(ctx, userID, goalID)
			assert.NoError(t, err)
			assert.NotNil(t, progress)
			assert.Equal(t, 1, progress.Progress)
			assert.Equal(t, domain.GoalStatusInProgress, progress.Status)
		}
	})
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
		statUpdates := map[string]int{"kills": 50}
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
		statUpdates = map[string]int{"kills": 75}
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
		statUpdates = map[string]int{"kills": 100}
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
		statUpdates := map[string]int{"kills": 150}
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
		statUpdates := map[string]int{"kills": -10}
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

// TestE2E_DailyGoalType tests the daily goal type (different from daily increment)
func TestE2E_DailyGoalType(t *testing.T) {
	db, repo, bufRepo, processor, _ := setupIntegrationTest(t)
	if db == nil {
		return
	}
	defer cleanupIntegrationTest(t, db, bufRepo)

	ctx := context.Background()

	t.Run("daily goal type - immediate completion", func(t *testing.T) {
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
		statUpdates := map[string]int{"daily_quest_completed": 1}
		err = processor.ProcessEvent(ctx, userID, "test-namespace", statUpdates)
		assert.NoError(t, err)

		// Flush
		err = bufRepo.Flush(ctx)
		assert.NoError(t, err)

		// Verify goal is immediately completed (daily goals complete on first trigger)
		progress, err := repo.GetProgress(ctx, userID, goalID)
		assert.NoError(t, err)
		assert.NotNil(t, progress)
		assert.Equal(t, 1, progress.Progress)
		assert.Equal(t, domain.GoalStatusCompleted, progress.Status, "Daily goals complete immediately")
		assert.NotNil(t, progress.CompletedAt)
	})
}

// TestE2E_MixedGoalTypes tests multiple goal types processing in a single event
func TestE2E_MixedGoalTypes(t *testing.T) {
	db, repo, bufRepo, processor, _ := setupIntegrationTest(t)
	if db == nil {
		return
	}
	defer cleanupIntegrationTest(t, db, bufRepo)

	ctx := context.Background()

	t.Run("single event triggers multiple goal types", func(t *testing.T) {
		userID := "test-user-7"

		// Initialize both goals (simulates goal assignment)
		oldTime := time.Now().UTC().Add(-72 * time.Hour)
		err := repo.BulkInsert(ctx, []*domain.UserGoalProgress{
			{
				UserID:      userID,
				GoalID:      "login_goal_increment",
				ChallengeID: "challenge1",
				Namespace:   "test-namespace",
				Progress:    0,
				Status:      domain.GoalStatusNotStarted,
				IsActive:    true,
			},
		})
		assert.NoError(t, err)

		// For daily goal, use old timestamp so first event increments
		_, err = db.ExecContext(ctx, `
			INSERT INTO user_goal_progress (
				user_id, goal_id, challenge_id, namespace,
				progress, status, is_active,
				created_at, updated_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		`, userID, "login_goal_daily", "challenge1", "test-namespace",
			0, domain.GoalStatusNotStarted, true, oldTime, oldTime)
		assert.NoError(t, err)

		// Single login event triggers both increment goals and daily increment goals
		statUpdates := map[string]int{"login_count": 1}
		err = processor.ProcessEvent(ctx, userID, "test-namespace", statUpdates)
		assert.NoError(t, err)

		// Flush
		err = bufRepo.Flush(ctx)
		assert.NoError(t, err)

		// Verify both goals were updated
		// 1. Regular increment goal
		progress1, err := repo.GetProgress(ctx, userID, "login_goal_increment")
		assert.NoError(t, err)
		assert.NotNil(t, progress1)
		assert.Equal(t, 1, progress1.Progress)

		// 2. Daily increment goal
		progress2, err := repo.GetProgress(ctx, userID, "login_goal_daily")
		assert.NoError(t, err)
		assert.NotNil(t, progress2)
		assert.Equal(t, 1, progress2.Progress)
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
		goalID := "login_goal_increment"

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
		statUpdates := map[string]int{"login_count": 1}
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
