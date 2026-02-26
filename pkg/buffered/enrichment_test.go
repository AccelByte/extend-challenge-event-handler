package buffered

import (
	"testing"
	"time"

	"github.com/AccelByte/extend-challenge-common/pkg/domain"
	"github.com/AccelByte/extend-challenge-common/pkg/rotation"
	"github.com/stretchr/testify/assert"
)

func TestEnrichCopyRow(t *testing.T) {
	// Fixed reference time: Wednesday 2025-06-25 14:30:00 UTC
	now := time.Date(2025, 6, 25, 14, 30, 0, 0, time.UTC)

	todayMidnight := time.Date(2025, 6, 25, 0, 0, 0, 0, time.UTC)
	tomorrowMidnight := time.Date(2025, 6, 26, 0, 0, 0, 0, time.UTC)
	lastMonday := time.Date(2025, 6, 23, 0, 0, 0, 0, time.UTC)      // Monday
	nextMonday := time.Date(2025, 6, 30, 0, 0, 0, 0, time.UTC)      // Next Monday
	firstOfMonth := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)     // June 1st
	firstOfNextMonth := time.Date(2025, 7, 1, 0, 0, 0, 0, time.UTC) // July 1st

	progress50 := 50

	tests := []struct {
		name             string
		event            *domain.BufferedEvent
		goal             *domain.Goal
		wantProgress     *int
		wantProgressMode string
		wantIncValue     int
		wantTargetValue  int
		wantBoundary     *time.Time
		wantExpiresAt    *time.Time
		wantAllowResel   bool
		wantResetProg    bool
	}{
		{
			name: "absolute goal, no rotation",
			event: &domain.BufferedEvent{
				UserID: "user1", GoalID: "goal1", ChallengeID: "ch1", Namespace: "ns",
				Progress: &progress50, IncValue: 0, ProgressMode: domain.ProgressModeAbsolute,
			},
			goal: &domain.Goal{
				ID: "goal1", ChallengeID: "ch1",
				Requirement: domain.Requirement{TargetValue: 100, ProgressMode: domain.ProgressModeAbsolute},
			},
			wantProgress:     &progress50,
			wantProgressMode: "absolute",
			wantTargetValue:  100,
			wantBoundary:     nil,
			wantExpiresAt:    nil,
			wantAllowResel:   false,
			wantResetProg:    false,
		},
		{
			name: "relative goal, daily rotation, reset=true",
			event: &domain.BufferedEvent{
				UserID: "user2", GoalID: "goal2", ChallengeID: "ch1", Namespace: "ns",
				Progress: &progress50, IncValue: 5, ProgressMode: domain.ProgressModeRelative,
			},
			goal: &domain.Goal{
				ID: "goal2", ChallengeID: "ch1",
				Requirement: domain.Requirement{TargetValue: 10, ProgressMode: domain.ProgressModeRelative},
				Rotation: &domain.RotationConfig{
					Enabled: true, Type: domain.RotationTypeGlobal, Schedule: domain.RotationScheduleDaily,
					OnExpiry: domain.OnExpiryConfig{ResetProgress: true, AllowReselection: false},
				},
			},
			wantProgress:     &progress50,
			wantProgressMode: "relative",
			wantIncValue:     5,
			wantTargetValue:  10,
			wantBoundary:     &todayMidnight,
			wantExpiresAt:    &tomorrowMidnight,
			wantAllowResel:   false,
			wantResetProg:    true,
		},
		{
			name: "relative goal, weekly rotation",
			event: &domain.BufferedEvent{
				UserID: "user3", GoalID: "goal3", ChallengeID: "ch1", Namespace: "ns",
				Progress: &progress50, IncValue: 3, ProgressMode: domain.ProgressModeRelative,
			},
			goal: &domain.Goal{
				ID: "goal3", ChallengeID: "ch1",
				Requirement: domain.Requirement{TargetValue: 20, ProgressMode: domain.ProgressModeRelative},
				Rotation: &domain.RotationConfig{
					Enabled: true, Type: domain.RotationTypeGlobal, Schedule: domain.RotationScheduleWeekly,
					OnExpiry: domain.OnExpiryConfig{ResetProgress: true, AllowReselection: true},
				},
			},
			wantProgress:     &progress50,
			wantProgressMode: "relative",
			wantIncValue:     3,
			wantTargetValue:  20,
			wantBoundary:     &lastMonday,
			wantExpiresAt:    &nextMonday,
			wantAllowResel:   true,
			wantResetProg:    true,
		},
		{
			name: "relative goal, monthly rotation",
			event: &domain.BufferedEvent{
				UserID: "user4", GoalID: "goal4", ChallengeID: "ch1", Namespace: "ns",
				Progress: &progress50, IncValue: 1, ProgressMode: domain.ProgressModeRelative,
			},
			goal: &domain.Goal{
				ID: "goal4", ChallengeID: "ch1",
				Requirement: domain.Requirement{TargetValue: 30, ProgressMode: domain.ProgressModeRelative},
				Rotation: &domain.RotationConfig{
					Enabled: true, Type: domain.RotationTypeGlobal, Schedule: domain.RotationScheduleMonthly,
					OnExpiry: domain.OnExpiryConfig{ResetProgress: false, AllowReselection: false},
				},
			},
			wantProgress:     &progress50,
			wantProgressMode: "relative",
			wantIncValue:     1,
			wantTargetValue:  30,
			wantBoundary:     &firstOfMonth,
			wantExpiresAt:    &firstOfNextMonth,
			wantAllowResel:   false,
			wantResetProg:    false,
		},
		{
			name: "rotation disabled",
			event: &domain.BufferedEvent{
				UserID: "user5", GoalID: "goal5", ChallengeID: "ch1", Namespace: "ns",
				Progress: &progress50, IncValue: 2, ProgressMode: domain.ProgressModeRelative,
			},
			goal: &domain.Goal{
				ID: "goal5", ChallengeID: "ch1",
				Requirement: domain.Requirement{TargetValue: 10, ProgressMode: domain.ProgressModeRelative},
				Rotation: &domain.RotationConfig{
					Enabled: false, Type: domain.RotationTypeGlobal, Schedule: domain.RotationScheduleDaily,
					OnExpiry: domain.OnExpiryConfig{ResetProgress: true, AllowReselection: false},
				},
			},
			wantProgress:     &progress50,
			wantProgressMode: "relative",
			wantIncValue:     2,
			wantTargetValue:  10,
			wantBoundary:     nil,
			wantExpiresAt:    nil,
			wantAllowResel:   false,
			wantResetProg:    false,
		},
		{
			name: "nil rotation config",
			event: &domain.BufferedEvent{
				UserID: "user6", GoalID: "goal6", ChallengeID: "ch1", Namespace: "ns",
				Progress: &progress50, IncValue: 1, ProgressMode: domain.ProgressModeRelative,
			},
			goal: &domain.Goal{
				ID: "goal6", ChallengeID: "ch1",
				Requirement: domain.Requirement{TargetValue: 5, ProgressMode: domain.ProgressModeRelative},
				Rotation:    nil,
			},
			wantProgress:     &progress50,
			wantProgressMode: "relative",
			wantIncValue:     1,
			wantTargetValue:  5,
			wantBoundary:     nil,
			wantExpiresAt:    nil,
			wantAllowResel:   false,
			wantResetProg:    false,
		},
		{
			name: "login event (nil Progress) with daily rotation",
			event: &domain.BufferedEvent{
				UserID: "user7", GoalID: "goal7", ChallengeID: "ch1", Namespace: "ns",
				Progress: nil, IncValue: 1, ProgressMode: domain.ProgressModeRelative,
			},
			goal: &domain.Goal{
				ID: "goal7", ChallengeID: "ch1",
				Requirement: domain.Requirement{TargetValue: 5, ProgressMode: domain.ProgressModeRelative},
				Rotation: &domain.RotationConfig{
					Enabled: true, Type: domain.RotationTypeGlobal, Schedule: domain.RotationScheduleDaily,
					OnExpiry: domain.OnExpiryConfig{ResetProgress: true, AllowReselection: false},
				},
			},
			wantProgress:     nil,
			wantProgressMode: "relative",
			wantIncValue:     1,
			wantTargetValue:  5,
			wantBoundary:     &todayMidnight,
			wantExpiresAt:    &tomorrowMidnight,
			wantAllowResel:   false,
			wantResetProg:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			row := EnrichCopyRow(tt.event, tt.goal, now)

			assert.Equal(t, tt.event.UserID, row.UserID)
			assert.Equal(t, tt.event.GoalID, row.GoalID)
			assert.Equal(t, tt.event.ChallengeID, row.ChallengeID)
			assert.Equal(t, tt.event.Namespace, row.Namespace)
			assert.Equal(t, tt.wantProgress, row.Progress)
			assert.Equal(t, tt.wantProgressMode, row.ProgressMode)
			assert.Equal(t, tt.wantIncValue, row.IncValue)
			assert.Equal(t, tt.wantTargetValue, row.TargetValue)
			assert.Equal(t, tt.wantBoundary, row.RotationBoundary)
			assert.Equal(t, tt.wantExpiresAt, row.NewExpiresAt)
			assert.Equal(t, tt.wantAllowResel, row.AllowReselection)
			assert.Equal(t, tt.wantResetProg, row.ResetProgress)
		})
	}
}

func TestEnrichCopyRow_BoundaryConsistentWithRotationPackage(t *testing.T) {
	// Verify EnrichCopyRow produces boundaries consistent with the rotation package
	now := time.Date(2025, 6, 25, 14, 30, 0, 0, time.UTC)
	progress := 100

	event := &domain.BufferedEvent{
		UserID: "user1", GoalID: "goal1", ChallengeID: "ch1", Namespace: "ns",
		Progress: &progress, IncValue: 5, ProgressMode: domain.ProgressModeRelative,
	}
	goal := &domain.Goal{
		ID: "goal1", ChallengeID: "ch1",
		Requirement: domain.Requirement{TargetValue: 10, ProgressMode: domain.ProgressModeRelative},
		Rotation: &domain.RotationConfig{
			Enabled: true, Type: domain.RotationTypeGlobal, Schedule: domain.RotationScheduleDaily,
			OnExpiry: domain.OnExpiryConfig{ResetProgress: true, AllowReselection: false},
		},
	}

	row := EnrichCopyRow(event, goal, now)

	expectedBoundary := rotation.CalculateLastRotationBoundary(domain.RotationScheduleDaily, now)
	expectedExpires := rotation.CalculateNextRotationBoundary(domain.RotationScheduleDaily, now)

	assert.NotNil(t, row.RotationBoundary)
	assert.Equal(t, expectedBoundary, *row.RotationBoundary)
	assert.NotNil(t, row.NewExpiresAt)
	assert.Equal(t, expectedExpires, *row.NewExpiresAt)
}
