package buffered

import (
	"time"

	"github.com/AccelByte/extend-challenge-common/pkg/domain"
	"github.com/AccelByte/extend-challenge-common/pkg/repository"
	"github.com/AccelByte/extend-challenge-common/pkg/rotation"
)

// EnrichCopyRow builds a CopyRow from a BufferedEvent and its Goal metadata.
// Copies event fields and enriches with goal cache data (TargetValue, rotation metadata).
// If the goal has no rotation config (nil or disabled), rotation fields remain zero values.
func EnrichCopyRow(event *domain.BufferedEvent, goal *domain.Goal, now time.Time) repository.CopyRow {
	row := repository.CopyRow{
		UserID:       event.UserID,
		GoalID:       event.GoalID,
		ChallengeID:  event.ChallengeID,
		Namespace:    event.Namespace,
		Progress:     event.Progress,
		ProgressMode: string(event.ProgressMode),
		IncValue:     event.IncValue,
		TargetValue:  goal.Requirement.TargetValue,
	}

	if goal.Rotation == nil || !goal.Rotation.Enabled {
		return row
	}

	boundary := rotation.CalculateLastRotationBoundary(goal.Rotation.Schedule, now)
	if boundary.IsZero() {
		return row
	}

	row.RotationBoundary = &boundary

	next := rotation.CalculateNextRotationBoundary(goal.Rotation.Schedule, now)
	if !next.IsZero() {
		row.NewExpiresAt = &next
	}

	row.AllowReselection = goal.Rotation.OnExpiry.AllowReselection
	row.ResetProgress = goal.Rotation.OnExpiry.ResetProgress

	return row
}
