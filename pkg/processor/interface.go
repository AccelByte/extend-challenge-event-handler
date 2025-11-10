// Copyright (c) 2025 AccelByte Inc. All Rights Reserved.
// This is licensed software from AccelByte Inc, for limitations
// and restrictions contact your company contract manager.

package processor

import "context"

// Processor defines the interface for processing challenge events.
// This interface allows for easy mocking in tests.
type Processor interface {
	// ProcessEvent processes events and routes to appropriate goal type handlers.
	//
	// Parameters:
	// - ctx: Context for request cancellation
	// - userID: User identifier
	// - namespace: AccelByte namespace
	// - statUpdates: Map of stat code to value (e.g., {"login_count": 1, "kills": 50})
	//
	// Returns:
	// - error: Non-nil if event processing failed
	ProcessEvent(ctx context.Context, userID, namespace string, statUpdates map[string]int) error
}
