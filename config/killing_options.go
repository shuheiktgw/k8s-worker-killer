package config

import "time"

// KillingOptions contain various options to customize how node killing works
type KillingOptions struct {
	// KillAtOnce sets the number of nodes killed at once
	KillAtOnce float64
	// KillDelayAfterDelete sets the delay after killing a node
	KillDelayAfterDelete time.Duration
	// MaxAge sets the maximum age of nodes
	MaxAge time.Duration
}
