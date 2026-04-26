package peer

import (
	"crypto/tls"
	"fmt"

	"talaria/config"
)

// Start is reserved for the node networking runtime.
//
// In the current branch, protocol policy and hodos pipeline work are in active
// development and the full peer runtime is intentionally not wired yet.
func (m *Manager) Start(_ *config.Config, _ *tls.Config, _ *tls.Config) error {
	return fmt.Errorf("peer manager start: not implemented in this build; use -hodos-only for hodos pipeline runs")
}
