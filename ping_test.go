package sync

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockPingTarget struct {
	sleepDuration time.Duration
}

func (m mockPingTarget) ping(columns []string) error {
	time.Sleep(m.sleepDuration)
	return nil
}

func TestPing_timeout(t *testing.T) {
	target := mockPingTarget{sleepDuration: 500 * time.Millisecond}

	// Should error when the ping operation times out
	err := pingWithTimeout(100*time.Millisecond, target, nil)
	require.Error(t, err)
	assert.ErrorContains(t, err, "ping operation timed out")

	// Should not error when the ping operation completes within the timeout
	err = pingWithTimeout(30*time.Second, target, nil)
	require.NoError(t, err)
}
