package router

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestQuietUntil(t *testing.T) {
	almaty, err := time.LoadLocation("Asia/Almaty")
	require.NoError(t, err)
	at := func(h, m int) time.Time { return time.Date(2026, 6, 1, h, m, 0, 0, time.UTC) }

	cases := []struct {
		name       string
		now        time.Time
		loc        *time.Location
		start, end string
		quiet      bool
		until      time.Time
	}{
		{"overnight before midnight", at(23, 0), time.UTC, "22:00", "08:00", true, time.Date(2026, 6, 2, 8, 0, 0, 0, time.UTC)},
		{"overnight after midnight", at(3, 0), time.UTC, "22:00", "08:00", true, at(8, 0)},
		{"overnight outside", at(12, 0), time.UTC, "22:00", "08:00", false, time.Time{}},
		{"same day inside", at(13, 0), time.UTC, "12:00", "14:00", true, at(14, 0)},
		{"same day end exclusive", at(14, 0), time.UTC, "12:00", "14:00", false, time.Time{}},
		{"empty window", at(3, 0), time.UTC, "08:00", "08:00", false, time.Time{}},
		{"invalid clock", at(3, 0), time.UTC, "xx", "08:00", false, time.Time{}},
		{"timezone aware", at(18, 0), almaty, "22:00", "08:00", true, time.Date(2026, 6, 2, 3, 0, 0, 0, time.UTC)},
	}
	for _, c := range cases {
		quiet, until := quietUntil(c.now, c.loc, c.start, c.end)
		require.Equal(t, c.quiet, quiet, c.name)
		if c.quiet {
			require.True(t, c.until.Equal(until), "%s: want %s got %s", c.name, c.until, until)
		}
	}
}
