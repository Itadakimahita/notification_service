package router

import (
	"strconv"
	"strings"
	"time"
)

// parseClock converts HH:MM into minutes after midnight.
func parseClock(s string) (int, bool) {
	h, m, ok := strings.Cut(s, ":")
	if !ok {
		return 0, false
	}
	hh, err1 := strconv.Atoi(h)
	mm, err2 := strconv.Atoi(m)
	if err1 != nil || err2 != nil || hh < 0 || hh > 23 || mm < 0 || mm > 59 {
		return 0, false
	}
	return hh*60 + mm, true
}

// ValidClock reports whether s is a valid HH:MM time.
func ValidClock(s string) bool {
	_, ok := parseClock(s)
	return ok
}

// quietUntil reports whether now falls in the user's quiet window and when the window ends.
func quietUntil(now time.Time, loc *time.Location, start, end string) (bool, time.Time) {
	s, ok1 := parseClock(start)
	e, ok2 := parseClock(end)
	if !ok1 || !ok2 || s == e {
		return false, time.Time{}
	}
	local := now.In(loc)
	m := local.Hour()*60 + local.Minute()
	at := func(dayOffset int) time.Time {
		return time.Date(local.Year(), local.Month(), local.Day()+dayOffset, e/60, e%60, 0, 0, loc)
	}
	if s < e {
		if m >= s && m < e {
			return true, at(0)
		}
		return false, time.Time{}
	}
	switch {
	case m >= s:
		return true, at(1)
	case m < e:
		return true, at(0)
	}
	return false, time.Time{}
}
