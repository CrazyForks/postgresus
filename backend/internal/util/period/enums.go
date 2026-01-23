package period

import "time"

type Period string

const (
	PeriodDay     Period = "DAY"
	PeriodWeek    Period = "WEEK"
	PeriodMonth   Period = "MONTH"
	Period3Month  Period = "3_MONTH"
	Period6Month  Period = "6_MONTH"
	PeriodYear    Period = "YEAR"
	Period2Years  Period = "2_YEARS"
	Period3Years  Period = "3_YEARS"
	Period4Years  Period = "4_YEARS"
	Period5Years  Period = "5_YEARS"
	PeriodForever Period = "FOREVER"
)

// ToDuration converts Period to time.Duration
func (p Period) ToDuration() time.Duration {
	switch p {
	case PeriodDay:
		return 24 * time.Hour
	case PeriodWeek:
		return 7 * 24 * time.Hour
	case PeriodMonth:
		return 30 * 24 * time.Hour
	case Period3Month:
		return 90 * 24 * time.Hour
	case Period6Month:
		return 180 * 24 * time.Hour
	case PeriodYear:
		return 365 * 24 * time.Hour
	case Period2Years:
		return 2 * 365 * 24 * time.Hour
	case Period3Years:
		return 3 * 365 * 24 * time.Hour
	case Period4Years:
		return 4 * 365 * 24 * time.Hour
	case Period5Years:
		return 5 * 365 * 24 * time.Hour
	case PeriodForever:
		return 0
	default:
		panic("unknown period: " + string(p))
	}
}

// CompareTo compares this period with another and returns:
// -1 if p < other
//
//	0 if p == other
//	1 if p > other
//
// FOREVER is treated as the longest period
func (p Period) CompareTo(other Period) int {
	if p == other {
		return 0
	}

	d1 := p.ToDuration()
	d2 := other.ToDuration()

	// FOREVER has duration 0, but should be treated as longest period
	if p == PeriodForever {
		return 1
	}
	if other == PeriodForever {
		return -1
	}

	if d1 < d2 {
		return -1
	}
	if d1 > d2 {
		return 1
	}

	return 0
}
