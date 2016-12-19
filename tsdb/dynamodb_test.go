package tsdb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestListTablesByRange_1m1h(t *testing.T) {
	s, e := time.Unix(100, 0), time.Unix(6000, 0)
	slots, step := listTimeSlots(s, e)
	assert.Equal(t, 60, step)
	expected := []*timeSlot{
		&timeSlot{
			tableName: "SeriesTest-1m1h-0",
			itemEpoch: 0,
		},
		&timeSlot{
			tableName: "SeriesTest-1m1h-0",
			itemEpoch: 3600,
		},
		&timeSlot{
			tableName: "SeriesTest-1m1h-0",
			itemEpoch: 7200,
		},
	}
	assert.Exactly(t, expected, slots)
}

func TestListTablesByRange_5m1d(t *testing.T) {
	s, e := time.Unix(10000, 0), time.Unix(100000, 0)
	slots, step := listTimeSlots(s, e)
	assert.Equal(t, 300, step)
	expected := []*timeSlot{
		&timeSlot{
			tableName: "SeriesTest-5m1d-0",
			itemEpoch: 0,
		},
		&timeSlot{
			tableName: "SeriesTest-5m1d-86400",
			itemEpoch: 86400,
		},
	}
	assert.Exactly(t, expected, slots)
}

func TestListTablesByRange_1h7d(t *testing.T) {
	s, e := time.Unix(100000, 0), time.Unix(1000000, 0)
	slots, step := listTimeSlots(s, e)
	assert.Equal(t, 3600, step)
	expected := []*timeSlot{
		&timeSlot{
			tableName: "SeriesTest-1h7d-0",
			itemEpoch: 0,
		},
		&timeSlot{
			tableName: "SeriesTest-1h7d-604800",
			itemEpoch: 604800,
		},
	}
	assert.Exactly(t, expected, slots)
}

func TestListTablesByRange_1d360d(t *testing.T) {
	s, e := time.Unix(1000000, 0), time.Unix(100000000, 0)
	slots, step := listTimeSlots(s, e)
	assert.Equal(t, 86400, step)
	expected := []*timeSlot{
		&timeSlot{
			tableName: "SeriesTest-1d360d-0",
			itemEpoch: 0,
		},
		&timeSlot{
			tableName: "SeriesTest-1d360d-31104000",
			itemEpoch: 31104000,
		},
		&timeSlot{
			tableName: "SeriesTest-1d360d-62208000",
			itemEpoch: 62208000,
		},
		&timeSlot{
			tableName: "SeriesTest-1d360d-93312000",
			itemEpoch: 93312000,
		},
	}
	assert.Exactly(t, expected, slots)
}
