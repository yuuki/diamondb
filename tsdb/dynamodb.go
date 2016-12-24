package tsdb

import (
	"fmt"
	"strings"
	"time"
)

type timeSlot struct {
	tableName string
	itemEpoch int64
}

const (
	tablePrefix string = "SeriesTest"
	oneYear time.Duration = time.Duration(24 * 360) * time.Hour
	oneWeek time.Duration = time.Duration(24 * 7) * time.Hour
	oneDay  time.Duration = time.Duration(24 * 1) * time.Hour
)

var (
	oneYearSeconds int = int(oneYear.Seconds())
	oneWeekSeconds int = int(oneWeek.Seconds())
	oneDaySeconds  int = int(oneDay.Seconds())
)


// roleA.r.{1,2,3,4}.loadavg
func splitName(name string) []string {
	open := strings.IndexRune(name, '{')
	close := strings.IndexRune(name, '}')
	var names []string
	if open >= 0 && close >= 0 {
		prefix := name[0:open]
		indices := name[open+1 : close]
		suffix := name[close+1:]
		for _, i := range strings.Split(indices, ",") {
			names = append(names, prefix+i+suffix)
		}
	} else {
		names = strings.Split(name, ",")
	}
	return names
}

func listTimeSlots(startTime, endTime time.Time) ([]*timeSlot, int) {
	var (
		tableName string
		step int
		tableEpochStep int
		itemEpochStep  int
	)
	diffTime := endTime.Sub(startTime)
	if oneYear <= diffTime {
		tableName = tablePrefix + "-1d360d"
		tableEpochStep = oneYearSeconds
		itemEpochStep = tableEpochStep
		step = 60 * 60 * 24
	} else if oneWeek <= diffTime {
		tableName = tablePrefix + "-1h7d"
		tableEpochStep = 60 * 60 * 24 * 7
		itemEpochStep = tableEpochStep
		step = 60 * 60
	} else if oneDay <= diffTime {
		tableName = tablePrefix + "-5m1d"
		tableEpochStep = 60 * 60 * 24
		itemEpochStep = tableEpochStep
		step = 5 * 60
	} else {
		tableName = tablePrefix + "-1m1h"
		tableEpochStep = 60 * 60 * 24
		itemEpochStep = 60 * 60
		step = 60
	}

	slots := make([]*timeSlot, 0, 5)
	startTableEpoch := startTime.Unix() - startTime.Unix() % int64(tableEpochStep)
	endTableEpoch := endTime.Unix()
	for tableEpoch := startTableEpoch; tableEpoch < endTableEpoch; tableEpoch += int64(tableEpochStep) {
		startItemEpoch := maxInt64(tableEpoch, startTime.Unix() - startTime.Unix() % int64(itemEpochStep))
		endItemEpoch := minInt64(tableEpoch + int64(tableEpochStep), endTime.Unix() + int64(itemEpochStep))
		for itemEpoch := startItemEpoch; itemEpoch < endItemEpoch; itemEpoch += int64(itemEpochStep) {
			slot := timeSlot{
				tableName: fmt.Sprintf("%s-%d", tableName, tableEpoch),
				itemEpoch: itemEpoch,
			}
			slots = append(slots, &slot)
		}
	}

	return slots, step
}


func minInt64(x, y int64) int64 {
	if x < y {
		return x
	}
	return y
}

func maxInt64(x, y int64) int64 {
	if x > y {
		return x
	}
	return y
}

