package timeparser

import (
	"testing"
	"time"
)

func TestParseAtTime_Empty(t *testing.T) {
	got, err := ParseAtTime("")
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	TestTimeNearlyEqual(t, got, time.Now())
}

func TestParseAtTime_UnixTime(t *testing.T) {
	got, err := ParseAtTime("100")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if got != time.Unix(100, 0) {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", time.Unix(100, 0), got)
	}
}

func TestParseAtTime_CurrentTime(t *testing.T) {
	got, err := ParseAtTime("now")
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	TestTimeNearlyEqual(t, got, time.Now())
}

func TestParseAtTime_RelativePlus(t *testing.T) {
	got, err := ParseAtTime("now+3d")
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	TestTimeNearlyEqual(t, got, time.Now().AddDate(0, 0, 3))
}

func TestParseAtTime_RelativeMinus(t *testing.T) {
	got, err := ParseAtTime("now-30d")
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	TestTimeNearlyEqual(t, got, time.Now().AddDate(0, 0, -30))
}

func TestParseAtTime_Absolute(t *testing.T) {
	got, err := ParseAtTime("19:22_20161010")
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	expected := time.Date(2016, 10, 10, 19, 22, 0, 0, time.UTC)
	if expected != got {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", expected, got)
	}
}

var parseOffsetTests = []struct {
	offset   string
	duration time.Duration
}{
	{"", time.Duration(0)},
	{"-", time.Duration(0)},
	{"+", time.Duration(0)},
	{"10days", time.Duration(10 * 24 * time.Hour)},
	{"0days", time.Duration(0)},
	{"-10days", time.Duration(-10 * 24 * time.Hour)},
	{"5seconds", time.Duration(5 * time.Second)},
	{"5minutes", time.Duration(5 * time.Minute)},
	{"5hours", time.Duration(5 * time.Hour)},
	{"5weeks", time.Duration(5 * 7 * 24 * time.Hour)},
	{"1month", time.Duration(30 * 24 * time.Hour)},
	{"2months", time.Duration(60 * 24 * time.Hour)},
	{"12months", time.Duration(360 * 24 * time.Hour)},
	{"1year", time.Duration(365 * 24 * time.Hour)},
	{"2years", time.Duration(730 * 24 * time.Hour)},
}

func TestParseTimeOffset(t *testing.T) {
	for i, tc := range parseOffsetTests {
		got, err := parseTimeOffset(tc.offset)
		if err != nil {
			t.Fatalf("err: %s", err)
		}
		if tc.duration != got {
			t.Fatalf("\nExpected: %+v\nActual:   %+v (#%d)", tc.duration, got, i)
		}
	}
}
