package timeparser

import (
	"testing"
	"time"
	"fmt"

	"github.com/stretchr/testify/assert"
)

func TestParseAtTime_Empty(t *testing.T) {
	got, err := ParseAtTime("")
	if assert.NoError(t, err) {
		expected := time.Now()
		gy, gm, gd := got.Date()
		gh, _, _ := got.Clock()
		ey, em, ed := expected.Date()
		eh, _, _ := expected.Clock()

		assert.Equal(t, gy, ey)
		assert.Equal(t, gm, em)
		assert.Equal(t, gd, ed)
		assert.Equal(t, gh, eh)
	}
}

func TestParseAtTime_UnixTime(t *testing.T) {
	got, err := ParseAtTime("1444508126")
	if assert.NoError(t, err) {
		expected := time.Unix(1444508126, 0)
		assert.EqualValues(t, expected, got)
	}
}

func TestParseAtTime_CurrentTime(t *testing.T) {
	got, err := ParseAtTime("now")
	if assert.NoError(t, err) {
		expected := time.Now()
		gy, gm, gd := got.Date()
		gh, _, _ := got.Clock()
		ey, em, ed := expected.Date()
		eh, _, _ := expected.Clock()

		assert.Equal(t, gy, ey)
		assert.Equal(t, gm, em)
		assert.Equal(t, gd, ed)
		assert.Equal(t, gh, eh)
	}
}

func TestParseAtTime_RelativePlus(t *testing.T) {
	got, err := ParseAtTime("now+3d")
	if assert.NoError(t, err) {
		expected := time.Now().AddDate(0, 0, 3)
		gy, gm, gd := got.Date()
		gh, _, _ := got.Clock()
		ey, em, ed := expected.Date()
		eh, _, _ := expected.Clock()

		assert.Equal(t, gy, ey)
		assert.Equal(t, gm, em)
		assert.Equal(t, gd, ed)
		assert.Equal(t, gh, eh)
	}
}

func TestParseAtTime_RelativeMinus(t *testing.T) {
	got, err := ParseAtTime("now-30d")
	if assert.NoError(t, err) {
		expected := time.Now().AddDate(0, 0, -30)
		gy, gm, gd := got.Date()
		gh, _, _ := got.Clock()
		ey, em, ed := expected.Date()
		eh, _, _ := expected.Clock()

		assert.Equal(t, gy, ey)
		assert.Equal(t, gm, em)
		assert.Equal(t, gd, ed)
		assert.Equal(t, gh, eh)
	}
}

func TestparseTimeOffset_Empty(t *testing.T) {
	got, err := parseTimeOffset("")
	if assert.NoError(t, err) {
		expected := time.Duration(0)
		assert.Equal(t, expected, got)
	}
}

type parseOffsetTest struct {
	offset		string
	duration	time.Duration
}

var parseOffsetTests = []parseOffsetTest{
	{"", time.Duration(0)},
	{"-", time.Duration(0)},
	{"+", time.Duration(0)},
	{"10days", time.Duration(10*24*time.Hour)},
	{"0days", time.Duration(0)},
	{"-10days", time.Duration(-10*24*time.Hour)},
	{"5seconds", time.Duration(5*time.Second)},
	{"5minutes", time.Duration(5*time.Minute)},
	{"5hours", time.Duration(5*time.Hour)},
	{"5weeks", time.Duration(5*7*24*time.Hour)},
	{"1month", time.Duration(30*24*time.Hour)},
	{"2months", time.Duration(60*24*time.Hour)},
	{"12months", time.Duration(360*24*time.Hour)},
	{"1year", time.Duration(365*24*time.Hour)},
	{"2years", time.Duration(730*24*time.Hour)},
}

func TestParseTimeOffset(t *testing.T) {
	for i, test := range parseOffsetTests {
		got, err := parseTimeOffset(test.offset)
		if assert.NoError(t, err) {
			assert.Equal(t, test.duration, got, fmt.Sprintf("#%d", i))
		}
	}
}
