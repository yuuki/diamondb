package series

import "testing"

func TestFormatedName_Uniq(t *testing.T) {
	ss := SeriesSlice{
		NewSeries("server1.cpu.system", nil, 0, 0),
		NewSeries("server2.cpu.system", nil, 0, 0),
		NewSeries("server3.cpu.system", nil, 0, 0),
	}
	name := ss.FormatedName()
	expected := "server1.cpu.system,server2.cpu.system,server3.cpu.system"
	if name != expected {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", expected, name)
	}
}

func TestFormatSeries_NotUniq(t *testing.T) {
	ss := SeriesSlice{
		NewSeries("server2.cpu.system", nil, 0, 0),
		NewSeries("server1.cpu.system", nil, 0, 0),
		NewSeries("server3.cpu.system", nil, 0, 0),
		NewSeries("server1.cpu.system", nil, 0, 0),
	}
	name := ss.FormatedName()
	expected := "server1.cpu.system,server2.cpu.system,server3.cpu.system"
	if name != expected {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", expected, name)
	}
}
