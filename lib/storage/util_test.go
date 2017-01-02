package storage

import (
	"fmt"
	"reflect"
	"testing"
)

func TestGroupNames(t *testing.T) {
	var names []string
	for i := 1; i <= 5; i++ {
		names = append(names, fmt.Sprintf("server%d.loadavg5", i))
	}
	nameGroups := GroupNames(names, 2)
	expected := [][]string{
		{"server1.loadavg5", "server2.loadavg5"},
		{"server3.loadavg5", "server4.loadavg5"},
		{"server5.loadavg5"},
	}
	if !reflect.DeepEqual(nameGroups, expected) {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", expected, nameGroups)
	}
}

func TestSplitName(t *testing.T) {
	name := "roleA.r.{1,2,3,4}.loadavg"
	names := SplitName(name)
	expected := []string{
		"roleA.r.1.loadavg",
		"roleA.r.2.loadavg",
		"roleA.r.3.loadavg",
		"roleA.r.4.loadavg",
	}
	if !reflect.DeepEqual(names, expected) {
		t.Fatalf("\nExpected: %+v\nActual:   %+v", expected, names)
	}
}
