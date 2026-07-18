package buildinfo

import (
	"strings"
	"testing"
)

func TestStringIncludesBuildFields(t *testing.T) {
	got := String()
	for _, value := range []string{Version, Commit, Date} {
		if !strings.Contains(got, value) {
			t.Fatalf("String() = %q, want %q", got, value)
		}
	}
}
