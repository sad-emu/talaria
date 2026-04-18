package peer

import (
	"testing"
	"time"
)

func TestNextBackoff_Doubles(t *testing.T) {
	got := nextBackoff(2 * time.Second)
	want := 4 * time.Second
	if got != want {
		t.Errorf("nextBackoff(2s) = %v, want %v", got, want)
	}
}

func TestNextBackoff_CapsAtMax(t *testing.T) {
	got := nextBackoff(dialMaxBackoff)
	if got != dialMaxBackoff {
		t.Errorf("nextBackoff(max) = %v, want %v", got, dialMaxBackoff)
	}
}

func TestNextBackoff_BelowMaxGrows(t *testing.T) {
	input := dialMaxBackoff / 2
	got := nextBackoff(input)
	if got > dialMaxBackoff {
		t.Errorf("nextBackoff exceeded max: %v > %v", got, dialMaxBackoff)
	}
	if got <= input {
		t.Errorf("nextBackoff did not grow: %v <= %v", got, input)
	}
}

func TestNextBackoff_LargeValueCaps(t *testing.T) {
	// Any value well above the cap should still return exactly dialMaxBackoff.
	got := nextBackoff(dialMaxBackoff * 100)
	if got != dialMaxBackoff {
		t.Errorf("nextBackoff(100x max) = %v, want %v", got, dialMaxBackoff)
	}
}

func TestJitter_WithinBounds(t *testing.T) {
	base := 10 * time.Second
	// Run many samples and verify all fall within the ±20% jitter band.
	for i := 0; i < 1000; i++ {
		j := jitter(base)
		low := time.Duration(float64(base) * (1 - dialJitterFraction))
		high := time.Duration(float64(base) * (1 + dialJitterFraction))
		if j < low || j > high {
			t.Errorf("jitter(%v) = %v, want in [%v, %v]", base, j, low, high)
		}
	}
}

func TestJitter_MinimumOneSecond(t *testing.T) {
	// A tiny input that would produce sub-second results should be floored to 1s.
	j := jitter(1 * time.Millisecond)
	if j < time.Second {
		t.Errorf("jitter floor: got %v, want >= 1s", j)
	}
}

func TestJitter_ZeroInput(t *testing.T) {
	// Zero duration should return at least 1 second (floor).
	j := jitter(0)
	if j < time.Second {
		t.Errorf("jitter(0) = %v, want >= 1s", j)
	}
}

func TestDialConstants(t *testing.T) {
	if dialInitialBackoff <= 0 {
		t.Error("dialInitialBackoff must be positive")
	}
	if dialMaxBackoff <= dialInitialBackoff {
		t.Error("dialMaxBackoff must be greater than dialInitialBackoff")
	}
	if dialBackoffFactor <= 1.0 {
		t.Error("dialBackoffFactor must be > 1.0")
	}
	if dialJitterFraction <= 0 || dialJitterFraction >= 1.0 {
		t.Error("dialJitterFraction must be in (0, 1)")
	}
}
