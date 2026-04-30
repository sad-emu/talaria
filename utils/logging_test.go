package utils

import "testing"

func TestParseLogLevel(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    LogLevel
		wantErr bool
	}{
		{name: "default empty", raw: "", want: LogLevelInfo},
		{name: "debug", raw: "DEBUG", want: LogLevelDebug},
		{name: "info", raw: "info", want: LogLevelInfo},
		{name: "error", raw: "Error", want: LogLevelError},
		{name: "invalid", raw: "TRACE", want: LogLevelInfo, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseLogLevel(tc.raw)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("ParseLogLevel(%q) expected error", tc.raw)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseLogLevel(%q) error = %v", tc.raw, err)
			}
			if got != tc.want {
				t.Fatalf("ParseLogLevel(%q) = %v, want %v", tc.raw, got, tc.want)
			}
		})
	}
}

func TestShouldLogByLevel(t *testing.T) {
	SetLogLevel(LogLevelInfo)
	if shouldLog(LogLevelDebug) {
		t.Fatalf("DEBUG should be filtered when level is INFO")
	}
	if !shouldLog(LogLevelInfo) {
		t.Fatalf("INFO should be logged when level is INFO")
	}
	if !shouldLog(LogLevelError) {
		t.Fatalf("ERROR should be logged when level is INFO")
	}

	SetLogLevel(LogLevelError)
	if shouldLog(LogLevelInfo) {
		t.Fatalf("INFO should be filtered when level is ERROR")
	}
	if !shouldLog(LogLevelError) {
		t.Fatalf("ERROR should be logged when level is ERROR")
	}

	SetLogLevel(LogLevelDebug)
	if !shouldLog(LogLevelDebug) {
		t.Fatalf("DEBUG should be logged when level is DEBUG")
	}
}
