package emaildomainstats_test

import (
	"context"
	emaildomainstats "github.com/fcuenca/go-tech-test"
	"io"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func BenchmarkProcessFile(b *testing.B) {
	testFilePath := filepath.Join("./", "customer_data.csv")

	benchmarks := []struct {
		name        string
		concurrency int
	}{
		{"SingleThreaded", 1},
		{"DualThreaded", 2},
		{"QuadThreaded", 4},
		{"OctoThreaded", 8},
		{"Max", runtime.GOMAXPROCS(0)},
	}

	for _, bm := range benchmarks {
		bm := bm
		b.Run(bm.name, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				tracker := emaildomainstats.NewDomainStatTracker()
				processor, err := emaildomainstats.New(
					tracker,
					emaildomainstats.WithCsvFileName(testFilePath),
					emaildomainstats.WithConcurrency(bm.concurrency),
				)
				if err != nil {
					b.Fatalf("Failed to create processor: %v", err)
				}
				ctx := context.Background()
				b.StartTimer()

				result := processor.Process(ctx)

				b.StopTimer()
				if len(result.Errors) > 0 {
					b.Fatalf("ProcessFile encountered errors: %v", result.Errors)
				}

				stats := tracker.GetSorted()
				if len(stats) == 0 {
					b.Fatalf("No stats returned, expected non-empty result")
				}

				var total int64
				for _, stat := range stats {
					total += stat.Count()
				}

				b.ReportMetric(float64(total), "total_emails")
				b.ReportMetric(float64(len(stats)), "unique_domains")
			}
		})
	}
}

func TestProcessor_EmailHandling(t *testing.T) {
	tests := []struct {
		name           string
		csvContent     string
		expectedDomain string
		wantErr        bool
		expectedErrMsg string
	}{
		{
			name:           "Valid email",
			csvContent:     "id,name,email\n1,Alice,alice@example.com",
			expectedDomain: "example.com",
			wantErr:        false,
		},
		{
			name:           "Email with subdomain",
			csvContent:     "id,name,email\n1,Bob,bob@sub.example.com",
			expectedDomain: "sub.example.com",
			wantErr:        false,
		},
		{
			name:           "Email with plus addressing",
			csvContent:     "id,name,email\n1,Charlie,charlie+tag@example.com",
			expectedDomain: "example.com",
			wantErr:        false,
		},
		{
			name:           "Email with IP address",
			csvContent:     "id,name,email\n1,David,david@[192.168.0.1]",
			expectedDomain: "[192.168.0.1]",
			wantErr:        false,
		},
		{
			name:           "Invalid email - no @",
			csvContent:     "id,name,email\n1,Eve,eveexample.com",
			wantErr:        true,
			expectedErrMsg: "extracting domain: (eveexample.com): mail: missing '@' or angle-addr",
		},
		{
			name:           "Invalid email - multiple sequential @",
			csvContent:     "id,name,email\n1,Frank,frank@@domainexample.com",
			wantErr:        true,
			expectedErrMsg: "extracting domain: (frank@@domainexample.com): mail: missing '@' or angle-addr",
		},
		{
			name:           "Invalid email - multiple @",
			csvContent:     "id,name,email\n1,Frank,frank@domain@example.com",
			wantErr:        true,
			expectedErrMsg: "extracting domain: (frank@domain@example.com): mail: expected single address, got \"@example.com\"",
		},
		{
			name:           "Invalid email - no domain",
			csvContent:     "id,name,email\n1,Grace,grace@",
			wantErr:        true,
			expectedErrMsg: "extracting domain: (grace@): mail: missing '@' or angle-addr",
		},
		{
			name:           "Invalid email - space in domain",
			csvContent:     "id,name,email\n1,Henry,henry@example. com",
			wantErr:        true,
			expectedErrMsg: "extracting domain: (henry@example. com): mail: missing '@' or angle-addr",
		},
		{
			name: "Invalid email - quoted local part",
			csvContent: `id,name,email
1,Jack,"jack.name"@example.com`,
			expectedErrMsg: "extracting domain: (jack.name\"@example.com): mail: missing '@' or angle-addr",
			wantErr:        true,
		},
		{
			name:           "Empty email",
			csvContent:     "id,name,email\n1,Kelly,",
			wantErr:        true,
			expectedErrMsg: "missing or empty email at line 1",
		},
		{
			name:           "Email with uppercase",
			csvContent:     "id,name,email\n1,Ivy,Ivy@EXAMPLE.COM",
			expectedDomain: "example.com",
			wantErr:        false,
		},
		{
			name:           "Extra columns",
			csvContent:     "id,name,email,extra\n1,Mia,mia@example.com,extra",
			expectedDomain: "example.com",
			wantErr:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := io.NopCloser(strings.NewReader(tt.csvContent))
			processor, err := emaildomainstats.New(
				emaildomainstats.NewDomainStatTracker(),
				emaildomainstats.WithCsvReader(reader),
				emaildomainstats.WithConcurrency(1),
			)
			if err != nil {
				t.Fatalf("Failed to create processor: %v", err)
			}

			result := processor.Process(context.Background())

			if tt.wantErr {
				if len(result.Errors) == 0 {
					t.Errorf("Expected error, but got none")
				} else {
					foundExpectedError := false
					for _, err := range result.Errors {
						if strings.Contains(err.Error(), tt.expectedErrMsg) {
							foundExpectedError = true
							break
						}
					}
					if !foundExpectedError {
						t.Errorf("Expected error containing '%s', but got: %v", tt.expectedErrMsg, result.Errors)
					}
				}
			} else {
				if len(result.Errors) > 0 {
					t.Errorf("Unexpected errors: %v", result.Errors)
				} else {
					stats := result.Store.GetSorted()
					if len(stats) != 1 {
						t.Errorf("Expected 1 domain stat, got %d", len(stats))
					} else if stats[0].Domain != tt.expectedDomain {
						t.Errorf("Expected domain %s, got %s", tt.expectedDomain, stats[0].Domain)
					}
				}
			}
		})
	}
}

func TestProcessor_EmailDomainStats(t *testing.T) {
	tests := []struct {
		name        string
		fileContent string
		concurrency int
		wantDomains map[string]int
	}{
		{
			name: "Single domain",
			fileContent: `id,name,email
1,Alice,alice@example.com
2,Bob,bob@example.com
3,Charlie,charlie@example.com`,
			concurrency: 2,
			wantDomains: map[string]int{"example.com": 3},
		},
		{
			name: "Multiple domains",
			fileContent: `id,name,email
1,Alice,alice@example.com
2,Bob,bob@gmail.com
3,Charlie,charlie@example.com
4,David,david@yahoo.com`,
			concurrency: 2,
			wantDomains: map[string]int{"example.com": 2, "gmail.com": 1, "yahoo.com": 1},
		},
		{
			name: "Case insensitive domains",
			fileContent: `id,name,email
1,Alice,alice@ExAmPlE.com
2,Bob,bob@GMAIL.COM
3,Charlie,charlie@example.COM
4,David,david@Yahoo.com`,
			concurrency: 2,
			wantDomains: map[string]int{"example.com": 2, "gmail.com": 1, "yahoo.com": 1},
		},
		{
			name: "High concurrency",
			fileContent: `id,name,email
` + strings.Repeat("1,User,user@example.com\n", 1000) +
				strings.Repeat("2,User,user@gmail.com\n", 500) +
				strings.Repeat("3,User,user@yahoo.com\n", 250),
			concurrency: 8,
			wantDomains: map[string]int{"example.com": 1000, "gmail.com": 500, "yahoo.com": 250},
		},
		{
			name: "Single worker",
			fileContent: `id,name,email
1,Alice,alice@example.com
2,Bob,bob@gmail.com
3,Charlie,charlie@yahoo.com
4,David,david@example.com`,
			concurrency: 1,
			wantDomains: map[string]int{"example.com": 2, "gmail.com": 1, "yahoo.com": 1},
		},
		{
			name:        "Empty fileReader (only header)",
			fileContent: `id,name,email`,
			concurrency: 1,
			wantDomains: map[string]int{},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tracker := emaildomainstats.NewDomainStatTracker()
			reader := io.NopCloser(strings.NewReader(tt.fileContent))
			processor, err := emaildomainstats.New(
				tracker,
				emaildomainstats.WithCsvReader(reader),
				emaildomainstats.WithConcurrency(tt.concurrency),
			)
			if err != nil {
				t.Fatalf("Failed to create processor: %v", err)
			}

			result := processor.Process(context.Background())

			if len(result.Errors) > 0 {
				t.Errorf("Unexpected errors: %v", result.Errors)
			}

			stats := result.Store.GetSorted()
			if len(stats) != len(tt.wantDomains) {
				t.Errorf("Got %d domains, want %d", len(stats), len(tt.wantDomains))
			}
			for _, stat := range stats {
				expectedCount, ok := tt.wantDomains[strings.ToLower(stat.Domain)]
				if !ok {
					t.Errorf("Unexpected domain: %s", stat.Domain)
					continue
				}
				if int(stat.Count()) != expectedCount {
					t.Errorf("For domain %s, got count %d, want %d", stat.Domain, stat.Count(), expectedCount)
				}
			}

			for i := 1; i < len(stats); i++ {
				if strings.ToLower(stats[i-1].Domain) > strings.ToLower(stats[i].Domain) {
					t.Errorf("Domains are not sorted correctly: %s comes after %s", stats[i-1].Domain, stats[i].Domain)
					break
				}
			}
		})
	}
}

func TestProcessor_CSVProcessing(t *testing.T) {
	tests := []struct {
		name            string
		csvContent      string
		expectedDomains []string
		expectedCounts  []int
		expectedErrors  []string
	}{
		{
			name: "Valid CSV with multiple domains",
			csvContent: `id,name,email
1,Alice,alice@example.com
2,Bob,bob@gmail.com
3,Charlie,charlie@example.com
4,David,david@yahoo.com`,
			expectedDomains: []string{"example.com", "gmail.com", "yahoo.com"},
			expectedCounts:  []int{2, 1, 1},
			expectedErrors:  nil,
		},
		{
			name: "CSV with invalid rows",
			csvContent: `id,name,email
1,Alice,alice@example.com
2,Bob,bobgmail.com
3,Charlie,charlie@example@com
4,David,david@yahoo.com`,
			expectedDomains: []string{"example.com", "yahoo.com"},
			expectedCounts:  []int{1, 1},
			expectedErrors: []string{
				"extracting domain: (bobgmail.com): mail: missing '@' or angle-addr",
				"extracting domain: (charlie@example@com): mail: expected single address, got \"@com\"",
			},
		},
		{
			name: "CSV with missing email column",
			csvContent: `id,name
1,Alice
2,Bob`,
			expectedDomains: nil,
			expectedCounts:  nil,
			expectedErrors: []string{
				"missing or empty email at line 1",
				"missing or empty email at line 2",
			},
		},
		{
			name:            "Empty CSV (only header)",
			csvContent:      "id,name,email",
			expectedDomains: nil,
			expectedCounts:  nil,
			expectedErrors:  nil,
		},
		{
			name: "CSV with mixed valid and invalid emails",
			csvContent: `id,name,email
1,Alice,alice@example.com
2,Bob,bob@gmail.com
3,Charlie,charlie@@invalid.com
4,David,david@yahoo.com
5,Eve,eve@example.com`,
			expectedDomains: []string{"example.com", "gmail.com", "yahoo.com"},
			expectedCounts:  []int{2, 1, 1},
			expectedErrors: []string{
				"extracting domain: (charlie@@invalid.com): mail: missing '@' or angle-addr",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := io.NopCloser(strings.NewReader(tt.csvContent))
			tracker := emaildomainstats.NewDomainStatTracker()
			processor, err := emaildomainstats.New(
				tracker,
				emaildomainstats.WithCsvReader(reader),
				emaildomainstats.WithConcurrency(1),
			)
			if err != nil {
				t.Fatalf("Failed to create processor: %v", err)
			}

			result := processor.Process(context.Background())

			if len(result.Errors) != len(tt.expectedErrors) {
				t.Errorf("Expected %d errors, got %d", len(tt.expectedErrors), len(result.Errors))
			} else {
				for i, expectedErr := range tt.expectedErrors {
					if result.Errors[i].Error() != expectedErr {
						t.Errorf("Expected error containing '%s', got '%s'", expectedErr, result.Errors[i].Error())
					}
				}
			}

			stats := result.Store.GetSorted()
			if len(stats) != len(tt.expectedDomains) {
				t.Errorf("Expected %d domains, got %d", len(tt.expectedDomains), len(stats))
			} else {
				for i, expectedDomain := range tt.expectedDomains {
					if stats[i].Domain != expectedDomain {
						t.Errorf("Expected domain %s at position %d, got %s", expectedDomain, i, stats[i].Domain)
					}
					if int(stats[i].Count()) != tt.expectedCounts[i] {
						t.Errorf("Expected count %d for domain %s, got %d", tt.expectedCounts[i], expectedDomain, stats[i].Count())
					}
				}
			}

			for i := 1; i < len(stats); i++ {
				if stats[i-1].Domain > stats[i].Domain {
					t.Errorf("Domains not in alphabetical order: %s comes before %s", stats[i-1].Domain, stats[i].Domain)
				}
			}

			if result.Store == nil {
				t.Error("Result.Store is nil")
			}
			if result.Errors == nil {
				t.Error("Result.Errors is nil")
			}
		})
	}
}

// SlowReader is a custom io.Reader that introduces a delay between reads
type SlowReader struct {
	Reader io.Reader
	Delay  time.Duration
}

func (s *SlowReader) Read(p []byte) (int, error) {
	time.Sleep(s.Delay)
	return s.Reader.Read(p)
}

func TestProcessor_ContextCancellation(t *testing.T) {
	tests := []struct {
		name           string
		setup          func() (*emaildomainstats.Processor, error)
		cancelAfter    time.Duration
		expectedResult func(emaildomainstats.Result) bool
	}{
		{
			name: "Cancel during processing",
			setup: func() (*emaildomainstats.Processor, error) {
				content := "id,name,email\n" + strings.Repeat("1,Test,test@example.com\n", 1000)
				slowReader := &SlowReader{
					Reader: strings.NewReader(content),
					Delay:  100 * time.Millisecond,
				}
				return emaildomainstats.New(emaildomainstats.NewDomainStatTracker(), emaildomainstats.WithCsvReader(io.NopCloser(slowReader)), emaildomainstats.WithConcurrency(1))
			},
			cancelAfter: 50 * time.Millisecond,
			expectedResult: func(r emaildomainstats.Result) bool {
				return len(r.Errors) > 0 && strings.Contains(r.Errors[len(r.Errors)-1].Error(), "process was cancelled")
			},
		},
		{
			name: "Complete before cancellation",
			setup: func() (*emaildomainstats.Processor, error) {
				content := "id,name,email\n1,Alice,alice@example.com"
				reader := io.NopCloser(strings.NewReader(content))
				return emaildomainstats.New(emaildomainstats.NewDomainStatTracker(), emaildomainstats.WithCsvReader(reader), emaildomainstats.WithConcurrency(1))
			},
			cancelAfter: 100 * time.Millisecond,
			expectedResult: func(r emaildomainstats.Result) bool {
				return len(r.Errors) == 0 && len(r.Store.GetSorted()) == 1
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			processor, err := tt.setup()
			if err != nil {
				t.Fatalf("Unexpected error in setup: %v", err)
			}

			ctx, cancel := context.WithCancel(context.Background())
			go func() {
				time.Sleep(tt.cancelAfter)
				cancel()
			}()

			result := processor.Process(ctx)

			if !tt.expectedResult(result) {
				t.Errorf("Unexpected result: %+v", result)
			}
		})
	}
}
