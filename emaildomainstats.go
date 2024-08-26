// Package emaildomainstats
// This package is required to provide functionality to process a csv file and return a sorted (by email domain) data
// structure of your choice containing the email domains along with the number of customers for each domain. The customer_data.csv
// file provides an example csv file to work with. Any errors should be logged (or handled) or returned to the consumer of
// this package. Performance matters, the sample file may only contain 1K lines but the package may be expected to be used on
// files with 10 million lines or run on a small machine.
//
// Write this package as you normally would for any production grade code that would be deployed to a live system.
//
// Please stick to using the standard library.

package emaildomainstats

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"net/mail"
	"os"
	"strings"
	"sync"
)

// Result represents the outcome of processing email domain statistics.
// contains a pointer to the DomainStatTracker and any errors encountered during processing.
type Result struct {
	Store  *DomainStatTracker
	Errors []error
}

// Processor handles the processing of email domain statistics from a CSV file or any reader.
type Processor struct {
	store       *DomainStatTracker
	fileReader  io.ReadCloser
	concurrency int
}

// ProcessorOption is a function type used to configure a Processor.
type ProcessorOption func(*Processor) error

// WithCsvFileName returns a ProcessorOption that configures the Processor to read from CSV file.
// takes a filename as an argument and opens the file for reading.
func WithCsvFileName(fileName string) ProcessorOption {
	return func(p *Processor) error {
		file, err := os.Open(fileName)
		if err != nil {
			return fmt.Errorf("failed to open file: %w", err)
		}
		p.fileReader = file
		return nil
	}
}

// WithCsvReader returns a ProcessorOption that configures the Processor with a custom io.ReadCloser.
func WithCsvReader(reader io.ReadCloser) ProcessorOption {
	return func(p *Processor) error {
		p.fileReader = reader
		return nil
	}
}

// WithConcurrency returns a ProcessorOption that sets the concurrency level for processing.
// concurrency must be at least 1.
func WithConcurrency(concurrency int) ProcessorOption {
	return func(p *Processor) error {
		if concurrency < 1 {
			return fmt.Errorf("concurrency must be at least 1")
		}
		p.concurrency = concurrency
		return nil
	}
}

// New creates a new Processor with the given DomainStatTracker and options.
// returns an error if the configuration is invalid.
func New(store *DomainStatTracker, opts ...ProcessorOption) (*Processor, error) {
	p := &Processor{
		store:       store,
		concurrency: 1,
	}

	for _, opt := range opts {
		if err := opt(p); err != nil {
			return nil, err
		}
	}

	if p.fileReader == nil {
		return nil, fmt.Errorf("no file or reader specified")
	}

	return p, nil
}

// Process starts processing the CSV data and returns a Result.
// uses the configured concurrency to process the data in parallel.
func (p *Processor) Process(ctx context.Context) Result {
	result := Result{
		Store:  p.store,
		Errors: []error{},
	}

	defer p.fileReader.Close()

	emailCh := make(chan string)
	errorCh := make(chan error)
	var wg sync.WaitGroup

	rd := csv.NewReader(p.fileReader)
	rd.FieldsPerRecord = -1
	rd.LazyQuotes = true

	// Skip header
	if _, err := rd.Read(); err != nil {
		result.Errors = append(result.Errors, fmt.Errorf("error reading CSV header: %w", err))
		return result
	}

	wg.Add(1)
	go p.reader(ctx, rd, emailCh, errorCh, &wg)

	for i := 0; i < p.concurrency; i++ {
		wg.Add(1)
		go p.worker(ctx, emailCh, errorCh, &wg)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	for {
		select {

		case <-ctx.Done():
			<-done
			close(errorCh)

			for err := range errorCh {
				result.Errors = append(result.Errors, err)
			}

			result.Errors = append(result.Errors, fmt.Errorf("process was cancelled: %w", ctx.Err()))
			return result

		case err, ok := <-errorCh:
			if !ok {
				return result
			}
			result.Errors = append(result.Errors, err)

		case <-done:
			close(errorCh)

			for err := range errorCh {
				result.Errors = append(result.Errors, err)
			}

			return result

		}
	}
}

// reader reads records from the CSV buffer and sends them to the emailCh channel.
// reports any errors encountered during reading to the errorCh channel.
func (p *Processor) reader(ctx context.Context, buffer *csv.Reader, emailCh chan<- string, errorCh chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()
	defer close(emailCh)

	lineNumber := 1

	for {
		select {
		case <-ctx.Done():
			return
		default:
			record, err := buffer.Read()
			if err == io.EOF {
				return
			}

			if err != nil {
				errorCh <- fmt.Errorf("error reading CSV at line %d: %w", lineNumber, err)
				lineNumber++
				continue
			}

			if len(record) <= 2 || record[2] == "" {
				errorCh <- fmt.Errorf("missing or empty email at line %d", lineNumber)
				lineNumber++
				continue
			}

			emailCh <- record[2]
			lineNumber++
		}
	}
}

// worker reads from emailCh and processes emails
func (p *Processor) worker(ctx context.Context, emailCh <-chan string, errorCh chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case email, ok := <-emailCh:
			if !ok {
				return
			}

			domain, err := extractDomain(strings.ToLower(email))
			if err != nil {
				errorCh <- fmt.Errorf("extracting domain: (%s): %w", email, err)
				continue
			}

			p.store.Add(domain)
		}
	}
}

// extractDomain is an internal function for extracting the domain
// supports RFC 5322
func extractDomain(email string) (string, error) {
	address, err := mail.ParseAddress(email)
	if err != nil {
		return "", err
	}

	parts := strings.Split(address.Address, "@")
	if len(parts) != 2 {
		return "", fmt.Errorf("mail: does not contain exactly one '@' symbol")
	}

	return parts[1], nil
}
