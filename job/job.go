// Package job provides types and functions for managing Gearman jobs.
//
// A Job represents a single Gearman job that can be submitted to a Gearman server.
// Jobs can be in various states (Running, Completed, Failed) and can provide
// status updates and data during execution.
package job

import (
	"context"
	"io"
	"log/slog"
	"strconv"

	slogctx "github.com/veqryn/slog-context"
	"github.com/wcn/gearman/v2/packet"
)

// State represents the current state of a Gearman job.
type State int

const (
	// Unknown is the default 'State' that should not be encountered
	Unknown State = iota
	// Running means that the job has not yet finished
	Running
	// Completed means that the job finished successfully
	Completed
	// Failed means that the job failed
	Failed
)

// String implements the fmt.Stringer interface for easy printing.
func (s State) String() string {
	switch s {
	case Unknown:
		return "Unknown"
	case Running:
		return "Running"
	case Completed:
		return "Completed"
	case Failed:
		return "Failed"
	}
	return "Unknown"
}

// Status represents the progress status of a Gearman job.
type Status struct {
	// Numerator is the numerator of the % complete
	Numerator int
	// Denominator is the denominator of the % complete
	Denominator int
}

// Job represents a Gearman job that can be submitted to a Gearman server.
// A job can be in various states and provides methods to check status and
// wait for completion.
type Job struct {
	handle         string
	data, warnings io.WriteCloser
	status         Status
	state          State
	done           chan struct{}
	ctx            context.Context // For context-aware logging
}

// Handle returns the job handle assigned by the Gearman server.
func (j *Job) Handle() string {
	return j.handle
}

// Status returns the current status of the gearman job.
func (j *Job) Status() Status {
	return j.status
}

// Run blocks until the job completes. Returns the final state, either Completed or Failed.
func (j *Job) Run() State {
	<-j.done
	return j.state
}

// handlePackets updates a job based off of incoming packets associated with this job.
func (j *Job) handlePackets(packets <-chan *packet.Packet) {
	logger := slogctx.FromCtx(j.ctx)

	defer func() {
		if r := recover(); r != nil {
			logger.Error("Gearman job panic recovered",
				slog.String("handle", j.handle),
				slog.Any("panic", r))
			j.state = Failed
			close(j.done)
		}
	}()

	for pack := range packets {
		if pack == nil {
			logger.Debug("Received nil packet, skipping")
			continue
		}

		switch pack.Type {
		case packet.WorkStatus:
			// check that packet is valid WORK_STATUS
			if len(pack.Arguments) != 3 {
				logger.Warn("Received invalid WORK_STATUS packet",
					slog.String("handle", j.handle),
					slog.Int("field_count", len(pack.Arguments)),
					slog.Int("expected_fields", 3))
				continue
			}

			num, err := strconv.Atoi(string(pack.Arguments[1]))
			if err != nil {
				logger.Warn("Error converting numerator in WORK_STATUS",
					slog.String("handle", j.handle),
					slog.Any("error", err))
				continue
			}
			den, err := strconv.Atoi(string(pack.Arguments[2]))
			if err != nil {
				logger.Warn("Error converting denominator in WORK_STATUS",
					slog.String("handle", j.handle),
					slog.Any("error", err))
				continue
			}
			j.status = Status{Numerator: num, Denominator: den}
		case packet.WorkComplete:
			if len(pack.Arguments) > 1 && len(pack.Arguments[1]) > 0 {
				dataSize := len(pack.Arguments[1])

				if dataSize > 1*1024*1024 { // 1MB limit
					logger.Warn("WorkComplete data too large",
						slog.String("handle", j.handle),
						slog.Int("data_size_bytes", dataSize),
						slog.Int("max_allowed_bytes", 1024*1024))
					j.state = Failed
					close(j.done)
					return
				}

				if _, err := j.data.Write(pack.Arguments[1]); err != nil {
					logger.Warn("Error writing data from WorkComplete",
						slog.String("handle", j.handle),
						slog.Any("error", err))
				}
			}
			j.state = Completed
			close(j.done)
		case packet.WorkFail:
			j.state = Failed
			close(j.done)
		case packet.WorkData:
			if len(pack.Arguments) < 2 {
				logger.Warn("WorkData packet missing data argument", slog.String("handle", j.handle))
				continue
			}

			dataSize := len(pack.Arguments[1])
			if dataSize > 1*1024*1024 { // 1MB limit
				logger.Warn("WorkData too large",
					slog.String("handle", j.handle),
					slog.Int("data_size_bytes", dataSize),
					slog.Int("max_allowed_bytes", 1024*1024))
				continue
			}

			if _, err := j.data.Write(pack.Arguments[1]); err != nil {
				logger.Warn("Error writing WorkData",
					slog.String("handle", j.handle),
					slog.Any("error", err))
			}
		case packet.WorkWarning:
			if len(pack.Arguments) < 2 {
				logger.Warn("WorkWarning packet missing warning argument", slog.String("handle", j.handle))
				continue
			}

			warningSize := len(pack.Arguments[1])
			if warningSize > 100*1024 { // 100KB limit for warnings
				logger.Warn("WorkWarning too large",
					slog.String("handle", j.handle),
					slog.Int("warning_size_bytes", warningSize),
					slog.Int("max_allowed_bytes", 100*1024))
				continue
			}

			if _, err := j.warnings.Write(pack.Arguments[1]); err != nil {
				logger.Warn("Error writing WorkWarning",
					slog.String("handle", j.handle),
					slog.Any("error", err))
			}
		default:
			logger.Warn("Unimplemented packet type",
				slog.String("handle", j.handle),
				slog.Any("packet_type", pack.Type))
		}
	}
}

// NewWithContext creates a new Gearman job with context support for logging.
func NewWithContext(ctx context.Context, handle string, data, warnings io.WriteCloser, packets chan *packet.Packet) *Job {
	j := &Job{
		handle:   handle,
		data:     data,
		warnings: warnings,
		status:   Status{},
		state:    Running,
		done:     make(chan struct{}),
		ctx:      ctx,
	}
	go j.handlePackets(packets)
	return j
}

// New creates a new Gearman job with the specified handle, updating the job based on the packets
// in the packets channel. The only packets coming down packets should be packets for this job.
// It also takes in two WriteClosers to right job data and warnings to.
// Deprecated: Use NewWithContext for context/slog support.
func New(handle string, data, warnings io.WriteCloser, packets chan *packet.Packet) *Job {
	return NewWithContext(context.Background(), handle, data, warnings, packets)
}
