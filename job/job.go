// Package job provides types and functions for managing Gearman jobs.
//
// A Job represents a single Gearman job that can be submitted to a Gearman server.
// Jobs can be in various states (Running, Completed, Failed) and can provide
// status updates and data during execution.
package job

import (
	"fmt"
	"gearman/packet"
	"io"
	"os"
	"strconv"
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
}

// Handle returns the job handle assigned by the Gearman server.
func (j Job) Handle() string {
	return j.handle
}

// Status returns the current status of the gearman job.
func (j Job) Status() Status {
	return j.status
}

// Run blocks until the job completes. Returns the final state, either Completed or Failed.
func (j *Job) Run() State {
	<-j.done
	return j.state
}

// handlePackets updates a job based off of incoming packets associated with this job.
func (j *Job) handlePackets(packets <-chan *packet.Packet) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "GEARMAN PANIC: recovered from panic in job handlePackets: %v\n", r)
			j.state = Failed
			close(j.done)
		}
	}()

	for pack := range packets {
		if pack == nil {
			fmt.Fprintf(os.Stderr, "GEARMAN WARNING: received nil packet, skipping\n")
			continue
		}

		switch pack.Type {
		case packet.WorkStatus:
			// check that packet is valid WORK_STATUS
			if len(pack.Arguments) != 3 {
				fmt.Fprintf(os.Stderr, "GEARMAN WARNING: Received invalid WORK_STATUS packet with '%d' fields\n",
					len(pack.Arguments))
				continue
			}

			num, err := strconv.Atoi(string(pack.Arguments[1]))
			if err != nil {
				fmt.Fprintln(os.Stderr, "GEARMAN WARNING: Error converting numerator", err)
				continue
			}
			den, err := strconv.Atoi(string(pack.Arguments[2]))
			if err != nil {
				fmt.Fprintln(os.Stderr, "GEARMAN WARNING: Error converting denominator", err)
				continue
			}
			j.status = Status{Numerator: num, Denominator: den}
		case packet.WorkComplete:
			if len(pack.Arguments) > 1 && len(pack.Arguments[1]) > 0 {
				dataSize := len(pack.Arguments[1])

				if dataSize > 1*1024*1024 { // 1MB limit
					fmt.Fprintf(os.Stderr, "GEARMAN WARNING: WorkComplete data too large: %d bytes (maximum 1MB allowed)\n", dataSize)
					j.state = Failed
					close(j.done)
					return
				}

				if _, err := j.data.Write(pack.Arguments[1]); err != nil {
					fmt.Fprintf(os.Stderr, "GEARMAN WARNING: Error writing data from WorkComplete: %v\n", err)
				}
			}
			j.state = Completed
			close(j.done)
		case packet.WorkFail:
			j.state = Failed
			close(j.done)
		case packet.WorkData:
			if len(pack.Arguments) < 2 {
				fmt.Fprintf(os.Stderr, "GEARMAN WARNING: WorkData packet missing data argument\n")
				continue
			}

			dataSize := len(pack.Arguments[1])
			if dataSize > 1*1024*1024 { // 1MB limit
				fmt.Fprintf(os.Stderr, "GEARMAN WARNING: WorkData too large: %d bytes (maximum 1MB allowed)\n", dataSize)
				continue
			}

			if _, err := j.data.Write(pack.Arguments[1]); err != nil {
				fmt.Fprintf(os.Stderr, "GEARMAN WARNING: Error writing data: %v\n", err)
			}
		case packet.WorkWarning:
			if len(pack.Arguments) < 2 {
				fmt.Fprintf(os.Stderr, "GEARMAN WARNING: WorkWarning packet missing warning argument\n")
				continue
			}

			warningSize := len(pack.Arguments[1])
			if warningSize > 100*1024 { // 100KB limit for warnings
				fmt.Fprintf(os.Stderr, "GEARMAN WARNING: WorkWarning too large: %d bytes (maximum 100KB allowed)\n", warningSize)
				continue
			}

			if _, err := j.warnings.Write(pack.Arguments[1]); err != nil {
				fmt.Fprintf(os.Stderr, "GEARMAN WARNING: Error writing warnings: %v\n", err)
			}
		default:
			fmt.Fprintln(os.Stderr, "GEARMAN WARNING: Unimplemented packet type", pack.Type)
		}
	}
}

// New creates a new Gearman job with the specified handle, updating the job based on the packets
// in the packets channel. The only packets coming down packets should be packets for this job.
// It also takes in two WriteClosers to right job data and warnings to.
func New(handle string, data, warnings io.WriteCloser, packets chan *packet.Packet) *Job {
	j := &Job{
		handle:   handle,
		data:     data,
		warnings: warnings,
		status:   Status{},
		state:    Running,
		done:     make(chan struct{}),
	}
	go j.handlePackets(packets)
	return j
}
