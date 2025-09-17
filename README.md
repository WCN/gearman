# gearman

    import "github.com/wcn/gearman/v2"

Package gearman provides a thread-safe Gearman client.

For complete usage examples and documentation, please see [https://pkg.go.dev/github.com/wcn/gearman/v2](https://pkg.go.dev/github.com/wcn/gearman/v2).


### Example

Basic example that makes an RPC call to a gearman reverse service like those
on the [gearman examples page](https://gearman.org/examples/reverse/)

We recommend using the `SimpleClient` for simple RPC usage. The original v2
`Client` is still available, but will probably be removed for v3 - the forking
authors have no requirement for streaming responses etc.

```go
package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	slogctx "github.com/veqryn/slog-context"
	"github.com/wcn/gearman/v2"
)

func main() {
	config := gearman.DefaultConfig("localhost:4730")
	config.Timeout = 60 * time.Second
	client, _ := gearman.NewSimpleClient(config)

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ctx := slogctx.With(context.Background(), logger)
	client.Start(ctx)
	defer client.Close()

	childCtx := slogctx.With(ctx, logger.With(slog.String("scope", "single-job")))
	response, err := client.Call(childCtx, "reverse", []byte("hello world"))
	if err != nil {
		panic(err)
	}
	println(string(response)) // !dlrow olleh
}
```

## Usage

### func NewSimpleClient

```go
func NewSimpleClient(config ClientConfig) (*SimpleClient, error)
```

NewSimpleClient returns a new Gearman client pointing at the configured server

#### func (*SimpleClient) Start

```go
func (c *SimpleClient) Start(context.Context ctx) error
```

Start() starts the packet-processing goroutines required for communication
with gearmand

#### func (*SimpleClient) Close

```go
func (c *SimpleClient) Close() error
```

Close terminates the connection to the server and stops the packet-processing
goroutines.

#### func (*Client) Call

```go
func (c *Client) Call(ctx context.Context, function string, payload []byte) ([]byte, error)
```

Call sends a new job to the server with the specified function and payload,
waits for the job to complete and returns any response data.

This library may log certain things via slog. It will
use [slogctx](https://github.com/veqryn/slog-context) to get a slog instance
from ctx where possible, but if you have not added a slog instance to your
context using that library then slog.Default will be used instead.

## Logging

Because of the async nature of the gearmand connection, sometimes things happen
that cannot be communicated by returning an error from a function call - for
example if we receive an invalid packet from gearmand. This library therefore
logs these events via slog. [slogctx](https://github.com/veqryn/slog-context) is
used to obtain slog instances - you can pass your own slog in the contexts
passed to `Start()` and `Call()`. The library will use the most-appropriately
scoped logger it can.

# HISTORY

Package was forked from https://github.com/graveyard/gearman, probably
originally https://github.com/Clever/gearman but that has gone away.
