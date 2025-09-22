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

	slogctx "github.com/veqryn/slog-context"
	"github.com/wcn/gearman/v2"
)

func main() {
	config := gearman.DefaultConfig("localhost:4730")
	client := gearman.NewSimpleClient(config)

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ctx := slogctx.With(context.Background(), log)
	err := client.Start(ctx)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	response, _ := client.Call(ctx, "reverse", []byte("hello world"))
	println(string(response)) // !dlrow olleh
}
```

## Usage

### func NewSimpleClient

```go
func NewSimpleClient(config ClientConfig) *SimpleClient
```

NewSimpleClient returns a new Gearman client pointing at the configured server.
Note the client must be `Start()`ed before you use it.

#### func (*SimpleClient) Start

```go
func (c *SimpleClient) Start(ctx context.Context) error
```

Start() connects to gearmand and starts the packet-processing goroutines
required to receive responses from it

#### func (*SimpleClient) Close

```go
func (c *SimpleClient) Close() error
```

Close terminates the connection to the server and stops the packet-processing
goroutines.

#### func (*SimpleClient) Call

```go
func (c *SimpleClient) Call(ctx context.Context, function string, payload []byte) ([]byte, error)
```

Call sends a new job to the server with the specified function and payload,
waits for the job to complete and returns any response data.

#### func (*SimpleClient) Ping

```go
func (c *SimpleClient) Ping(ctx context.Context) error
```

Ping uses gearmand's ECHO_REQ/ECHO_RES commands to check if the connection is
working. Will honour the context's timeout/deadline, or use the `timeout`
from the client's config if not.

## Logging

Because of the async nature of the gearmand connection, sometimes things happen
that cannot be communicated by returning an error from a function call - for
example if we receive an invalid packet from gearmand. This library therefore
logs these events via slog. [slogctx](https://github.com/veqryn/slog-context) is
used to obtain slog instances - you can pass your own slog in the contexts
passed to `Start()` and `Call()`. The library will use the most-appropriately
scoped logger it can. If it cannot get one from the context then
slog.Default will be used instead (via slogctx's
fallback behaviour)

# HISTORY

Package was forked from https://github.com/graveyard/gearman, probably
originally https://github.com/Clever/gearman but that has gone away.
