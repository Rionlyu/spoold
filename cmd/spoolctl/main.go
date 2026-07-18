package main

import (
	"context"
	"os"
	"os/signal"

	"github.com/Rionlyu/spoold/internal/spoolctl"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	code := spoolctl.Run(ctx, os.Args[1:], os.Stdin, os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}
