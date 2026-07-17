package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Rionlyu/spoold/internal/api"
	"github.com/Rionlyu/spoold/internal/compactor"
	"github.com/Rionlyu/spoold/internal/store"
	"github.com/Rionlyu/spoold/internal/target"
	"github.com/Rionlyu/spoold/internal/worker"
)

func main() {
	var (
		listen              = flag.String("listen", "127.0.0.1:8080", "HTTP listen address")
		journalPath         = flag.String("journal", "data/spoold.journal", "append-only journal path")
		concurrency         = flag.Int("workers", 4, "number of delivery workers")
		allowPrivateTargets = flag.Bool("allow-private-targets", false, "allow private and loopback delivery targets")
		requestTimeout      = flag.Duration("request-timeout", 10*time.Second, "outbound request timeout")
		shutdownTimeout     = flag.Duration("shutdown-timeout", 10*time.Second, "graceful shutdown timeout")
		compactThreshold    = flag.Int64("compact-threshold-bytes", compactor.DefaultThresholdBytes, "minimum journal size for compaction (0 disables)")
		compactInterval     = flag.Duration("compact-check-interval", compactor.DefaultCheckInterval, "journal compaction check interval")
	)
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	journal, err := store.Open(*journalPath)
	if err != nil {
		logger.Error("open journal", "error", err)
		os.Exit(1)
	}
	defer journal.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	client := target.NewClient(*allowPrivateTargets, *requestTimeout)
	pool := worker.New(journal, client, logger, worker.Config{
		Concurrency: *concurrency,
	})
	pool.Start(ctx)
	journalCompactor := compactor.New(journal, logger, compactor.Config{
		ThresholdBytes: *compactThreshold,
		CheckInterval:  *compactInterval,
	})
	journalCompactor.Start(ctx)

	httpServer := &http.Server{
		Addr:              *listen,
		Handler:           api.New(journal, pool, logger),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	serverErrors := make(chan error, 1)
	go func() {
		logger.Info("spoold started",
			"listen", *listen,
			"journal", *journalPath,
			"workers", *concurrency,
			"allow_private_targets", *allowPrivateTargets,
			"compact_threshold_bytes", *compactThreshold,
			"compact_check_interval", *compactInterval,
		)
		serverErrors <- httpServer.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		logger.Info("shutdown requested")
	case err := <-serverErrors:
		if !errors.Is(err, http.ErrServerClosed) {
			logger.Error("HTTP server stopped", "error", err)
			stop()
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), *shutdownTimeout)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("graceful HTTP shutdown", "error", err)
	}
	stop()
	pool.Wait()
	journalCompactor.Wait()
	logger.Info("spoold stopped")
}
