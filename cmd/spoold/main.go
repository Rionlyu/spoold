package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/Rionlyu/spoold/internal/api"
	"github.com/Rionlyu/spoold/internal/buildinfo"
	"github.com/Rionlyu/spoold/internal/compactor"
	"github.com/Rionlyu/spoold/internal/store"
	"github.com/Rionlyu/spoold/internal/target"
	"github.com/Rionlyu/spoold/internal/worker"
)

const defaultMaxJournalBytes int64 = 1 << 30

type config struct {
	listen              string
	unixSocket          string
	journalPath         string
	maxJournalBytes     int64
	terminalRetention   time.Duration
	concurrency         int
	perTargetWorkers    int
	allowPrivateTargets bool
	requestTimeout      time.Duration
	shutdownTimeout     time.Duration
	compactThreshold    int64
	compactInterval     time.Duration
	showVersion         bool
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	cfg, code := parseConfig(args, stderr)
	if code != 0 {
		return code
	}
	if cfg.showVersion {
		fmt.Fprintf(stdout, "spoold %s\n", buildinfo.String())
		return 0
	}

	logger := slog.New(slog.NewJSONHandler(stdout, nil))
	journal, err := store.Open(cfg.journalPath, store.Options{
		MaxJournalBytes: cfg.maxJournalBytes,
	})
	if err != nil {
		logger.Error("open journal", "error", err)
		return 1
	}

	listener, cleanupListener, err := openListener(cfg.listen, cfg.unixSocket)
	if err != nil {
		logger.Error("listen", "error", err)
		_ = journal.Close()
		return 1
	}
	defer func() {
		if err := cleanupListener(); err != nil {
			logger.Warn("clean up listener", "error", err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	client := target.NewClient(cfg.allowPrivateTargets, cfg.requestTimeout)
	pool := worker.New(journal, client, logger, worker.Config{
		Concurrency: cfg.concurrency,
		PerTarget:   cfg.perTargetWorkers,
	})
	pool.Start(ctx)
	journalCompactor := compactor.New(journal, logger, compactor.Config{
		ThresholdBytes: cfg.compactThreshold,
		CheckInterval:  cfg.compactInterval,
		Retention:      cfg.terminalRetention,
	})
	journalCompactor.Start(ctx)

	httpServer := &http.Server{
		Handler:           api.New(journal, pool, logger),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	serverErrors := make(chan error, 1)
	go func() {
		logger.Info("spoold started",
			"network", listener.Addr().Network(),
			"address", listener.Addr().String(),
			"journal", cfg.journalPath,
			"journal_max_bytes", cfg.maxJournalBytes,
			"terminal_retention", cfg.terminalRetention,
			"workers", cfg.concurrency,
			"per_target_workers", cfg.perTargetWorkers,
			"allow_private_targets", cfg.allowPrivateTargets,
			"compact_threshold_bytes", cfg.compactThreshold,
			"compact_check_interval", cfg.compactInterval,
		)
		serverErrors <- httpServer.Serve(listener)
	}()

	exitCode := 0
	select {
	case <-ctx.Done():
		logger.Info("shutdown requested")
	case err := <-serverErrors:
		if !errors.Is(err, http.ErrServerClosed) {
			logger.Error("HTTP server stopped", "error", err)
			exitCode = 1
		}
		stop()
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.shutdownTimeout)
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("graceful HTTP shutdown", "error", err)
		exitCode = 1
	}
	cancel()
	stop()
	pool.Wait()
	journalCompactor.Wait()
	if err := journal.Close(); err != nil {
		logger.Error("close journal", "error", err)
		exitCode = 1
	}
	logger.Info("spoold stopped")
	return exitCode
}

func parseConfig(args []string, stderr io.Writer) (config, int) {
	var cfg config
	flags := flag.NewFlagSet("spoold", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.StringVar(&cfg.listen, "listen", "127.0.0.1:8080", "HTTP listen address")
	flags.StringVar(&cfg.unixSocket, "unix-socket", "", "owner-only Unix socket path (overrides -listen)")
	flags.StringVar(&cfg.journalPath, "journal", "data/spoold.journal", "append-only journal path")
	flags.Int64Var(&cfg.maxJournalBytes, "max-journal-bytes", defaultMaxJournalBytes, "reject new deliveries when the journal reaches this size (0 disables)")
	flags.DurationVar(&cfg.terminalRetention, "terminal-retention", compactor.DefaultRetention, "retain succeeded, failed, and canceled deliveries for this duration (0 retains forever)")
	flags.IntVar(&cfg.concurrency, "workers", 4, "number of delivery workers")
	flags.IntVar(&cfg.perTargetWorkers, "per-target-workers", 1, "maximum concurrent requests to one target origin")
	flags.BoolVar(&cfg.allowPrivateTargets, "allow-private-targets", false, "allow private and loopback delivery targets")
	flags.DurationVar(&cfg.requestTimeout, "request-timeout", 10*time.Second, "outbound request timeout")
	flags.DurationVar(&cfg.shutdownTimeout, "shutdown-timeout", 10*time.Second, "graceful shutdown timeout")
	flags.Int64Var(&cfg.compactThreshold, "compact-threshold-bytes", compactor.DefaultThresholdBytes, "minimum journal size for compaction (0 disables size-based compaction)")
	flags.DurationVar(&cfg.compactInterval, "compact-check-interval", compactor.DefaultCheckInterval, "journal maintenance interval")
	flags.BoolVar(&cfg.showVersion, "version", false, "print version information")
	if err := flags.Parse(args); err != nil {
		return config{}, 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintln(stderr, "spoold: positional arguments are not supported")
		return config{}, 2
	}
	if cfg.showVersion {
		return cfg, 0
	}

	switch {
	case cfg.journalPath == "":
		fmt.Fprintln(stderr, "spoold: -journal must not be empty")
	case cfg.maxJournalBytes < 0:
		fmt.Fprintln(stderr, "spoold: -max-journal-bytes must not be negative")
	case cfg.terminalRetention < 0:
		fmt.Fprintln(stderr, "spoold: -terminal-retention must not be negative")
	case cfg.concurrency < 1:
		fmt.Fprintln(stderr, "spoold: -workers must be at least 1")
	case cfg.perTargetWorkers < 1 || cfg.perTargetWorkers > cfg.concurrency:
		fmt.Fprintln(stderr, "spoold: -per-target-workers must be between 1 and -workers")
	case cfg.requestTimeout <= 0:
		fmt.Fprintln(stderr, "spoold: -request-timeout must be positive")
	case cfg.shutdownTimeout <= 0:
		fmt.Fprintln(stderr, "spoold: -shutdown-timeout must be positive")
	case cfg.compactThreshold < 0:
		fmt.Fprintln(stderr, "spoold: -compact-threshold-bytes must not be negative")
	case cfg.compactInterval <= 0:
		fmt.Fprintln(stderr, "spoold: -compact-check-interval must be positive")
	case cfg.unixSocket == "" && cfg.listen == "":
		fmt.Fprintln(stderr, "spoold: -listen must not be empty")
	default:
		return cfg, 0
	}
	return config{}, 2
}

func openListener(address, socketPath string) (net.Listener, func() error, error) {
	if socketPath == "" {
		listener, err := net.Listen("tcp", address)
		if err != nil {
			return nil, nil, fmt.Errorf("listen on %q: %w", address, err)
		}
		return listener, func() error {
			if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				return err
			}
			return nil
		}, nil
	}
	if !filepath.IsAbs(socketPath) {
		return nil, nil, errors.New("unix socket path must be absolute")
	}
	if err := os.MkdirAll(filepath.Dir(socketPath), 0o700); err != nil {
		return nil, nil, fmt.Errorf("create Unix socket directory: %w", err)
	}
	if info, err := os.Lstat(socketPath); err == nil {
		if info.Mode()&os.ModeSocket == 0 {
			return nil, nil, fmt.Errorf("unix socket path %q exists and is not a socket", socketPath)
		}
		connection, dialErr := net.DialTimeout("unix", socketPath, 200*time.Millisecond)
		if dialErr == nil {
			connection.Close()
			return nil, nil, fmt.Errorf("unix socket %q is already in use", socketPath)
		}
		if !errors.Is(dialErr, syscall.ECONNREFUSED) {
			return nil, nil, fmt.Errorf("inspect existing Unix socket %q: %w", socketPath, dialErr)
		}
		if err := os.Remove(socketPath); err != nil {
			return nil, nil, fmt.Errorf("remove stale Unix socket: %w", err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, nil, fmt.Errorf("inspect Unix socket: %w", err)
	}

	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		return nil, nil, fmt.Errorf("listen on Unix socket %q: %w", socketPath, err)
	}
	if err := os.Chmod(socketPath, 0o600); err != nil {
		listener.Close()
		_ = os.Remove(socketPath)
		return nil, nil, fmt.Errorf("set Unix socket permissions: %w", err)
	}
	return listener, func() error {
		var closeErr error
		if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			closeErr = err
		}
		removeErr := os.Remove(socketPath)
		if errors.Is(removeErr, os.ErrNotExist) {
			removeErr = nil
		}
		return errors.Join(closeErr, removeErr)
	}, nil
}
