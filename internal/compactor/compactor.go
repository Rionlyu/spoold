package compactor

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/Rionlyu/spoold/internal/store"
)

const (
	DefaultThresholdBytes int64 = 64 << 20
	DefaultCheckInterval        = time.Minute
)

type Config struct {
	ThresholdBytes int64
	CheckInterval  time.Duration
}

type journalStore interface {
	Stats() store.Stats
	Compact() error
}

type Compactor struct {
	store     journalStore
	log       *slog.Logger
	threshold int64
	interval  time.Duration
	wg        sync.WaitGroup
}

func New(journal journalStore, logger *slog.Logger, config Config) *Compactor {
	if config.CheckInterval <= 0 {
		config.CheckInterval = DefaultCheckInterval
	}
	return &Compactor{
		store:     journal,
		log:       logger,
		threshold: config.ThresholdBytes,
		interval:  config.CheckInterval,
	}
}

func (c *Compactor) Start(ctx context.Context) {
	if c.threshold <= 0 {
		return
	}
	c.wg.Add(1)
	go c.run(ctx)
}

func (c *Compactor) Wait() {
	c.wg.Wait()
}

func (c *Compactor) run(ctx context.Context) {
	defer c.wg.Done()
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.compactIfNeeded()
		}
	}
}

func (c *Compactor) compactIfNeeded() {
	before := c.store.Stats()
	if before.JournalSizeBytes < c.threshold {
		return
	}
	if before.JournalRecords/2 < before.LiveDeliveries {
		return
	}

	started := time.Now()
	if err := c.store.Compact(); err != nil {
		c.log.Warn("journal compaction failed",
			"error", err,
			"duration_ms", time.Since(started).Milliseconds(),
			"size_bytes", before.JournalSizeBytes,
			"records", before.JournalRecords,
		)
		return
	}

	after := c.store.Stats()
	c.log.Info("journal compacted",
		"duration_ms", time.Since(started).Milliseconds(),
		"size_bytes_before", before.JournalSizeBytes,
		"size_bytes_after", after.JournalSizeBytes,
		"size_bytes_reduced", reducedInt64(before.JournalSizeBytes, after.JournalSizeBytes),
		"records_before", before.JournalRecords,
		"records_after", after.JournalRecords,
		"records_reduced", reducedUint64(before.JournalRecords, after.JournalRecords),
	)
}

func reducedInt64(before, after int64) int64 {
	if after >= before {
		return 0
	}
	return before - after
}

func reducedUint64(before, after uint64) uint64 {
	if after >= before {
		return 0
	}
	return before - after
}
