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
	DefaultRetention            = 7 * 24 * time.Hour
)

type Config struct {
	ThresholdBytes int64
	CheckInterval  time.Duration
	Retention      time.Duration
}

type journalStore interface {
	Stats() store.Stats
	Compact() error
	PruneTerminal(time.Time) (int, error)
}

type Compactor struct {
	store     journalStore
	log       *slog.Logger
	threshold int64
	interval  time.Duration
	retention time.Duration
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
		retention: config.Retention,
	}
}

func (c *Compactor) Start(ctx context.Context) {
	if c.threshold <= 0 && c.retention <= 0 {
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
			c.maintain()
		}
	}
}

func (c *Compactor) maintain() {
	if c.retention > 0 {
		started := time.Now()
		pruned, err := c.store.PruneTerminal(started.Add(-c.retention))
		if err != nil {
			c.log.Warn("journal retention failed", "error", err)
			return
		}
		if pruned > 0 {
			if err := c.compact("retention", started); err != nil {
				return
			}
			c.log.Info("terminal deliveries pruned",
				"deliveries", pruned,
				"retention", c.retention,
			)
			return
		}
	}
	c.compactIfNeeded()
}

func (c *Compactor) compactIfNeeded() {
	if c.threshold <= 0 {
		return
	}
	before := c.store.Stats()
	if before.JournalSizeBytes < c.threshold {
		return
	}
	if before.JournalRecords/2 < before.LiveDeliveries {
		return
	}

	started := time.Now()
	_ = c.compact("redundancy", started)
}

func (c *Compactor) compact(reason string, started time.Time) error {
	before := c.store.Stats()
	if err := c.store.Compact(); err != nil {
		c.log.Warn("journal compaction failed",
			"error", err,
			"reason", reason,
			"duration_ms", time.Since(started).Milliseconds(),
			"size_bytes", before.JournalSizeBytes,
			"records", before.JournalRecords,
		)
		return err
	}

	after := c.store.Stats()
	c.log.Info("journal compacted",
		"reason", reason,
		"duration_ms", time.Since(started).Milliseconds(),
		"size_bytes_before", before.JournalSizeBytes,
		"size_bytes_after", after.JournalSizeBytes,
		"size_bytes_reduced", reducedInt64(before.JournalSizeBytes, after.JournalSizeBytes),
		"records_before", before.JournalRecords,
		"records_after", after.JournalRecords,
		"records_reduced", reducedUint64(before.JournalRecords, after.JournalRecords),
	)
	return nil
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
