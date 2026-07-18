package store

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Rionlyu/spoold/internal/delivery"
)

var (
	ErrNotFound            = errors.New("delivery not found")
	ErrIdempotencyConflict = errors.New("idempotency key was already used for a different request")
	ErrInvalidTransition   = errors.New("invalid delivery state transition")
	ErrStaleLease          = errors.New("delivery lease is no longer current")
	ErrPersistence         = errors.New("delivery persistence failed")
	ErrJournalFull         = errors.New("journal admission limit reached")
	ErrJournalLocked       = errors.New("journal is already owned by another spoold process")
)

const currentJournalVersion = 2

type Options struct {
	MaxJournalBytes int64
}

type Store struct {
	mu                   sync.Mutex
	path                 string
	file                 *os.File
	lock                 *os.File
	items                map[string]delivery.Delivery
	hashes               map[string]string
	keyToID              map[string]string
	maxJournalBytes      int64
	persistenceErr       error
	records              uint64
	prunedDeliveries     uint64
	compactionsSucceeded uint64
	compactionsFailed    uint64
	compactionHook       func(compactionStage) error
}

type journalRecord struct {
	Version     int                `json:"version"`
	Delivery    *delivery.Delivery `json:"delivery,omitempty"`
	RequestHash string             `json:"requestHash,omitempty"`
	DeletedID   string             `json:"deletedId,omitempty"`
}

type Counts map[delivery.Status]int

type Stats struct {
	JournalSizeBytes     int64
	JournalRecords       uint64
	LiveDeliveries       uint64
	MaxJournalBytes      int64
	PrunedDeliveries     uint64
	CompactionsSucceeded uint64
	CompactionsFailed    uint64
}

type compactionStage string

const (
	compactionBeforeRename compactionStage = "before_rename"
	compactionAfterRename  compactionStage = "after_rename"
	compactionAfterDirSync compactionStage = "after_directory_sync"
)

func Open(path string) (*Store, error) {
	return OpenWithOptions(path, Options{})
}

func OpenWithOptions(path string, options Options) (*Store, error) {
	if options.MaxJournalBytes < 0 {
		return nil, errors.New("maximum journal size must not be negative")
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("create journal directory: %w", err)
	}

	lock, err := acquireJournalLock(path)
	if err != nil {
		return nil, err
	}
	releaseLock := true
	defer func() {
		if releaseLock {
			_ = releaseJournalLock(lock)
		}
	}()

	if err := removeAbandonedCompactions(path); err != nil {
		return nil, err
	}

	_, statErr := os.Stat(path)
	newJournal := errors.Is(statErr, os.ErrNotExist)
	if statErr != nil && !newJournal {
		return nil, fmt.Errorf("stat journal: %w", statErr)
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open journal: %w", err)
	}
	if newJournal {
		if err := syncDirectory(dir); err != nil {
			file.Close()
			_ = os.Remove(path)
			return nil, fmt.Errorf("persist new journal: %w", err)
		}
	}

	store := &Store{
		path:            path,
		file:            file,
		lock:            lock,
		items:           make(map[string]delivery.Delivery),
		hashes:          make(map[string]string),
		keyToID:         make(map[string]string),
		maxJournalBytes: options.MaxJournalBytes,
	}
	if err := store.replay(); err != nil {
		file.Close()
		return nil, err
	}
	releaseLock = false
	return store, nil
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var fileErr error
	if s.file != nil {
		fileErr = s.file.Close()
		s.file = nil
	}
	lockErr := releaseJournalLock(s.lock)
	s.lock = nil
	return errors.Join(fileErr, lockErr)
}

func (s *Store) Create(req delivery.CreateRequest, now time.Time) (delivery.Delivery, bool, error) {
	candidate, requestHash, err := delivery.New(req, now)
	if err != nil {
		return delivery.Delivery{}, false, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if candidate.IdempotencyKey != "" {
		if id, ok := s.keyToID[candidate.IdempotencyKey]; ok {
			if s.hashes[id] != requestHash {
				return delivery.Delivery{}, false, ErrIdempotencyConflict
			}
			return delivery.Clone(s.items[id]), false, nil
		}
	}

	if err := s.checkAdmissionLocked(candidate, requestHash); err != nil {
		return delivery.Delivery{}, false, err
	}
	if err := s.appendLocked(candidate, requestHash); err != nil {
		return delivery.Delivery{}, false, err
	}
	s.setLocked(candidate, requestHash)
	return delivery.Clone(candidate), true, nil
}

func (s *Store) Get(id string) (delivery.Delivery, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	item, ok := s.items[id]
	if !ok {
		return delivery.Delivery{}, ErrNotFound
	}
	return delivery.Clone(item), nil
}

func (s *Store) List(status delivery.Status) []delivery.Delivery {
	s.mu.Lock()
	defer s.mu.Unlock()

	result := make([]delivery.Delivery, 0, len(s.items))
	for _, item := range s.items {
		if status == "" || item.Status == status {
			result = append(result, delivery.Clone(item))
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].CreatedAt.Equal(result[j].CreatedAt) {
			return result[i].ID < result[j].ID
		}
		return result[i].CreatedAt.Before(result[j].CreatedAt)
	})
	return result
}

func (s *Store) ClaimDue(now time.Time, leaseDuration time.Duration, limit int) ([]delivery.Delivery, error) {
	if limit < 1 {
		return nil, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	candidates := make([]delivery.Delivery, 0)
	for _, item := range s.items {
		pending := item.Status == delivery.StatusPending && !item.NextAttemptAt.After(now)
		expired := item.Status == delivery.StatusInFlight && !item.LeaseUntil.After(now)
		if pending || expired {
			candidates = append(candidates, item)
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].NextAttemptAt.Equal(candidates[j].NextAttemptAt) {
			if candidates[i].CreatedAt.Equal(candidates[j].CreatedAt) {
				return candidates[i].ID < candidates[j].ID
			}
			return candidates[i].CreatedAt.Before(candidates[j].CreatedAt)
		}
		return candidates[i].NextAttemptAt.Before(candidates[j].NextAttemptAt)
	})
	if len(candidates) > limit {
		candidates = candidates[:limit]
	}

	claimed := make([]delivery.Delivery, 0, len(candidates))
	for _, item := range candidates {
		item.Status = delivery.StatusInFlight
		item.Attempts++
		item.LeaseUntil = now.Add(leaseDuration).UTC()
		item.UpdatedAt = now.UTC()
		if err := s.appendLocked(item, s.hashes[item.ID]); err != nil {
			return claimed, err
		}
		s.setLocked(item, s.hashes[item.ID])
		claimed = append(claimed, delivery.Clone(item))
	}
	return claimed, nil
}

func (s *Store) Succeed(id string, attempt, responseCode int, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	item, err := s.currentLeaseLocked(id, attempt)
	if err != nil {
		return err
	}
	item.Status = delivery.StatusSucceeded
	item.LastResponseCode = responseCode
	item.LastError = ""
	item.NextAttemptAt = time.Time{}
	item.LeaseUntil = time.Time{}
	item.UpdatedAt = now.UTC()
	return s.persistLocked(item)
}

func (s *Store) Fail(id string, attempt, responseCode int, message string, next time.Time, terminal bool, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	item, err := s.currentLeaseLocked(id, attempt)
	if err != nil {
		return err
	}
	item.LastResponseCode = responseCode
	item.LastError = message
	item.LeaseUntil = time.Time{}
	item.UpdatedAt = now.UTC()
	if terminal {
		item.Status = delivery.StatusFailed
		item.NextAttemptAt = time.Time{}
	} else {
		item.Status = delivery.StatusPending
		item.NextAttemptAt = next.UTC()
	}
	return s.persistLocked(item)
}

func (s *Store) Cancel(id string, now time.Time) (delivery.Delivery, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	item, ok := s.items[id]
	if !ok {
		return delivery.Delivery{}, ErrNotFound
	}
	if item.Status != delivery.StatusPending && item.Status != delivery.StatusInFlight {
		return delivery.Delivery{}, ErrInvalidTransition
	}
	item.Status = delivery.StatusCanceled
	item.NextAttemptAt = time.Time{}
	item.LeaseUntil = time.Time{}
	item.UpdatedAt = now.UTC()
	if err := s.persistLocked(item); err != nil {
		return delivery.Delivery{}, err
	}
	return delivery.Clone(item), nil
}

func (s *Store) Retry(id string, now time.Time) (delivery.Delivery, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	item, ok := s.items[id]
	if !ok {
		return delivery.Delivery{}, ErrNotFound
	}
	if item.Status != delivery.StatusFailed {
		return delivery.Delivery{}, ErrInvalidTransition
	}
	item.Status = delivery.StatusPending
	item.Attempts = 0
	item.NextAttemptAt = now.UTC()
	item.LeaseUntil = time.Time{}
	item.LastError = ""
	item.LastResponseCode = 0
	item.UpdatedAt = now.UTC()
	if err := s.persistLocked(item); err != nil {
		return delivery.Delivery{}, err
	}
	return delivery.Clone(item), nil
}

func (s *Store) Counts() Counts {
	s.mu.Lock()
	defer s.mu.Unlock()

	counts := make(Counts)
	for _, item := range s.items {
		counts[item.Status]++
	}
	return counts
}

func (s *Store) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()

	var size int64
	if s.file != nil {
		if info, err := s.file.Stat(); err == nil {
			size = info.Size()
		}
	}
	return Stats{
		JournalSizeBytes:     size,
		JournalRecords:       s.records,
		LiveDeliveries:       uint64(len(s.items)),
		MaxJournalBytes:      s.maxJournalBytes,
		PrunedDeliveries:     s.prunedDeliveries,
		CompactionsSucceeded: s.compactionsSucceeded,
		CompactionsFailed:    s.compactionsFailed,
	}
}

func (s *Store) Ready() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.persistenceErr != nil {
		return s.persistenceErr
	}
	if s.file == nil {
		return errors.New("journal is closed")
	}
	if _, err := s.file.Stat(); err != nil {
		return fmt.Errorf("stat journal: %w", err)
	}
	return nil
}

func (s *Store) PruneTerminal(before time.Time) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ids := make([]string, 0)
	for id, item := range s.items {
		terminal := item.Status == delivery.StatusSucceeded ||
			item.Status == delivery.StatusFailed ||
			item.Status == delivery.StatusCanceled
		if terminal && !item.UpdatedAt.After(before) {
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		return 0, nil
	}
	sort.Strings(ids)

	var records bytes.Buffer
	for _, id := range ids {
		if err := writeDeleteRecord(&records, id); err != nil {
			return 0, err
		}
	}
	if err := s.appendDataLocked(records.Bytes(), uint64(len(ids))); err != nil {
		return 0, err
	}
	for _, id := range ids {
		s.deleteLocked(id)
	}
	s.prunedDeliveries += uint64(len(ids))
	return len(ids), nil
}

func (s *Store) Compact() (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	defer func() {
		if err != nil {
			s.compactionsFailed++
		}
	}()
	if s.file == nil {
		return errors.New("compact closed journal")
	}

	temp, err := os.CreateTemp(filepath.Dir(s.path), compactionPrefix(s.path))
	if err != nil {
		return fmt.Errorf("create compacted journal: %w", err)
	}
	tempPath := temp.Name()
	renamed := false
	adopted := false
	defer func() {
		if !adopted {
			if closeErr := temp.Close(); err == nil && closeErr != nil {
				err = fmt.Errorf("close compacted journal: %w", closeErr)
			}
		}
		if !renamed {
			if removeErr := os.Remove(tempPath); err == nil && removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
				err = fmt.Errorf("remove compacted journal: %w", removeErr)
			}
		}
	}()
	if err := temp.Chmod(0o600); err != nil {
		return fmt.Errorf("set compacted journal permissions: %w", err)
	}

	ids := make([]string, 0, len(s.items))
	for id := range s.items {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		if err := writeRecord(temp, s.items[id], s.hashes[id]); err != nil {
			return err
		}
	}
	if err := temp.Sync(); err != nil {
		return fmt.Errorf("sync compacted journal: %w", err)
	}
	if err := s.runCompactionHook(compactionBeforeRename); err != nil {
		return err
	}
	if err := os.Rename(tempPath, s.path); err != nil {
		return fmt.Errorf("replace journal: %w", err)
	}
	renamed = true
	if err := s.runCompactionHook(compactionAfterRename); err != nil {
		err = errors.Join(err, s.adoptCompactedLocked(temp, uint64(len(ids))))
		adopted = true
		return err
	}
	if err := syncDirectory(filepath.Dir(s.path)); err != nil {
		err = errors.Join(err, s.adoptCompactedLocked(temp, uint64(len(ids))))
		adopted = true
		return err
	}
	if err := s.runCompactionHook(compactionAfterDirSync); err != nil {
		err = errors.Join(err, s.adoptCompactedLocked(temp, uint64(len(ids))))
		adopted = true
		return err
	}

	if err := s.adoptCompactedLocked(temp, uint64(len(ids))); err != nil {
		adopted = true
		return err
	}
	adopted = true
	s.persistenceErr = nil
	s.compactionsSucceeded++
	return nil
}

func (s *Store) currentLeaseLocked(id string, attempt int) (delivery.Delivery, error) {
	item, ok := s.items[id]
	if !ok {
		return delivery.Delivery{}, ErrNotFound
	}
	if item.Status != delivery.StatusInFlight || item.Attempts != attempt {
		return delivery.Delivery{}, ErrStaleLease
	}
	return item, nil
}

func (s *Store) persistLocked(item delivery.Delivery) error {
	if err := s.appendLocked(item, s.hashes[item.ID]); err != nil {
		return err
	}
	s.setLocked(item, s.hashes[item.ID])
	return nil
}

func (s *Store) setLocked(item delivery.Delivery, requestHash string) {
	s.items[item.ID] = delivery.Clone(item)
	s.hashes[item.ID] = requestHash
	if item.IdempotencyKey != "" {
		s.keyToID[item.IdempotencyKey] = item.ID
	}
}

func (s *Store) deleteLocked(id string) {
	item, ok := s.items[id]
	if !ok {
		return
	}
	delete(s.items, id)
	delete(s.hashes, id)
	if item.IdempotencyKey != "" && s.keyToID[item.IdempotencyKey] == id {
		delete(s.keyToID, item.IdempotencyKey)
	}
}

func (s *Store) appendLocked(item delivery.Delivery, requestHash string) error {
	data, err := marshalRecord(item, requestHash)
	if err != nil {
		return err
	}
	return s.appendDataLocked(data, 1)
}

func (s *Store) appendDataLocked(data []byte, records uint64) error {
	if s.persistenceErr != nil {
		return s.persistenceErr
	}
	if s.file == nil {
		return s.failPersistenceLocked(errors.New("journal is closed"))
	}
	if _, err := s.file.Write(data); err != nil {
		return s.failPersistenceLocked(fmt.Errorf("append journal record: %w", err))
	}
	if err := s.file.Sync(); err != nil {
		return s.failPersistenceLocked(fmt.Errorf("sync journal: %w", err))
	}
	s.records += records
	return nil
}

func writeRecord(writer io.Writer, item delivery.Delivery, requestHash string) error {
	data, err := marshalRecord(item, requestHash)
	if err != nil {
		return err
	}
	if _, err := writer.Write(data); err != nil {
		return fmt.Errorf("append journal record: %w", err)
	}
	return nil
}

func marshalRecord(item delivery.Delivery, requestHash string) ([]byte, error) {
	data, err := json.Marshal(journalRecord{
		Version:     currentJournalVersion,
		Delivery:    &item,
		RequestHash: requestHash,
	})
	if err != nil {
		return nil, fmt.Errorf("encode journal record: %w", err)
	}
	data = append(data, '\n')
	return data, nil
}

func writeDeleteRecord(writer io.Writer, id string) error {
	data, err := json.Marshal(journalRecord{
		Version:   currentJournalVersion,
		DeletedID: id,
	})
	if err != nil {
		return fmt.Errorf("encode journal deletion: %w", err)
	}
	data = append(data, '\n')
	if _, err := writer.Write(data); err != nil {
		return fmt.Errorf("append journal record: %w", err)
	}
	return nil
}

func (s *Store) replay() error {
	if _, err := s.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek journal: %w", err)
	}

	reader := bufio.NewReader(s.file)
	line := 0
	for {
		data, err := reader.ReadBytes('\n')
		if err == io.EOF {
			if len(data) > 0 {
				var record journalRecord
				if json.Unmarshal(data, &record) == nil {
					line++
					if err := s.applyRecord(record, line); err != nil {
						return err
					}
					s.records++
				}
			}
			break
		}
		if err != nil {
			return fmt.Errorf("read journal: %w", err)
		}
		line++

		var record journalRecord
		if err := json.Unmarshal(data, &record); err != nil {
			return fmt.Errorf("decode journal line %d: %w", line, err)
		}
		if err := s.applyRecord(record, line); err != nil {
			return err
		}
		s.records++
	}

	if _, err := s.file.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("seek journal end: %w", err)
	}
	return nil
}

func (s *Store) applyRecord(record journalRecord, line int) error {
	if record.Version != 1 && record.Version != currentJournalVersion {
		return fmt.Errorf("journal line %d uses unsupported version %d", line, record.Version)
	}
	if record.DeletedID != "" {
		if record.Version < 2 {
			return fmt.Errorf("journal line %d uses a deletion with version %d", line, record.Version)
		}
		if record.Delivery != nil {
			return fmt.Errorf("journal line %d contains both a delivery and deletion", line)
		}
		s.deleteLocked(record.DeletedID)
		return nil
	}
	if record.Delivery == nil || record.Delivery.ID == "" {
		return fmt.Errorf("journal line %d has no delivery id", line)
	}
	s.setLocked(*record.Delivery, record.RequestHash)
	return nil
}

func (s *Store) checkAdmissionLocked(item delivery.Delivery, requestHash string) error {
	if s.maxJournalBytes == 0 {
		return nil
	}
	data, err := marshalRecord(item, requestHash)
	if err != nil {
		return err
	}
	info, err := s.file.Stat()
	if err != nil {
		return s.failPersistenceLocked(fmt.Errorf("stat journal: %w", err))
	}
	if info.Size()+int64(len(data)) > s.maxJournalBytes {
		return ErrJournalFull
	}
	return nil
}

func (s *Store) failPersistenceLocked(err error) error {
	if s.persistenceErr == nil {
		s.persistenceErr = fmt.Errorf("%w: %w", ErrPersistence, err)
	}
	return s.persistenceErr
}

func (s *Store) adoptCompactedLocked(file *os.File, records uint64) error {
	old := s.file
	s.file = file
	s.records = records
	if err := old.Close(); err != nil {
		return fmt.Errorf("close replaced journal: %w", err)
	}
	return nil
}

func (s *Store) runCompactionHook(stage compactionStage) error {
	if s.compactionHook == nil {
		return nil
	}
	if err := s.compactionHook(stage); err != nil {
		return fmt.Errorf("compact journal at %s: %w", stage, err)
	}
	return nil
}

func compactionPrefix(path string) string {
	return "." + filepath.Base(path) + ".compact-"
}

func removeAbandonedCompactions(path string) error {
	dir := filepath.Dir(path)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("read journal directory: %w", err)
	}

	prefix := compactionPrefix(path)
	removed := false
	for _, entry := range entries {
		if !entry.Type().IsRegular() || !strings.HasPrefix(entry.Name(), prefix) {
			continue
		}
		if err := os.Remove(filepath.Join(dir, entry.Name())); err != nil {
			return fmt.Errorf("remove abandoned compacted journal %q: %w", entry.Name(), err)
		}
		removed = true
	}
	if removed {
		if err := syncDirectory(dir); err != nil {
			return err
		}
	}
	return nil
}

func syncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open journal directory: %w", err)
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("sync journal directory: %w", err)
	}
	return nil
}
