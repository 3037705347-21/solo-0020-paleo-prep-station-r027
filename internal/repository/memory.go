package repository

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"example.com/paleo-prep-station/internal/domain"
)

const (
	journalFileName  = "journal.log"
	journalMagic     = 0x50504a31
	journalHeaderLen = 12
	maxRecordBytes   = 8 << 20
)

type state struct {
	specimens map[string]domain.Specimen
	plans     map[string]domain.PrepPlan
	progress  map[string][]domain.ProgressEntry
	releases  map[string]domain.ReleaseRecord
}

type MemoryRepository struct {
	mu          sync.Mutex
	state       state
	commitSeq   uint64
	journal     *os.File
	journalPath string
}

func NewMemory() *MemoryRepository {
	return &MemoryRepository{state: emptyState()}
}

// commitRecord is one durable committed snapshot of the whole workspace.
// Replaying the newest complete record restores the workspace after a restart.
type commitRecord struct {
	Seq       uint64                            `json:"seq"`
	Specimens []domain.Specimen                 `json:"specimens"`
	Plans     []domain.PrepPlan                 `json:"plans"`
	Progress  map[string][]domain.ProgressEntry `json:"progress"`
	Releases  []domain.ReleaseRecord            `json:"releases"`
}

func emptyState() state {
	return state{
		specimens: make(map[string]domain.Specimen),
		plans:     make(map[string]domain.PrepPlan),
		progress:  make(map[string][]domain.ProgressEntry),
		releases:  make(map[string]domain.ReleaseRecord),
	}
}

// Open loads a durable repository rooted at dir. Every committed mutation is
// recorded so a fresh process can restore the same workspace.
func Open(dir string) (*MemoryRepository, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, journalFileName)
	_, statErr := os.Stat(path)
	journalMissing := errors.Is(statErr, os.ErrNotExist)
	if statErr != nil && !journalMissing {
		return nil, statErr
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if journalMissing {
		// Make the freshly created journal entry visible on disk; otherwise a
		// crash immediately after startup could leave the directory without the
		// file even though records were synced into it.
		if err := syncDir(dir); err != nil {
			_ = file.Close()
			return nil, err
		}
	}
	repo := &MemoryRepository{
		state:       emptyState(),
		journal:     file,
		journalPath: path,
	}
	records, validBytes, err := readJournal(file)
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	// A crash mid-append leaves a truncated record tail that fails the magic,
	// length or checksum checks. Drop only that incomplete tail so it cannot
	// block records committed afterwards, while every complete record before
	// it stays intact.
	if size, err := file.Seek(0, io.SeekEnd); err != nil {
		_ = file.Close()
		return nil, err
	} else if size != validBytes {
		if err := file.Truncate(validBytes); err != nil {
			_ = file.Close()
			return nil, err
		}
		if err := file.Sync(); err != nil {
			_ = file.Close()
			return nil, err
		}
	}
	if len(records) > 0 {
		last := records[len(records)-1]
		repo.state = last.toState()
		repo.commitSeq = last.Seq
	}
	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		_ = file.Close()
		return nil, err
	}
	return repo, nil
}

// Close releases the durable journal held by the repository.
func (r *MemoryRepository) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.journal == nil {
		return nil
	}
	if err := r.journal.Sync(); err != nil {
		return err
	}
	err := r.journal.Close()
	r.journal = nil
	return err
}

// CommitSeq reports how many committed mutations the store has accepted.
func (r *MemoryRepository) CommitSeq() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.commitSeq
}

// readJournal replays complete records and reports the offset of the first
// byte that does not belong to a complete, checksummed record.
func readJournal(file *os.File) ([]commitRecord, int64, error) {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, 0, err
	}
	var records []commitRecord
	var offset int64
	header := make([]byte, journalHeaderLen)
	for {
		n, err := io.ReadFull(file, header)
		if errors.Is(err, io.EOF) {
			return records, offset, nil
		}
		if err != nil || n != journalHeaderLen {
			return records, offset, nil
		}
		magic := binary.BigEndian.Uint32(header[0:4])
		length := binary.BigEndian.Uint32(header[4:8])
		checksum := binary.BigEndian.Uint32(header[8:12])
		if magic != journalMagic || length == 0 || length > maxRecordBytes {
			return records, offset, nil
		}
		payload := make([]byte, length)
		if _, err := io.ReadFull(file, payload); err != nil {
			return records, offset, nil
		}
		if crc32.ChecksumIEEE(payload) != checksum {
			return records, offset, nil
		}
		var record commitRecord
		if err := json.Unmarshal(payload, &record); err != nil {
			return records, offset, nil
		}
		records = append(records, record)
		offset += int64(journalHeaderLen) + int64(length)
	}
}

func (r commitRecord) toState() state {
	result := emptyState()
	for _, value := range r.Specimens {
		result.specimens[value.ID] = value
	}
	for _, value := range r.Plans {
		result.plans[value.ID] = value
	}
	for key, values := range r.Progress {
		result.progress[key] = append([]domain.ProgressEntry(nil), values...)
	}
	for _, value := range r.Releases {
		result.releases[value.ID] = value
	}
	return result
}

func recordFrom(source state, seq uint64) commitRecord {
	record := commitRecord{
		Seq:      seq,
		Progress: make(map[string][]domain.ProgressEntry, len(source.progress)),
	}
	for _, value := range source.specimens {
		record.Specimens = append(record.Specimens, value)
	}
	for _, value := range source.plans {
		record.Plans = append(record.Plans, value)
	}
	for key, values := range source.progress {
		record.Progress[key] = append([]domain.ProgressEntry(nil), values...)
	}
	for _, value := range source.releases {
		record.Releases = append(record.Releases, value)
	}
	return record
}

func (r *MemoryRepository) appendRecord(record commitRecord) (err error) {
	payload, err := json.Marshal(record)
	if err != nil {
		return err
	}
	buffer := make([]byte, journalHeaderLen+len(payload))
	binary.BigEndian.PutUint32(buffer[0:4], journalMagic)
	binary.BigEndian.PutUint32(buffer[4:8], uint32(len(payload)))
	binary.BigEndian.PutUint32(buffer[8:12], crc32.ChecksumIEEE(payload))
	copy(buffer[journalHeaderLen:], payload)
	// One contiguous append keeps header and payload from interleaving; the
	// checksum still rejects a record torn short by a crash during the write.
	start, err := r.journal.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	if _, err = r.journal.Write(buffer); err != nil {
		r.discardTail(start)
		return err
	}
	if err = r.journal.Sync(); err != nil {
		// The bytes may or may not be on disk, but the caller never received an
		// acknowledgement, so roll the journal back to the last known-good
		// boundary instead of letting later commits pile up behind a torn tail.
		r.discardTail(start)
		return err
	}
	return nil
}

// discardTail best-effort rewinds the journal to offset so a failed append
// cannot poison records committed afterwards.
func (r *MemoryRepository) discardTail(offset int64) {
	if r.journal == nil {
		return
	}
	_ = r.journal.Truncate(offset)
	_, _ = r.journal.Seek(offset, io.SeekStart)
}

func (r *MemoryRepository) Read(fn func(*Mutation) error) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return fn(cloneMutation(r.state))
}

func (r *MemoryRepository) Mutate(fn func(*Mutation) error) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	tx := cloneMutation(r.state)
	if err := fn(tx); err != nil {
		return err
	}
	if r.journal != nil {
		// Record the post-mutation snapshot: durability must capture exactly
		// what the caller is about to see acknowledged. The append and its
		// sync complete before the in-memory state is published, so an
		// acknowledged change always has a durable record behind it.
		record := recordFrom(tx.state, r.commitSeq+1)
		if err := r.appendRecord(record); err != nil {
			return err
		}
	}
	r.commitSeq++
	r.state = tx.snapshot()
	return nil
}

func cloneMutation(source state) *Mutation {
	copied := state{
		specimens: make(map[string]domain.Specimen, len(source.specimens)),
		plans:     make(map[string]domain.PrepPlan, len(source.plans)),
		progress:  make(map[string][]domain.ProgressEntry, len(source.progress)),
		releases:  make(map[string]domain.ReleaseRecord, len(source.releases)),
	}
	for key, value := range source.specimens {
		copied.specimens[key] = value
	}
	for key, value := range source.plans {
		copied.plans[key] = value
	}
	for key, values := range source.progress {
		copied.progress[key] = append([]domain.ProgressEntry(nil), values...)
	}
	for key, value := range source.releases {
		copied.releases[key] = value
	}
	return &Mutation{state: copied}
}

type Mutation struct {
	state state
}

func (m *Mutation) snapshot() state {
	return m.state
}

func (m *Mutation) GetSpecimen(id string) (domain.Specimen, bool) {
	value, ok := m.state.specimens[id]
	return value, ok
}

func (m *Mutation) FindSpecimenByCatalog(catalogNumber string) (domain.Specimen, bool) {
	for _, value := range m.state.specimens {
		if value.CatalogNumber == catalogNumber {
			return value, true
		}
	}
	return domain.Specimen{}, false
}

func (m *Mutation) ListSpecimens() []domain.Specimen {
	values := make([]domain.Specimen, 0, len(m.state.specimens))
	for _, value := range m.state.specimens {
		values = append(values, value)
	}
	sort.Slice(values, func(i, j int) bool {
		if values[i].CreatedAt.Equal(values[j].CreatedAt) {
			return values[i].ID < values[j].ID
		}
		return values[i].CreatedAt.Before(values[j].CreatedAt)
	})
	return values
}

func (m *Mutation) ListPlans() []domain.PrepPlan {
	values := make([]domain.PrepPlan, 0, len(m.state.plans))
	for _, value := range m.state.plans {
		values = append(values, value)
	}
	sort.Slice(values, func(i, j int) bool {
		return values[i].ID < values[j].ID
	})
	return values
}

func (m *Mutation) ListReleases() []domain.ReleaseRecord {
	values := make([]domain.ReleaseRecord, 0, len(m.state.releases))
	for _, value := range m.state.releases {
		values = append(values, value)
	}
	sort.Slice(values, func(i, j int) bool {
		return values[i].ID < values[j].ID
	})
	return values
}

func (m *Mutation) PutSpecimen(value domain.Specimen) {
	m.state.specimens[value.ID] = value
}

func (m *Mutation) GetPlan(id string) (domain.PrepPlan, bool) {
	value, ok := m.state.plans[id]
	return value, ok
}

func (m *Mutation) ListPlansForSpecimen(specimenID string) []domain.PrepPlan {
	values := make([]domain.PrepPlan, 0)
	for _, value := range m.state.plans {
		if value.SpecimenID == specimenID {
			values = append(values, value)
		}
	}
	sort.Slice(values, func(i, j int) bool {
		if values[i].CreatedAt.Equal(values[j].CreatedAt) {
			return values[i].ID < values[j].ID
		}
		return values[i].CreatedAt.Before(values[j].CreatedAt)
	})
	return values
}

func (m *Mutation) PutPlan(value domain.PrepPlan) {
	m.state.plans[value.ID] = value
}

func (m *Mutation) HasOpenPlan(specimenID string) bool {
	for _, value := range m.state.plans {
		if value.SpecimenID == specimenID && value.IsOpen() {
			return true
		}
	}
	return false
}

func (m *Mutation) ProgressForPlan(planID string) []domain.ProgressEntry {
	return append([]domain.ProgressEntry(nil), m.state.progress[planID]...)
}

func (m *Mutation) AppendProgress(value domain.ProgressEntry) {
	m.state.progress[value.PlanID] = append(m.state.progress[value.PlanID], value)
}

func (m *Mutation) ReleaseForSpecimen(specimenID string) (domain.ReleaseRecord, bool) {
	for _, value := range m.state.releases {
		if value.SpecimenID == specimenID {
			return value, true
		}
	}
	return domain.ReleaseRecord{}, false
}

func (m *Mutation) PutRelease(value domain.ReleaseRecord) {
	m.state.releases[value.ID] = value
}

// syncDir flushes a directory so a newly created journal entry is durable.
func syncDir(dir string) error {
	handle, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer handle.Close()
	return handle.Sync()
}
