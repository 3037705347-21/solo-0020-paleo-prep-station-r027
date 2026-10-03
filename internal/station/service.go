package station

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync/atomic"
	"time"

	"example.com/paleo-prep-station/internal/domain"
	"example.com/paleo-prep-station/internal/repository"
)

type Service struct {
	repo *repository.MemoryRepository
	now  func() time.Time
	seq  atomic.Uint64
}

func New(repo *repository.MemoryRepository) *Service {
	if repo == nil {
		repo = repository.NewMemory()
	}
	return &Service{
		repo: repo,
		now:  func() time.Time { return time.Now().UTC() },
	}
}

// Open restores a service from the durable store rooted at dir.
func Open(dir string) (*Service, error) {
	repo, err := repository.Open(dir)
	if err != nil {
		return nil, err
	}
	service := New(repo)
	if err := service.verifyRecoveredState(); err != nil {
		_ = repo.Close()
		return nil, err
	}
	return service, nil
}

// Close flushes and releases the service's durable store.
func (s *Service) Close() error {
	return s.repo.Close()
}

// verifyRecoveredState rejects a restored workspace whose cross-entity facts
// disagree, so a partial recovery cannot silently serve an inconsistent view.
func (s *Service) verifyRecoveredState() error {
	return s.repo.Read(func(m *repository.Mutation) error {
		for _, plan := range m.ListPlans() {
			if _, ok := m.GetSpecimen(plan.SpecimenID); !ok {
				return fmt.Errorf(
					"恢复出的修复方案 %s 引用了不存在的标本 %s",
					plan.ID,
					plan.SpecimenID,
				)
			}
			for _, entry := range m.ProgressForPlan(plan.ID) {
				if entry.PlanID != plan.ID {
					return fmt.Errorf("恢复出的进度 %s 与方案 %s 不匹配", entry.ID, plan.ID)
				}
			}
		}
		for _, release := range m.ListReleases() {
			specimen, ok := m.GetSpecimen(release.SpecimenID)
			if !ok {
				return fmt.Errorf(
					"恢复出的放行记录 %s 引用了不存在的标本 %s",
					release.ID,
					release.SpecimenID,
				)
			}
			if specimen.Status != domain.SpecimenAvailable {
				return fmt.Errorf(
					"恢复出的放行记录 %s 与标本状态 %s 不一致",
					release.ID,
					specimen.Status,
				)
			}
		}
		return nil
	})
}

func (s *Service) nextID(prefix string) string {
	var random [6]byte
	if _, err := rand.Read(random[:]); err == nil {
		return prefix + "-" + hex.EncodeToString(random[:])
	}
	local := s.seq.Add(1)
	return prefix + "-" + time.Now().UTC().Format("20060102150405") + "-" + hex.EncodeToString([]byte{byte(local)})
}

type PlanSnapshot struct {
	Plan     domain.PrepPlan        `json:"plan"`
	Progress []domain.ProgressEntry `json:"progress"`
}

func (s *Service) ListSpecimens(ctx context.Context) ([]domain.Specimen, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var values []domain.Specimen
	err := s.repo.Read(func(m *repository.Mutation) error {
		values = m.ListSpecimens()
		return nil
	})
	return values, err
}

func (s *Service) GetSpecimen(ctx context.Context, specimenID string) (domain.Specimen, error) {
	if err := ctx.Err(); err != nil {
		return domain.Specimen{}, err
	}
	var value domain.Specimen
	err := s.repo.Read(func(m *repository.Mutation) error {
		found, ok := m.GetSpecimen(specimenID)
		if !ok {
			return notFoundSpecimen(specimenID)
		}
		value = found
		return nil
	})
	return value, err
}
