package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"example.com/paleo-prep-station/internal/contracts"
	"example.com/paleo-prep-station/internal/station"
)

// durablecheck exercises the acknowledged-change-survives-restart guarantee
// through the public station API, including a torn journal tail produced by a
// write interrupted mid-record.
func main() {
	dir := flag.String("data-dir", "", "journal directory to use (temporary if empty)")
	flag.Parse()

	root := *dir
	if root == "" {
		var err error
		root, err = os.MkdirTemp("", "paleo-durable-*")
		if err != nil {
			panic(err)
		}
		defer os.RemoveAll(root)
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		panic(err)
	}

	if err := runFullLifecycle(root); err != nil {
		fmt.Fprintln(os.Stderr, "durable check failed:", err)
		os.Exit(1)
	}
	if err := runTornTail(root); err != nil {
		fmt.Fprintln(os.Stderr, "durable check failed:", err)
		os.Exit(1)
	}
	fmt.Println("durable check passed: full lifecycle survives restarts and torn tails")
}

func runFullLifecycle(dir string) error {
	ctx := context.Background()

	svc, err := station.Open(filepath.Join(dir, "lifecycle"))
	if err != nil {
		return err
	}
	specimen, err := svc.RegisterSpecimen(ctx, contracts.RegisterSpecimenRequest{
		CatalogNumber: "PPS-DURABLE-001",
		FieldLabel:    "北坡-持久",
		MatrixKind:    "粉砂岩",
	})
	if err != nil {
		return err
	}
	plan, err := svc.CreatePlan(ctx, specimen.ID, contracts.CreatePlanRequest{
		Objective:      "暴露主要关节面并加固",
		Method:         "手工显微剥离",
		TargetExposure: 80,
	})
	if err != nil {
		return err
	}
	if _, err := svc.ApprovePlan(ctx, plan.ID, contracts.ApprovalRequest{
		ApprovedBy: "curator-lin",
	}); err != nil {
		return err
	}
	if _, err := svc.LogProgress(ctx, plan.ID, contracts.ProgressRequest{
		Stage: "矩阵剥离", Percent: 35, RecordedBy: "preparator-chen",
	}); err != nil {
		return err
	}
	// Restart 1: approved plan and the first progress entry must survive.
	if err := svc.Close(); err != nil {
		return err
	}
	svc, err = station.Open(filepath.Join(dir, "lifecycle"))
	if err != nil {
		return err
	}
	snapshot, err := svc.GetPlan(ctx, plan.ID)
	if err != nil {
		return err
	}
	if snapshot.Plan.Status != "active" || len(snapshot.Progress) != 1 ||
		snapshot.Progress[0].Percent != 35 {
		return fmt.Errorf("after restart snapshot mismatch: %+v", snapshot)
	}

	if _, err := svc.LogProgress(ctx, plan.ID, contracts.ProgressRequest{
		Stage: "表面加固", Percent: 100, RecordedBy: "preparator-chen",
	}); err != nil {
		return err
	}
	release, err := svc.ReleaseSpecimen(ctx, specimen.ID, contracts.ReleaseRequest{
		Purpose: "比较解剖研究", Recipient: "地层研究组", ApprovedBy: "keeper-zhou",
	})
	if err != nil {
		return err
	}
	// Restart 2: completion, stabilization and the release must survive.
	if err := svc.Close(); err != nil {
		return err
	}
	svc, err = station.Open(filepath.Join(dir, "lifecycle"))
	if err != nil {
		return err
	}
	defer svc.Close()

	specimen, err = svc.GetSpecimen(ctx, specimen.ID)
	if err != nil {
		return err
	}
	if specimen.Status != "available" {
		return fmt.Errorf("specimen status after restart = %s, want available", specimen.Status)
	}
	snapshot, err = svc.GetPlan(ctx, plan.ID)
	if err != nil {
		return err
	}
	if snapshot.Plan.Status != "completed" || len(snapshot.Progress) != 2 {
		return fmt.Errorf("plan snapshot after restart mismatch: %+v", snapshot)
	}
	list, err := svc.ListPlansForSpecimen(ctx, specimen.ID)
	if err != nil {
		return err
	}
	if len(list) != 1 || list[0].ID != plan.ID {
		return fmt.Errorf("expected exactly one plan, got %d", len(list))
	}

	// The release must still be present and must not be repeatable.
	if _, err := svc.ReleaseSpecimen(ctx, specimen.ID, contracts.ReleaseRequest{
		Purpose: "重复借阅", Recipient: "其他组", ApprovedBy: "someone-else",
	}); err == nil {
		return errors.New("duplicate release was accepted after restart")
	}
	_ = release
	return nil
}

func runTornTail(dir string) error {
	ctx := context.Background()
	root := filepath.Join(dir, "torn")

	svc, err := station.Open(root)
	if err != nil {
		return err
	}
	first, err := svc.RegisterSpecimen(ctx, contracts.RegisterSpecimenRequest{
		CatalogNumber: "PPS-DURABLE-002",
		FieldLabel:    "断尾-之前",
		MatrixKind:    "灰岩",
	})
	if err != nil {
		return err
	}
	// Second acknowledged commit: its record is the one the interrupted write
	// will corrupt, so recovery must fall back to the still-complete first one.
	torn, err := svc.RegisterSpecimen(ctx, contracts.RegisterSpecimenRequest{
		CatalogNumber: "PPS-DURABLE-099",
		FieldLabel:    "断尾-写入",
		MatrixKind:    "泥岩",
	})
	if err != nil {
		return err
	}
	if err := svc.Close(); err != nil {
		return err
	}

	// Simulate a crash mid-append: corrupt the last record's payload by
	// truncating the journal somewhere inside the final record.
	journal := filepath.Join(root, "journal.log")
	info, err := os.Stat(journal)
	if err != nil {
		return err
	}
	tornSize := info.Size() - 5
	if tornSize <= 0 {
		return errors.New("journal too small to simulate torn write")
	}
	if err := os.Truncate(journal, tornSize); err != nil {
		return err
	}

	svc, err = station.Open(root)
	if err != nil {
		return err
	}
	if found, err := svc.GetSpecimen(ctx, first.ID); err != nil {
		return fmt.Errorf("earlier acknowledged specimen lost after torn write: %w", err)
	} else if found.CatalogNumber != "PPS-DURABLE-002" {
		return errors.New("recovered specimen has unexpected catalog number")
	}
	if _, err := svc.GetSpecimen(ctx, torn.ID); err == nil {
		return errors.New("torn (unconfirmed) record was resurrected after recovery")
	}

	// New writes after the interruption must survive the next restart too.
	second, err := svc.RegisterSpecimen(ctx, contracts.RegisterSpecimenRequest{
		CatalogNumber: "PPS-DURABLE-003",
		FieldLabel:    "断尾-之后",
		MatrixKind:    "页岩",
	})
	if err != nil {
		return err
	}
	if err := svc.Close(); err != nil {
		return err
	}

	svc, err = station.Open(root)
	if err != nil {
		return err
	}
	defer svc.Close()
	if _, err := svc.GetSpecimen(ctx, first.ID); err != nil {
		return fmt.Errorf("first specimen missing after recovery: %w", err)
	}
	if _, err := svc.GetSpecimen(ctx, second.ID); err != nil {
		return fmt.Errorf("post-crash commit missing after restart: %w", err)
	}
	specimens, err := svc.ListSpecimens(ctx)
	if err != nil {
		return err
	}
	if len(specimens) != 2 {
		return fmt.Errorf("expected exactly 2 specimens, got %d (phantom or lost record)", len(specimens))
	}
	return nil
}
