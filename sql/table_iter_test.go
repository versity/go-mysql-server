// Copyright 2026 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sql

import (
	"context"
	"fmt"
	"io"
	"sort"
	"sync/atomic"
	"testing"
)

// --- Mock implementations for testing ConcurrentTableRowIter ---

// mockPartition implements Partition.
type mockPartition struct {
	id int
}

func (p *mockPartition) Key() []byte {
	return []byte(fmt.Sprintf("partition-%d", p.id))
}

// mockPartitionIter implements PartitionIter.
type mockPartitionIter struct {
	partitions []Partition
	idx        int
}

func (i *mockPartitionIter) Next(_ *Context) (Partition, error) {
	if i.idx >= len(i.partitions) {
		return nil, io.EOF
	}
	p := i.partitions[i.idx]
	i.idx++
	return p, nil
}

func (i *mockPartitionIter) Close(_ *Context) error {
	return nil
}

// mockRowIter implements RowIter and returns rows from a fixed slice.
type mockRowIter struct {
	rows []Row
	idx  int
}

func (i *mockRowIter) Next(_ *Context) (Row, error) {
	if i.idx >= len(i.rows) {
		return nil, io.EOF
	}
	r := i.rows[i.idx]
	i.idx++
	return r, nil
}

func (i *mockRowIter) Close(_ *Context) error {
	return nil
}

// mockTable implements Table by returning fixed rows per partition.
type mockTable struct {
	name           string
	schema         Schema
	partitions     []Partition
	rowsByPartID   map[int][]Row
	partitionCalls atomic.Int64
}

func (t *mockTable) Name() string           { return t.name }
func (t *mockTable) String() string         { return t.name }
func (t *mockTable) Schema() Schema         { return t.schema }
func (t *mockTable) Collation() CollationID { return Collation_Default }

func (t *mockTable) Partitions(_ *Context) (PartitionIter, error) {
	return &mockPartitionIter{partitions: t.partitions}, nil
}

func (t *mockTable) PartitionRows(_ *Context, partition Partition) (RowIter, error) {
	t.partitionCalls.Add(1)
	mp := partition.(*mockPartition)
	rows := t.rowsByPartID[mp.id]
	rowsCopy := make([]Row, len(rows))
	copy(rowsCopy, rows)
	return &mockRowIter{rows: rowsCopy}, nil
}

// mockConcurrentTable extends mockTable and implements ConcurrentTable.
type mockConcurrentTable struct {
	mockTable
	numWorkers int
}

func (t *mockConcurrentTable) NumWorkers() int {
	return t.numWorkers
}

// --- errorPartitionRowIter returns an error after the configured number of rows ---

type errorRowIter struct {
	count   int
	maxRows int
}

func (i *errorRowIter) Next(_ *Context) (Row, error) {
	if i.count >= i.maxRows {
		return nil, fmt.Errorf("synthetic partition error")
	}
	i.count++
	return Row{i.count}, nil
}

func (i *errorRowIter) Close(_ *Context) error { return nil }

// mockErrorTable returns an error from PartitionRows for a specific partition.
type mockErrorTable struct {
	mockTable
	errorPartitionID int
	rowsBeforeError  int
}

func (t *mockErrorTable) NumWorkers() int { return 4 }

func (t *mockErrorTable) PartitionRows(ctx *Context, partition Partition) (RowIter, error) {
	mp := partition.(*mockPartition)
	if mp.id == t.errorPartitionID {
		return &errorRowIter{maxRows: t.rowsBeforeError}, nil
	}
	return t.mockTable.PartitionRows(ctx, partition)
}

// --- Helper to build a test table ---

func newMockConcurrentTable(numPartitions, rowsPerPartition, numWorkers int) *mockConcurrentTable {
	partitions := make([]Partition, numPartitions)
	rowsByPartID := make(map[int][]Row)
	for p := 0; p < numPartitions; p++ {
		partitions[p] = &mockPartition{id: p}
		rows := make([]Row, rowsPerPartition)
		for r := 0; r < rowsPerPartition; r++ {
			// Row values encode partition and row index for easy verification.
			rows[r] = Row{p, r}
		}
		rowsByPartID[p] = rows
	}
	return &mockConcurrentTable{
		mockTable: mockTable{
			name:         "test_concurrent",
			partitions:   partitions,
			rowsByPartID: rowsByPartID,
		},
		numWorkers: numWorkers,
	}
}

// collectAllRows reads all rows from a RowIter until io.EOF.
func collectAllRows(ctx *Context, iter RowIter) ([]Row, error) {
	var rows []Row
	for {
		row, err := iter.Next(ctx)
		if err == io.EOF {
			return rows, nil
		}
		if err != nil {
			return rows, err
		}
		rows = append(rows, row)
	}
}

// --- Tests ---

func TestConcurrentTableRowIter_AllRowsReturned(t *testing.T) {
	ctx := NewEmptyContext()
	table := newMockConcurrentTable(5, 10, 3)

	partitions, err := table.Partitions(ctx)
	if err != nil {
		t.Fatal(err)
	}

	iter := NewConcurrentTableRowIter(ctx, table, partitions, table.NumWorkers())
	defer iter.Close(ctx)

	rows, err := collectAllRows(ctx, iter)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := 5 * 10
	if len(rows) != expected {
		t.Fatalf("expected %d rows, got %d", expected, len(rows))
	}

	// Verify every (partition, row) pair is present exactly once.
	type key struct{ p, r int }
	seen := make(map[key]bool)
	for _, row := range rows {
		k := key{row[0].(int), row[1].(int)}
		if seen[k] {
			t.Fatalf("duplicate row: partition=%d row=%d", k.p, k.r)
		}
		seen[k] = true
	}
	if len(seen) != expected {
		t.Fatalf("expected %d unique rows, got %d", expected, len(seen))
	}
}

func TestConcurrentTableRowIter_ZeroPartitions(t *testing.T) {
	ctx := NewEmptyContext()
	table := newMockConcurrentTable(0, 0, 4)

	partitions, err := table.Partitions(ctx)
	if err != nil {
		t.Fatal(err)
	}

	iter := NewConcurrentTableRowIter(ctx, table, partitions, table.NumWorkers())
	defer iter.Close(ctx)

	_, err = iter.Next(ctx)
	if err != io.EOF {
		t.Fatalf("expected io.EOF, got %v", err)
	}
}

func TestConcurrentTableRowIter_SinglePartition(t *testing.T) {
	ctx := NewEmptyContext()
	table := newMockConcurrentTable(1, 5, 4)

	partitions, err := table.Partitions(ctx)
	if err != nil {
		t.Fatal(err)
	}

	iter := NewConcurrentTableRowIter(ctx, table, partitions, table.NumWorkers())
	defer iter.Close(ctx)

	rows, err := collectAllRows(ctx, iter)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(rows) != 5 {
		t.Fatalf("expected 5 rows, got %d", len(rows))
	}

	// All rows should be from partition 0, in some order.
	sort.Slice(rows, func(i, j int) bool {
		return rows[i][1].(int) < rows[j][1].(int)
	})
	for idx, row := range rows {
		if row[0].(int) != 0 || row[1].(int) != idx {
			t.Fatalf("unexpected row at %d: %v", idx, row)
		}
	}
}

func TestConcurrentTableRowIter_WorkersCappedAtPartitions(t *testing.T) {
	ctx := NewEmptyContext()
	// numWorkers (10) > partitions (3): should still work fine.
	table := newMockConcurrentTable(3, 5, 10)

	partitions, err := table.Partitions(ctx)
	if err != nil {
		t.Fatal(err)
	}

	iter := NewConcurrentTableRowIter(ctx, table, partitions, table.NumWorkers())
	defer iter.Close(ctx)

	rows, err := collectAllRows(ctx, iter)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(rows) != 15 {
		t.Fatalf("expected 15 rows, got %d", len(rows))
	}
}

func TestConcurrentTableRowIter_SingleWorker(t *testing.T) {
	ctx := NewEmptyContext()
	table := newMockConcurrentTable(4, 3, 1)

	partitions, err := table.Partitions(ctx)
	if err != nil {
		t.Fatal(err)
	}

	iter := NewConcurrentTableRowIter(ctx, table, partitions, table.NumWorkers())
	defer iter.Close(ctx)

	rows, err := collectAllRows(ctx, iter)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(rows) != 12 {
		t.Fatalf("expected 12 rows, got %d", len(rows))
	}
}

func TestConcurrentTableRowIter_ContextCancellation(t *testing.T) {
	baseCtx := NewEmptyContext()
	cancelCtx, cancel := context.WithCancel(baseCtx.Context)
	ctx := baseCtx.WithContext(cancelCtx)

	table := newMockConcurrentTable(10, 1000, 4)

	partitions, err := table.Partitions(ctx)
	if err != nil {
		t.Fatal(err)
	}

	iter := NewConcurrentTableRowIter(ctx, table, partitions, table.NumWorkers())

	// Read a few rows then cancel.
	for i := 0; i < 5; i++ {
		_, err := iter.Next(ctx)
		if err != nil {
			t.Fatalf("unexpected error on row %d: %v", i, err)
		}
	}
	cancel()

	// Subsequent reads should eventually return an error (context.Canceled).
	var readErr error
	for {
		_, readErr = iter.Next(ctx)
		if readErr != nil {
			break
		}
	}
	if readErr != context.Canceled && readErr != io.EOF {
		t.Fatalf("expected context.Canceled or io.EOF, got %v", readErr)
	}

	// Close should not return an error.
	if err := iter.Close(ctx); err != nil {
		t.Fatalf("unexpected Close error: %v", err)
	}
}

func TestConcurrentTableRowIter_CloseMidIteration(t *testing.T) {
	ctx := NewEmptyContext()
	table := newMockConcurrentTable(10, 1000, 4)

	partitions, err := table.Partitions(ctx)
	if err != nil {
		t.Fatal(err)
	}

	iter := NewConcurrentTableRowIter(ctx, table, partitions, table.NumWorkers())

	// Read a few rows then close.
	for i := 0; i < 10; i++ {
		_, err := iter.Next(ctx)
		if err != nil {
			t.Fatalf("unexpected error on row %d: %v", i, err)
		}
	}

	if err := iter.Close(ctx); err != nil {
		t.Fatalf("unexpected Close error: %v", err)
	}

	// Double close should be safe.
	if err := iter.Close(ctx); err != nil {
		t.Fatalf("unexpected double Close error: %v", err)
	}
}

func TestConcurrentTableRowIter_ErrorPropagation(t *testing.T) {
	ctx := NewEmptyContext()

	// Build a table where partition 2 returns an error after 3 rows.
	table := &mockErrorTable{
		mockTable: mockTable{
			name: "error_table",
			partitions: []Partition{
				&mockPartition{id: 0},
				&mockPartition{id: 1},
				&mockPartition{id: 2},
			},
			rowsByPartID: map[int][]Row{
				0: {{0, 0}, {0, 1}, {0, 2}},
				1: {{1, 0}, {1, 1}, {1, 2}},
				2: {}, // will be overridden by errorPartitionID logic
			},
		},
		errorPartitionID: 2,
		rowsBeforeError:  3,
	}

	partitions, err := table.Partitions(ctx)
	if err != nil {
		t.Fatal(err)
	}

	iter := NewConcurrentTableRowIter(ctx, table, partitions, table.NumWorkers())
	defer iter.Close(ctx)

	// Read all rows — should get an error from the failing partition.
	_, err = collectAllRows(ctx, iter)
	if err == nil {
		t.Fatal("expected an error from the failing partition")
	}
	if err.Error() != "synthetic partition error" {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestConcurrentTableRowIter_MatchesSequential verifies that the concurrent iterator
// returns exactly the same set of rows as the sequential TableRowIter.
func TestConcurrentTableRowIter_MatchesSequential(t *testing.T) {
	ctx := NewEmptyContext()
	table := newMockConcurrentTable(8, 20, 4)

	// Collect rows using sequential iterator.
	seqParts, err := table.Partitions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	seqIter := NewTableRowIter(ctx, table, seqParts)
	seqRows, err := collectAllRows(ctx, seqIter)
	if err != nil {
		t.Fatalf("sequential: %v", err)
	}
	if err := seqIter.Close(ctx); err != nil {
		t.Fatal(err)
	}

	// Collect rows using concurrent iterator.
	concParts, err := table.Partitions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	concIter := NewConcurrentTableRowIter(ctx, table, concParts, table.NumWorkers())
	concRows, err := collectAllRows(ctx, concIter)
	if err != nil {
		t.Fatalf("concurrent: %v", err)
	}
	if err := concIter.Close(ctx); err != nil {
		t.Fatal(err)
	}

	if len(seqRows) != len(concRows) {
		t.Fatalf("row count mismatch: sequential=%d, concurrent=%d", len(seqRows), len(concRows))
	}

	// Sort both sets for comparison.
	sortRows := func(rows []Row) {
		sort.Slice(rows, func(i, j int) bool {
			pi, ri := rows[i][0].(int), rows[i][1].(int)
			pj, rj := rows[j][0].(int), rows[j][1].(int)
			if pi != pj {
				return pi < pj
			}
			return ri < rj
		})
	}
	sortRows(seqRows)
	sortRows(concRows)

	for idx := range seqRows {
		if seqRows[idx][0] != concRows[idx][0] || seqRows[idx][1] != concRows[idx][1] {
			t.Fatalf("mismatch at row %d: sequential=%v, concurrent=%v", idx, seqRows[idx], concRows[idx])
		}
	}
}
