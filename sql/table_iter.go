// Copyright 2020-2021 Dolthub, Inc.
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
	"io"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/dolthub/go-mysql-server/errguard"
)

// TableRowIter is an iterator over the partitions in a table.
type TableRowIter struct {
	table      Table
	partitions PartitionIter
	partition  Partition
	rows       RowIter
	valueRows  ValueRowIter
}

var _ RowIter = (*TableRowIter)(nil)

// NewTableRowIter returns a new iterator over the rows in the partitions of the table given.
func NewTableRowIter(ctx *Context, table Table, partitions PartitionIter) *TableRowIter {
	return &TableRowIter{table: table, partitions: partitions}
}

func (i *TableRowIter) Next(ctx *Context) (Row, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	if i.partition == nil {
		partition, err := i.partitions.Next(ctx)
		if err != nil {
			if err == io.EOF {
				if e := i.partitions.Close(ctx); e != nil {
					return nil, e
				}
			}

			return nil, err
		}

		i.partition = partition
	}

	if i.rows == nil {
		rows, err := i.table.PartitionRows(ctx, i.partition)
		if err != nil {
			return nil, err
		}

		i.rows = rows
	}

	row, err := i.rows.Next(ctx)
	if err != nil && err == io.EOF {
		if err = i.rows.Close(ctx); err != nil {
			return nil, err
		}

		i.partition = nil
		i.rows = nil
		row, err = i.Next(ctx)
	}
	return row, err
}

// NextValueRow implements the sql.ValueRowIter interface
func (i *TableRowIter) NextValueRow(ctx *Context) (ValueRow, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	if i.partition == nil {
		partition, err := i.partitions.Next(ctx)
		if err != nil {
			if err == io.EOF {
				if e := i.partitions.Close(ctx); e != nil {
					return nil, e
				}
			}
			return nil, err
		}
		i.partition = partition
	}

	if i.valueRows == nil {
		rows, err := i.table.PartitionRows(ctx, i.partition)
		if err != nil {
			return nil, err
		}
		i.valueRows = rows.(ValueRowIter)
	}

	row, err := i.valueRows.NextValueRow(ctx)
	if err != nil && err == io.EOF {
		if err = i.valueRows.Close(ctx); err != nil {
			return nil, err
		}
		i.partition = nil
		i.valueRows = nil
		row, err = i.NextValueRow(ctx)
	}
	return row, err
}

// IsValueRowIter implements the sql.ValueRowIter interface.
func (i *TableRowIter) IsValueRowIter(ctx *Context) bool {
	if i.partition == nil {
		partition, err := i.partitions.Next(ctx)
		if err != nil {
			return false
		}
		i.partition = partition
	}
	if i.valueRows == nil {
		rows, err := i.table.PartitionRows(ctx, i.partition)
		if err != nil {
			return false
		}
		valRowIter, ok := rows.(ValueRowIter)
		if !ok {
			return false
		}
		i.valueRows = valRowIter
	}
	return i.valueRows.IsValueRowIter(ctx)
}

func (i *TableRowIter) Close(ctx *Context) error {
	if i.rows != nil {
		if err := i.rows.Close(ctx); err != nil {
			_ = i.partitions.Close(ctx)
			return err
		}
	}
	if i.valueRows != nil {
		if err := i.valueRows.Close(ctx); err != nil {
			_ = i.partitions.Close(ctx)
			return err
		}
	}
	return i.partitions.Close(ctx)
}

// ConcurrentTableRowIter is an iterator that reads rows from multiple table partitions concurrently
// using a pool of worker goroutines. Rows from different partitions may be interleaved in non-deterministic
// order. Workers are started lazily on the first call to Next.
type ConcurrentTableRowIter struct {
	table      Table
	partitions PartitionIter
	numWorkers int

	// started is true once the worker goroutines have been launched.
	started bool
	// rowChan receives rows produced by worker goroutines.
	rowChan chan Row
	// eg coordinates worker goroutines; its Wait closes rowChan.
	eg *errgroup.Group
	// cancel cancels the derived context shared by workers.
	cancel context.CancelFunc
	// closeMu serializes calls to Close.
	closeMu sync.Mutex
	// closed prevents double-close.
	closed bool
}

var _ RowIter = (*ConcurrentTableRowIter)(nil)

// NewConcurrentTableRowIter returns an iterator that reads rows from the given table's
// partitions concurrently. numWorkers controls the maximum number of goroutines that will
// call PartitionRows simultaneously. The actual worker count is capped at the number of
// partitions available.
func NewConcurrentTableRowIter(ctx *Context, table Table, partitions PartitionIter, numWorkers int) *ConcurrentTableRowIter {
	if numWorkers < 1 {
		numWorkers = 1
	}
	ctx.GetLogger().Debugf("ConcurrentTableRowIter: creating for table %s with %d workers", table.Name(), numWorkers)
	return &ConcurrentTableRowIter{
		table:      table,
		partitions: partitions,
		numWorkers: numWorkers,
	}
}

// start drains all partitions, creates workers, and begins reading rows concurrently.
func (i *ConcurrentTableRowIter) start(ctx *Context) error {
	ctx.GetLogger().Debugf("ConcurrentTableRowIter: starting for table %s", i.table.Name())
	// Drain all partitions into a slice.
	var parts []Partition
	for {
		p, err := i.partitions.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			ctx.GetLogger().Debugf("ConcurrentTableRowIter: error draining partitions for table %s: %v", i.table.Name(), err)
			_ = i.partitions.Close(ctx)
			return err
		}
		parts = append(parts, p)
	}
	if err := i.partitions.Close(ctx); err != nil {
		ctx.GetLogger().Debugf("ConcurrentTableRowIter: error closing partition iter for table %s: %v", i.table.Name(), err)
		return err
	}

	ctx.GetLogger().Debugf("ConcurrentTableRowIter: drained %d partitions for table %s", len(parts), i.table.Name())

	if len(parts) == 0 {
		i.started = true
		// Make a closed channel so Next returns io.EOF immediately.
		ch := make(chan Row)
		close(ch)
		i.rowChan = ch
		ctx.GetLogger().Debugf("ConcurrentTableRowIter: zero partitions for table %s, returning immediately", i.table.Name())
		return nil
	}

	// Cap workers at partition count.
	workers := i.numWorkers
	if workers > len(parts) {
		workers = len(parts)
	}

	eg, egCtx := ctx.NewErrgroup()
	i.eg = eg
	i.cancel = func() { egCtx.Done() } // placeholder; real cancel comes from errgroup's WithContext

	// To get a proper cancel func we create a cancellable context wrapping the errgroup context.
	cancelCtx, cancel := context.WithCancel(egCtx.Context)
	workerCtx := egCtx.WithContext(cancelCtx)
	i.cancel = cancel

	// Buffered work channel: workers pull partitions from here.
	workChan := make(chan Partition, len(parts))
	for _, p := range parts {
		workChan <- p
	}
	close(workChan)

	// Buffered row channel.
	i.rowChan = make(chan Row, 512)

	// Launch workers.
	ctx.GetLogger().Debugf("ConcurrentTableRowIter: launching %d workers for table %s", workers, i.table.Name())
	for w := 0; w < workers; w++ {
		workerID := w
		errguard.Go(eg, func() error {
			workerCtx.GetLogger().Debugf("ConcurrentTableRowIter: worker %d started for table %s", workerID, i.table.Name())
			for part := range workChan {
				workerCtx.GetLogger().Debugf("ConcurrentTableRowIter: worker %d reading partition %v for table %s", workerID, part, i.table.Name())
				rowIter, err := i.table.PartitionRows(workerCtx, part)
				if err != nil {
					workerCtx.GetLogger().Debugf("ConcurrentTableRowIter: worker %d error opening partition %v: %v", workerID, part, err)
					return err
				}
				for {
					select {
					case <-cancelCtx.Done():
						_ = rowIter.Close(workerCtx)
						return cancelCtx.Err()
					default:
					}

					row, err := rowIter.Next(workerCtx)
					if err == io.EOF {
						if cerr := rowIter.Close(workerCtx); cerr != nil {
							return cerr
						}
						break
					}
					if err != nil {
						workerCtx.GetLogger().Debugf("ConcurrentTableRowIter: worker %d error reading row from partition %v: %v", workerID, part, err)
						_ = rowIter.Close(workerCtx)
						return err
					}

					select {
					case i.rowChan <- row:
					case <-cancelCtx.Done():
						workerCtx.GetLogger().Debugf("ConcurrentTableRowIter: worker %d context cancelled for table %s", workerID, i.table.Name())
						_ = rowIter.Close(workerCtx)
						return cancelCtx.Err()
					}
				}
			}
			workerCtx.GetLogger().Debugf("ConcurrentTableRowIter: worker %d finished for table %s", workerID, i.table.Name())
			return nil
		})
	}

	// Closer goroutine: waits for all workers then closes rowChan.
	go func() {
		_ = eg.Wait()
		close(i.rowChan)
	}()

	i.started = true
	return nil
}

func (i *ConcurrentTableRowIter) Next(ctx *Context) (Row, error) {
	if !i.started {
		if err := i.start(ctx); err != nil {
			return nil, err
		}
	}

	select {
	case <-ctx.Done():
		ctx.GetLogger().Debugf("ConcurrentTableRowIter: Next context done for table %s: %v", i.table.Name(), ctx.Err())
		return nil, ctx.Err()
	case row, ok := <-i.rowChan:
		if !ok {
			// Channel closed — either all rows consumed or an error occurred.
			if i.eg != nil {
				if err := i.eg.Wait(); err != nil {
					ctx.GetLogger().Debugf("ConcurrentTableRowIter: workers returned error for table %s: %v", i.table.Name(), err)
					return nil, err
				}
			}
			ctx.GetLogger().Debugf("ConcurrentTableRowIter: all rows consumed for table %s", i.table.Name())
			return nil, io.EOF
		}
		return row, nil
	}
}

func (i *ConcurrentTableRowIter) Close(ctx *Context) error {
	ctx.GetLogger().Debugf("ConcurrentTableRowIter: closing for table %s", i.table.Name())
	i.closeMu.Lock()
	defer i.closeMu.Unlock()
	if i.closed {
		ctx.GetLogger().Debugf("ConcurrentTableRowIter: already closed for table %s", i.table.Name())
		return nil
	}
	i.closed = true

	// Cancel workers.
	if i.cancel != nil {
		i.cancel()
	}

	// Drain the row channel to unblock any workers stuck on send.
	if i.rowChan != nil {
		for range i.rowChan {
		}
	}

	// Wait for all workers to finish.
	if i.eg != nil {
		// Ignore context.Canceled since we just cancelled it.
		if err := i.eg.Wait(); err != nil && err != context.Canceled {
			ctx.GetLogger().Debugf("ConcurrentTableRowIter: close error for table %s: %v", i.table.Name(), err)
			return err
		}
	}

	ctx.GetLogger().Debugf("ConcurrentTableRowIter: closed successfully for table %s", i.table.Name())
	return nil
}
