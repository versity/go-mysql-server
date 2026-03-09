// Copyright 2023 Dolthub, Inc.
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

package rowexec

import (
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/dolthub/go-mysql-server/errguard"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression/function/aggregation"
	"github.com/dolthub/go-mysql-server/sql/hash"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

type groupByIter struct {
	selectedExprs []sql.Expression
	child         sql.RowIter
	ctx           *sql.Context
	buf           []sql.AggregationBuffer
	done          bool
}

func newGroupByIter(selectedExprs []sql.Expression, child sql.RowIter) *groupByIter {
	return &groupByIter{
		selectedExprs: selectedExprs,
		child:         child,
		buf:           make([]sql.AggregationBuffer, len(selectedExprs)),
	}
}

func (i *groupByIter) Next(ctx *sql.Context) (sql.Row, error) {
	if i.done {
		return nil, io.EOF
	}

	// special case for any_value
	var err error
	onlyAnyValue := true
	for j, a := range i.selectedExprs {
		i.buf[j], err = newAggregationBuffer(a)
		if err != nil {
			return nil, err
		}
		if agg, ok := a.(sql.Aggregation); ok {
			if _, ok = agg.(*aggregation.AnyValue); !ok {
				onlyAnyValue = false
			}
		}
	}

	// if no aggregate functions other than any_value, it's just a normal select
	if onlyAnyValue {
		row, err := i.child.Next(ctx)
		if err != nil {
			i.done = true
			return nil, err
		}

		if err := updateBuffers(ctx, i.buf, row); err != nil {
			return nil, err
		}
		return evalBuffers(ctx, i.buf)
	}
	i.done = true

	for {
		row, err := i.child.Next(ctx)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		if err := updateBuffers(ctx, i.buf, row); err != nil {
			return nil, err
		}
	}

	row, err := evalBuffers(ctx, i.buf)
	if err != nil {
		return nil, err
	}
	return row, nil
}

func (i *groupByIter) Close(ctx *sql.Context) error {
	i.Dispose()
	i.buf = nil
	return i.child.Close(ctx)
}

func (i *groupByIter) Dispose() {
	for _, b := range i.buf {
		if b != nil {
			b.Dispose()
		}
	}
}

type groupByGroupingIter struct {
	aggregations  sql.KeyValueCache
	child         sql.RowIter
	dispose       sql.DisposeFunc
	selectedExprs []sql.Expression
	groupByExprs  []sql.Expression
	keys          []uint64
	// buffers to reduce slice allocations
	keyRow sql.Row
	keySch sql.Schema
	pos    int
}

func newGroupByGroupingIter(
	ctx *sql.Context,
	selectedExprs, groupByExprs []sql.Expression,
	child sql.RowIter,
) *groupByGroupingIter {
	keySch := make(sql.Schema, len(groupByExprs))
	for i := range groupByExprs {
		keySch[i] = &sql.Column{Type: groupByExprs[i].Type()}
	}
	return &groupByGroupingIter{
		selectedExprs: selectedExprs,
		groupByExprs:  groupByExprs,
		child:         child,
		keyRow:        make(sql.Row, len(groupByExprs)),
		keySch:        keySch,
	}
}

func (i *groupByGroupingIter) Next(ctx *sql.Context) (sql.Row, error) {
	if i.aggregations == nil {
		i.aggregations, i.dispose = ctx.Memory.NewHistoryCache()
		if err := i.compute(ctx); err != nil {
			return nil, err
		}
	}

	if i.pos >= len(i.keys) {
		return nil, io.EOF
	}

	buffers, err := i.get(i.keys[i.pos])
	if err != nil {
		return nil, err
	}
	i.pos++

	row, err := evalBuffers(ctx, buffers)
	if err != nil {
		return nil, err
	}

	return row, nil
}

func (i *groupByGroupingIter) compute(ctx *sql.Context) error {
	eg, subCtx := ctx.NewErrgroup()

	var rowChan = make(chan sql.Row, 512)
	errguard.Go(eg, func() error {
		defer close(rowChan)
		for {
			row, err := i.child.Next(subCtx)
			if err != nil {
				if err == io.EOF {
					return nil
				}
				return err
			}
			rowChan <- row
		}
	})

	errguard.Go(eg, func() error {
		for {
			row, ok := <-rowChan
			if !ok {
				return nil
			}
			key, err := i.groupingKey(subCtx, row)
			if err != nil {
				return err
			}

			buf, err := i.get(key)
			if errors.Is(err, sql.ErrKeyNotFound) {
				buf = make([]sql.AggregationBuffer, len(i.selectedExprs))
				for j, a := range i.selectedExprs {
					buf[j], err = newAggregationBuffer(a)
					if err != nil {
						return err
					}
				}
				if err = i.aggregations.Put(key, buf); err != nil {
					return err
				}
				i.keys = append(i.keys, key)
			} else if err != nil {
				return err
			}
			err = updateBuffers(subCtx, buf, row)
			if err != nil {
				return err
			}
		}
	})

	err := eg.Wait()
	if err != nil {
		return err
	}

	return nil
}

func (i *groupByGroupingIter) get(key uint64) ([]sql.AggregationBuffer, error) {
	v, err := i.aggregations.Get(key)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, nil
	}
	return v.([]sql.AggregationBuffer), nil
}

func (i *groupByGroupingIter) put(key uint64, val []sql.AggregationBuffer) error {
	return i.aggregations.Put(key, val)
}

func (i *groupByGroupingIter) Close(ctx *sql.Context) error {
	i.Dispose()
	i.aggregations = nil
	if i.dispose != nil {
		i.dispose()
		i.dispose = nil
	}

	return i.child.Close(ctx)
}

func (i *groupByGroupingIter) Dispose() {
	for _, k := range i.keys {
		bs, _ := i.get(k)
		if bs != nil {
			for _, b := range bs {
				b.Dispose()
			}
		}
	}
}

func (i *groupByGroupingIter) groupingKey(ctx *sql.Context, row sql.Row) (uint64, error) {
	for idx, expr := range i.groupByExprs {
		v, err := expr.Eval(ctx, row)
		if err != nil {
			return 0, err
		}

		// TODO: this should be moved into hash.HashOf
		typ := expr.Type()
		if extTyp, isExtTyp := typ.(sql.ExtendedType); isExtTyp {
			val, vErr := extTyp.SerializeValue(ctx, v)
			if vErr != nil {
				return 0, vErr
			}
			v = string(val)
		}

		i.keyRow[idx] = v
	}
	return hash.HashOf(ctx, i.keySch, i.keyRow)
}

func newAggregationBuffer(expr sql.Expression) (sql.AggregationBuffer, error) {
	switch n := expr.(type) {
	case sql.Aggregation:
		return n.NewBuffer()
	default:
		// The semantics for a non-aggregation expression in a group by node is First.
		// When ONLY_FULL_GROUP_BY is enabled, this is an error, but it's allowed otherwise.
		return aggregation.NewFirst(expr).NewBuffer()
	}
}

func updateBuffers(
	ctx *sql.Context,
	buffers []sql.AggregationBuffer,
	row sql.Row,
) error {
	for _, b := range buffers {
		if err := b.Update(ctx, row); err != nil {
			return err
		}
	}

	return nil
}

func evalBuffers(
	ctx *sql.Context,
	buffers []sql.AggregationBuffer,
) (sql.Row, error) {
	var row = make(sql.Row, len(buffers))

	var err error
	for i, b := range buffers {
		row[i], err = b.Eval(ctx)
		if err != nil {
			return nil, err
		}
	}

	return row, nil
}

// buildPartitionAggregation builds the row iterator for PartitionAggregation nodes.
// It reads pre-aggregated results from each partition and merges them.
func (b *BaseBuilder) buildPartitionAggregation(ctx *sql.Context, n *plan.PartitionAggregation, row sql.Row) (sql.RowIter, error) {
	span, ctx := ctx.Span("plan.PartitionAggregation")
	defer span.End()

	childIter, err := b.buildNodeExec(ctx, n.Child, row)
	if err != nil {
		return nil, err
	}

	return &partitionAggregationIter{
		child: childIter,
		aggs:  n.Aggregations,
	}, nil
}

// partitionAggregationIter merges partition-level aggregation results.
type partitionAggregationIter struct {
	child sql.RowIter
	aggs  []sql.ColumnAggregation
	done  bool
}

func (i *partitionAggregationIter) Next(ctx *sql.Context) (sql.Row, error) {
	if i.done {
		return nil, io.EOF
	}
	i.done = true

	// Initialize merged results - SUM will be initialized on first value to preserve type
	merged := make([]interface{}, len(i.aggs))
	for j, agg := range i.aggs {
		switch agg.Type {
		case sql.AggregationTypeCount:
			merged[j] = int64(0)
		case sql.AggregationTypeSum:
			// Leave nil - will be initialized with correct type on first value
			merged[j] = nil
		case sql.AggregationTypeMin, sql.AggregationTypeMax:
			merged[j] = nil
		}
	}

	// Read all partition results and merge
	for {
		row, err := i.child.Next(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}

		// Merge each aggregation value
		for j, agg := range i.aggs {
			if j >= len(row) {
				continue
			}

			val := row[j]
			if val == nil {
				continue
			}

			switch agg.Type {
			case sql.AggregationTypeCount:
				// Sum up counts
				if count, ok := aggToInt64(val); ok {
					merged[j] = merged[j].(int64) + count
				}

			case sql.AggregationTypeSum:
				// Sum up sums - preserve int64 or float64 based on partition value type
				if merged[j] == nil {
					// First value - initialize with same type
					switch v := val.(type) {
					case int64:
						merged[j] = v
					case float64:
						merged[j] = v
					default:
						// Fallback to float64 for other numeric types
						if f, ok := aggToFloat64(val); ok {
							merged[j] = f
						}
					}
				} else {
					// Subsequent values - accumulate with matching type
					switch current := merged[j].(type) {
					case int64:
						if v, ok := val.(int64); ok {
							merged[j] = current + v
						}
					case float64:
						if f, ok := aggToFloat64(val); ok {
							merged[j] = current + f
						}
					}
				}

			case sql.AggregationTypeMin:
				// Take minimum
				if merged[j] == nil {
					merged[j] = val
				} else if aggCompareValues(val, merged[j]) < 0 {
					merged[j] = val
				}

			case sql.AggregationTypeMax:
				// Take maximum
				if merged[j] == nil {
					merged[j] = val
				} else if aggCompareValues(val, merged[j]) > 0 {
					merged[j] = val
				}
			}
		}
	}

	return sql.Row(merged), nil
}

func (i *partitionAggregationIter) Close(ctx *sql.Context) error {
	return i.child.Close(ctx)
}

// aggToInt64 converts a value to int64 if possible.
func aggToInt64(val interface{}) (int64, bool) {
	switch v := val.(type) {
	case int:
		return int64(v), true
	case int8:
		return int64(v), true
	case int16:
		return int64(v), true
	case int32:
		return int64(v), true
	case int64:
		return v, true
	case uint:
		return int64(v), true
	case uint8:
		return int64(v), true
	case uint16:
		return int64(v), true
	case uint32:
		return int64(v), true
	case uint64:
		return int64(v), true
	case float32:
		return int64(v), true
	case float64:
		return int64(v), true
	default:
		return 0, false
	}
}

// aggToFloat64 converts a value to float64 if possible.
func aggToFloat64(val interface{}) (float64, bool) {
	switch v := val.(type) {
	case int:
		return float64(v), true
	case int8:
		return float64(v), true
	case int16:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case uint:
		return float64(v), true
	case uint8:
		return float64(v), true
	case uint16:
		return float64(v), true
	case uint32:
		return float64(v), true
	case uint64:
		return float64(v), true
	case float32:
		return float64(v), true
	case float64:
		return v, true
	default:
		return 0, false
	}
}

// aggCompareValues compares two values, returning -1, 0, or 1.
func aggCompareValues(a, b interface{}) int {
	// Handle numeric comparisons
	if af, aok := aggToFloat64(a); aok {
		if bf, bok := aggToFloat64(b); bok {
			if af < bf {
				return -1
			} else if af > bf {
				return 1
			}
			return 0
		}
	}

	// Handle string comparisons
	if as, ok := a.(string); ok {
		if bs, ok := b.(string); ok {
			if as < bs {
				return -1
			} else if as > bs {
				return 1
			}
			return 0
		}
	}

	// Fallback: treat as equal if we can't compare
	return 0
}

// buildPartitionGroupedAggregation builds the row iterator for PartitionGroupedAggregation nodes.
// It reads pre-aggregated grouped results from each partition and merges them by group key.
func (b *BaseBuilder) buildPartitionGroupedAggregation(ctx *sql.Context, n *plan.PartitionGroupedAggregation, row sql.Row) (sql.RowIter, error) {
	span, ctx := ctx.Span("plan.PartitionGroupedAggregation")
	defer span.End()

	childIter, err := b.buildNodeExec(ctx, n.Child, row)
	if err != nil {
		return nil, err
	}

	return &partitionGroupedAggregationIter{
		child:       childIter,
		aggs:        n.Aggregations,
		groupByCols: n.GroupByCols,
		numGroupBy:  len(n.GroupByCols),
	}, nil
}

// partitionGroupedAggregationIter merges partition-level grouped aggregation results.
// Each partition returns rows of: [groupByCol1, ..., groupByColN, agg1, agg2, ...]
// where AVG aggregations occupy two columns (partialSum, partialCount).
type partitionGroupedAggregationIter struct {
	child       sql.RowIter
	aggs        []sql.ColumnAggregation
	groupByCols []sql.GroupByColumn
	numGroupBy  int
	computed    bool
	results     []sql.Row
	pos         int
}

// groupKeyString builds a string key from the group-by column values of a partition row.
func (i *partitionGroupedAggregationIter) groupKeyString(row sql.Row) string {
	if i.numGroupBy == 1 {
		return fmt.Sprintf("%v", row[0])
	}
	parts := make([]string, i.numGroupBy)
	for k := 0; k < i.numGroupBy; k++ {
		parts[k] = fmt.Sprintf("%v", row[k])
	}
	return strings.Join(parts, "\x00")
}

func (i *partitionGroupedAggregationIter) Next(ctx *sql.Context) (sql.Row, error) {
	if !i.computed {
		if err := i.computeAll(ctx); err != nil {
			return nil, err
		}
		i.computed = true
	}

	if i.pos >= len(i.results) {
		return nil, io.EOF
	}
	row := i.results[i.pos]
	i.pos++
	return row, nil
}

// aggSlotCount returns the number of slots an aggregation occupies in the partition row.
// AVG takes 2 slots (partialSum, partialCount); everything else takes 1.
func aggSlotCount(agg sql.ColumnAggregation) int {
	if agg.Type == sql.AggregationTypeAvg {
		return 2
	}
	return 1
}

// computeAll reads all partition rows and merges by group key.
func (i *partitionGroupedAggregationIter) computeAll(ctx *sql.Context) error {
	// Calculate expected number of agg slots per partition row
	aggSlots := 0
	for _, agg := range i.aggs {
		aggSlots += aggSlotCount(agg)
	}

	// Ordered list of group keys to preserve insertion order
	var groupOrder []string

	// Map from group key → merged agg values (same layout as partition agg slots)
	type mergedGroup struct {
		groupKey sql.Row
		aggVals  []interface{}
	}
	groups := make(map[string]*mergedGroup)

	for {
		row, err := i.child.Next(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}

		key := i.groupKeyString(row)

		mg, exists := groups[key]
		if !exists {
			// Initialize new group
			groupKeyCopy := make(sql.Row, i.numGroupBy)
			copy(groupKeyCopy, row[:i.numGroupBy])

			vals := make([]interface{}, aggSlots)
			slotIdx := 0
			for _, agg := range i.aggs {
				switch agg.Type {
				case sql.AggregationTypeCount:
					vals[slotIdx] = int64(0)
				case sql.AggregationTypeSum:
					vals[slotIdx] = nil
				case sql.AggregationTypeAvg:
					vals[slotIdx] = float64(0) // partial sum
					vals[slotIdx+1] = int64(0) // partial count
				case sql.AggregationTypeMin, sql.AggregationTypeMax:
					vals[slotIdx] = nil
				}
				slotIdx += aggSlotCount(agg)
			}

			mg = &mergedGroup{groupKey: groupKeyCopy, aggVals: vals}
			groups[key] = mg
			groupOrder = append(groupOrder, key)
		}

		// Merge aggregation values from this partition row
		partSlotIdx := i.numGroupBy // start after group-by columns in the row
		mergeSlotIdx := 0
		for _, agg := range i.aggs {
			switch agg.Type {
			case sql.AggregationTypeCount:
				val := row[partSlotIdx]
				if val != nil {
					if count, ok := aggToInt64(val); ok {
						mg.aggVals[mergeSlotIdx] = mg.aggVals[mergeSlotIdx].(int64) + count
					}
				}

			case sql.AggregationTypeSum:
				val := row[partSlotIdx]
				if val != nil {
					if mg.aggVals[mergeSlotIdx] == nil {
						switch v := val.(type) {
						case int64:
							mg.aggVals[mergeSlotIdx] = v
						case float64:
							mg.aggVals[mergeSlotIdx] = v
						default:
							if f, ok := aggToFloat64(val); ok {
								mg.aggVals[mergeSlotIdx] = f
							}
						}
					} else {
						switch current := mg.aggVals[mergeSlotIdx].(type) {
						case int64:
							if v, ok := val.(int64); ok {
								mg.aggVals[mergeSlotIdx] = current + v
							}
						case float64:
							if f, ok := aggToFloat64(val); ok {
								mg.aggVals[mergeSlotIdx] = current + f
							}
						}
					}
				}

			case sql.AggregationTypeAvg:
				// Partition returns (partialSum, partialCount) in two slots
				partialSum := row[partSlotIdx]
				partialCount := row[partSlotIdx+1]
				if partialSum != nil && partialCount != nil {
					if ps, ok := aggToFloat64(partialSum); ok {
						mg.aggVals[mergeSlotIdx] = mg.aggVals[mergeSlotIdx].(float64) + ps
					}
					if pc, ok := aggToInt64(partialCount); ok {
						mg.aggVals[mergeSlotIdx+1] = mg.aggVals[mergeSlotIdx+1].(int64) + pc
					}
				}

			case sql.AggregationTypeMin:
				val := row[partSlotIdx]
				if val != nil {
					if mg.aggVals[mergeSlotIdx] == nil {
						mg.aggVals[mergeSlotIdx] = val
					} else if aggCompareValues(val, mg.aggVals[mergeSlotIdx]) < 0 {
						mg.aggVals[mergeSlotIdx] = val
					}
				}

			case sql.AggregationTypeMax:
				val := row[partSlotIdx]
				if val != nil {
					if mg.aggVals[mergeSlotIdx] == nil {
						mg.aggVals[mergeSlotIdx] = val
					} else if aggCompareValues(val, mg.aggVals[mergeSlotIdx]) > 0 {
						mg.aggVals[mergeSlotIdx] = val
					}
				}
			}

			partSlotIdx += aggSlotCount(agg)
			mergeSlotIdx += aggSlotCount(agg)
		}
	}

	// Build result rows: [groupByCol1, ..., groupByColN, finalAgg1, finalAgg2, ...]
	// For AVG, compute finalSum / finalCount as a single output value.
	i.results = make([]sql.Row, 0, len(groupOrder))
	for _, key := range groupOrder {
		mg := groups[key]
		// Output row: group-by columns + one value per aggregation
		outRow := make(sql.Row, i.numGroupBy+len(i.aggs))
		copy(outRow, mg.groupKey)

		mergeSlotIdx := 0
		for aggIdx, agg := range i.aggs {
			outIdx := i.numGroupBy + aggIdx
			switch agg.Type {
			case sql.AggregationTypeAvg:
				sum := mg.aggVals[mergeSlotIdx].(float64)
				count := mg.aggVals[mergeSlotIdx+1].(int64)
				if count > 0 {
					outRow[outIdx] = sum / float64(count)
				} else {
					outRow[outIdx] = nil
				}
			default:
				outRow[outIdx] = mg.aggVals[mergeSlotIdx]
			}
			mergeSlotIdx += aggSlotCount(agg)
		}
		i.results = append(i.results, outRow)
	}

	return nil
}

func (i *partitionGroupedAggregationIter) Close(ctx *sql.Context) error {
	return i.child.Close(ctx)
}
