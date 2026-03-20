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

package plan

import (
	"fmt"
	"strings"

	"github.com/dolthub/vitess/go/sqltypes"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// PartitionAggregation is a plan node that represents aggregations pushed down
// to the table partition level. It reads pre-aggregated results from each partition
// and merges them to produce the final result.
type PartitionAggregation struct {
	UnaryNode
	Aggregations []sql.ColumnAggregation
	Aliases      []string
	OriginalAggs []sql.Expression
}

var _ sql.Node = (*PartitionAggregation)(nil)
var _ sql.CollationCoercible = (*PartitionAggregation)(nil)

// NewPartitionAggregation creates a new PartitionAggregation node.
func NewPartitionAggregation(child *ResolvedTable, aggs []sql.ColumnAggregation, aliases []string, originalAggs []sql.Expression) *PartitionAggregation {
	return &PartitionAggregation{
		UnaryNode:    UnaryNode{Child: child},
		Aggregations: aggs,
		Aliases:      aliases,
		OriginalAggs: originalAggs,
	}
}

// Resolved implements sql.Node.
func (p *PartitionAggregation) Resolved() bool {
	return p.Child.Resolved()
}

// IsReadOnly implements sql.Node.
func (p *PartitionAggregation) IsReadOnly() bool {
	return true
}

// Schema implements sql.Node.
func (p *PartitionAggregation) Schema() sql.Schema {
	schema := make(sql.Schema, len(p.Aggregations))
	for i, agg := range p.Aggregations {
		name := p.Aliases[i]
		typ := p.typeForAggregation(agg)
		schema[i] = &sql.Column{
			Name:     name,
			Type:     typ,
			Nullable: true,
		}
	}
	return schema
}

// typeForAggregation returns the appropriate type for an aggregation result.
func (p *PartitionAggregation) typeForAggregation(agg sql.ColumnAggregation) sql.Type {
	switch agg.Type {
	case sql.AggregationTypeCount:
		return types.Int64
	case sql.AggregationTypeSum:
		// For SUM, examine the original expression to determine return type
		// Integer types return int64, float types return float64
		if rt, ok := p.Child.(*ResolvedTable); ok {
			for _, col := range rt.Schema() {
				if col.Name == agg.ColumnName {
					if isIntegerSQLType(col.Type) {
						// Currently dolt uses float64 for sum
						// TODO: update this to return int64 once sum returns int64 for integer types
						return types.Float64
					}
					return types.Float64
				}
			}
		}
		// Check OriginalAggs for type info
		for i, origAgg := range p.OriginalAggs {
			if i < len(p.Aggregations) && p.Aggregations[i].Type == sql.AggregationTypeSum {
				if isIntegerSQLType(origAgg.Type()) {
					// Currently dolt uses float64 for sum
					// TODO: update this to return int64 once sum returns int64 for integer types
					return types.Float64
				}
				return types.Float64
			}
		}
		return types.Float64
	case sql.AggregationTypeMin, sql.AggregationTypeMax:
		// For MIN/MAX, try to get column type from child table
		if rt, ok := p.Child.(*ResolvedTable); ok {
			for _, col := range rt.Schema() {
				if col.Name == agg.ColumnName {
					return col.Type
				}
			}
		}
		// Fallback to the original expression type
		if agg.ColumnIndex >= 0 && agg.ColumnIndex < len(p.OriginalAggs) {
			return p.OriginalAggs[agg.ColumnIndex].Type()
		}
		return types.Float64
	default:
		return types.Float64
	}
}

// isIntegerSQLType checks if a SQL type is an integer type.
func isIntegerSQLType(t sql.Type) bool {
	switch t.Type() {
	case
		sqltypes.Int8, sqltypes.Int16, sqltypes.Int24, sqltypes.Int32, sqltypes.Int64,
		sqltypes.Uint8, sqltypes.Uint16, sqltypes.Uint24, sqltypes.Uint32, sqltypes.Uint64:
		return true
	default:
		return false
	}
}

// WithChildren implements sql.Node.
func (p *PartitionAggregation) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(children), 1)
	}
	// Accept any node type as child to support transformations that wrap the table
	return &PartitionAggregation{
		UnaryNode:    UnaryNode{Child: children[0]},
		Aggregations: p.Aggregations,
		Aliases:      p.Aliases,
		OriginalAggs: p.OriginalAggs,
	}, nil
}

// CollationCoercibility implements sql.CollationCoercible.
func (p *PartitionAggregation) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.GetCoercibility(ctx, p.Child)
}

// String implements sql.Node.
func (p *PartitionAggregation) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("PartitionAggregation")

	var aggStrs []string
	for i, agg := range p.Aggregations {
		aggStrs = append(aggStrs, fmt.Sprintf("%s(%s) AS %s",
			aggregationTypeName(agg.Type),
			agg.ColumnName,
			p.Aliases[i]))
	}

	_ = pr.WriteChildren(
		fmt.Sprintf("Aggregations(%s)", strings.Join(aggStrs, ", ")),
		p.Child.String(),
	)
	return pr.String()
}

// DebugString implements sql.Node.
func (p *PartitionAggregation) DebugString() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("PartitionAggregation")

	var aggStrs []string
	for i, agg := range p.Aggregations {
		aggStrs = append(aggStrs, fmt.Sprintf("%s(col[%d]:%s) AS %s",
			aggregationTypeName(agg.Type),
			agg.ColumnIndex,
			agg.ColumnName,
			p.Aliases[i]))
	}

	_ = pr.WriteChildren(
		fmt.Sprintf("Aggregations(%s)", strings.Join(aggStrs, ", ")),
		sql.DebugString(p.Child),
	)
	return pr.String()
}

// aggregationTypeName returns the string name for an aggregation type.
func aggregationTypeName(t sql.AggregationType) string {
	switch t {
	case sql.AggregationTypeCount:
		return "COUNT"
	case sql.AggregationTypeSum:
		return "SUM"
	case sql.AggregationTypeMin:
		return "MIN"
	case sql.AggregationTypeMax:
		return "MAX"
	case sql.AggregationTypeAvg:
		return "AVG"
	default:
		return "UNKNOWN"
	}
}

// PartitionGroupedAggregation is a plan node that represents grouped aggregations
// pushed down to the table partition level. Each partition returns one row per group
// with the group-by key columns followed by aggregation results. This node merges
// partial results across partitions.
type PartitionGroupedAggregation struct {
	UnaryNode
	Aggregations []sql.ColumnAggregation
	GroupByCols  []sql.GroupByColumn
	Aliases      []string
	OriginalAggs []sql.Expression
	GroupByExprs []sql.Expression
}

var _ sql.Node = (*PartitionGroupedAggregation)(nil)
var _ sql.CollationCoercible = (*PartitionGroupedAggregation)(nil)

// NewPartitionGroupedAggregation creates a new PartitionGroupedAggregation node.
func NewPartitionGroupedAggregation(
	child *ResolvedTable,
	aggs []sql.ColumnAggregation,
	groupByCols []sql.GroupByColumn,
	aliases []string,
	originalAggs []sql.Expression,
	groupByExprs []sql.Expression,
) *PartitionGroupedAggregation {
	return &PartitionGroupedAggregation{
		UnaryNode:    UnaryNode{Child: child},
		Aggregations: aggs,
		GroupByCols:  groupByCols,
		Aliases:      aliases,
		OriginalAggs: originalAggs,
		GroupByExprs: groupByExprs,
	}
}

// Resolved implements sql.Node.
func (p *PartitionGroupedAggregation) Resolved() bool {
	return p.Child.Resolved()
}

// IsReadOnly implements sql.Node.
func (p *PartitionGroupedAggregation) IsReadOnly() bool {
	return true
}

// partitionRowWidth returns the number of columns each partition row will have.
// The layout is: [groupByCol1, groupByCol2, ..., agg1, agg2, ...].
// For AVG aggregations, the partition returns two columns (partialSum, partialCount).
func (p *PartitionGroupedAggregation) partitionRowWidth() int {
	width := len(p.GroupByCols)
	for _, agg := range p.Aggregations {
		if agg.Type == sql.AggregationTypeAvg {
			width += 2 // partialSum + partialCount
		} else {
			width += 1
		}
	}
	return width
}

// Schema implements sql.Node.
// The output schema is the aliases in order. The first entries correspond to
// GROUP BY columns, followed by aggregation result columns.
func (p *PartitionGroupedAggregation) Schema() sql.Schema {
	schema := make(sql.Schema, len(p.Aliases))
	for i, alias := range p.Aliases {
		schema[i] = &sql.Column{
			Name:     alias,
			Type:     p.typeForOutputColumn(i),
			Nullable: true,
		}
	}
	return schema
}

// typeForOutputColumn determines the type for an output column at index i.
func (p *PartitionGroupedAggregation) typeForOutputColumn(i int) sql.Type {
	numGroupBy := len(p.GroupByCols)
	if i < numGroupBy {
		// Group-by column: get type from child table schema
		if rt, ok := p.Child.(*ResolvedTable); ok {
			colName := p.GroupByCols[i].ColumnName
			for _, col := range rt.Schema() {
				if col.Name == colName {
					return col.Type
				}
			}
		}
		return types.Int64 // fallback
	}

	// Aggregation column
	aggIdx := i - numGroupBy
	if aggIdx >= len(p.Aggregations) {
		return types.Float64
	}
	agg := p.Aggregations[aggIdx]
	switch agg.Type {
	case sql.AggregationTypeCount:
		return types.Int64
	case sql.AggregationTypeSum:
		return types.Float64
	case sql.AggregationTypeAvg:
		return types.Float64
	case sql.AggregationTypeMin, sql.AggregationTypeMax:
		if rt, ok := p.Child.(*ResolvedTable); ok {
			for _, col := range rt.Schema() {
				if col.Name == agg.ColumnName {
					return col.Type
				}
			}
		}
		return types.Float64
	default:
		return types.Float64
	}
}

// WithChildren implements sql.Node.
func (p *PartitionGroupedAggregation) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(children), 1)
	}
	return &PartitionGroupedAggregation{
		UnaryNode:    UnaryNode{Child: children[0]},
		Aggregations: p.Aggregations,
		GroupByCols:  p.GroupByCols,
		Aliases:      p.Aliases,
		OriginalAggs: p.OriginalAggs,
		GroupByExprs: p.GroupByExprs,
	}, nil
}

// CollationCoercibility implements sql.CollationCoercible.
func (p *PartitionGroupedAggregation) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.GetCoercibility(ctx, p.Child)
}

// String implements sql.Node.
func (p *PartitionGroupedAggregation) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("PartitionGroupedAggregation")

	var groupStrs []string
	for _, gc := range p.GroupByCols {
		groupStrs = append(groupStrs, gc.ColumnName)
	}

	var aggStrs []string
	for i, agg := range p.Aggregations {
		aliasIdx := len(p.GroupByCols) + i
		alias := ""
		if aliasIdx < len(p.Aliases) {
			alias = p.Aliases[aliasIdx]
		}
		aggStrs = append(aggStrs, fmt.Sprintf("%s(%s) AS %s",
			aggregationTypeName(agg.Type),
			agg.ColumnName,
			alias))
	}

	_ = pr.WriteChildren(
		fmt.Sprintf("GroupBy(%s)", strings.Join(groupStrs, ", ")),
		fmt.Sprintf("Aggregations(%s)", strings.Join(aggStrs, ", ")),
		p.Child.String(),
	)
	return pr.String()
}

// DebugString implements sql.Node.
func (p *PartitionGroupedAggregation) DebugString() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("PartitionGroupedAggregation")

	var groupStrs []string
	for _, gc := range p.GroupByCols {
		groupStrs = append(groupStrs, fmt.Sprintf("%s(col[%d])", gc.ColumnName, gc.ColumnIndex))
	}

	var aggStrs []string
	for i, agg := range p.Aggregations {
		aliasIdx := len(p.GroupByCols) + i
		alias := ""
		if aliasIdx < len(p.Aliases) {
			alias = p.Aliases[aliasIdx]
		}
		aggStrs = append(aggStrs, fmt.Sprintf("%s(col[%d]:%s) AS %s",
			aggregationTypeName(agg.Type),
			agg.ColumnIndex,
			agg.ColumnName,
			alias))
	}

	_ = pr.WriteChildren(
		fmt.Sprintf("GroupBy(%s)", strings.Join(groupStrs, ", ")),
		fmt.Sprintf("Aggregations(%s)", strings.Join(aggStrs, ", ")),
		sql.DebugString(p.Child),
	)
	return pr.String()
}

// GetAggregations returns the partition aggregations.
func (p *PartitionAggregation) GetAggregations() []sql.ColumnAggregation {
	return p.Aggregations
}

// GetAliases returns the column aliases.
func (p *PartitionAggregation) GetAliases() []string {
	return p.Aliases
}
