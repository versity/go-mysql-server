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

package analyzer

import (
	"github.com/dolthub/vitess/go/sqltypes"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function/aggregation"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

// pushdownAggregations attempts to push simple aggregations (COUNT, SUM, MIN, MAX)
// down to tables that implement the AggregableTable interface. This allows tables
// to compute aggregates at the partition level, reducing the number of rows returned.
func pushdownAggregations(ctx *sql.Context, a *Analyzer, n sql.Node, scope *plan.Scope, sel RuleSelector, qFlags *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	span, ctx := ctx.Span("pushdown_aggregations")
	defer span.End()

	if !n.Resolved() {
		return n, transform.SameTree, nil
	}

	// Don't push down aggregations inside subqueries - the row context
	// may cause issues with schema expectations when outer scope rows are prepended.
	// A non-empty scope indicates we're inside a subquery.
	if scope != nil && !scope.IsEmpty() {
		return n, transform.SameTree, nil
	}

	return transform.Node(n, func(node sql.Node) (sql.Node, transform.TreeIdentity, error) {
		groupBy, ok := node.(*plan.GroupBy)
		if !ok {
			return node, transform.SameTree, nil
		}

		// Check if child is a table we can push down to
		rt, ok := getResolvedTableForAgg(groupBy.Child)
		if !ok {
			return node, transform.SameTree, nil
		}

		// Check if table implements AggregableTable
		aggTable, ok := rt.Table.(sql.AggregableTable)
		if !ok {
			return node, transform.SameTree, nil
		}

		// Dispatch to grouped or ungrouped pushdown
		if len(groupBy.GroupByExprs) > 0 {
			return pushdownGroupedAggregation(ctx, a, groupBy, rt, aggTable)
		}

		// Ungrouped aggregation pushdown (existing behavior)

		// Extract pushable aggregations from selected expressions
		aggs, aliases, ok := extractPushableAggregations(groupBy.SelectDeps, rt.Table.Schema())
		if !ok || len(aggs) == 0 {
			return node, transform.SameTree, nil
		}

		// Check if table can handle these aggregations
		if !aggTable.CanAggregate(ctx, aggs) {
			return node, transform.SameTree, nil
		}

		// Create new table with aggregation pushdown
		newTable := aggTable.WithAggregates(ctx, aggs)
		newRT, err := rt.WithTable(newTable)
		if err != nil {
			return node, transform.SameTree, nil
		}

		// Create partition aggregation node to merge partition results
		partAgg := plan.NewPartitionAggregation(
			newRT.(*plan.ResolvedTable),
			aggs,
			aliases,
			groupBy.SelectDeps,
		)

		a.Log("aggregations pushed down to table %s", rt.Name())
		return partAgg, transform.NewTree, nil
	})
}

// pushdownGroupedAggregation handles aggregation pushdown for queries with GROUP BY.
// Only supports GROUP BY on simple column references (not expressions).
func pushdownGroupedAggregation(
	ctx *sql.Context,
	a *Analyzer,
	groupBy *plan.GroupBy,
	rt *plan.ResolvedTable,
	aggTable sql.AggregableTable,
) (sql.Node, transform.TreeIdentity, error) {
	schema := rt.Table.Schema()

	// Extract GROUP BY columns - only support simple column references
	groupByCols, ok := extractGroupByColumns(groupBy.GroupByExprs, schema)
	if !ok || len(groupByCols) == 0 {
		return groupBy, transform.SameTree, nil
	}

	// Classify each SelectDep as either a group-by column passthrough or an aggregation
	aggs, aliases, ok := extractGroupedPushableExpressions(groupBy.SelectDeps, groupBy.GroupByExprs, schema)
	if !ok {
		return groupBy, transform.SameTree, nil
	}

	// Check if table can handle these grouped aggregations
	if !aggTable.CanGroupedAggregate(ctx, aggs, groupByCols) {
		return groupBy, transform.SameTree, nil
	}

	// Create new table with grouped aggregation pushdown
	newTable := aggTable.WithGroupedAggregates(ctx, aggs, groupByCols)
	newRT, err := rt.WithTable(newTable)
	if err != nil {
		return groupBy, transform.SameTree, nil
	}

	// Create partition grouped aggregation node
	partAgg := plan.NewPartitionGroupedAggregation(
		newRT.(*plan.ResolvedTable),
		aggs,
		groupByCols,
		aliases,
		groupBy.SelectDeps,
		groupBy.GroupByExprs,
	)

	a.Log("grouped aggregations pushed down to table %s", rt.Name())
	return partAgg, transform.NewTree, nil
}

// extractGroupByColumns extracts GroupByColumn descriptors from GROUP BY expressions.
// Returns false if any expression is not a simple column reference.
func extractGroupByColumns(exprs []sql.Expression, schema sql.Schema) ([]sql.GroupByColumn, bool) {
	cols := make([]sql.GroupByColumn, 0, len(exprs))
	for _, expr := range exprs {
		gf, ok := expr.(*expression.GetField)
		if !ok {
			return nil, false
		}
		colName := gf.Name()
		colIdx := findColumnIndexByName(schema, colName)
		if colIdx < 0 {
			return nil, false
		}
		cols = append(cols, sql.GroupByColumn{
			ColumnName:  colName,
			ColumnIndex: colIdx,
		})
	}
	return cols, true
}

// extractGroupedPushableExpressions analyzes the SELECT expressions in a GROUP BY query
// and returns ColumnAggregation descriptors for the aggregation expressions, plus aliases
// for all output columns (group-by columns first, then aggregations).
// Returns false if any expression cannot be pushed down.
func extractGroupedPushableExpressions(
	selectDeps []sql.Expression,
	groupByExprs []sql.Expression,
	schema sql.Schema,
) ([]sql.ColumnAggregation, []string, bool) {
	// Build a set of GROUP BY column names for matching
	groupByColNames := make(map[string]bool, len(groupByExprs))
	for _, gbe := range groupByExprs {
		if gf, ok := gbe.(*expression.GetField); ok {
			groupByColNames[gf.Name()] = true
		}
	}

	// Separate select expressions into group-by column refs and aggregation functions
	aggs := make([]sql.ColumnAggregation, 0)
	// Aliases: group-by column names first (in the order they appear in groupByExprs),
	// then aggregation aliases
	aliases := make([]string, 0, len(selectDeps))

	// First, add aliases for group-by columns in their canonical order
	for _, gbe := range groupByExprs {
		if gf, ok := gbe.(*expression.GetField); ok {
			aliases = append(aliases, gf.Name())
		}
	}

	for _, expr := range selectDeps {
		// Unwrap alias
		var innerExpr sql.Expression
		alias := ""
		if a, ok := expr.(*expression.Alias); ok {
			alias = a.Name()
			innerExpr = a.Child
		} else {
			alias = expr.String()
			innerExpr = expr
		}

		// Check if this is a reference to a GROUP BY column
		if gf, ok := innerExpr.(*expression.GetField); ok {
			if groupByColNames[gf.Name()] {
				// This is a group-by column passthrough - already in aliases from above
				continue
			}
		}

		// Must be an aggregation function
		agg, _, ok := extractSingleGroupedAggregation(innerExpr, schema, alias)
		if !ok {
			return nil, nil, false
		}
		aggs = append(aggs, agg)
		aliases = append(aliases, alias)
	}

	if len(aggs) == 0 {
		return nil, nil, false
	}

	return aggs, aliases, true
}

// extractSingleGroupedAggregation extracts a single aggregation from a grouped query expression.
// This extends extractSingleAggregation to also support AVG.
func extractSingleGroupedAggregation(expr sql.Expression, schema sql.Schema, alias string) (sql.ColumnAggregation, string, bool) {
	switch agg := expr.(type) {
	case *aggregation.Count:
		return extractCountAggregation(agg, schema, alias)
	case *aggregation.Sum:
		return extractColumnAggregation(agg.Child, sql.AggregationTypeSum, schema, alias)
	case *aggregation.Min:
		return extractColumnAggregation(agg.Child, sql.AggregationTypeMin, schema, alias)
	case *aggregation.Max:
		return extractColumnAggregation(agg.Child, sql.AggregationTypeMax, schema, alias)
	case *aggregation.Avg:
		return extractColumnAggregation(agg.Child, sql.AggregationTypeAvg, schema, alias)
	default:
		return sql.ColumnAggregation{}, "", false
	}
}

// getResolvedTableForAgg extracts the ResolvedTable from a node, handling TableAlias wrappers.
func getResolvedTableForAgg(node sql.Node) (*plan.ResolvedTable, bool) {
	switch n := node.(type) {
	case *plan.ResolvedTable:
		return n, true
	case *plan.TableAlias:
		if rt, ok := n.Child.(*plan.ResolvedTable); ok {
			return rt, true
		}
	}
	return nil, false
}

// extractPushableAggregations analyzes expressions and returns ColumnAggregation descriptors
// for aggregations that can be pushed down. Returns false if any expression cannot be pushed.
func extractPushableAggregations(exprs []sql.Expression, schema sql.Schema) ([]sql.ColumnAggregation, []string, bool) {
	aggs := make([]sql.ColumnAggregation, 0, len(exprs))
	aliases := make([]string, 0, len(exprs))

	for _, expr := range exprs {
		agg, alias, ok := extractSingleAggregation(expr, schema)
		if !ok {
			return nil, nil, false
		}
		aggs = append(aggs, agg)
		aliases = append(aliases, alias)
	}

	return aggs, aliases, true
}

// extractSingleAggregation extracts a single pushable aggregation from an expression.
func extractSingleAggregation(expr sql.Expression, schema sql.Schema) (sql.ColumnAggregation, string, bool) {
	var aggExpr sql.Expression
	alias := ""

	// Handle alias wrapper
	if a, ok := expr.(*expression.Alias); ok {
		alias = a.Name()
		aggExpr = a.Child
	} else {
		alias = expr.String()
		aggExpr = expr
	}

	switch agg := aggExpr.(type) {
	case *aggregation.Count:
		// COUNT(*) or COUNT(column)
		return extractCountAggregation(agg, schema, alias)
	case *aggregation.Sum:
		return extractColumnAggregation(agg.Child, sql.AggregationTypeSum, schema, alias)
	case *aggregation.Min:
		return extractColumnAggregation(agg.Child, sql.AggregationTypeMin, schema, alias)
	case *aggregation.Max:
		return extractColumnAggregation(agg.Child, sql.AggregationTypeMax, schema, alias)
	default:
		// Not a pushable aggregation
		return sql.ColumnAggregation{}, "", false
	}
}

// extractCountAggregation handles COUNT(*) and COUNT(column).
func extractCountAggregation(count *aggregation.Count, schema sql.Schema, alias string) (sql.ColumnAggregation, string, bool) {
	child := count.Child

	// COUNT(*) is represented as COUNT(1) after replaceCountStar
	if lit, ok := child.(*expression.Literal); ok {
		if _, ok := lit.Value().(int64); ok {
			return sql.ColumnAggregation{
				Type:        sql.AggregationTypeCount,
				ColumnIndex: -1, // -1 indicates COUNT(*)
				ColumnName:  "",
			}, alias, true
		}
		// Handle quoted column names that come through as string literals
		if colName, ok := lit.Value().(string); ok {
			colIdx := findColumnIndexByName(schema, colName)
			if colIdx < 0 {
				return sql.ColumnAggregation{}, "", false
			}
			return sql.ColumnAggregation{
				Type:        sql.AggregationTypeCount,
				ColumnIndex: colIdx,
				ColumnName:  colName,
			}, alias, true
		}
	}

	// COUNT(column)
	if gf, ok := child.(*expression.GetField); ok {
		// Find the column index in the table schema by name
		colName := gf.Name()
		colIdx := findColumnIndexByName(schema, colName)
		if colIdx < 0 {
			return sql.ColumnAggregation{}, "", false
		}
		return sql.ColumnAggregation{
			Type:        sql.AggregationTypeCount,
			ColumnIndex: colIdx,
			ColumnName:  colName,
		}, alias, true
	}

	return sql.ColumnAggregation{}, "", false
}

// extractColumnAggregation handles SUM, MIN, MAX on columns.
func extractColumnAggregation(child sql.Expression, aggType sql.AggregationType, schema sql.Schema, alias string) (sql.ColumnAggregation, string, bool) {
	var colName string

	switch c := child.(type) {
	case *expression.GetField:
		colName = c.Name()
	case *expression.Literal:
		// Handle quoted column names that come through as string literals
		s, ok := c.Value().(string)
		if !ok {
			return sql.ColumnAggregation{}, "", false
		}
		colName = s
	default:
		return sql.ColumnAggregation{}, "", false
	}

	// Find the column index in the table schema by name
	colIdx := findColumnIndexByName(schema, colName)
	if colIdx < 0 {
		return sql.ColumnAggregation{}, "", false
	}

	// For SUM and AVG, only support simple numeric types (not DECIMAL which requires precision)
	if aggType == sql.AggregationTypeSum || aggType == sql.AggregationTypeAvg {
		colType := schema[colIdx].Type
		if !isSafeNumericType(colType) {
			return sql.ColumnAggregation{}, "", false
		}
	}

	return sql.ColumnAggregation{
		Type:        aggType,
		ColumnIndex: colIdx,
		ColumnName:  colName,
	}, alias, true
}

// isSafeNumericType checks if a type can be safely aggregated with float64.
// Returns false for DECIMAL types which need higher precision.
func isSafeNumericType(t sql.Type) bool {
	switch t.Type() {
	case
		// Integer types
		sqltypes.Int8, sqltypes.Int16, sqltypes.Int24, sqltypes.Int32, sqltypes.Int64,
		sqltypes.Uint8, sqltypes.Uint16, sqltypes.Uint24, sqltypes.Uint32, sqltypes.Uint64,
		// Float types
		sqltypes.Float32, sqltypes.Float64:
		return true
	default:
		return false
	}
}

// findColumnIndexByName finds the index of a column in a schema by name.
func findColumnIndexByName(schema sql.Schema, name string) int {
	for i, col := range schema {
		if col.Name == name {
			return i
		}
	}
	return -1
}
