## Aggregation Pushdown to Table Partitions

This feature allows tables to optionally compute COUNT, SUM, MIN, MAX, and AVG aggregates directly at the partition level, avoiding the need to return all rows to the query engine for aggregation. This is analogous to how `FilteredTable` and `ProjectedTable` allow filter and projection pushdown. Both ungrouped and grouped (GROUP BY) aggregations are supported. Additionally, tables can opt into concurrent partition scanning via the `ConcurrentTable` interface.

## Implementation Status: ✅ Complete

### Files Modified/Created

1. **`sql/tables.go`** - Added:
   - `ConcurrentTable` interface with `NumWorkers() int` — tables that implement this opt into concurrent partition scanning when `NumWorkers() > 1`
   - `AggregationType` enum (`AggregationTypeCount`, `AggregationTypeSum`, `AggregationTypeMin`, `AggregationTypeMax`, `AggregationTypeAvg`)
   - `ColumnAggregation` struct (describes a single aggregation: type, column index, column name)
   - `GroupByColumn` struct (describes a GROUP BY column: name, index)
   - `AggregableTable` interface with methods:
     - `CanAggregate(ctx *Context, aggs []ColumnAggregation) bool`
     - `WithAggregates(ctx *Context, aggs []ColumnAggregation) Table`
     - `Aggregates() []ColumnAggregation`
     - `CanGroupedAggregate(ctx *Context, aggs []ColumnAggregation, groupByCols []GroupByColumn) bool`
     - `WithGroupedAggregates(ctx *Context, aggs []ColumnAggregation, groupByCols []GroupByColumn) Table`
     - `GroupByColumns() []GroupByColumn`

2. **`sql/table_iter.go`** - Added `ConcurrentTableRowIter`:
   - Reads rows from multiple table partitions concurrently using a bounded worker pool
   - Workers pull partitions from a work channel and push rows into a buffered row channel
   - Uses `errgroup.Group` for structured concurrency and error propagation
   - Supports context cancellation to kill workers mid-flight
   - Worker count is capped at the number of partitions
   - Safe close semantics: drains row channel, cancels context, waits for workers

3. **`sql/table_iter_test.go`** - New file:
   - Comprehensive tests for `ConcurrentTableRowIter` using mock partitions and tables
   - Tests cover: basic row reading, empty partitions, worker errors, context cancellation, concurrent access safety

4. **`sql/analyzer/pushdown_aggregations.go`** - New file:
   - `pushdownAggregations` rule function that transforms `GroupBy` → `PartitionAggregation` or `PartitionGroupedAggregation` when applicable
   - Dispatches to ungrouped or grouped pushdown based on presence of GROUP BY expressions
   - Only applies when not inside a subquery context (to avoid schema issues)
   - Only pushes down SUM/AVG on safe numeric types (excludes DECIMAL for precision)
   - Validates column exists in table schema by name
   - Grouped pushdown requires GROUP BY expressions to be simple column references
   - AVG is decomposed into SUM + COUNT pair at partition level

5. **`sql/analyzer/pushdown.go`** - Enhanced filter pushdown:
   - Added `applyFiltersToTable` — pushes filters directly into tables implementing `sql.FilteredTable`
   - Added `pushFiltersIntoTable` — handles `Filter` nodes above `ResolvedTable`/`TableAlias` by splitting predicates and applying handled filters directly
   - Modified `pushdownFiltersToAboveTable` to attempt direct filter application before adding Filter nodes

6. **`sql/analyzer/rule_ids.go`** - Added:
   - `pushdownAggregationsId` rule ID

7. **`sql/analyzer/rules.go`** - Added:
   - Registered `{pushdownAggregationsId, pushdownAggregations}` in `OnceAfterDefault` ruleset

8. **`sql/plan/partition_aggregation.go`** - New file:
   - `PartitionAggregation` plan node for ungrouped aggregations — merges pre-aggregated partition results into a single row
   - `PartitionGroupedAggregation` plan node for grouped aggregations — merges per-group results across partitions
   - Both implement `sql.Node`, `sql.CollationCoercible`, `sql.Expressioner`
   - Determines result types based on aggregation type (e.g., COUNT → INT64, AVG → FLOAT64)

9. **`sql/rowexec/agg.go`** - New file:
   - `buildPartitionAggregation` — builds row iterator for `PartitionAggregation` nodes
   - `partitionAggregationIter` — merges partition-level results for ungrouped aggregations:
     - COUNT: sums partition counts
     - SUM: sums partition sums
     - MIN: takes minimum across partitions
     - MAX: takes maximum across partitions
     - AVG: accumulates `(partialSum, partialCount)` pairs, computes `totalSum / totalCount`
   - `buildPartitionGroupedAggregation` — builds row iterator for `PartitionGroupedAggregation` nodes
   - `partitionGroupedAggregationIter` — merges per-group partition results:
     - Reads all partition rows, groups by key, merges aggregates per group
     - For AVG: each partition returns `(partialSum, partialCount)` which are merged then divided
   - Helper functions: `aggToInt64`, `aggToFloat64`, `aggCompareValues`, `aggSlotCount`

10. **`sql/rowexec/node_builder.gen.go`** - Added:
    - Cases for `*plan.PartitionAggregation` and `*plan.PartitionGroupedAggregation` in node builder switch

11. **`sql/rowexec/builder_gen_test.go`** - Added:
    - `PartitionAggregation` and `PartitionGroupedAggregation` to nodes map for test coverage

12. **`sql/rowexec/rel.go`** - Modified `buildResolvedTable` and `buildIndexedTableAccess`:
    - Check if the underlying table implements `sql.ConcurrentTable` with `NumWorkers() > 1`
    - If so, use `sql.NewConcurrentTableRowIter` instead of `sql.NewTableRowIter`

13. **`memory/table.go`** - Added:
    - `aggregates` and `groupByCols` fields on Table struct
    - Implemented `AggregableTable` interface (all 6 methods):
      - `CanAggregate` — returns true for valid aggregations (COUNT, SUM, MIN, MAX, AVG)
      - `WithAggregates` — returns table copy with ungrouped aggregations set
      - `Aggregates` — returns current aggregations
      - `CanGroupedAggregate` — returns true for valid grouped aggregations
      - `WithGroupedAggregates` — returns table copy with aggregations and group-by columns set
      - `GroupByColumns` — returns configured group-by columns
    - Modified `PartitionRows`:
      - When grouped aggregates are configured, calls `computeGroupedAggregates`
      - When ungrouped aggregates are configured, calls `computeAggregates`
    - `computeAggregates` — computes ungrouped aggregates directly from partition rows
    - `computeGroupedAggregates` — computes per-group aggregates within a partition; for AVG returns `(partialSum, partialCount)` pairs
    - Helper functions: `toFloat64ForAgg`, `compareForAgg`

14. **`enginetest/partition_agg_test.go`** - New file:
    - `TestMultiPartitionAggregation` — tests ungrouped and grouped aggregation pushdown across multiple partitions
    - Covers COUNT, SUM, MIN, MAX, AVG for both ungrouped queries and queries with GROUP BY
    - Tests queries with multiple aggregations in a single SELECT
    - Uses a 5-partition memory table to verify correct cross-partition merging

15. **`Makefile`** - Added macOS ICU4C detection for CGO builds

16. **`_example/main.go`** - Updated example table schema to include an `id` primary key column

### Behavior

- **Ungrouped aggregation pushdown** applies to: `SELECT COUNT(*) FROM t`, `SELECT SUM(col) FROM t`, `SELECT MIN(col) FROM t`, `SELECT MAX(col) FROM t`, `SELECT AVG(col) FROM t`
- **Grouped aggregation pushdown** applies to: `SELECT col, COUNT(*) FROM t GROUP BY col`, `SELECT col, AVG(val) FROM t GROUP BY col`
- **Concurrent partition scanning** applies to: Any query on a table implementing `ConcurrentTable` with `NumWorkers() > 1`
- **Does NOT apply to**:
  - Queries with DISTINCT (e.g., `COUNT(DISTINCT col)`)
  - SUM/AVG on DECIMAL columns (precision requirements)
  - GROUP BY on expressions (only simple column references)
  - Queries inside subqueries (to avoid schema conflicts with outer scope)
  - Queries on tables that don't implement `AggregableTable` / `ConcurrentTable`


### Example Query Plan Transformations

**Ungrouped aggregation:**

Before:
```
Project
 └─ GroupBy
     ├─ select: COUNT(1)
     ├─ group: 
     └─ Table(mytable)
```

After:
```
Project
 └─ PartitionAggregation
     ├─ Aggregations(COUNT(*) AS count(1))
     └─ Table(mytable)
```

**Grouped aggregation:**

Before:
```
Project
 └─ GroupBy
     ├─ select: col, COUNT(1)
     ├─ group: col
     └─ Table(mytable)
```

After:
```
Project
 └─ PartitionGroupedAggregation
     ├─ GroupBy(col)
     ├─ Aggregations(COUNT(*) AS count(1))
     └─ Table(mytable)
```

**AVG aggregation (internally decomposed):**

Before:
```
Project
 └─ GroupBy
     ├─ select: AVG(val)
     ├─ group: 
     └─ Table(mytable)
```

After:
```
Project
 └─ PartitionAggregation
     ├─ Aggregations(AVG(val) AS avg(val))
     └─ Table(mytable)
```

Each partition returns `(partialSum, partialCount)` for AVG columns. The merge layer computes `totalSum / totalCount`.
