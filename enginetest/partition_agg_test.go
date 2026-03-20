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

package enginetest

import (
	"context"
	"testing"

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/stretchr/testify/require"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// TestMultiPartitionAggregation tests that aggregation pushdown works correctly
// when merging results from multiple partitions.
func TestMultiPartitionAggregation(t *testing.T) {
	ctx := context.Background()

	// Create database and provider
	db := memory.NewDatabase("testdb")
	pro := memory.NewDBProvider(db)

	// Create a table with 5 partitions
	schema := sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "id", Type: types.Int64, Source: "test_table", PrimaryKey: true},
		{Name: "value", Type: types.Int64, Source: "test_table"},
	})

	// Create table with 5 partitions - need to use db.Database() to get *BaseDatabase
	table := memory.NewPartitionedTable(db.Database(), "test_table", schema, nil, 5)
	db.AddTable("test_table", table)

	// Create engine
	engine := sqle.NewDefault(pro)

	// Create session
	session := memory.NewSession(sql.NewBaseSession(), pro)
	sqlCtx := sql.NewContext(ctx, sql.WithSession(session))
	sqlCtx.SetCurrentDatabase("testdb")

	// Insert data - it will be distributed across partitions
	_, iter, _, err := engine.Query(sqlCtx, "INSERT INTO test_table (id, value) VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)")
	require.NoError(t, err)
	// Drain the insert result
	for {
		_, err := iter.Next(sqlCtx)
		if err != nil {
			break
		}
	}
	iter.Close(sqlCtx)

	// Verify we have multiple partitions
	partitions, err := table.Partitions(sqlCtx)
	require.NoError(t, err)
	partCount := 0
	for {
		_, err := partitions.Next(sqlCtx)
		if err != nil {
			break
		}
		partCount++
	}
	t.Logf("Table has %d partitions", partCount)
	require.True(t, partCount > 1, "Table should have multiple partitions for this test")

	// Test aggregation queries
	tests := []struct {
		query    string
		expected interface{}
	}{
		{"SELECT COUNT(*) FROM test_table", int64(5)},
		{"SELECT SUM(value) FROM test_table", float64(150)}, // int64 for integer columns
		{"SELECT MIN(value) FROM test_table", int64(10)},
		{"SELECT MAX(value) FROM test_table", int64(50)},
		{"SELECT COUNT(value) FROM test_table", int64(5)},
	}

	for _, test := range tests {
		t.Run(test.query, func(t *testing.T) {
			_, iter, _, err := engine.Query(sqlCtx, test.query)
			require.NoError(t, err)

			row, err := iter.Next(sqlCtx)
			require.NoError(t, err)
			iter.Close(sqlCtx)

			require.Len(t, row, 1, "Result should have 1 column")

			result := row[0]

			// Compare based on type
			switch expected := test.expected.(type) {
			case int64:
				v, ok := result.(int64)
				require.True(t, ok, "Expected int64, got %T", result)
				require.Equal(t, expected, v)
			case float64:
				v, ok := result.(float64)
				require.True(t, ok, "Expected float64, got %T", result)
				require.Equal(t, expected, v)
			}

			t.Logf("%s = %v", test.query, result)
		})
	}
}

// TestMultiPartitionAggregationWithNulls tests aggregation with NULL values
func TestMultiPartitionAggregationWithNulls(t *testing.T) {
	ctx := context.Background()

	db := memory.NewDatabase("testdb2")
	pro := memory.NewDBProvider(db)

	schema := sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "id", Type: types.Int64, Source: "test_nulls", PrimaryKey: true},
		{Name: "value", Type: types.Int64, Source: "test_nulls", Nullable: true},
	})

	table := memory.NewPartitionedTable(db.Database(), "test_nulls", schema, nil, 3)
	db.AddTable("test_nulls", table)

	engine := sqle.NewDefault(pro)
	session := memory.NewSession(sql.NewBaseSession(), pro)
	sqlCtx := sql.NewContext(ctx, sql.WithSession(session))
	sqlCtx.SetCurrentDatabase("testdb2")

	// Insert data with NULLs
	_, iter, _, err := engine.Query(sqlCtx, "INSERT INTO test_nulls (id, value) VALUES (1, 10), (2, NULL), (3, 30)")
	require.NoError(t, err)
	for {
		_, err := iter.Next(sqlCtx)
		if err != nil {
			break
		}
	}
	iter.Close(sqlCtx)

	tests := []struct {
		query    string
		expected interface{}
	}{
		{"SELECT COUNT(*) FROM test_nulls", int64(3)},      // COUNT(*) counts all rows
		{"SELECT COUNT(value) FROM test_nulls", int64(2)},  // COUNT(col) skips nulls
		{"SELECT SUM(value) FROM test_nulls", float64(40)}, // SUM skips nulls, int64 for integer columns
		{"SELECT MIN(value) FROM test_nulls", int64(10)},   // MIN skips nulls
		{"SELECT MAX(value) FROM test_nulls", int64(30)},   // MAX skips nulls
	}

	for _, test := range tests {
		t.Run(test.query, func(t *testing.T) {
			_, iter, _, err := engine.Query(sqlCtx, test.query)
			require.NoError(t, err)

			row, err := iter.Next(sqlCtx)
			require.NoError(t, err)
			iter.Close(sqlCtx)

			require.Len(t, row, 1)
			result := row[0]

			switch expected := test.expected.(type) {
			case int64:
				v, ok := result.(int64)
				require.True(t, ok, "Expected int64, got %T: %v", result, result)
				require.Equal(t, expected, v)
			case float64:
				v, ok := result.(float64)
				require.True(t, ok, "Expected float64, got %T: %v", result, result)
				require.Equal(t, expected, v)
			}

			t.Logf("%s = %v", test.query, result)
		})
	}
}

// TestMultiPartitionGroupedAggregation tests that grouped aggregation pushdown works correctly
// when merging results from multiple partitions with GROUP BY.
func TestMultiPartitionGroupedAggregation(t *testing.T) {
	ctx := context.Background()

	db := memory.NewDatabase("testdb3")
	pro := memory.NewDBProvider(db)

	schema := sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "id", Type: types.Int64, Source: "sales", PrimaryKey: true},
		{Name: "category", Type: types.MustCreateStringWithDefaults(sqltypes.VarChar, 50), Source: "sales"},
		{Name: "amount", Type: types.Int64, Source: "sales"},
	})

	// Create table with 5 partitions so data spans multiple partitions
	table := memory.NewPartitionedTable(db.Database(), "sales", schema, nil, 5)
	db.AddTable("sales", table)

	engine := sqle.NewDefault(pro)
	session := memory.NewSession(sql.NewBaseSession(), pro)
	sqlCtx := sql.NewContext(ctx, sql.WithSession(session))
	sqlCtx.SetCurrentDatabase("testdb3")

	// Insert data that will be distributed across partitions
	// Category A: amounts 10, 20, 30 (total=60, count=3, min=10, max=30, avg=20)
	// Category B: amounts 100, 200 (total=300, count=2, min=100, max=200, avg=150)
	// Category C: amount 50 (total=50, count=1, min=50, max=50, avg=50)
	insertQ := `INSERT INTO sales (id, category, amount) VALUES
		(1, 'A', 10), (2, 'B', 100), (3, 'A', 20),
		(4, 'B', 200), (5, 'C', 50), (6, 'A', 30)`
	_, iter, _, err := engine.Query(sqlCtx, insertQ)
	require.NoError(t, err)
	for {
		_, err := iter.Next(sqlCtx)
		if err != nil {
			break
		}
	}
	iter.Close(sqlCtx)

	// Verify we have multiple partitions
	partitions, err := table.Partitions(sqlCtx)
	require.NoError(t, err)
	partCount := 0
	for {
		_, err := partitions.Next(sqlCtx)
		if err != nil {
			break
		}
		partCount++
	}
	t.Logf("Table has %d partitions", partCount)
	require.True(t, partCount > 1, "Table should have multiple partitions for this test")

	type expectedRow struct {
		category string
		value    interface{}
	}

	tests := []struct {
		name     string
		query    string
		expected []expectedRow
	}{
		{
			name:  "COUNT with GROUP BY",
			query: "SELECT category, COUNT(*) FROM sales GROUP BY category",
			expected: []expectedRow{
				{"A", int64(3)},
				{"B", int64(2)},
				{"C", int64(1)},
			},
		},
		{
			name:  "SUM with GROUP BY",
			query: "SELECT category, SUM(amount) FROM sales GROUP BY category",
			expected: []expectedRow{
				{"A", float64(60)},
				{"B", float64(300)},
				{"C", float64(50)},
			},
		},
		{
			name:  "MIN with GROUP BY",
			query: "SELECT category, MIN(amount) FROM sales GROUP BY category",
			expected: []expectedRow{
				{"A", int64(10)},
				{"B", int64(100)},
				{"C", int64(50)},
			},
		},
		{
			name:  "MAX with GROUP BY",
			query: "SELECT category, MAX(amount) FROM sales GROUP BY category",
			expected: []expectedRow{
				{"A", int64(30)},
				{"B", int64(200)},
				{"C", int64(50)},
			},
		},
		{
			name:  "AVG with GROUP BY",
			query: "SELECT category, AVG(amount) FROM sales GROUP BY category",
			expected: []expectedRow{
				{"A", float64(20)},
				{"B", float64(150)},
				{"C", float64(50)},
			},
		},
		{
			name:     "Multiple aggregations with GROUP BY",
			query:    "SELECT category, COUNT(*), SUM(amount), MIN(amount), MAX(amount) FROM sales GROUP BY category",
			expected: nil, // checked separately below
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, iter, _, err := engine.Query(sqlCtx, test.query)
			require.NoError(t, err)

			// Collect all rows
			resultMap := make(map[string]sql.Row)
			for {
				row, err := iter.Next(sqlCtx)
				if err != nil {
					break
				}
				cat, ok := row[0].(string)
				require.True(t, ok, "Expected string category, got %T: %v", row[0], row[0])
				resultMap[cat] = row
				t.Logf("  %s -> %v", cat, row)
			}
			iter.Close(sqlCtx)

			if test.name == "Multiple aggregations with GROUP BY" {
				// Check A: COUNT=3, SUM=60, MIN=10, MAX=30
				rowA := resultMap["A"]
				require.NotNil(t, rowA, "Missing group A")
				require.Equal(t, int64(3), rowA[1])
				require.Equal(t, float64(60), rowA[2])
				require.Equal(t, int64(10), rowA[3])
				require.Equal(t, int64(30), rowA[4])

				// Check B: COUNT=2, SUM=300, MIN=100, MAX=200
				rowB := resultMap["B"]
				require.NotNil(t, rowB, "Missing group B")
				require.Equal(t, int64(2), rowB[1])
				require.Equal(t, float64(300), rowB[2])
				require.Equal(t, int64(100), rowB[3])
				require.Equal(t, int64(200), rowB[4])
				return
			}

			// Check single aggregation tests
			require.Len(t, resultMap, len(test.expected), "Expected %d groups", len(test.expected))
			for _, exp := range test.expected {
				row, ok := resultMap[exp.category]
				require.True(t, ok, "Missing group %s", exp.category)
				require.Len(t, row, 2)

				switch expected := exp.value.(type) {
				case int64:
					v, ok := row[1].(int64)
					require.True(t, ok, "Expected int64, got %T: %v (category=%s)", row[1], row[1], exp.category)
					require.Equal(t, expected, v, "category=%s", exp.category)
				case float64:
					v, ok := row[1].(float64)
					require.True(t, ok, "Expected float64, got %T: %v (category=%s)", row[1], row[1], exp.category)
					require.InDelta(t, expected, v, 0.001, "category=%s", exp.category)
				}
			}
		})
	}
}

// TestMultiPartitionGroupedAggregationWithNulls tests grouped aggregation with NULL values.
func TestMultiPartitionGroupedAggregationWithNulls(t *testing.T) {
	ctx := context.Background()

	db := memory.NewDatabase("testdb4")
	pro := memory.NewDBProvider(db)

	schema := sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "id", Type: types.Int64, Source: "data_nulls", PrimaryKey: true},
		{Name: "grp", Type: types.MustCreateStringWithDefaults(sqltypes.VarChar, 20), Source: "data_nulls"},
		{Name: "val", Type: types.Int64, Source: "data_nulls", Nullable: true},
	})

	table := memory.NewPartitionedTable(db.Database(), "data_nulls", schema, nil, 3)
	db.AddTable("data_nulls", table)

	engine := sqle.NewDefault(pro)
	session := memory.NewSession(sql.NewBaseSession(), pro)
	sqlCtx := sql.NewContext(ctx, sql.WithSession(session))
	sqlCtx.SetCurrentDatabase("testdb4")

	// Insert data with NULLs in the aggregated column
	// Group X: values 10, NULL, 30 -> COUNT(*)=3, COUNT(val)=2, SUM=40, MIN=10, MAX=30
	// Group Y: values NULL -> COUNT(*)=1, COUNT(val)=0, SUM=NULL, MIN=NULL, MAX=NULL
	insertQ := `INSERT INTO data_nulls (id, grp, val) VALUES
		(1, 'X', 10), (2, 'X', NULL), (3, 'X', 30), (4, 'Y', NULL)`
	_, iter, _, err := engine.Query(sqlCtx, insertQ)
	require.NoError(t, err)
	for {
		_, err := iter.Next(sqlCtx)
		if err != nil {
			break
		}
	}
	iter.Close(sqlCtx)

	// Test COUNT(*) with GROUP BY - counts all rows including NULLs
	t.Run("COUNT(*) with GROUP BY and nulls", func(t *testing.T) {
		_, iter, _, err := engine.Query(sqlCtx, "SELECT grp, COUNT(*) FROM data_nulls GROUP BY grp")
		require.NoError(t, err)
		resultMap := make(map[string]int64)
		for {
			row, err := iter.Next(sqlCtx)
			if err != nil {
				break
			}
			resultMap[row[0].(string)] = row[1].(int64)
		}
		iter.Close(sqlCtx)
		require.Equal(t, int64(3), resultMap["X"])
		require.Equal(t, int64(1), resultMap["Y"])
	})

	// Test COUNT(col) with GROUP BY - skips NULLs
	t.Run("COUNT(val) with GROUP BY and nulls", func(t *testing.T) {
		_, iter, _, err := engine.Query(sqlCtx, "SELECT grp, COUNT(val) FROM data_nulls GROUP BY grp")
		require.NoError(t, err)
		resultMap := make(map[string]int64)
		for {
			row, err := iter.Next(sqlCtx)
			if err != nil {
				break
			}
			resultMap[row[0].(string)] = row[1].(int64)
		}
		iter.Close(sqlCtx)
		require.Equal(t, int64(2), resultMap["X"])
		require.Equal(t, int64(0), resultMap["Y"])
	})

	// Test SUM with GROUP BY and nulls
	t.Run("SUM with GROUP BY and nulls", func(t *testing.T) {
		_, iter, _, err := engine.Query(sqlCtx, "SELECT grp, SUM(val) FROM data_nulls GROUP BY grp")
		require.NoError(t, err)
		resultMap := make(map[string]interface{})
		for {
			row, err := iter.Next(sqlCtx)
			if err != nil {
				break
			}
			resultMap[row[0].(string)] = row[1]
		}
		iter.Close(sqlCtx)
		require.Equal(t, float64(40), resultMap["X"])
	})
}

// TestMultiPartitionMultiColumnGroupBy tests GROUP BY on multiple columns.
func TestMultiPartitionMultiColumnGroupBy(t *testing.T) {
	ctx := context.Background()

	db := memory.NewDatabase("testdb5")
	pro := memory.NewDBProvider(db)

	schema := sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "id", Type: types.Int64, Source: "multi_grp", PrimaryKey: true},
		{Name: "region", Type: types.MustCreateStringWithDefaults(sqltypes.VarChar, 20), Source: "multi_grp"},
		{Name: "product", Type: types.MustCreateStringWithDefaults(sqltypes.VarChar, 20), Source: "multi_grp"},
		{Name: "sales", Type: types.Int64, Source: "multi_grp"},
	})

	table := memory.NewPartitionedTable(db.Database(), "multi_grp", schema, nil, 5)
	db.AddTable("multi_grp", table)

	engine := sqle.NewDefault(pro)
	session := memory.NewSession(sql.NewBaseSession(), pro)
	sqlCtx := sql.NewContext(ctx, sql.WithSession(session))
	sqlCtx.SetCurrentDatabase("testdb5")

	// Insert data across multiple groups
	insertQ := `INSERT INTO multi_grp (id, region, product, sales) VALUES
		(1, 'East', 'Widget', 10),
		(2, 'East', 'Widget', 20),
		(3, 'East', 'Gadget', 30),
		(4, 'West', 'Widget', 40),
		(5, 'West', 'Gadget', 50),
		(6, 'West', 'Gadget', 60)`
	_, iter, _, err := engine.Query(sqlCtx, insertQ)
	require.NoError(t, err)
	for {
		_, err := iter.Next(sqlCtx)
		if err != nil {
			break
		}
	}
	iter.Close(sqlCtx)

	t.Run("Multi-column GROUP BY with COUNT and SUM", func(t *testing.T) {
		_, iter, _, err := engine.Query(sqlCtx, "SELECT region, product, COUNT(*), SUM(sales) FROM multi_grp GROUP BY region, product")
		require.NoError(t, err)

		type groupResult struct {
			count int64
			sum   float64
		}
		resultMap := make(map[string]groupResult)
		for {
			row, err := iter.Next(sqlCtx)
			if err != nil {
				break
			}
			key := row[0].(string) + "/" + row[1].(string)
			resultMap[key] = groupResult{
				count: row[2].(int64),
				sum:   row[3].(float64),
			}
			t.Logf("  %s -> count=%d, sum=%v", key, row[2], row[3])
		}
		iter.Close(sqlCtx)

		require.Len(t, resultMap, 4)
		require.Equal(t, groupResult{2, float64(30)}, resultMap["East/Widget"])
		require.Equal(t, groupResult{1, float64(30)}, resultMap["East/Gadget"])
		require.Equal(t, groupResult{1, float64(40)}, resultMap["West/Widget"])
		require.Equal(t, groupResult{2, float64(110)}, resultMap["West/Gadget"])
	})
}
