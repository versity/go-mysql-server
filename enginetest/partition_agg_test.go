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
