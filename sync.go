package sync

import (
	"fmt"
	"sync"

	sq "github.com/Masterminds/squirrel"
	"github.com/google/go-cmp/cmp"
)

type SyncResult struct {
	Target Table
	Error  error
}

func syncTargets(
	primaryKey string,
	columns []string,
	source Table,
	targets []Table,
) ([]SyncResult, error) {
	var primaryKeyIndex int
	var primaryKeyFound bool

	// Determine the index of the primary key in the columns slice
	for i, col := range columns {
		if col == primaryKey {
			primaryKeyIndex = i
			primaryKeyFound = true
			break
		}
	}

	if !primaryKeyFound {
		return nil, fmt.Errorf("primary key '%s' not found in columns", primaryKey)
	}

	fetchAll := sq.Select(columns...).From(source.Config.Table).OrderBy(primaryKey)

	// Get all rows from the source table and put them in a map by their primary key
	sourceMap, err := getEntriesAsMap(source, fetchAll, primaryKeyIndex)
	if err != nil {
		return nil, err
	}

	var wg sync.WaitGroup
	resultChan := make(chan SyncResult, len(targets))

	for _, target := range targets {
		wg.Add(1)
		go func(target Table) {
			defer wg.Done()
			err := syncTarget(target, fetchAll, primaryKey, primaryKeyIndex, columns, sourceMap)
			resultChan <- SyncResult{Target: target, Error: err}
		}(target)
	}

	wg.Wait()         // Wait for all goroutines to finish
	close(resultChan) // Close the channel to signal that all results have been sent

	// Collect the results from the channel
	results := make([]SyncResult, 0, len(targets))
	for result := range resultChan {
		results = append(results, result)
	}

	return results, nil
}

func syncTarget(
	target Table,
	fetchAll sq.SelectBuilder,
	primaryKey string,
	primaryKeyIndex int,
	columns []string,
	sourceMap map[any][]any,
) error {
	targetMap, err := getEntriesAsMap(target, fetchAll, primaryKeyIndex)
	if err != nil {
		return err
	}

	tableName := target.Config.Table

	// Iterate over source rows and perform INSERTs or UPDATEs as needed
	for key, val := range sourceMap {
		// If the key doesn't exist in targetMap, then we need to INSERT
		if _, ok := targetMap[key]; !ok {
			insert := sq.Insert(tableName).Columns(columns...).Values(val...)

			if _, err := insert.RunWith(target.DB).Exec(); err != nil {
				return err
			}
		} else {
			// If the key exists in targetMap, then we need to check if there is a diff
			if cmp.Equal(val, targetMap[key]) {
				continue // No diff, so we skip this row
			}

			// Perform an UPDATE
			update := sq.Update(tableName).Where(sq.Eq{primaryKey: key})

			for i, col := range columns {
				if col == primaryKey {
					continue
				}

				update = update.Set(col, val[i])
			}

			if _, err := update.RunWith(target.DB).Exec(); err != nil {
				return err
			}

			// Remove the key from the targetMap to keep track of which rows we need to delete
			delete(targetMap, key)
		}
	}

	// Iterate over target rows and DELETE any that weren't in the source
	for key := range targetMap {
		delete := sq.Delete(tableName).Where(sq.Eq{primaryKey: key})

		if _, err := delete.RunWith(target.DB).Exec(); err != nil {
			return err
		}
	}

	return nil
}

func getEntriesAsMap(
	table Table,
	query sq.SelectBuilder,
	primaryKeyIndex int,
) (map[any][]any, error) {
	sql, args, err := query.ToSql()
	if err != nil {
		return nil, err
	}

	rows, err := table.Queryx(sql, args...)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	entryMap := map[any][]any{}

	for rows.Next() {
		cols, err := rows.SliceScan()
		if err != nil {
			return nil, err
		}

		pk := cols[primaryKeyIndex]
		entryMap[pk] = cols
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return entryMap, nil
}
