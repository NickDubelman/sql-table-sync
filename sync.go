package sync

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"

	sq "github.com/Masterminds/squirrel"
	"github.com/google/go-cmp/cmp"
)

type SyncResult struct {
	Target         Table
	TargetChecksum string
	Synced         bool
	Error          error
}

func syncTargets(
	primaryKey string,
	columns []string,
	source Table,
	targets []Table,
) (string, []SyncResult, error) {
	if source.DB == nil {
		return "", nil, fmt.Errorf("source unreachable")
	}

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
		return "", nil, fmt.Errorf("primary key '%s' not found in columns", primaryKey)
	}

	fetchAll := sq.Select(columns...).From(source.Config.Table).OrderBy(primaryKey)

	// Get all rows from the source table and put them in a map by their primary key
	sourceEntries, sourceMap, err := getEntries(source, fetchAll, primaryKeyIndex)
	if err != nil {
		return "", nil, err
	}

	sourceChecksum, err := checksumData(sourceEntries)
	if err != nil {
		return "", nil, err
	}

	var wg sync.WaitGroup
	resultChan := make(chan SyncResult, len(targets))

	for _, target := range targets {
		wg.Add(1)
		go func(target Table) {
			defer wg.Done()
			checksum, synced, err := syncTarget(
				target,
				primaryKey,
				primaryKeyIndex,
				columns,
				sourceChecksum,
				sourceMap,
			)

			resultChan <- SyncResult{
				Target:         target,
				TargetChecksum: checksum,
				Synced:         synced,
				Error:          err,
			}
		}(target)
	}

	wg.Wait()         // Wait for all goroutines to finish
	close(resultChan) // Close the channel to signal that all results have been sent

	// Collect the results from the channel
	results := make([]SyncResult, 0, len(targets))
	for result := range resultChan {
		results = append(results, result)
	}

	return sourceChecksum, results, nil
}

func syncTarget(
	target Table,
	primaryKey string,
	primaryKeyIndex int,
	columns []string,
	sourceChecksum string,
	sourceMap map[any][]any,
) (string, bool, error) {
	if target.DB == nil {
		var err error
		target, err = Connect(target.Config)
		if err != nil {
			return "", false, fmt.Errorf("failed to connect to target: %w", err)
		}
	}

	fetchAll := sq.Select(columns...).From(target.Config.Table).OrderBy(primaryKey)

	targetEntries, targetMap, err := getEntries(target, fetchAll, primaryKeyIndex)
	if err != nil {
		return "", false, err
	}

	targetChecksum, err := checksumData(targetEntries)
	if err != nil {
		return "", false, err
	}

	// If the checksums match, then the data is already in sync
	if sourceChecksum == targetChecksum {
		return targetChecksum, false, nil
	}

	tableName := target.Config.Table

	// Iterate over source rows and perform INSERTs or UPDATEs as needed
	for key, val := range sourceMap {
		// If the key doesn't exist in targetMap, then we need to INSERT
		if _, ok := targetMap[key]; !ok {
			insert := sq.Insert(tableName).Columns(columns...).Values(val...)

			if _, err := insert.RunWith(target.DB).Exec(); err != nil {
				return "", false, err
			}
		} else {
			// If the key exists in targetMap, then we need to check if there is a diff

			// Remove the key from the targetMap (to keep track of which rows we need to delete)
			delete(targetMap, key)

			if cmp.Equal(val, targetMap[key]) {
				continue // No diff, so we skip this row
			}

			// There is a diff, perform an UPDATE
			update := sq.Update(tableName).Where(sq.Eq{primaryKey: key})

			for i, col := range columns {
				if col == primaryKey {
					continue
				}

				update = update.Set(col, val[i])
			}

			if _, err := update.RunWith(target.DB).Exec(); err != nil {
				return "", false, err
			}
		}
	}

	// Iterate over target rows and DELETE any that weren't in the source
	for key := range targetMap {
		delete := sq.Delete(tableName).Where(sq.Eq{primaryKey: key})

		if _, err := delete.RunWith(target.DB).Exec(); err != nil {
			return "", false, err
		}
	}

	return targetChecksum, true, nil
}

func checksumData(data [][]any) (string, error) {
	// Serialize the data to JSON
	jsonData, err := json.Marshal(data)
	if err != nil {
		return "", err
	}

	// Compute the MD5 checksum of the JSON data
	hash := md5.New()
	if _, err := hash.Write(jsonData); err != nil {
		return "", err
	}

	// Convert the checksum to a hexadecimal string
	checksum := hex.EncodeToString(hash.Sum(nil))
	return checksum, nil
}

func getEntries(
	table Table,
	query sq.SelectBuilder,
	primaryKeyIndex int,
) ([][]any, map[any][]any, error) {
	sql, args, err := query.ToSql()
	if err != nil {
		return nil, nil, err
	}

	rows, err := table.Queryx(sql, args...)
	if err != nil {
		return nil, nil, err
	}

	defer rows.Close()

	entryList := [][]any{}
	entryMap := map[any][]any{}

	for rows.Next() {
		cols, err := rows.SliceScan()
		if err != nil {
			return nil, nil, err
		}

		entryList = append(entryList, cols)

		pk := cols[primaryKeyIndex]
		entryMap[pk] = cols
	}

	if err = rows.Err(); err != nil {
		return nil, nil, err
	}

	return entryList, entryMap, nil
}
