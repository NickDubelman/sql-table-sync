package sync

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
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
	primaryKeys []string,
	columns []string,
	source Table,
	targets []Table,
) (string, []SyncResult, error) {
	if source.DB == nil {
		return "", nil, fmt.Errorf("source unreachable")
	}

	var primaryKeyIndices []int

	// Create a map of column names to their index in the columns slice
	columnIndices := map[string]int{}
	for i, col := range columns {
		columnIndices[col] = i
	}

	// Determine the indices of the primary keys in the columns slice
	for _, pk := range primaryKeys {
		if _, ok := columnIndices[pk]; ok {
			primaryKeyIndices = append(primaryKeyIndices, columnIndices[pk])
		}
	}

	order := strings.Join(primaryKeys, ", ")
	fetchAll := sq.Select(columns...).From(source.Config.Table).OrderBy(order)

	// Get all rows from the source table and put them in a map by their primary key
	sourceEntries, sourceMap, err := getEntries(source, fetchAll, primaryKeyIndices)
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
				primaryKeys,
				primaryKeyIndices,
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
	primaryKeys []string,
	primaryKeyIndices []int,
	columns []string,
	sourceChecksum string,
	sourceMap map[primaryKeyTuple][]any,
) (string, bool, error) {
	if target.DB == nil {
		var err error
		target, err = Connect(target.Config)
		if err != nil {
			return "", false, fmt.Errorf("failed to connect to target: %w", err)
		}
	}

	order := strings.Join(primaryKeys, ", ")
	fetchAll := sq.Select(columns...).From(target.Config.Table).OrderBy(order)

	targetEntries, targetMap, err := getEntries(target, fetchAll, primaryKeyIndices)
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
			where := getWhereClauseFromPK(key, primaryKeys, primaryKeyIndices)
			update := sq.Update(tableName).Where(where)

			pkSet := map[string]struct{}{}
			for _, pk := range primaryKeys {
				pkSet[pk] = struct{}{}
			}

			for i, col := range columns {
				if _, ok := pkSet[col]; ok {
					continue // Skip updating primary key columns
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
		where := getWhereClauseFromPK(key, primaryKeys, primaryKeyIndices)
		delete := sq.Delete(tableName).Where(where)

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
	primaryKeyIndices []int,
) ([][]any, map[primaryKeyTuple][]any, error) {
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
	entryMap := map[primaryKeyTuple][]any{}

	for rows.Next() {
		cols, err := rows.SliceScan()
		if err != nil {
			return nil, nil, err
		}

		entryList = append(entryList, cols)

		pkTuple := primaryKeyTuple{}
		for i, idx := range primaryKeyIndices {
			switch i {
			case 0:
				pkTuple.First = cols[idx]
			case 1:
				pkTuple.Second = cols[idx]
			case 2:
				pkTuple.Third = cols[idx]
			}
		}

		entryMap[pkTuple] = cols
	}

	if err = rows.Err(); err != nil {
		return nil, nil, err
	}

	return entryList, entryMap, nil
}

// We are not allowed to have a slice as a map key, so we use a struct instead
// For now, we limit to a maximum of 3 primary key columns
type primaryKeyTuple struct {
	First  any
	Second any
	Third  any
}

// getWhereClauseFromPK returns a WHERE clause for the given primary key tuple
func getWhereClauseFromPK(
	key primaryKeyTuple,
	primaryKeys []string,
	primaryKeyIndices []int,
) sq.Eq {
	where := sq.Eq{}

	for i, idx := range primaryKeyIndices {
		columnName := primaryKeys[idx]

		switch i {
		case 0:
			where[columnName] = key.First
		case 1:
			where[columnName] = key.Second
		case 2:
			where[columnName] = key.Third
		}
	}

	return where
}
