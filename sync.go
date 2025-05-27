package sync

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"sync"

	sq "github.com/Masterminds/squirrel"
)

// SyncResult contains the results of syncing a single target table
type SyncResult struct {
	Target         TableConfig
	TargetChecksum string
	Synced         bool
	Error          error
}

func (job JobConfig) syncTargets() (string, []SyncResult, error) {
	primaryKeyIndices := job.getPrimaryKeyIndices()

	source := table{
		config:            job.Source,
		primaryKeys:       job.PrimaryKeys,
		primaryKeyIndices: primaryKeyIndices,
		columns:           job.Columns,
	}

	// Connect to the source
	if err := source.connect(); err != nil {
		return "", nil, err
	}

	targets := make([]table, len(job.Targets))
	for i, target := range job.Targets {
		targets[i] = table{
			config:            target,
			primaryKeys:       job.PrimaryKeys,
			primaryKeyIndices: primaryKeyIndices,
			columns:           job.Columns,
		}
	}

	// Get all rows from the source table and put them in a map by their primary key
	sourceEntries, sourceMap, err := source.getEntries()
	if err != nil {
		return "", nil, err
	}

	// Close the source connection pool
	source.Close()

	sourceChecksum, err := checksumData(sourceEntries)
	if err != nil {
		return "", nil, err
	}

	var wg sync.WaitGroup
	resultChan := make(chan SyncResult, len(targets))

	for _, target := range targets {
		wg.Add(1)
		go func(target table) {
			defer wg.Done()

			// Connect to each target
			if err := target.connect(); err != nil {
				resultChan <- SyncResult{
					Target: target.config,
					Error:  err,
				}
				return
			}

			checksum, synced, err := target.syncTarget(sourceChecksum, sourceMap)
			target.Close() // Close the target's connection pool

			resultChan <- SyncResult{
				Target:         target.config,
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

func (t table) syncTarget(
	sourceChecksum string,
	sourceMap map[primaryKeyTuple][]any,
) (string, bool, error) {
	targetEntries, targetMap, err := t.getEntries()
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

	tableName := t.config.Table

	var inserts []sq.InsertBuilder
	var updates []sq.UpdateBuilder
	var deletes []sq.DeleteBuilder

	// Iterate over source rows and perform INSERTs or UPDATEs as needed
	for key, val := range sourceMap {
		// If the key doesn't exist in targetMap, then we need to INSERT
		if _, ok := targetMap[key]; !ok {
			insert := sq.Insert(tableName).Columns(t.columns...).Values(val...)
			inserts = append(inserts, insert)
		} else {
			// If the key exists in targetMap, then we need to check if there is a diff

			// Remove the key from the targetMap (to keep track of which rows we need to delete)
			delete(targetMap, key)

			if reflect.DeepEqual(val, targetMap[key]) {
				continue // No diff, so we skip this row
			}

			// There is a diff, perform an UPDATE
			update := sq.
				Update(tableName).
				Where(key.whereClause(t.columns, t.primaryKeyIndices))

			pkSet := map[string]struct{}{}
			for _, pk := range t.primaryKeys {
				pkSet[pk] = struct{}{}
			}

			var hasUpdate bool
			for i, col := range t.columns {
				if _, ok := pkSet[col]; ok {
					continue // Skip updating primary key columns
				}

				update = update.Set(col, val[i])
				hasUpdate = true
			}

			if hasUpdate {
				updates = append(updates, update)
			}
		}
	}

	// Iterate over target rows and DELETE any that weren't in the source
	for key := range targetMap {
		delete := sq.
			Delete(tableName).
			Where(key.whereClause(t.columns, t.primaryKeyIndices))

		deletes = append(deletes, delete)
	}

	// Actually execute the statements (DELETEs -> UPDATEs -> INSERTs)
	for _, delete := range deletes {
		if _, err := delete.RunWith(t.DB).Exec(); err != nil {
			return "", false, err
		}
	}

	for _, update := range updates {
		if _, err := update.RunWith(t.DB).Exec(); err != nil {
			return "", false, err
		}
	}

	for _, insert := range inserts {
		if _, err := insert.RunWith(t.DB).Exec(); err != nil {
			return "", false, err
		}
	}

	return targetChecksum, true, nil
}

func (t table) getEntries() ([][]any, map[primaryKeyTuple][]any, error) {
	fetchAll := sq.
		Select(t.columns...).
		From(t.config.Table).
		OrderBy(t.primaryKeys...)

	sql, args, err := fetchAll.ToSql()
	if err != nil {
		return nil, nil, err
	}

	rows, err := t.Queryx(sql, args...)
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
		for i, idx := range t.primaryKeyIndices {
			val := cols[idx]

			// Convert []byte to string (because []byte is unhashable and can't be in a map key)
			if _, ok := val.([]byte); ok {
				val = string(val.([]byte))
			}

			switch i {
			case 0:
				pkTuple.First = val
			case 1:
				pkTuple.Second = val
			case 2:
				pkTuple.Third = val
			}
		}

		entryMap[pkTuple] = cols
	}

	if err = rows.Err(); err != nil {
		return nil, nil, err
	}

	return entryList, entryMap, nil
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

func (job JobConfig) getPrimaryKeyIndices() []int {
	// Create a map of column names to their index in the columns slice
	columnIndices := map[string]int{}
	for i, col := range job.Columns {
		columnIndices[col] = i
	}

	// Determine the indices of the primary keys in the columns slice
	var primaryKeyIndices []int
	for _, pk := range job.PrimaryKeys {
		if _, ok := columnIndices[pk]; ok {
			primaryKeyIndices = append(primaryKeyIndices, columnIndices[pk])
		}
	}

	return primaryKeyIndices
}

// We are not allowed to have a slice as a map key, so we use a struct instead
// For now, we limit to a maximum of 3 primary key columns
type primaryKeyTuple struct{ First, Second, Third any }

func (key primaryKeyTuple) whereClause(columns []string, primaryKeyIndices []int) sq.Eq {
	where := sq.Eq{}

	for i, idx := range primaryKeyIndices {
		columnName := columns[idx]

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
