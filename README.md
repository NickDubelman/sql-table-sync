# sql-table-sync

A Go library and CLI for syncing SQL tables.

> [!CAUTION]
> This should only be used for "small" tables. It has not been benchmarked, so it is not clear how much data it can handle.

## Library Usage

### LoadConfig

Before you can exec or ping jobs, you must initialize a `Config`. The intended way to do this is to load it from a YAML file. `LoadConfig` takes a file path, validates it, and returns a `Config` object.

```go
cfg, err := sync.LoadConfig("example_config.yaml")

// All of the behavior exposed by this library is done via the `Config` object:
result, err := cfg.ExecJob("users")
resultMap, errMap := cfg.ExecAllJobs()
results, err := cfg.PingJob("users", timeout)
resultsMap, err := cfg.PingAllJobs(timeout)
```

For an example config file, please see [sample_config.yaml](sample_config.yaml).

For more information on the config file format (including default values), see [Configuration](#configuration).

### ExecJob

This takes a `jobName` and executes the corresponding job. Executing a job attempts to sync the job's source table to all of its target tables. It returns a `ExecJobResult` and an error.

`ExecJobResult` contains:

- the `Checksum` of the source table
- an array of `Results`, which is the `SyncResult` for each target table

`SyncResult` contains:

- the `Target` table definition
- the `TargetChecksum`
- a `Synced` boolean (false if the target's checksum was already the same as the source's)
- an `Error` (if one occurred)

### ExecAllJobs

This executes all of the jobs in the configuration. It returns:

- a map of job names to the corresponding `ExecJobResult`
- a map of job names to the corresponding error (if one occurred)

### PingJob

> [!TIP]
> You can use this to validate that your configuration has the correct database credentials, the tables exist, the columns exist, etc.

This takes a `jobName` and a `timeout`. It attempts to "ping" the job's source and target tables-- this is a health check that ensures each table is:

- reachable
- has the correct credentials
- exists
- has the expected columns

It returns a list of `PingResult` and an error. The first element in the list is the result of pinging the source table. Each subsequent element is the result of pinging a target table (no particular order).

`PingResult` contains:

- the `Config` definition of the table that was pinged
- an `Error` enounctered while pinging (if one occurred)

### PingAllJobs

This pings all of the jobs in the configuration. It returns:

- a map of job names to the corresponding list of `PingResult`
- a single error (if one occurred)

### Full Example

```go
// Example config file
const _ = `
  defaults:
    driver: mysql

  jobs:
    users:
      columns: [id, name, age]
      source:
        table: users
        db: app
        host: somehost.com
        port: 3420
      targets:
        - table: users
          db: app
          host: someotherhost.com
          port: 3420
        - table: users
          db: app
          host: yetanotherhost.com
          port: 3420
`

// Load a config file
cfg, err := sync.LoadConfig("example_config.yaml")

// Exec a single job by name
result, err := cfg.ExecJob("users")

// Exec all jobs
results, errs := cfg.ExecAllJobs()
usersResult := results["users"]
usersErr := errs["users"]

// "Ping" a single jobs by name, to make sure sources/targets are reachable and tables exist
results, err := config.PingJob(jobName, 30*time.Second)

// "Ping" all jobs
allResults, err := config.PingAllJobs(30 * time.Second)
usersPingResult := allResults["users"]
```

## CLI Usage

By default, the CLI will look for a file named `sync-config.yaml` in the current directory. You can specify a different file with the `--config` flag.

```bash
# Exec a single job
sql-table-sync exec users

# Exec multiple jobs
sql-table-sync exec users pets posts

# Exec all jobs
sql-table-sync exec

# Ping a single job (with default 10s timeout)
sql-table-sync ping users

# Ping a single job (with custom timeout)
sql-table-sync ping users --timeout 5s

# Ping multiple jobs
sql-table-sync ping users pets posts

# Ping all jobs
sql-table-sync ping
```

## Configuration

A config file consists of two top-level sections: `defaults` (optional) and `jobs`. The `defaults` section allows you to specify your own custom default values for jobs. The `jobs` section is a map of _names_ to corresponding job definitions.

### Job Definition

- `columns` is a list of column names for the source and target tables.
- `primaryKey` (optional) is the name of the primary key column, which is used to uniquely identify rows. This must be a subset of `columns`. (Default: `id`)
- `primaryKeys` (optional) is a list of primary key column names (for cases where the primary key is a composite key). These must be a subset of `columns`.
- `source` is the table whose data we want to sync _from_.
- `targets` are the tables we want to sync data _to_.

### Table Definition

- `label` (optional) is a human-readable name for the table. This is used in logs and error messages. (Default: If no label is provided, one of the following is used `DSN`, `Host:Port`, `Host`, `:Port`)
- `table` is the name of the table.
- `driver` is the SQL driver to use. (For now, only `mysql` and `sqlite3` are supported.)
- `dsn` (optional) is the data source name for the database connection. This is driver-specific. ([mysql](https://github.com/go-sql-driver/mysql?tab=readme-ov-file#dsn-data-source-name), [sqlite3](https://github.com/mattn/go-sqlite3?tab=readme-ov-file#connection-string)). If `DSN` is not provided, it will be automatically inferred from the below fields.
- `user` (optional) is the username for the database connection.
- `password` (optional) is the password for the database connection.
- `host` (optional) is the hostname for the database connection.
- `port` (optional) is the port for the database connection.
- `db` (optional) is the name of the database.

### User-provided defaults

The `defaults` section allows you to specify your own custom default values. These can either be global (affects all jobs) or host-specific (affects only jobs with a matching host). You can also specify a default `source` and default `targets`.

#### Global Defaults

- `driver` is the SQL driver to use. (For now, only `mysql` and `sqlite3` are supported.)

#### Host-specific Defaults

In the `defaults` section, you can specify `hosts` which is a mapping of hostnames to host-specific defaults. These defaults will be applied to jobs with a matching host.

- `label` is a human-readable name for the host. This is used in logs and error messages.
- `driver` is the SQL driver to use. (For now, only `mysql` and `sqlite3` are supported.)
- `dsn` is the data source name for the database connection. This is driver-specific. ([mysql](https://github.com/go-sql-driver/mysql?tab=readme-ov-file#dsn-data-source-name), [sqlite3](https://github.com/mattn/go-sqlite3?tab=readme-ov-file#connection-string)).
- `user` is the username for the database connection.
- `password` is the password for the database connection.
- `port` is the port for the database connection.
- `db` is the name of the database.

#### Default Source

In the `defaults` section, you can specify `source`. This specifies the default db connection parameters (DSN, host, port, etc) but does NOT include the table-- so each job must still specify a `source.table`.

> [!WARNING]  
> When using `defaults.source`, you must still specify a `source.table` for each job.

#### Default Targets

In the `defaults` section, you can specify `targets`. This allows you to omit the `targets` section in each job definition. This can only be used if each target table has the same name as the source table.

> [!WARNING]  
> When using `defaults.targets`, each target table must have the same name as the source table.

## Sync Algorithm

1. The rows of the source table are put into a map, where the key is the primary key and the value is the full row.
1. The rows of each target table are put into a similar map.
1. The source map is iterated over. For each row:
   - If the row is not in the target map, it is inserted.
   - If the row is in the target map, but the value is different, it is updated.
1. The target map is iterated over. For each row:
   - If the row is not in the source map, it is deleted.

In order to determine if a target needs to be synced, an MD5 checksum is calculated for the source and target tables. If the checksums are the same, the target is considered "synced" and no sync is performed.
