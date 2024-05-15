# sql-table-sync

A Go library and CLI for syncing SQL tables.

## Library Usage

TODO:

```go
func main() {
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
    userResult := results["users"]
    userErr := errs["users"]

    // "Ping" a single jobs by name, to make sure sources/targets are reachable and tables exist
    results, err := config.PingJob(jobName, 30*time.Second)

    // "Ping" all jobs
    allResults, err := config.PingAllJobs(30 * time.Second)
    userPingResult := allResults["users"]
}
```

## CLI Usage

TODO:

By default, the CLI will look for a file named `sync-config.yaml` in the current directory. You can specify a different file with the `--config` flag.

```bash
# Exec a single job
sql-table-sync exec users

# Exec all jobs
sql-table-sync exec

# Ping all jobs
sql-table-sync ping

# Ping a single job
sql-table-sync ping users
```

## Configuration

TODO:
