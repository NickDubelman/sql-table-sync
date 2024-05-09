# sql-table-sync

A Go library and CLI for syncing SQL tables.

## Library Usage

TODO:

```go
func main() {
    // Example config file
    const _ = `
        driver: mysql

        jobs:
          - name: users
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

    // "Ping" all jobs to make sure sources/targets are reachable and tables exist
    pingResults, err := cfg.Ping(30 * time.Second)
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
