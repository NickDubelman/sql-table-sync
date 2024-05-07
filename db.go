package sync

import (
	"fmt"

	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
)

type Table struct {
	*sqlx.DB
	Config TableConfig
}

func Connect(config TableConfig) (Table, error) {
	dsn := config.DSN

	table := Table{Config: config}

	if dsn == "" {
		// If DSN is not directly provided, construct it from the other fields
		if config.Driver == "mysql" {
			cfg := mysql.NewConfig()

			cfg.User = config.User
			cfg.Passwd = config.Password
			cfg.Addr = fmt.Sprintf("%s:%d", config.Host, config.Port)
			cfg.DBName = config.DB
			cfg.Net = "tcp"

			dsn = cfg.FormatDSN()
		} else if config.Driver == "sqlite3" {
			return table, fmt.Errorf("for sqlite3, DSN must be provided directly")
		} else {
			return table, fmt.Errorf("unsupported driver: %s", config.Driver)
		}
	}

	db, err := sqlx.Connect(config.Driver, dsn)
	if err != nil {
		return table, err
	}
	table.DB = db

	return table, nil
}
