package sync

import (
	"fmt"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
)

type table struct {
	*sqlx.DB
	config TableConfig

	primaryKeys       []string
	primaryKeyIndices []int // Indices of the primary keys in the Columns slice
	columns           []string
}

func (t *table) connect() error {
	if t.DB != nil {
		return nil // Already connected
	}

	dsn := t.config.DSN

	if dsn == "" {
		// If DSN is not directly provided, construct it from the other fields
		if t.config.Driver == "mysql" {
			cfg := mysql.NewConfig()

			cfg.User = t.config.User
			cfg.Passwd = t.config.Password
			cfg.Addr = fmt.Sprintf("%s:%d", t.config.Host, t.config.Port)
			cfg.DBName = t.config.DB
			cfg.Net = "tcp"

			dsn = cfg.FormatDSN()
		} else if t.config.Driver == "sqlite3" {
			return fmt.Errorf("for sqlite3, DSN must be provided directly")
		} else {
			return fmt.Errorf("unsupported driver: %s", t.config.Driver)
		}
	}

	var err error
	t.DB, err = sqlx.Connect(t.config.Driver, dsn)
	if err != nil {
		return err
	}

	t.DB.SetMaxOpenConns(5)
	t.DB.SetMaxIdleConns(5)
	t.DB.SetConnMaxLifetime(5 * time.Minute)

	return nil
}
