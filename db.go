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

func (t *Table) connect() error {
	if t.DB != nil {
		return nil // Already connected
	}

	dsn := t.Config.DSN

	if dsn == "" {
		// If DSN is not directly provided, construct it from the other fields
		if t.Config.Driver == "mysql" {
			cfg := mysql.NewConfig()

			cfg.User = t.Config.User
			cfg.Passwd = t.Config.Password
			cfg.Addr = fmt.Sprintf("%s:%d", t.Config.Host, t.Config.Port)
			cfg.DBName = t.Config.DB
			cfg.Net = "tcp"

			dsn = cfg.FormatDSN()
		} else if t.Config.Driver == "sqlite3" {
			return fmt.Errorf("for sqlite3, DSN must be provided directly")
		} else {
			return fmt.Errorf("unsupported driver: %s", t.Config.Driver)
		}
	}

	var err error
	t.DB, err = sqlx.Connect(t.Config.Driver, dsn)
	return err
}
