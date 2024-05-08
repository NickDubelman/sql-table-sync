package main

import (
	"fmt"
	"log"

	sq "github.com/Masterminds/squirrel"
	sync "github.com/NickDubelman/sql-table-sync"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

func main() {
	setup()

	cfg, err := sync.LoadConfig("sample_config.yaml")
	if err != nil {
		log.Fatal(err)
	}

	pingResult, err := cfg.Ping()
	if err != nil {
		fmt.Println(err)
	}

	for _, jobResult := range pingResult.Results {
		for _, result := range jobResult.Results {
			if result.Error != nil {
				fmt.Println(jobResult.Job.Name, result.Label, result.Error)
			}
		}
	}

	_, errs := cfg.ExecAllJobs()
	for _, err := range errs {
		if err != nil {
			log.Println(err)
		}
	}
}

func setup() {
	createTableStmt := func(name string) string {
		return fmt.Sprintf(
			`CREATE TABLE IF NOT EXISTS %s (
				id INT AUTO_INCREMENT PRIMARY KEY,
				name VARCHAR(255) NOT NULL,
				age INT NOT NULL
			)
			`,
			name,
		)
	}

	dsn := "root@tcp(localhost:3420)/app"

	db, err := sqlx.Connect("mysql", dsn)
	if err != nil {
		log.Fatal(err)
	}

	db.MustExec(createTableStmt("users_source"))
	db.MustExec(createTableStmt("users_target1"))
	db.MustExec(createTableStmt("users_target2"))

	// Insert some data into the source table
	insert := sq.
		Insert("users_source").
		Columns("name", "age").
		Values("Alice", 30).
		Values("Bob", 25).
		Values("Charlie", 35)

	if _, err := insert.RunWith(db).Exec(); err != nil {
		log.Fatal(err)
	}
}
