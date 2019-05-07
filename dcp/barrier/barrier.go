package barrier

import (
	"database/sql"
	"time"

	"github.com/moiot/gravity/pkg/config"

	"github.com/moiot/gravity/pkg/utils"
	log "github.com/sirupsen/logrus"
)

const (
	DB_NAME           = "drc"
	TABLE_NAME        = "barrier"
	DB_TABLE_NAME     = DB_NAME + "." + TABLE_NAME
	RECORD_ID         = "1"
	OFFSET_COLUMN_SEQ = 1
)

type Config struct {
	Db            config.DBConfig
	TickerSeconds int
	TestDB        *sql.DB
}

func Start(config *Config, shutdown chan struct{}) {
	var db *sql.DB
	var err error
	if config.TestDB == nil {
		db, err = utils.CreateDBConnection(&config.Db)
		if err != nil {
			db.Close()
			log.Errorf("error connect to db %+v. %s", config.Db, err.Error())
		}
	} else {
		db = config.TestDB
	}

	_, err = db.Exec("CREATE database if not EXISTS " + DB_NAME)

	if err != nil {
		db.Close()
		log.Panic("error creating barrier db", err)
	}

	_, err = db.Exec(`
CREATE TABLE IF NOT EXISTS ` + DB_TABLE_NAME + ` (
  id int(11) unsigned NOT NULL AUTO_INCREMENT,
  offset BIGINT unsigned NOT NULL DEFAULT 0,
  ts timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;`)

	if err != nil {
		db.Close()
		log.Panic("error creating barrier table. ", err)
	}

	r, err := db.Exec("INSERT IGNORE INTO " + DB_TABLE_NAME + "(id, offset) VALUES (" + RECORD_ID + ", 0);")

	if err != nil {
		db.Close()
		log.Panic("error insert first record into barrier table. ", err)
	}

	affected, err := r.RowsAffected()
	if err != nil {
		db.Close()
		log.Panic("error get RowsAffected for insert ignore. ", err)
	}

	if affected == 0 {
		updateOffset(db, time.Now())
	}

	ticker := time.NewTicker(time.Second * time.Duration(config.TickerSeconds))
	go barrierLoop(ticker, db, shutdown)
}

func barrierLoop(ticker *time.Ticker, db *sql.DB, shutdown chan struct{}) {
	defer db.Close()
	for {
		select {
		case <-shutdown:
			ticker.Stop()
			log.Info("barrier stopped")
			return

		case t := <-ticker.C:
			updateOffset(db, t)
		}
	}
}
func updateOffset(db *sql.DB, t time.Time) {
	_, err := db.Exec("UPDATE " + DB_TABLE_NAME + " SET offset = offset + 1 WHERE id = " + RECORD_ID + ";")
	if err != nil {
		log.Error("barrier error ", err, " at ticker ", t)
		return
	}
	log.Info("barrier ticker at ", t)
}
