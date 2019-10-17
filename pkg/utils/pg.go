package utils

import (
	"github.com/moiot/gravity/pkg/config"
	"database/sql"
	"fmt"
	"net/url"
	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"
)

// CreateDB creates db connection using the cfg
func CreateDB(cfg *config.DBPGConfig) (*sql.DB, error) {
	if err := cfg.ValidateAndSetDefault();err != nil {
		return nil, errors.Trace(err)
	}
	dbDSN := fmt.Sprintf("postgres://%s:%s@%s:%d/?interpolateParams=true&timeout=%s&readTimeout=%s&writeTimeout=%s&parseTime=true&collation=utf8mb4_general_ci",
		cfg.Username, cfg.Password, cfg.Host, cfg.Port,  url.QueryEscape(cfg.Schema), cfg.Timeout, cfg.ReadTimeout, cfg.WriteTimeout)
	if cfg.Location != "" {
		dbDSN += "&loc=" + url.QueryEscape(cfg.Location)
	}
	log.Infof("DSN is %s", dbDSN)
	db, err := sql.Open("postgres", dbDSN)
	if err != nil {
		return nil, errors.Trace(err)
	}
	//判断数据库是否可以ping通
	if err = db.Ping(); err != nil {
		return nil, errors.Trace(err)
	}
	return db, nil
}

// CloseDB closes the db connection
func CloseDB(db *sql.DB) error {
	if db == nil {
		return nil
	}
	return errors.Trace(db.Close())
}





