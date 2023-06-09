package cmd

import (
	"database/sql"
	"fmt"
	"github.com/spf13/viper"
	"gomysql2pg/connect"
	"time"
)

var srcDb *sql.DB
var destDb *sql.DB

func getConn() (connStr *connect.DbConnStr) {
	connStr = new(connect.DbConnStr)
	connStr.SrcHost = viper.GetString("src.host")
	connStr.SrcUserName = viper.GetString("src.username")
	connStr.SrcPassword = viper.GetString("src.password")
	connStr.SrcDatabase = viper.GetString("src.database")
	connStr.SrcPort = viper.GetInt("src.port")
	connStr.DestHost = viper.GetString("dest.host")
	connStr.DestPort = viper.GetInt("dest.port")
	connStr.DestUserName = viper.GetString("dest.username")
	connStr.DestPassword = viper.GetString("dest.password")
	connStr.DestDatabase = viper.GetString("dest.database")
	return connStr
}

func PrepareSrc(connStr *connect.DbConnStr) {
	// 生成源库连接
	srcHost := connStr.SrcHost
	srcUserName := connStr.SrcUserName
	srcPassword := connStr.SrcPassword
	srcDatabase := connStr.SrcDatabase
	srcPort := connStr.SrcPort
	srcConn := fmt.Sprintf("%s:%s@tcp(%s:%v)/%s?charset=utf8&maxAllowedPacket=0", srcUserName, srcPassword, srcHost, srcPort, srcDatabase)
	var err error
	srcDb, err = sql.Open("mysql", srcConn)
	if err != nil {
		log.Fatal("please check MySQL yml file", err)
	}
	c := srcDb.Ping()
	if c != nil {
		log.Fatal("connect MySQL failed", c)
	}
	srcDb.SetConnMaxLifetime(2 * time.Hour)
	srcDb.SetMaxIdleConns(0)
	srcDb.SetMaxOpenConns(30)
	log.Info("connect MySQL success")
}

func PrepareDest(connStr *connect.DbConnStr) {
	// 生成目标库连接
	destHost := connStr.DestHost
	destPort := connStr.DestPort
	destUserName := connStr.DestUserName
	destPassword := connStr.DestPassword
	destDatabase := connStr.DestDatabase
	conn := fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%v sslmode=disable", destHost,
		destUserName, destPassword, destDatabase, destPort)
	var err error
	destDb, err = sql.Open("postgres", conn)
	if err != nil {
		log.Fatal("please check Postgres yml file", err)
	}
	c := destDb.Ping()
	if c != nil {
		log.Fatal("connect Postgres failed", c)
	}
	log.Info("connect Postgres success")
}
