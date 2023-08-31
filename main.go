package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
	_ "github.com/go-sql-driver/mysql"
	"gopkg.in/ini.v1"
)

const (
	CONFFILENAME = "app.conf" //configuration file name
)

type Config struct {
	SERVER    string `ini:"SERVER"`
	PORT      int    `ini:"PORT"`
	NETWORK   string `ini:"NETWORK"`
	USERNAME  string `ini:"USERNAME"`
	PASSWORD  string `ini:"PASSWORD"`
	DATABASE  string `ini:"DATABASE"`
	TABLENAME string `ini:"TABLENAME"`
	BATCHNUM  int    `ini:"BATCHNUM"`
	TIMEFIELD string `ini:"TIMEFIELD"`
	STARTTIME string `ini:"STARTTIME"`
	ENDTIME   string `ini:"ENDTIME"`
}

func ReadConfig(path string) (Config, error) {
	var config Config
	conf, err := ini.Load(path) //load configuration from file
	if err != nil {
		log.Println("load config file fail!")
		return config, err
	}
	conf.BlockMode = false
	err = conf.MapTo(&config) //mapping to struct
	if err != nil {
		log.Println("mapto config file fail!")
		return config, err
	}
	return config, nil
}

func main() {
	// read configurations from ini file
	PthSep := string(os.PathSeparator)
	cmdDir := "."
	configFile := cmdDir + PthSep + CONFFILENAME
	config, err := ReadConfig(configFile)
	if err != nil {
		log.Fatal(err)
	}
	//dsn := fmt.Sprintf("%s:%s@%s(%s:%d)/%s",USERNAME,PASSWORD,NETWORK,SERVER,PORT,DATABASE)
	dsn := fmt.Sprintf("%s:%s@%s(%s:%d)/%s", config.USERNAME, config.PASSWORD, config.NETWORK, config.SERVER, config.PORT, config.DATABASE)
	//log.Println(dsn)
	DB, err := sql.Open("mysql", dsn)
	if err != nil {
		fmt.Printf("Connect mysql failed,err:%v\n", err)
		return
	}
	DB.SetConnMaxLifetime(10 * time.Second) //timeout setting
	DB.SetMaxOpenConns(10)                  //maximum active connections
	DB.SetMaxIdleConns(5)                   //minimum active connections
	defer DB.Close()

	// count records in table
	var totalRows int
	sqlString := fmt.Sprintf("select count(1) from %s where %s between '%s' and '%s'", config.TABLENAME,config.TIMEFIELD,config.STARTTIME,config.ENDTIME)// gain number of table records
	row := DB.QueryRow(sqlString)
	if err := row.Scan(&totalRows); err != nil {
		fmt.Printf("Select failed, err:%v", err)
		return
	}
	log.Println("Table", config.TABLENAME, "rows:", totalRows,"Sql:"+sqlString)
	if totalRows == 0{
		return
	}
	// start delete cycle
	var times int = totalRows / config.BATCHNUM
	for i := 0; i <= times; i++ {
		sqlString = fmt.Sprintf("delete from %s where %s between '%s' and '%s' limit %d",config.TABLENAME,config.TIMEFIELD,config.STARTTIME,config.ENDTIME,config.BATCHNUM)
		result, err := DB.Exec(sqlString)
		if err != nil {
			fmt.Printf("Delete failed, err:%v", err)
			return
		}

		RowsAff, err := result.RowsAffected()
		if err != nil {
			fmt.Printf("Delete failed, err:%v", err)
			return
		} else {
			log.Println("RowsAffected:", strconv.FormatInt(RowsAff, 10))
		}

		time.Sleep(time.Duration(1) * time.Second) //idle one second
	}
	return
}
