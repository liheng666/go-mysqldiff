package main

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"fmt"
	"io/ioutil"
	"encoding/json"
	"regexp"
	"log"
)

type mysqlDiff struct {
	Conns               map[string]*sql.DB
	successAddTables    []string
	successRepairTables []string
	errorAddTables      []string
	errorRepairTables   []string
	addTables           []string            // 需要添加的表
	repairTables        map[string][]string // 需要修复的表
}

type DbConfig map[string]string

func main() {
	md := &mysqlDiff{
		Conns:        make(map[string]*sql.DB),
		repairTables: make(map[string][]string),
	}
	config, err := readJsonFile("./config.json")
	checkErr(err)
	//fmt.Println(config)
	md.connDb(config)
	md.contrastDd()
	// 添加确实的表
	md.addTable()
	md.repairTable()

	fmt.Println("添加的表:")
	var tableNameStr string
	for _, tableName := range md.addTables {
		tableNameStr = tableNameStr + " " + tableName
	}
	fmt.Println(tableNameStr)
	fmt.Println("-------------------------")

	fmt.Println("修复的表:")
	for tableName, fieldSlice := range md.repairTables {
		repairStr := tableName + ":"
		for _, field := range fieldSlice {
			repairStr = repairStr + " " + field
		}
		fmt.Println(repairStr)
	}
	fmt.Println("-------------------------")

}

func (md *mysqlDiff) connDb(config map[string]DbConfig) {
	DSN1 := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8",
		config["master"]["user"], config["master"]["pwd"], config["master"]["host"], config["master"]["port"], config["master"]["db"])
	db1, err := sql.Open("mysql", DSN1)
	checkErr(err)
	md.Conns["master"] = db1

	DSN2 := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8",
		config["slave"]["user"], config["slave"]["pwd"], config["slave"]["host"], config["slave"]["port"], config["slave"]["db"])
	db2, err := sql.Open("mysql", DSN2)
	checkErr(err)
	md.Conns["slave"] = db2
}

func (md *mysqlDiff) contrastDd() {
	masterDb := md.Conns["master"]
	master := readDatabaseTables(masterDb)

	slaveDb := md.Conns["slave"]
	slave := readDatabaseTables(slaveDb)

	for _, masterTableName := range master {
		if !in_slice(masterTableName, slave) {
			md.addTables = append(md.addTables, masterTableName)
		} else {
			fieldDiff := md.fieldDiff(masterTableName)
			if fieldDiff != nil {
				md.repairTables[masterTableName] = fieldDiff
			}
		}
	}
}

func (md *mysqlDiff) fieldDiff(tableName string) ([]string) {
	var data []string

	master, err := readTableField(md.Conns["master"], tableName)
	checkErr(err)

	slave, err := readTableField(md.Conns["slave"], tableName)
	checkErr(err)

	for _, masterField := range master {
		if !in_slice(masterField, slave) {
			data = append(data, masterField)
		}
	}

	return data
}

func (md *mysqlDiff) getTableCreateSql(tableName string) string {
	sql := "show create table " + tableName
	rowsMaster, err := md.Conns["master"].Query(sql)
	checkErr(err)
	var str string
	rowsMaster.Next()
	rowsMaster.Scan(&str, &str)
	return str
}

func (md *mysqlDiff) addTable() {
	for _, name := range md.addTables {
		sql := md.getTableCreateSql(name)
		_, err := md.Conns["slave"].Query(sql)
		checkErr(err)
	}
}

func (md *mysqlDiff) repairTable() {
	for tableName, fieldSlice := range md.repairTables {
		createSql := md.getTableCreateSql(tableName)

		for _, field := range fieldSlice {
			pattern := fmt.Sprintf("`%s`.*?(,|\n)", field)
			re, err := regexp.Compile(pattern)
			checkErr(err)
			sql := re.FindString(createSql)
			if sql == "" {
				continue
			}

			bytes := []rune(sql)
			bytes = bytes[:len(bytes)-1]
			sql = string(bytes)

			sql = fmt.Sprintf("alter table %s add %s", tableName, sql)
			_, err = md.Conns["slave"].Query(sql)
			checkErr(err)
		}
	}
}

func in_slice(search string, sli []string) (bool) {
	for _, str := range sli {
		if search == str {
			return true
		}
	}
	return false
}

func readDatabaseTables(db *sql.DB) ([]string) {
	var data []string
	rowsMaster, err := db.Query("show tables")
	checkErr(err)

	for rowsMaster.Next() {
		var str string
		rowsMaster.Scan(&str)
		data = append(data, str)
	}
	return data
}

func readTableField(db *sql.DB, tableName string) ([]string, error) {
	var data []string
	rowsMaster, err := db.Query("desc " + tableName)
	if err != nil {
		fmt.Printf("%v", err)
		return nil, err
	}

	for rowsMaster.Next() {
		var str string
		var str1 string
		rowsMaster.Scan(&str, &str1, &str1, &str1, &str1, &str1)
		data = append(data, str)
	}
	return data, nil
}

func readJsonFile(filename string) (map[string]DbConfig, error) {
	var config = make(map[string]DbConfig)
	bytes, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(bytes, &config)
	checkErr(err)
	return config, nil
}

func checkErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
