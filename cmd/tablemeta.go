package cmd

import (
	"fmt"
)

var (
	columnName             string
	dataType               string
	characterMaximumLength string
	isNullable             string
	columnDefault          string
	numericPrecision       string
	numericScale           string
	datetimePrecision      string
	columnKey              string
	columnComment          string
	ordinalPosition        int
	destType               string
	destNullable           string
	destDefault            string
)

func TableMeta(tableMap map[string][]string) {
	// 获取tableMap键值对中的表名
	for tblName, _ := range tableMap {
		var colTotal int
		pgCreateTbl := "create table " + tblName + "("
		// 查询当前表总共有多少个列字段
		colTotalSql := fmt.Sprintf("select count(*) from information_schema.COLUMNS  where table_schema=database() and table_name='%s'", tblName)
		err := srcDb.QueryRow(colTotalSql).Scan(&colTotal)
		if err != nil {
			log.Error(err)
		}
		// 查询MySQL表结构
		sql := fmt.Sprintf("select concat('\"',lower(column_name),'\"'),data_type,ifnull(character_maximum_length,'null'),is_nullable,case  column_default when '( \\'user\\' )' then 'user' else ifnull(column_default,'null') end as column_default,ifnull(numeric_precision,'null'),ifnull(numeric_scale,'null'),ifnull(datetime_precision,'null'),ifnull(column_key,'null'),ifnull(column_comment,'null'),ORDINAL_POSITION from information_schema.COLUMNS where table_schema=database() and table_name='%s'", tblName)
		//fmt.Println(sql)
		rows, err := srcDb.Query(sql)
		if err != nil {
			log.Error(err)
		}
		// 遍历MySQL表字段,一行就是一个字段的基本信息
		for rows.Next() {
			if err := rows.Scan(&columnName, &dataType, &characterMaximumLength, &isNullable, &columnDefault, &numericPrecision, &numericScale, &datetimePrecision, &columnKey, &columnComment, &ordinalPosition); err != nil {
				log.Error(err)
			}
			//fmt.Println(columnName,dataType,characterMaximumLength,isNullable,columnDefault,numericPrecision,numericScale,datetimePrecision,columnKey,columnComment,ordinalPosition)
			//适配MySQL字段类型到PostgreSQL字段类型
			// 列字段是否允许null
			switch isNullable {
			case "NO":
				destNullable = "not null"
			default:
				destNullable = "null"
			}
			// 列字段default默认值的处理
			switch {
			case columnDefault != "null": // 默认值不是null并且是字符串类型下面就需要使用fmt.Sprintf格式化让字符串单引号包围，否则这个字符串是没有引号包围的
				if dataType == "varchar" {
					destDefault = fmt.Sprintf("default '%s'", columnDefault)
				} else if dataType == "char" {
					destDefault = fmt.Sprintf("default '%s'", columnDefault)
				} else {
					destDefault = fmt.Sprintf("default %s", columnDefault) // 非字符串类型无需使用单引号包围
				}
			default:
				destDefault = "" // 如果没有默认值，默认值就是空字符串，即目标没有默认值
			}
			// 列字段类型的处理
			switch dataType {
			case "int", "mediumint", "tinyint":
				destType = "int"
			case "varchar":
				destType = "varchar(" + characterMaximumLength + ")"
			case "char":
				destType = "char(" + characterMaximumLength + ")"
			case "text", "tinytext", "mediumtext", "longtext":
				destType = "text"
			case "datetime", "timestamp":
				destType = "timestamp"
			case "decimal", "double", "float":
				if numericScale == "null" {
					destType = "decimal(" + numericPrecision + ")"
				} else {
					destType = "decimal(" + numericPrecision + "," + numericScale + ")"
				}
			case "tinyblob", "blob", "mediumblob", "longblob":
				destType = "bytea"
			// 其余类型，源库使用什么类型，目标库就使用什么类型
			default:
				destType = dataType
			}
			// 在目标库创建的语句
			pgCreateTbl += fmt.Sprintf("%s %s %s %s,", columnName, destType, destNullable, destDefault)
			if ordinalPosition == colTotal {
				pgCreateTbl = pgCreateTbl[:len(pgCreateTbl)-1] + ")" // 最后一个列字段结尾去掉逗号,并且加上语句的右括号
			}
		}
		//fmt.Println(pgCreateTbl) // 打印创建表语句
		// 创建前先删除目标表
		dropDestTbl := "drop table if exists " + tblName + " cascade"
		if _, err = destDb.Exec(dropDestTbl); err != nil {
			log.Error(err)
		}
		// 创建PostgreSQL表结构
		log.Info("Processing create table " + tblName)
		if _, err = destDb.Exec(pgCreateTbl); err != nil {
			log.Error("table ", tblName, " create failed", err)
		}
	}
	log.Info("Table structure synced from MySQL to PostgreSQL")
}
