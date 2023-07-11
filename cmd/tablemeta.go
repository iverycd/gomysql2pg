package cmd

import (
	"fmt"
	"strconv"
	"time"
)

var tabRet []string
var tableCount int
var failedCount int

type Database interface {
	// TableCreate (logDir string, tableMap map[string][]string) (result []string) 单线程
	TableCreate(logDir string, tblName string, ch chan struct{})
	SeqCreate(logDir string) (result []string)
	IdxCreate(logDir string) (result []string)
	ViewCreate(logDir string) (result []string)
	FKCreate(logDir string) (result []string)
	TriggerCreate(logDir string) (result []string)
}

type Table struct {
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
	autoIncrement          int
	destSeqSql             string
	destDefaultSeq         string
	dropSeqSql             string
	destIdxSql             string
	viewSql                string
}

func (tb *Table) TableCreate(logDir string, tblName string, ch chan struct{}) {
	defer wg2.Done()
	var newTable Table
	tableCount += 1
	// 使用goroutine并发的创建多个表
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
		if err := rows.Scan(&newTable.columnName, &newTable.dataType, &newTable.characterMaximumLength, &newTable.isNullable, &newTable.columnDefault, &newTable.numericPrecision, &newTable.numericScale, &newTable.datetimePrecision, &newTable.columnKey, &newTable.columnComment, &newTable.ordinalPosition); err != nil {
			log.Error(err)
		}
		//fmt.Println(columnName,dataType,characterMaximumLength,isNullable,columnDefault,numericPrecision,numericScale,datetimePrecision,columnKey,columnComment,ordinalPosition)
		//适配MySQL字段类型到PostgreSQL字段类型
		// 列字段是否允许null
		switch newTable.isNullable {
		case "NO":
			newTable.destNullable = "not null"
		default:
			newTable.destNullable = "null"
		}
		// 列字段default默认值的处理
		switch {
		case newTable.columnDefault != "null": // 默认值不是null并且是字符串类型下面就需要使用fmt.Sprintf格式化让字符串单引号包围，否则这个字符串是没有引号包围的
			if newTable.dataType == "varchar" {
				newTable.destDefault = fmt.Sprintf("default '%s'", newTable.columnDefault)
			} else if newTable.dataType == "char" {
				newTable.destDefault = fmt.Sprintf("default '%s'", newTable.columnDefault)
			} else {
				newTable.destDefault = fmt.Sprintf("default %s", newTable.columnDefault) // 非字符串类型无需使用单引号包围
			}
		default:
			newTable.destDefault = "" // 如果没有默认值，默认值就是空字符串，即目标没有默认值
		}
		// 列字段类型的处理
		switch newTable.dataType {
		case "int", "mediumint", "tinyint":
			newTable.destType = "int"
		case "varchar":
			newTable.destType = "varchar(" + newTable.characterMaximumLength + ")"
		case "char":
			newTable.destType = "char(" + newTable.characterMaximumLength + ")"
		case "text", "tinytext", "mediumtext", "longtext":
			newTable.destType = "text"
		case "datetime", "timestamp":
			newTable.destType = "timestamp"
		case "decimal", "double", "float":
			if newTable.numericScale == "null" {
				newTable.destType = "decimal(" + newTable.numericPrecision + ")"
			} else {
				newTable.destType = "decimal(" + newTable.numericPrecision + "," + newTable.numericScale + ")"
			}
		case "tinyblob", "blob", "mediumblob", "longblob":
			newTable.destType = "bytea"
		// 其余类型，源库使用什么类型，目标库就使用什么类型
		default:
			newTable.destType = newTable.dataType
		}
		// 在目标库创建的语句
		pgCreateTbl += fmt.Sprintf("%s %s %s %s,", newTable.columnName, newTable.destType, newTable.destNullable, newTable.destDefault)
		if newTable.ordinalPosition == colTotal {
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
	log.Info(fmt.Sprintf("%v ProcessingID %s create table %s", time.Now().Format("2006-01-02 15:04:05.000000"), strconv.Itoa(tableCount), tblName))
	if _, err = destDb.Exec(pgCreateTbl); err != nil {
		log.Error("table ", tblName, " create failed ", err)
		LogError(logDir, "tableCreateFailed", pgCreateTbl, err)
		failedCount += 1
	}
	<-ch
}

func (tb *Table) SeqCreate(logDir string) (result []string) {
	startTime := time.Now()
	tableCount := 0
	failedCount := 0
	var tableName string
	// 查询MySQL自增列信息，批量生成创建序列sql
	sql := fmt.Sprintf("select table_name,COLUMN_NAME,Auto_increment,lower(concat('drop sequence if exists ','seq_',TABLE_NAME,'_',COLUMN_NAME,';')) drop_seq,lower(concat('create sequence ','seq_',TABLE_NAME,'_',COLUMN_NAME,' INCREMENT BY 1 START ',Auto_increment,';')) create_seq, lower(concat('alter table ',table_name,' alter column ',COLUMN_NAME, ' set default nextval(', '''' ,'seq_',TABLE_NAME,'_',COLUMN_NAME,  '''',');')) alter_default  from (select Auto_increment,column_name,a.table_name from (select TABLE_NAME, Auto_increment,case when Auto_increment  is not null then 'auto_increment' else '0' end ai from information_schema. TABLES where TABLE_SCHEMA =database() and  AUTO_INCREMENT is not null) a join (select table_name,COLUMN_NAME,EXTRA from information_schema. COLUMNS where TABLE_SCHEMA =database() and table_name in(select t.TABLE_NAME from information_schema. TABLES t where TABLE_SCHEMA =database() and AUTO_INCREMENT is not null)  and EXTRA='auto_increment' ) b on a.ai = b.EXTRA and a.table_name =b.table_name) aaa;")
	//fmt.Println(sql)
	rows, err := srcDb.Query(sql)
	if err != nil {
		log.Error(err)
	}
	// 从sql结果集遍历，获取到删除序列，创建序列，默认值为自增列
	for rows.Next() {
		tableCount += 1
		if err := rows.Scan(&tableName, &tb.columnName, &tb.autoIncrement, &tb.dropSeqSql, &tb.destSeqSql, &tb.destDefaultSeq); err != nil {
			log.Error(err)
		}
		// 创建前先删除目标序列
		if _, err = destDb.Exec(tb.dropSeqSql); err != nil {
			log.Error(err)
		}
		// 创建目标序列
		log.Info(fmt.Sprintf("%v ProcessingID %s create sequence %s", time.Now().Format("2006-01-02 15:04:05.000000"), strconv.Itoa(tableCount), tableName))
		if _, err = destDb.Exec(tb.destSeqSql); err != nil {
			log.Error("table ", tableName, " create sequence failed ", err)
			LogError(logDir, "seqCreateFailed", tb.destSeqSql, err)
			failedCount += 1
		}
		// 设置表自增列为序列，如果表不存并单独创建序列会有error但是毫无影响
		log.Info(fmt.Sprintf("%v ProcessingID %s set default sequence %s", time.Now().Format("2006-01-02 15:04:05.000000"), strconv.Itoa(tableCount), tableName))
		if _, err = destDb.Exec(tb.destDefaultSeq); err != nil {
			log.Error("table ", tableName, " set default sequence failed ", err)
			LogError(logDir, "seqCreateFailed", tb.destDefaultSeq, err)
			failedCount += 1
		}
	}
	endTime := time.Now()
	cost := time.Since(startTime)
	result = append(result, "Sequence", startTime.Format("2006-01-02 15:04:05.000000"), endTime.Format("2006-01-02 15:04:05.000000"), strconv.Itoa(failedCount), cost.String())
	log.Info("sequence count ", tableCount)
	return result
}

func (tb *Table) IdxCreate(logDir string) (result []string) {
	startTime := time.Now()
	failedCount := 0
	id := 0
	// 查询MySQL索引、主键、唯一约束等信息，批量生成创建语句
	sql := fmt.Sprintf("SELECT IF ( INDEX_NAME = 'PRIMARY', CONCAT( 'ALTER TABLE ', TABLE_NAME, ' ', 'ADD ', IF ( NON_UNIQUE = 1, CASE UPPER( INDEX_TYPE ) WHEN 'FULLTEXT' THEN 'FULLTEXT INDEX' WHEN 'SPATIAL' THEN 'SPATIAL INDEX' ELSE CONCAT( 'INDEX ', INDEX_NAME, '' ) END, IF ( UPPER( INDEX_NAME ) = 'PRIMARY', CONCAT( 'PRIMARY KEY ' ), CONCAT( 'UNIQUE INDEX ', INDEX_NAME ) ) ), '(', GROUP_CONCAT( DISTINCT CONCAT( '', COLUMN_NAME, '' ) ORDER BY SEQ_IN_INDEX ASC SEPARATOR ', ' ), ');' ), IF ( UPPER( INDEX_NAME ) != 'PRIMARY' AND non_unique = 0,CONCAT( 'CREATE UNIQUE INDEX ', index_name, '_', substr( uuid(), 1, 8 ), substr( MD5( RAND()), 1, 3 ), ' ON ', table_name, '(', GROUP_CONCAT( DISTINCT CONCAT( '', COLUMN_NAME, '' ) ORDER BY SEQ_IN_INDEX ASC SEPARATOR ', ' ), ');' ),REPLACE ( REPLACE ( CONCAT( 'CREATE INDEX ', index_name, '_', substr( uuid(), 1, 8 ), substr( MD5( RAND()), 1, 3 ), ' ON ', IF ( NON_UNIQUE = 1, CASE UPPER( INDEX_TYPE ) WHEN 'FULLTEXT' THEN 'FULLTEXT INDEX' WHEN 'SPATIAL' THEN 'SPATIAL INDEX' ELSE CONCAT( ' ', table_name, '' ) END, IF ( UPPER( INDEX_NAME ) = 'PRIMARY', CONCAT( 'PRIMARY KEY ' ), CONCAT( table_name, ' xxx' ) ) ), '(', GROUP_CONCAT( DISTINCT CONCAT( '', COLUMN_NAME, '' ) ORDER BY SEQ_IN_INDEX ASC SEPARATOR ', ' ), ');' ), CHAR ( 13 ), '' ), CHAR ( 10 ), '' ) ) ) sql_text FROM information_schema.STATISTICS WHERE TABLE_SCHEMA IN ( SELECT DATABASE()) GROUP BY TABLE_NAME, INDEX_NAME ORDER BY TABLE_NAME ASC, INDEX_NAME ASC;")
	//fmt.Println(sql)
	rows, err := srcDb.Query(sql)
	if err != nil {
		log.Error(err)
	}
	// 从sql结果集遍历，获取到创建语句
	for rows.Next() {
		id += 1
		if err := rows.Scan(&tb.destIdxSql); err != nil {
			log.Error(err)
		}
		// 创建目标索引，主键、其余约束
		log.Info(fmt.Sprintf("%v ProcessingID %s %s", time.Now().Format("2006-01-02 15:04:05.000000"), strconv.Itoa(id), tb.destIdxSql))
		if _, err = destDb.Exec(tb.destIdxSql); err != nil {
			log.Error("index ", tb.destIdxSql, " create index failed ", err)
			LogError(logDir, "idxCreateFailed", tb.destIdxSql, err)
			failedCount += 1
		}
	}
	endTime := time.Now()
	cost := time.Since(startTime)
	log.Info("index  count ", id)
	result = append(result, "Index", startTime.Format("2006-01-02 15:04:05.000000"), endTime.Format("2006-01-02 15:04:05.000000"), strconv.Itoa(failedCount), cost.String())
	return result
}

func (tb *Table) FKCreate(logDir string) (result []string) {
	failedCount := 0
	startTime := time.Now()
	id := 0
	var createSql string
	// 查询MySQL外键，批量生成创建语句
	sql := fmt.Sprintf("SELECT ifnull(concat('ALTER TABLE ',K.TABLE_NAME,' ADD CONSTRAINT ',K.CONSTRAINT_NAME,' FOREIGN KEY(',GROUP_CONCAT(COLUMN_NAME),')',' REFERENCES ',K.REFERENCED_TABLE_NAME,'(',GROUP_CONCAT(REFERENCED_COLUMN_NAME),')',' ON DELETE ',DELETE_RULE,' ON UPDATE ',UPDATE_RULE),'null') FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE k INNER JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS r on k.CONSTRAINT_NAME = r.CONSTRAINT_NAME where k.CONSTRAINT_SCHEMA =database() AND r.CONSTRAINT_SCHEMA=database()  and k.REFERENCED_TABLE_NAME is not null order by k.ORDINAL_POSITION;")
	//fmt.Println(sql)
	rows, err := srcDb.Query(sql)
	if err != nil {
		log.Error(err)
	}
	// 从sql结果集遍历，获取到创建语句
	for rows.Next() {
		id += 1
		if err := rows.Scan(&createSql); err != nil {
			log.Error(err)
		}
		// 创建目标外键
		if createSql != "null" {
			log.Info(fmt.Sprintf("%v ProcessingID %s create foreign key %s", time.Now().Format("2006-01-02 15:04:05.000000"), strconv.Itoa(id), createSql))
			if _, err = destDb.Exec(createSql); err != nil {
				log.Error(createSql, " create foreign key failed ", err)
				LogError(logDir, "FkCreateFailed", createSql, err)
				failedCount += 1
			}
		}
	}
	log.Info("foreign key count ", id)
	endTime := time.Now()
	cost := time.Since(startTime)
	result = append(result, "ForeignKey", startTime.Format("2006-01-02 15:04:05.000000"), endTime.Format("2006-01-02 15:04:05.000000"), strconv.Itoa(failedCount), cost.String())
	return result
}

func (tb *Table) ViewCreate(logDir string) (result []string) {
	failedCount := 0
	startTime := time.Now()
	id := 0
	var viewName string
	// 查询视图并拼接生成目标数据库创建视图的SQL
	sql := fmt.Sprintf("select table_name,concat('create or replace view ',table_name,' as ',  replace(replace(replace(replace(VIEW_DEFINITION,'`',''),concat(table_schema,'.'),''),'convert(',''),'using utf8mb4)','')  ,';') create_view_sql from information_schema.VIEWS where TABLE_SCHEMA=database();")
	rows, err := srcDb.Query(sql)
	if err != nil {
		log.Error(err)
	}
	// 从sql结果集遍历，获取到创建语句
	for rows.Next() {
		id += 1
		if err := rows.Scan(&viewName, &tb.viewSql); err != nil {
			log.Error(err)
		}
		// 创建目标视图
		log.Info(fmt.Sprintf("%v ProcessingID %s create view %s", time.Now().Format("2006-01-02 15:04:05.000000"), strconv.Itoa(id), viewName))
		if _, err = destDb.Exec(tb.viewSql); err != nil {
			log.Error("view ", viewName, " create view failed ", err)
			err = nil
			LogError(logDir, "viewCreateFailed", tb.viewSql, err)
			failedCount += 1
		}
	}
	log.Info("view total ", id)
	endTime := time.Now()
	cost := time.Since(startTime)
	result = append(result, "View", startTime.Format("2006-01-02 15:04:05.000000"), endTime.Format("2006-01-02 15:04:05.000000"), strconv.Itoa(failedCount), cost.String())
	return result
}

func (tb *Table) TriggerCreate(logDir string) (result []string) {
	id := 0
	failedCount := 0
	startTime := time.Now()
	var createSql string
	// 查询触发器，批量生成创建语句
	sql := fmt.Sprintf("SELECT replace(upper(concat('create or replace trigger ',trigger_name,' ',action_timing,' ',event_manipulation,' on ',event_object_table,' for each row as ',action_statement)),'#','-- ') FROM information_schema.triggers WHERE trigger_schema=database();")
	//fmt.Println(sql)
	rows, err := srcDb.Query(sql)
	if err != nil {
		log.Error(err)
	}
	// 从sql结果集遍历，获取到创建语句
	for rows.Next() {
		id += 1
		if err := rows.Scan(&createSql); err != nil {
			log.Error(err)
		}
		// 创建目标触发器
		log.Info(fmt.Sprintf("%v ProcessingID %s create trigger %s", time.Now().Format("2006-01-02 15:04:05.000000"), strconv.Itoa(id), createSql))
		if _, err = destDb.Exec(createSql); err != nil {
			log.Error(createSql, " create trigger failed ", err)
			LogError(logDir, "TriggerCreateFailed", createSql, err)
			failedCount += 1
		}
	}
	log.Info("trigger count ", id)
	endTime := time.Now()
	cost := time.Since(startTime)
	result = append(result, "Trigger", startTime.Format("2006-01-02 15:04:05.000000"), endTime.Format("2006-01-02 15:04:05.000000"), strconv.Itoa(failedCount), cost.String())
	return result
}

// TableCreate 单线程创建表
//func (tb *Table) TableCreate(logDir string, tableMap map[string][]string) (result []string) {
//	tableCount := 0
//	startTime := time.Now()
//	failedCount := 0
//	// 获取tableMap键值对中的表名
//	for tblName, _ := range tableMap {
//		var colTotal int
//		tableCount += 1
//		pgCreateTbl := "create table " + tblName + "("
//		// 查询当前表总共有多少个列字段
//		colTotalSql := fmt.Sprintf("select count(*) from information_schema.COLUMNS  where table_schema=database() and table_name='%s'", tblName)
//		err := srcDb.QueryRow(colTotalSql).Scan(&colTotal)
//		if err != nil {
//			log.Error(err)
//		}
//		// 查询MySQL表结构
//		sql := fmt.Sprintf("select concat('\"',lower(column_name),'\"'),data_type,ifnull(character_maximum_length,'null'),is_nullable,case  column_default when '( \\'user\\' )' then 'user' else ifnull(column_default,'null') end as column_default,ifnull(numeric_precision,'null'),ifnull(numeric_scale,'null'),ifnull(datetime_precision,'null'),ifnull(column_key,'null'),ifnull(column_comment,'null'),ORDINAL_POSITION from information_schema.COLUMNS where table_schema=database() and table_name='%s'", tblName)
//		//fmt.Println(sql)
//		rows, err := srcDb.Query(sql)
//		if err != nil {
//			log.Error(err)
//		}
//		// 遍历MySQL表字段,一行就是一个字段的基本信息
//		for rows.Next() {
//			if err := rows.Scan(&tb.columnName, &tb.dataType, &tb.characterMaximumLength, &tb.isNullable, &tb.columnDefault, &tb.numericPrecision, &tb.numericScale, &tb.datetimePrecision, &tb.columnKey, &tb.columnComment, &tb.ordinalPosition); err != nil {
//				log.Error(err)
//			}
//			//fmt.Println(columnName,dataType,characterMaximumLength,isNullable,columnDefault,numericPrecision,numericScale,datetimePrecision,columnKey,columnComment,ordinalPosition)
//			//适配MySQL字段类型到PostgreSQL字段类型
//			// 列字段是否允许null
//			switch tb.isNullable {
//			case "NO":
//				tb.destNullable = "not null"
//			default:
//				tb.destNullable = "null"
//			}
//			// 列字段default默认值的处理
//			switch {
//			case tb.columnDefault != "null": // 默认值不是null并且是字符串类型下面就需要使用fmt.Sprintf格式化让字符串单引号包围，否则这个字符串是没有引号包围的
//				if tb.dataType == "varchar" {
//					tb.destDefault = fmt.Sprintf("default '%s'", tb.columnDefault)
//				} else if tb.dataType == "char" {
//					tb.destDefault = fmt.Sprintf("default '%s'", tb.columnDefault)
//				} else {
//					tb.destDefault = fmt.Sprintf("default %s", tb.columnDefault) // 非字符串类型无需使用单引号包围
//				}
//			default:
//				tb.destDefault = "" // 如果没有默认值，默认值就是空字符串，即目标没有默认值
//			}
//			// 列字段类型的处理
//			switch tb.dataType {
//			case "int", "mediumint", "tinyint":
//				tb.destType = "int"
//			case "varchar":
//				tb.destType = "varchar(" + tb.characterMaximumLength + ")"
//			case "char":
//				tb.destType = "char(" + tb.characterMaximumLength + ")"
//			case "text", "tinytext", "mediumtext", "longtext":
//				tb.destType = "text"
//			case "datetime", "timestamp":
//				tb.destType = "timestamp"
//			case "decimal", "double", "float":
//				if tb.numericScale == "null" {
//					tb.destType = "decimal(" + tb.numericPrecision + ")"
//				} else {
//					tb.destType = "decimal(" + tb.numericPrecision + "," + tb.numericScale + ")"
//				}
//			case "tinyblob", "blob", "mediumblob", "longblob":
//				tb.destType = "bytea"
//			// 其余类型，源库使用什么类型，目标库就使用什么类型
//			default:
//				tb.destType = tb.dataType
//			}
//			// 在目标库创建的语句
//			pgCreateTbl += fmt.Sprintf("%s %s %s %s,", tb.columnName, tb.destType, tb.destNullable, tb.destDefault)
//			if tb.ordinalPosition == colTotal {
//				pgCreateTbl = pgCreateTbl[:len(pgCreateTbl)-1] + ")" // 最后一个列字段结尾去掉逗号,并且加上语句的右括号
//			}
//		}
//		//fmt.Println(pgCreateTbl) // 打印创建表语句
//		// 创建前先删除目标表
//		dropDestTbl := "drop table if exists " + tblName + " cascade"
//		if _, err = destDb.Exec(dropDestTbl); err != nil {
//			log.Error(err)
//		}
//		// 创建PostgreSQL表结构
//		log.Info("Processing ID " + strconv.Itoa(tableCount) + " create table " + tblName)
//		if _, err = destDb.Exec(pgCreateTbl); err != nil {
//			log.Error("table ", tblName, " create failed", err)
//			failedCount += 1
//			LogError(logDir, "tableCreateFailed", pgCreateTbl, err)
//		}
//	}
//	endTime := time.Now()
//	cost := time.Since(startTime)
//	log.Info("Table structure synced from MySQL to PostgreSQL ,Source Table Total ", tableCount, " Failed Total ", strconv.Itoa(failedCount))
//	result = append(result, "Table", startTime.Format("2006-01-02 15:04:05.000000"), endTime.Format("2006-01-02 15:04:05.000000"), strconv.Itoa(failedCount), cost.String())
//	return result
//}
