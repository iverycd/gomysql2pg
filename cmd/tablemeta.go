package cmd

import (
	"fmt"
	"strconv"
	"sync"
	"time"
)

type Database interface {
	TableCreate(logDir string, tableMap map[string][]string)
	SeqCreate(logDir string)
	IdxCreate(logDir string)
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
}

func (tb *Table) TableCreate(logDir string, tableMap map[string][]string) {
	// 声明一个等待组
	var wg sync.WaitGroup
	tableCount := 0
	// 获取tableMap键值对中的表名
	for tblName, _ := range tableMap {
		tableCount += 1
		// 每一个任务开始时, 将等待组增加1
		wg.Add(1)
		// 使用goroutine并发的创建多个表
		go func(tblName string, tb Table, tableCount int) {
			var colTotal int
			// 使用defer, 表示函数完成时将等待组值减1
			defer wg.Done()
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
				if err := rows.Scan(&tb.columnName, &tb.dataType, &tb.characterMaximumLength, &tb.isNullable, &tb.columnDefault, &tb.numericPrecision, &tb.numericScale, &tb.datetimePrecision, &tb.columnKey, &tb.columnComment, &tb.ordinalPosition); err != nil {
					log.Error(err)
				}
				//fmt.Println(columnName,dataType,characterMaximumLength,isNullable,columnDefault,numericPrecision,numericScale,datetimePrecision,columnKey,columnComment,ordinalPosition)
				//适配MySQL字段类型到PostgreSQL字段类型
				// 列字段是否允许null
				switch tb.isNullable {
				case "NO":
					tb.destNullable = "not null"
				default:
					tb.destNullable = "null"
				}
				// 列字段default默认值的处理
				switch {
				case tb.columnDefault != "null": // 默认值不是null并且是字符串类型下面就需要使用fmt.Sprintf格式化让字符串单引号包围，否则这个字符串是没有引号包围的
					if tb.dataType == "varchar" {
						tb.destDefault = fmt.Sprintf("default '%s'", tb.columnDefault)
					} else if tb.dataType == "char" {
						tb.destDefault = fmt.Sprintf("default '%s'", tb.columnDefault)
					} else {
						tb.destDefault = fmt.Sprintf("default %s", tb.columnDefault) // 非字符串类型无需使用单引号包围
					}
				default:
					tb.destDefault = "" // 如果没有默认值，默认值就是空字符串，即目标没有默认值
				}
				// 列字段类型的处理
				switch tb.dataType {
				case "int", "mediumint", "tinyint":
					tb.destType = "int"
				case "varchar":
					tb.destType = "varchar(" + tb.characterMaximumLength + ")"
				case "char":
					tb.destType = "char(" + tb.characterMaximumLength + ")"
				case "text", "tinytext", "mediumtext", "longtext":
					tb.destType = "text"
				case "datetime", "timestamp":
					tb.destType = "timestamp"
				case "decimal", "double", "float":
					if tb.numericScale == "null" {
						tb.destType = "decimal(" + tb.numericPrecision + ")"
					} else {
						tb.destType = "decimal(" + tb.numericPrecision + "," + tb.numericScale + ")"
					}
				case "tinyblob", "blob", "mediumblob", "longblob":
					tb.destType = "bytea"
				// 其余类型，源库使用什么类型，目标库就使用什么类型
				default:
					tb.destType = tb.dataType
				}
				// 在目标库创建的语句
				pgCreateTbl += fmt.Sprintf("%s %s %s %s,", tb.columnName, tb.destType, tb.destNullable, tb.destDefault)
				if tb.ordinalPosition == colTotal {
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
			}
		}(tblName, *tb, tableCount)
	}
	// 等待所有的任务完成
	wg.Wait()
	log.Info("Table structure synced from MySQL to PostgreSQL Table count ", tableCount)
}

//func (tb *Table) SeqCreate(logDir string, tableMap map[string][]string)()  {
//	// 声明一个等待组
//	var wg sync.WaitGroup
//	tableCount := 0
//	// 获取tableMap键值对中的表名
//	for tblName, _ := range tableMap {
//		tableCount += 1
//		// 每一个任务开始时, 将等待组增加1
//		wg.Add(1)
//		// 使用goroutine并发的创建多个序列
//		go func(tblName string, tb Table, tableCount int) {
//			// 使用defer, 表示函数完成时将等待组值减1
//			defer wg.Done()
//			// 查询MySQL自增列信息，批量生成创建序列sql
//			sql := fmt.Sprintf("select COLUMN_NAME,Auto_increment,lower(concat('drop sequence if exists ','seq_',TABLE_NAME,'_',COLUMN_NAME,';')) drop_seq,lower(concat('create sequence ','seq_',TABLE_NAME,'_',COLUMN_NAME,' INCREMENT BY 1 START ',Auto_increment,';')) create_seq, lower(concat('alter table ',table_name,' alter column ',COLUMN_NAME, ' set default nextval(', '''' ,'seq_',TABLE_NAME,'_',COLUMN_NAME,  '''',');')) alter_default  from (select Auto_increment,column_name,a.table_name from (select TABLE_NAME, Auto_increment,case when Auto_increment  is not null then 'auto_increment' else '0' end ai from information_schema. TABLES where TABLE_SCHEMA =database() and  AUTO_INCREMENT is not null) a join (select table_name,COLUMN_NAME,EXTRA from information_schema. COLUMNS where TABLE_SCHEMA =database() and table_name in(select t.TABLE_NAME from information_schema. TABLES t where TABLE_SCHEMA =database() and AUTO_INCREMENT is not null)  and EXTRA='auto_increment' ) b on a.ai = b.EXTRA and a.table_name =b.table_name) aaa where table_name='%s';", tblName)
//			//fmt.Println(sql)
//			rows, err := srcDb.Query(sql)
//			if err != nil {
//				log.Error(err)
//			}
//			// 从sql结果集遍历，获取到删除序列，创建序列，默认值为自增列
//			for rows.Next() {
//				if err := rows.Scan(&tb.columnName,&tb.autoIncrement,&tb.dropSeqSql,&tb.destSeqSql,&tb.destDefaultSeq); err != nil {
//					log.Error(err)
//				}
//			}
//			if tb.columnName != ""{
//				// 创建前先删除目标序列
//				if _, err = destDb.Exec(tb.dropSeqSql); err != nil {
//					log.Error(err)
//				}
//				// 创建目标序列
//				log.Info(fmt.Sprintf("%v ProcessingID %s create sequence %s", time.Now().Format("2006-01-02 15:04:05.000000"), strconv.Itoa(tableCount), tblName))
//				if _, err = destDb.Exec(tb.destSeqSql); err != nil {
//					log.Error("table ", tblName, " create sequence failed ", err)
//					LogError(logDir, "seqCreateFailed", tb.destSeqSql, err)
//				}
//			}
//
//		}(tblName, *tb, tableCount)
//	}
//	// 等待所有的任务完成
//	wg.Wait()
//	log.Info("sequence count ", tableCount)
//	// 如果指定-t选项，表创建完毕之后就退出程序
//	if tableOnly {
//		os.Exit(0)
//	}
//}

func (tb *Table) SeqCreate(logDir string) {
	tableCount := 0
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
		}
		// 设置表自增列为序列，如果表不存并单独创建序列会有error但是毫无影响
		log.Info(fmt.Sprintf("%v ProcessingID %s set default sequence %s", time.Now().Format("2006-01-02 15:04:05.000000"), strconv.Itoa(tableCount), tableName))
		if _, err = destDb.Exec(tb.destDefaultSeq); err != nil {
			log.Error("table ", tableName, " set default sequence failed ", err)
			LogError(logDir, "seqCreateFailed", tb.destDefaultSeq, err)
		}
	}
	log.Info("sequence count ", tableCount)
}

func (tb *Table) IdxCreate(logDir string) {
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
		log.Info(fmt.Sprintf("%v ProcessingID %s create index and constraint %s", time.Now().Format("2006-01-02 15:04:05.000000"), strconv.Itoa(id), tb.destIdxSql))
		if _, err = destDb.Exec(tb.destIdxSql); err != nil {
			log.Error("index ", tb.destIdxSql, " create index failed ", err)
			LogError(logDir, "idxCreateFailed", tb.destIdxSql, err)
		}
	}
	log.Info("index  count ", id)
}
