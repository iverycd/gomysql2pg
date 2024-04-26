package cmd

import (
	"bytes"
	"database/sql"
	"encoding/hex"
	"fmt"
	"github.com/lib/pq"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"github.com/liushuochen/gotable"
	"github.com/mitchellh/go-homedir"
	"github.com/spf13/viper"
	"gomysql2pg/connect"
)

var log = logrus.New()
var cfgFile string
var selFromYml bool

var wg sync.WaitGroup
var wg2 sync.WaitGroup
var responseChannel = make(chan string, 1) // 设定为全局变量，用于在goroutine协程里接收copy行数据失败的计数
// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "gomysql2pg",
	Short: "",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		// 获取配置文件中的数据库连接字符串
		connStr := getConn()
		mysql2pg(connStr)
	},
}

// 表行数据迁移失败的计数
var errDataCount int

// 处理全局变量通道，responseChannel，在协程的这个通道里遍历获取到copy方法失败的计数
func response() {
	for rc := range responseChannel {
		fmt.Println("response:", rc)
		errDataCount += 1
	}
}

func mysql2pg(connStr *connect.DbConnStr) {
	// 自动侦测终端是否输入Ctrl+c,若按下,主动关闭数据库查询
	exitChan := make(chan os.Signal)
	signal.Notify(exitChan, os.Interrupt, os.Kill, syscall.SIGTERM)
	go exitHandle(exitChan)
	// 创建运行日志目录
	logDir, _ := filepath.Abs(CreateDateDir(""))
	// 输出调用文件以及方法位置
	log.SetReportCaller(true)
	f, err := os.OpenFile(logDir+"/"+"run.log", os.O_CREATE|os.O_APPEND|os.O_RDWR, os.ModePerm)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			log.Fatal(err) // 或设置到函数返回值中
		}
	}()
	// log信息重定向到平面文件
	multiWriter := io.MultiWriter(os.Stdout, f)
	log.SetOutput(multiWriter)
	start := time.Now()
	// map结构，表名以及该表用来迁移查询源库的语句
	var tableMap map[string][]string
	// 从配置文件中获取需要排除的表
	excludeTab := viper.GetStringSlice("exclude")
	log.Info("running MySQL check connect")
	// 生成源库数据库连接
	PrepareSrc(connStr)
	defer srcDb.Close()
	// 每页的分页记录数,仅全库迁移时有效
	pageSize := viper.GetInt("pageSize")
	log.Info("running Postgres check connect")
	// 生成目标库的数据库连接
	PrepareDest(connStr)
	defer destDb.Close()
	// 以下是迁移数据前的准备工作，获取要迁移的表名以及该表查询源库的sql语句(如果有主键生成该表的分页查询切片集合，没有主键的统一是全表查询sql)
	if selFromYml { // 如果用了-s选项，从配置文件中获取表名以及sql语句
		tableMap = viper.GetStringMapStringSlice("tables")
	} else { // 不指定-s选项，查询源库所有表名
		tableMap = fetchTableMap(pageSize, excludeTab)
	}
	// 实例初始化，调用接口中创建目标表的方法
	var db Database
	db = new(Table)
	// 从yml配置文件中获取迁移数据时最大运行协程数
	maxParallel := viper.GetInt("maxParallel")
	if maxParallel == 0 {
		maxParallel = 20
	}
	// 用于控制协程goroutine运行时候的并发数,例如3个一批，3个一批的goroutine并发运行
	ch := make(chan struct{}, maxParallel)
	startTbl := time.Now()
	for tableName := range tableMap { //获取单个表名
		ch <- struct{}{}
		wg2.Add(1)
		go db.TableCreate(logDir, tableName, ch)
	}
	wg2.Wait()
	endTbl := time.Now()
	tableCost := time.Since(startTbl)
	// 创建表完毕
	log.Info("Table structure synced from MySQL to PostgreSQL ,Source Table Total ", tableCount, " Failed Total ", strconv.Itoa(failedCount))
	tabRet = append(tabRet, "Table", startTbl.Format("2006-01-02 15:04:05.000000"), endTbl.Format("2006-01-02 15:04:05.000000"), strconv.Itoa(failedCount), tableCost.String())
	fmt.Println("Table Create finish elapsed time ", tableCost)
	// 创建表之后，开始准备迁移表行数据
	// 同时执行goroutine的数量，这里是每个表查询语句切片集合的长度
	var goroutineSize int
	//遍历每个表需要执行的切片查询SQL，累计起来获得总的goroutine并发大小，即所有goroutine协程的数量
	for _, sqlList := range tableMap {
		goroutineSize += len(sqlList)
	}
	// 每个goroutine运行开始以及结束之后使用的通道，主要用于控制内层的goroutine任务与外层main线程的同步，即主线程需要等待子任务完成
	// ch := make(chan int, goroutineSize)  //v0.1.4及之前的版本通道使用的通道，配合下面for循环遍历行数据迁移失败的计数
	// 在协程里运行函数response，主要是从下面调用协程go runMigration的时候获取到里面迁移行数据失败的数量
	go response()
	//遍历tableMap，先遍历表，再遍历该表的sql切片集合
	migDataStart := time.Now()
	for tableName, sqlFullSplit := range tableMap { //获取单个表名
		colName, colType, tableNotExist := preMigData(tableName, sqlFullSplit) //获取单表的列名，列字段类型
		if !tableNotExist {                                                    //目标表存在就执行数据迁移
			// 遍历该表的sql切片(多个分页查询或者全表查询sql)
			for index, sqlSplitSql := range sqlFullSplit {
				ch <- struct{}{} //在没有被接收的情况下，至多发送n个消息到通道则被阻塞，若缓存区满，则阻塞，这里相当于占位置排队
				wg.Add(1)        // 每运行一个goroutine等待组加1
				go runMigration(logDir, index, tableName, sqlSplitSql, ch, colName, colType)
			}
		} else { //目标表不存在就往通道写1
			log.Info("table not exists ", tableName)
		}
	}
	// 单独计算迁移表行数据的耗时
	migDataEnd := time.Now()
	// 这里等待上面所有迁移数据的goroutine协程任务完成才会接着运行下面的主程序，如果这里不wait，上面还在迁移行数据的goroutine会被强制中断
	wg.Wait()
	migCost := migDataEnd.Sub(migDataStart)
	// v0.1.4版本之前通过循环获取ch通道里写的int数据判断是否有迁移行数据失败的表，如果通道里发送的数据是2说明copy失败了
	//migDataFailed := 0
	// 这里是等待上面所有goroutine任务完成，才会执行for循环下面的动作
	//for i := 0; i < goroutineSize; i++ {
	//	migDataRet := <-ch
	//	log.Info("goroutine[", i, "]", " finish ", time.Now().Format("2006-01-02 15:04:05.000000"))
	//	if migDataRet == 2 {
	//		migDataFailed += 1
	//	}
	//}
	tableDataRet := []string{"TableData", migDataStart.Format("2006-01-02 15:04:05.000000"), migDataEnd.Format("2006-01-02 15:04:05.000000"), strconv.Itoa(errDataCount), migCost.String()}
	// 数据库对象的迁移结果
	var rowsAll = [][]string{{}}
	// 表结构创建以及数据迁移结果追加到切片,进行整合
	rowsAll = append(rowsAll, tabRet, tableDataRet)
	// 如果指定-s模式不创建下面对象
	if selFromYml != true {
		// 创建序列
		seqRet := db.SeqCreate(logDir)
		// 创建索引、约束
		idxRet := db.IdxCreate(logDir)
		// 创建外键
		fkRet := db.FKCreate(logDir)
		// 创建视图
		viewRet := db.ViewCreate(logDir)
		// 创建触发器
		triRet := db.TriggerCreate(logDir)
		// 以上对象迁移结果追加到切片,进行整合
		rowsAll = append(rowsAll, seqRet, idxRet, fkRet, viewRet, triRet)
	}
	// 输出配置文件信息
	fmt.Println("------------------------------------------------------------------------------------------------------------------------------")
	Info()
	tblConfig, err := gotable.Create("SourceDb", "DestDb", "MaxParallel", "PageSize", "ExcludeCount")
	if err != nil {
		fmt.Println("Create tblConfig failed: ", err.Error())
		return
	}
	ymlConfig := []string{connStr.SrcHost + "-" + connStr.SrcDatabase, connStr.DestHost + "-" + connStr.DestDatabase, strconv.Itoa(maxParallel), strconv.Itoa(pageSize), strconv.Itoa(len(excludeTab))}
	tblConfig.AddRow(ymlConfig)
	fmt.Println(tblConfig)
	// 输出迁移摘要
	table, err := gotable.Create("Object", "BeginTime", "EndTime", "FailedTotal", "ElapsedTime")
	if err != nil {
		fmt.Println("Create table failed: ", err.Error())
		return
	}
	for _, r := range rowsAll {
		_ = table.AddRow(r)
	}
	table.Align("Object", 1)
	table.Align("FailedTotal", 1)
	table.Align("ElapsedTime", 1)
	fmt.Println(table)
	// 总耗时
	cost := time.Since(start)
	log.Info(fmt.Sprintf("All complete totalTime %s The Report Dir %s", cost, logDir))
}

// 自动对表分析，然后生成每个表用来迁移查询源库SQL的集合(全表查询或者分页查询)
// 自动分析是否有排除的表名
// 最后返回map结构即 表:[查询SQL]
func fetchTableMap(pageSize int, excludeTable []string) (tableMap map[string][]string) {
	var tableNumber int // 表总数
	var sqlStr string   // 查询源库获取要迁移的表名
	// 声明一个等待组
	var wg sync.WaitGroup
	// 使用互斥锁 sync.Mutex才能使用并发的goroutine
	mutex := &sync.Mutex{}
	log.Info("exclude table ", excludeTable)
	// 如果配置文件中exclude存在表名，使用not in排除掉这些表，否则获取到所有表名
	if excludeTable != nil {
		sqlStr = "select table_name from information_schema.tables where table_schema=database() and table_type='BASE TABLE' and table_name not in ("
		buffer := bytes.NewBufferString("")
		for index, tabName := range excludeTable {
			if index < len(excludeTable)-1 {
				buffer.WriteString("'" + tabName + "'" + ",")
			} else {
				buffer.WriteString("'" + tabName + "'" + ")")
			}
		}
		sqlStr += buffer.String()
	} else {
		sqlStr = "select table_name from information_schema.tables where table_schema=database() and table_type='BASE TABLE';" // 获取库里全表名称
	}
	// 查询下源库总共的表，获取到表名
	rows, err := srcDb.Query(sqlStr)
	defer rows.Close()
	if err != nil {
		log.Error(fmt.Sprintf("Query "+sqlStr+" failed,\nerr:%v\n", err))
		return
	}
	var tableName string
	//初始化外层的map，键值对，即 表名:[sql语句...]
	tableMap = make(map[string][]string)
	for rows.Next() {
		tableNumber++
		// 每一个任务开始时, 将等待组增加1
		wg.Add(1)
		var sqlFullList []string
		err = rows.Scan(&tableName)
		if err != nil {
			log.Error(err)
		}
		// 使用多个并发的goroutine调用函数获取该表用来执行的sql语句
		log.Info(time.Now().Format("2006-01-02 15:04:05.000000"), "ID[", tableNumber, "] ", "prepare ", tableName, " TableMap")
		go func(tableName string, sqlFullList []string) {
			// 使用defer, 表示函数完成时将等待组值减1
			defer wg.Done()
			// !tableOnly即没有指定-t选项，生成全库的分页查询语句，否则就是指定了-t选项,sqlFullList仅追加空字符串
			if !tableOnly {
				sqlFullList = prepareSqlStr(tableName, pageSize)
			} else {
				sqlFullList = append(sqlFullList, "")
			}
			// 追加到内层的切片，sql全表扫描语句或者分页查询语句，例如tableMap[test1]="select * from test1"
			for i := 0; i < len(sqlFullList); i++ {
				mutex.Lock()
				tableMap[tableName] = append(tableMap[tableName], sqlFullList[i])
				mutex.Unlock()
			}
		}(tableName, sqlFullList)
	}
	// 等待所有的任务完成
	wg.Wait()
	return tableMap
}

// 迁移数据前先清空目标表数据，并获取每个表查询语句的列名以及列字段类型,表如果不存在返回布尔值true
func preMigData(tableName string, sqlFullSplit []string) (dbCol []string, dbColType []string, tableNotExist bool) {
	var sqlCol string
	// 在写数据前，先清空下目标表数据
	truncateSql := "truncate table " + fmt.Sprintf("\"") + tableName + fmt.Sprintf("\"")
	if _, err := destDb.Exec(truncateSql); err != nil {
		log.Error("truncate ", tableName, " failed   ", err)
		tableNotExist = true
		return // 表不存在return布尔值
	}
	// 获取表的字段名以及类型
	// 如果指定了参数-s，就读取yml文件中配置的sql获取"自定义查询sql生成的列名"，否则按照select * 查全表获取
	if selFromYml {
		sqlCol = "select * from (" + sqlFullSplit[0] + " )aa where 1=0;" // 在自定义sql外层套一个select * from (自定义sql) where 1=0
	} else {
		sqlCol = "select * from " + "`" + tableName + "`" + " where 1=0;"
	}
	rows, err := srcDb.Query(sqlCol) //源库 SQL查询语句
	defer rows.Close()
	if err != nil {
		log.Error(fmt.Sprintf("Query "+sqlCol+" failed,\nerr:%v\n", err))
		return
	}
	//获取列名，这是字符串切片
	columns, err := rows.Columns()
	if err != nil {
		log.Fatal(err.Error())
	}
	//获取字段类型，看下是varchar等还是blob
	colType, err := rows.ColumnTypes()
	if err != nil {
		log.Fatal(err.Error())
	}
	// 循环遍历列名,把列名全部转为小写
	for i, value := range columns {
		dbCol = append(dbCol, strings.ToLower(value)) //由于CopyIn方法每个列都会使用双引号包围，这里把列名全部转为小写(pg库默认都是小写的列名)，这样即便加上双引号也能正确查询到列
		dbColType = append(dbColType, strings.ToUpper(colType[i].DatabaseTypeName()))
	}
	return dbCol, dbColType, tableNotExist
}

// 根据表是否有主键，自动生成每个表查询sql，有主键就生成分页查询组成的切片，没主键就拼成全表查询sql，最后返回sql切片
func prepareSqlStr(tableName string, pageSize int) (sqlList []string) {
	var scanColPk string   // 每个表主键列字段名称
	var colFullPk []string // 每个表所有主键列字段生成的切片
	var totalPageNum int   // 每个表的分页查询记录总数，即总共有多少页记录
	var sqlStr string      // 分页查询或者全表扫描sql
	//先获取下主键字段名称,可能是1个，或者2个以上组成的联合主键
	sql1 := "SELECT lower(COLUMN_NAME) FROM information_schema.key_column_usage t WHERE constraint_name='PRIMARY' AND table_schema=DATABASE() AND table_name=? order by ORDINAL_POSITION;"
	rows, err := srcDb.Query(sql1, tableName)
	defer rows.Close()
	if err != nil {
		log.Fatal(sql1, " exec failed ", err)
	}
	// 获取主键集合，追加到切片里面
	for rows.Next() {
		err = rows.Scan(&scanColPk)
		if err != nil {
			log.Println(err)
		}
		colFullPk = append(colFullPk, scanColPk)
	}
	// 没有主键，就返回全表扫描的sql语句,即使这个表没有数据，迁移也不影响，测试通过
	if colFullPk == nil {
		sqlList = append(sqlList, "select * from "+"`"+tableName+"`")
		return sqlList
	}
	// 遍历主键集合，使用逗号隔开,生成主键列或者组合，以及join on的连接字段
	buffer1 := bytes.NewBufferString("")
	buffer2 := bytes.NewBufferString("")
	for i, col := range colFullPk {
		if i < len(colFullPk)-1 {
			buffer1.WriteString(col + ",")
			buffer2.WriteString("temp." + col + "=t." + col + " and ")
		} else {
			buffer1.WriteString(col)
			buffer2.WriteString("temp." + col + "=t." + col)
		}
	}
	// 如果有主键,根据当前表总数以及每页的页记录大小pageSize，自动计算需要多少页记录数，即总共循环多少次，如果表没有数据，后面判断下切片长度再做处理
	sql2 := "/* gomysql2pg */" + "select ceil(count(*)/" + strconv.Itoa(pageSize) + ") as total_page_num from " + "`" + tableName + "`"
	//以下是直接使用QueryRow
	err = srcDb.QueryRow(sql2).Scan(&totalPageNum)
	if err != nil {
		log.Fatal(sql2, " exec failed ", err)
		return
	}
	// 以下生成分页查询语句
	for i := 0; i <= totalPageNum; i++ { // 使用小于等于，包含没有行数据的表
		sqlStr = "SELECT t.* FROM (SELECT " + buffer1.String() + " FROM " + "`" + tableName + "`" + " ORDER BY " + buffer1.String() + " LIMIT " + strconv.Itoa(i*pageSize) + "," + strconv.Itoa(pageSize) + ") temp LEFT JOIN " + "`" + tableName + "`" + " t ON " + buffer2.String() + ";"
		sqlList = append(sqlList, strings.ToLower(sqlStr))
	}
	return sqlList
}

// 根据源sql查询语句，按行遍历使用copy方法迁移到目标数据库
func runMigration(logDir string, startPage int, tableName string, sqlStr string, ch chan struct{}, columns []string, colType []string) {
	defer wg.Done()
	log.Info(fmt.Sprintf("%v Taskid[%d] Processing TableData %v", time.Now().Format("2006-01-02 15:04:05.000000"), startPage, tableName))
	start := time.Now()
	// 直接查询,即查询全表或者分页查询(SELECT t.* FROM (SELECT id FROM test  ORDER BY id LIMIT ?, ?) temp LEFT JOIN test t ON temp.id = t.id;)
	sqlStr = "/* gomysql2pg */" + sqlStr
	// 查询源库的sql
	rows, err := srcDb.Query(sqlStr) //传入参数之后执行
	defer rows.Close()
	if err != nil {
		log.Error(fmt.Sprintf("[exec  %v failed ] ", sqlStr), err)
		return
	}
	//fmt.Println(dbCol)  //输出查询语句里各个字段名称
	values := make([]sql.RawBytes, len(columns)) // 列的值切片,包含多个列,即单行数据的值
	scanArgs := make([]interface{}, len(values)) // 用来做scan的参数，将上面的列值value保存到scan
	for i := range values {                      // 这里也是取决于有几列，就循环多少次
		scanArgs[i] = &values[i] // 这里scanArgs是指向列值的指针,scanArgs里每个元素存放的都是地址
	}
	txn, err := destDb.Begin() //开始一个事务
	if err != nil {
		log.Error(err)
	}
	stmt, err := txn.Prepare(pq.CopyIn(tableName, columns...)) //prepare里的方法CopyIn只是把copy语句拼接好并返回，并非直接执行copy
	if err != nil {
		log.Error("txn Prepare pq.CopyIn failed ", err)
		//ch <- 1 // 执行pg的copy异常就往通道写入1
		<-ch   // 通道向外发送
		return // 遇到CopyIn异常就直接return
	}
	var totalRow int                                   // 表总行数
	prepareValues := make([]interface{}, len(columns)) //用于给copy方法，一行数据的切片，里面各个元素是各个列字段值
	var value interface{}                              // 单个字段的列值
	for rows.Next() {                                  // 从游标里获取一行行数据
		totalRow++                   // 源表行数+1
		err = rows.Scan(scanArgs...) //scanArgs切片里的元素是指向values的指针，通过rows.Scan方法将获取游标结果集的各个列值复制到变量scanArgs各个切片元素(指针)指向的对象即values切片里，这里是一行完整的值
		//fmt.Println(scanArgs[0],scanArgs[1])
		if err != nil {
			log.Error("ScanArgs Failed ", err.Error())
		}
		// 以下for将单行的byte数据循环转换成string类型(大字段就是用byte类型，剩余非大字段类型获取的值再使用string函数转为字符串)
		for i, colValue := range values { //values是完整的一行所有列值，这里从values遍历，获取每一列的值并赋值到col变量，col是单列的列值
			//fmt.Println(i)
			if colValue == nil {
				value = nil //空值判断
			} else {
				if colType[i] == "BLOB" { //大字段类型就无需使用string函数转为字符串类型，即使用sql.RawBytes类型
					value = colValue
				} else if colType[i] == "GEOMETRY" { //gis类型的数据处理
					value = hex.EncodeToString(colValue)[8:] //golang把gis类型列数据转成16进制字符串后，会在开头多出来8个0，所以下面进行截取，从第9位开始获取数据
				} else if colType[i] == "BIT" {
					value = hex.EncodeToString(colValue)[1:] //mysql中获取bit类型转为16进制是00或者01,但是在pgsql中如果只需要1位类型为bit(1)，那么就从第1位后面开始截取数据
				} else {
					value = string(colValue) //非大字段类型,显式使用string函数强制转换为字符串文本，否则都是字节类型文本(即sql.RawBytes)
				}
			}
			// 检查varchar类型的数据中是否包含Unicode中的非法字符0
			//colNameStr := strings.ToLower(columns[i])
			//if colNameStr == "enable" {
			//	fmt.Println(colNameStr, colType[i], value)
			//}
			if colType[i] == "VARCHAR" || colType[i] == "TEXT" {
				// 由于在varchar类型下操作，这里直接断言成字符串类型
				newStr, _ := value.(string)
				// 将列值转换成Unicode码值，便于在码值中发现一些非法字符
				sliceRune := []rune(newStr)
				// 以下是通过遍历rune类型切片数据，找出列值中包含Unicode的非法字符0
				uniStr := 0    // 待查找的非合法的Unicode码值0
				found := false // 如果后面遍历切片找到非法值，值为true
				for _, val := range sliceRune {
					if val == rune(uniStr) {
						found = true
						break
					}
				}
				if found {
					//log.Warning("invalid is in sliceRune ", value, columns[i])
					LogError(logDir, "invalidTableData", "[Warning] invalid string found ! tableName:"+tableName+"     column value:["+newStr+"]      columnName:["+columns[i]+"]", err)
					// 直接批量替换，使用\x00去除掉列值中非法的Unicode码值0
					value = strings.Replace(string(sliceRune), "\x00", "", -1)
				}
			}
			if colType[i] == "TIMESTAMP" || colType[i] == "DATETIME" {
				newStr, _ := value.(string)
				if newStr == "0000-00-00 00:00:00" {
					value = "2000-01-01 01:01:01"
				}
			}
			if colType[i] == "DATE" {
				newStr, _ := value.(string)
				if newStr == "0000-00-00" {
					value = "2000-01-01"
				}
			}
			prepareValues[i] = value //把第1列的列值追加到任意类型的切片里面，然后把第2列，第n列的值加到任意类型的切片里面,这里的切片即一行完整的数据
		}
		_, err = stmt.Exec(prepareValues...) //这里Exec只传入实参，即上面prepare的CopyIn所需的参数，这里理解为把stmt所有数据先存放到buffer里面
		if err != nil {
			log.Error("stmt.Exec(prepareValues...) failed ", tableName, " ", err) // 这里是按行来的，不建议在这里输出错误信息,建议如果遇到一行错误就直接return返回
			LogError(logDir, "errorTableData", StrVal(prepareValues), err)
			//ch <- 1
			// 通过外部的全局变量通道获取到迁移行数据失败的计数
			responseChannel <- fmt.Sprintf("data error %s", tableName)
			<-ch   // 通道向外发送数据
			return // 如果prepare异常就return
		}
	}
	err = rows.Close()
	if err != nil {
		return
	}
	_, err = stmt.Exec() //把所有的buffer进行flush，一次性写入数据
	if err != nil {
		log.Error("prepareValues Error PG Copy Failed: ", tableName, " ", err) //注意这里不能使用Fatal，否则会直接退出程序，也就没法遇到错误继续了
		// 在copy过程中异常的表，将异常信息输出到平面文件
		LogError(logDir, "errorTableData", StrVal(prepareValues), err)
		//ch <- 2
		// 通过外部的全局变量通道获取到迁移行数据失败的计数
		responseChannel <- fmt.Sprintf("data error %s", tableName)
		<-ch // 通道向外发送数据
	}
	err = stmt.Close() //关闭stmt
	if err != nil {
		log.Error(err)
	}
	err = txn.Commit() // 提交事务，这里注意Commit在上面Close之后
	if err != nil {
		err := txn.Rollback()
		if err != nil {
			return
		}
		log.Error("Commit failed ", err)
	}
	cost := time.Since(start) //计算时间差
	log.Info(fmt.Sprintf("%v Taskid[%d] table %v complete,processed %d rows,execTime %s", time.Now().Format("2006-01-02 15:04:05.000000"), startPage, tableName, totalRow, cost))
	//ch <- 0
	<-ch // 通道向外发送数据
}

func Execute() { // init 函数初始化之后再运行此Execute函数
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

// 程序中第一个调用的函数,先初始化config
func init() {
	cobra.OnInitialize(initConfig)
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.gomysql2pg.yaml)")
	rootCmd.PersistentFlags().BoolVarP(&selFromYml, "selFromYml", "s", false, "select from yml true")
	//rootCmd.PersistentFlags().BoolVarP(&tableOnly, "tableOnly", "t", false, "only create table true")
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := homedir.Dir()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		// Search config in home directory with name ".gomysql2pg" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigName(".gomysql2pg")
	}

	viper.AutomaticEnv() // read in environment variables that match

	// 通过viper读取配置文件进行加载
	if err := viper.ReadInConfig(); err == nil {
		log.Info("Using config file:", viper.ConfigFileUsed())
	} else {
		log.Fatal(viper.ConfigFileUsed(), " has some error please check your yml file ! ", "Detail-> ", err)
	}
	log.Info("Using selFromYml:", selFromYml)
}
