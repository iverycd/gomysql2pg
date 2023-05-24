package cmd

import (
	"bytes"
	"database/sql"
	"fmt"
	"github.com/lib/pq"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"github.com/mitchellh/go-homedir"
	"github.com/spf13/viper"
)

var log = logrus.New()
var cfgFile string
var selFromYml bool
var srcDb *sql.DB
var destDb *sql.DB

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "gomysql2pg",
	Short: "",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		mysql2pg()
	},
}

func mysql2pg() {
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
	// map结构，表名以及查询语句
	var tableMap map[string][]string
	excludeTab := viper.GetStringSlice("exclude")
	log.Info("正则检查Mysql连接")
	PrepareSrc()
	defer srcDb.Close()
	// 每页的分页记录数,仅全库迁移时有效
	pageSize := viper.GetInt("pageSize")
	log.Info("正在检查Postgres连接")
	PrepareDest()
	defer destDb.Close()
	// 在迁移前的准备工作，获取要迁移的表名以及该表查询源库的sql语句(如果有主键生成分页查询切片，没有主键的统一是全表查询sql)
	if selFromYml {
		tableMap = viper.GetStringMapStringSlice("tables")
	} else {
		tableMap = fetchTableMap(pageSize, excludeTab)
	}
	// 打印出map集合里每个表以及表分页查询
	//for tableName,sqlstr := range tableMap{
	//	fmt.Println(tableName,"length slice sql ",len(sqlstr),sqlstr)
	//}
	// 从yml文件获取键值对信息，得到是一个键值对字典类型map[string][]string，如test:select * from test
	//tables := viper.GetStringMapStringSlice("tables")
	// maxRows := viper.GetInt("maxRows")
	// 同时执行goroutine的数量，这里实际上是每个表查询QL组成切片集合的长度
	var goroutineSize int
	//遍历每个表需要执行的切片查询SQL，累计起来获得总的goroutine并发大小
	for _, sqlList := range tableMap {
		goroutineSize += len(sqlList)
	}
	// 每个goroutine运行开始以及结束之后使用的通道
	ch := make(chan int, goroutineSize)
	//遍历tableMap，先遍历表，再遍历该表的sql切片集合
	for tableName, sqlFullSplit := range tableMap { //获取单个表名
		colName, colType := preMigdata(tableName, sqlFullSplit) //获取单表的列名，列字段类型
		// 遍历该表的sql切片(多个分页查询或者全表查询sql)
		for index, sqlSplitSql := range sqlFullSplit {
			go runMigration(index, tableName, sqlSplitSql, ch, colName, colType)
		}
	}
	// 计数加 2，表示要等待两个goroutine
	//wg.Add(2)
	//go runMigration(0, "test", sqlSplit[0], ch,colName,colType)
	//go runMigration(1, "test", sqlSplit[1], ch,colName,colType)
	for i := 0; i < goroutineSize; i++ {
		<-ch
		log.Info("goroutine[", i, "]", " finish ", time.Now())
	}
	//go func() {
	//	for {
	//		select {
	//		case num := <-ch:
	//			fmt.Println(num)
	//		case <-time.After(20*time.Second):
	//			fmt.Println("超时")
	//			quit <- true
	//		}
	//	}
	//}()
	//<-quit
	// 等待游戏结束
	//wg.Wait()
	cost := time.Since(start)

	log.Info(fmt.Sprintf("执行完成，耗时%s，运行日志请查看%s", cost, logDir))
}

// 自动对表分析，然后生成每个表用来迁移查询源库SQL的集合(全表查询或者分页查询)
// 自动分析是否有排除的表名
// 最后返回map结构即 表:[查询SQL]
func fetchTableMap(pageSize int, excludeTable []string) (tableMap map[string][]string) {
	var tableNumber int // 表总数
	var sqlStr string   // 查询源库获取要迁移的表名
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
		err = rows.Scan(&tableName)
		if err != nil {
			log.Error(err)
		}
		// 调用函数获取该表用来执行的sql语句
		log.Info("ID[", tableNumber, "] ", "正在生成表", tableName, " TableMap")
		sqlFullList := prepareSqlStr(tableName, pageSize)
		// 追加到内层的切片，sql全表扫描语句或者分页查询语句
		for i := 0; i < len(sqlFullList); i++ {
			tableMap[tableName] = append(tableMap[tableName], sqlFullList[i])
		}
	}
	return tableMap
}

// 迁移数据前先清空目标表数据，并获取每个表查询语句的列名以及列字段类型
func preMigdata(tableName string, sqlFullSplit []string) (dbCol []string, dbColType []string) {
	var sqlCol string
	//log.Info(fmt.Sprintf("%v 开始预先处理表 ", time.Now()))
	// 在写数据前，先清空下目标表数据
	truncateSql := "truncate table " + tableName
	if _, err := destDb.Exec(truncateSql); err != nil {
		log.Error("truncate ", tableName, " failed   ", err)
		return // 表不存在接直接return
	}
	// 获取表的字段名以及类型
	// 如果指定了参数selfromyml，就读取yml文件中配置的sql，否则按照select * 查全表获取
	if selFromYml {
		sqlCol = sqlFullSplit[0]
	} else {
		sqlCol = "select * from " + tableName + " where 1=0;"
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
	return dbCol, dbColType
}

// 根据表是否有主键，自动生成每个表查询sql，有主键就生成分页查询组成的切片，没主键就拼成全表查询sql
func prepareSqlStr(tableName string, pageSize int) (sqlList []string) {
	var scanColPk string
	var colFullPk []string
	var totalPageNum int
	var sqlStr string
	//先获取下主键字段名称,可能是1个，或者2个以上组成的联合主键
	sql1 := "SELECT lower(COLUMN_NAME) FROM information_schema.key_column_usage t WHERE constraint_name='PRIMARY' AND table_schema=DATABASE() AND table_name=? order by ORDINAL_POSITION;"
	rows, err := srcDb.Query(sql1, tableName)
	defer rows.Close()
	if err != nil {
		if err == sql.ErrNoRows {
			//log.Println("no pk col")
			sqlList = append(sqlList, "select * from "+tableName)
			return sqlList
		} else {
			log.Fatal(sql1, " exec failed ", err)
		}
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
		sqlList = append(sqlList, "select * from "+tableName)
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
	sql2 := "select ceil(count(*)/" + strconv.Itoa(pageSize) + ") as total_page_num from " + tableName
	//以下是直接使用QueryRow
	err = srcDb.QueryRow(sql2).Scan(&totalPageNum)
	if err != nil {
		log.Fatal(sql2, " exec failed ", err)
		return
	}
	// 以下生成分页查询语句
	for i := 0; i < totalPageNum; i++ {
		sqlStr = "SELECT t.* FROM (SELECT " + buffer1.String() + " FROM " + tableName + " ORDER BY " + buffer1.String() + " LIMIT " + strconv.Itoa(i*pageSize) + "," + strconv.Itoa(pageSize) + ") temp LEFT JOIN " + tableName + " t ON " + buffer2.String() + ";"
		//sqlStr = "SELECT t.* FROM (SELECT " + colPk +" FROM " +tableName +" ORDER BY " + colPk + " LIMIT "+ strconv.Itoa(i*pageSize) + "," +strconv.Itoa(pageSize) + ") temp LEFT JOIN "+tableName + " t ON temp."+colPk+" = t."+colPk  // 主键是单列的示例
		sqlList = append(sqlList, strings.ToLower(sqlStr))
	}
	return sqlList
}

// 根据源sql查询语句，按行遍历使用copy方法迁移到目标数据库
func runMigration(startPage int, tableName string, sqlStr string, ch chan int, columns []string, colType []string) {
	// 在函数退出时调用Done 来通知main 函数工作已经完成
	//defer wg.Done()
	log.Info(fmt.Sprintf("%v 任务goroutine[%d] 正在迁移表%v", time.Now(), startPage, tableName))
	start := time.Now()
	// 直接查询,即查询全表或者分页查询(SELECT t.* FROM (SELECT id FROM test  ORDER BY id LIMIT ?, ?) temp LEFT JOIN test t ON temp.id = t.id;)
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
		log.Error("Prepare pq.CopyIn failed ", err)
		ch <- 1
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
				} else {
					value = string(colValue) //非大字段类型,显式使用string函数强制转换为字符串文本，否则都是字节类型文本(即sql.RawBytes)
				}
			}
			prepareValues[i] = value //把第1列的列值追加到任意类型的切片里面，然后把第2列，第n列的值加到任意类型的切片里面,这里的切片即一行完整的数据
		}
		_, err = stmt.Exec(prepareValues...) //这里Exec只传入实参，即上面prepare的CopyIn所需的参数，这里理解为把stmt所有数据先存放到buffer里面
		if err != nil {
			log.Error("stmt.Exec(prepareValues...) failed ", err, prepareValues) // 这里是按行来的，不建议在这里输出错误信息,建议如果遇到一行错误就直接return返回
			ch <- 1
			return // 如果prepare异常就return
		}
	}
	_, err = stmt.Exec() //把所有的buffer进行flush，一次性写入数据
	if err != nil {
		log.Error("prepareValues Error: ", prepareValues, err) //注意这里不能使用Fatal，否则会直接退出程序，也就没法遇到错误继续了
		ch <- 1
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
	log.Info(fmt.Sprintf("%v 任务goroutine[%d] 表%v执行完成,迁移%d行,耗时%s", time.Now(), startPage, tableName, totalRow, cost))
	ch <- 0
}

func PrepareSrc() {
	// 生成源库连接
	srcHost := viper.GetString("src.host")
	srcUserName := viper.GetString("src.username")
	srcPassword := viper.GetString("src.password")
	srcDatabase := viper.GetString("src.database")
	srcConn := fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8&maxAllowedPacket=0", srcUserName, srcPassword, srcHost, srcDatabase)
	var err error
	srcDb, err = sql.Open("mysql", srcConn)
	if err != nil {
		log.Fatal("请检查Mysql配置", err)
	}
	c := srcDb.Ping()
	if c != nil {
		log.Fatal("连接Mysql失败", err)
	}
	srcDb.SetConnMaxLifetime(2 * time.Hour)
	srcDb.SetMaxIdleConns(0)
	srcDb.SetMaxOpenConns(30)
	log.Info("连接Mysql成功")
}

func PrepareDest() {
	// 生成目标库连接
	destHost := viper.GetString("dest.host")
	destUserName := viper.GetString("dest.username")
	destPassword := viper.GetString("dest.password")
	destDatabase := viper.GetString("dest.database")

	conn := fmt.Sprintf("host=%s user=%s password=%s dbname=%s sslmode=disable", destHost,
		destUserName, destPassword, destDatabase)
	var err error
	destDb, err = sql.Open("postgres", conn)
	if err != nil {
		log.Fatal("请检查Postgres配置", err)
	}
	c := destDb.Ping()
	if c != nil {
		log.Fatal("连接Postgres失败", err)
	}
	log.Info("连接Postgres成功")
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)

	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.gomysql2pg.yaml)")
	rootCmd.PersistentFlags().BoolVarP(&selFromYml, "selfromyml", "s", false, "select from yml true")

	// Cobra also supports local flags, which will only run
	// when this action is called directly.
	// rootCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
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

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		log.Info("Using config file:", viper.ConfigFileUsed())
	} else {
		log.Fatal(viper.ConfigFileUsed(), " has some error please check your yml file ! ", "Detail-> ", err)
	}
	log.Info("Using selfromyml:", selFromYml)
}

// CreateDateDir 根据当前日期来创建文件夹
func CreateDateDir(basePath string) string {
	folderName := time.Now().Format("2006_01_02_15_04_05")
	folderPath := filepath.Join(basePath, folderName)
	if _, err := os.Stat(folderPath); os.IsNotExist(err) {
		// 必须分成两步
		// 先创建文件夹
		os.Mkdir(folderPath, 0777)
		// 再修改权限
		os.Chmod(folderPath, 0777)
	}
	return folderPath
}
