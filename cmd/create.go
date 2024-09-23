package cmd

import (
	"fmt"
	"github.com/liushuochen/gotable"
	"github.com/spf13/viper"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/spf13/cobra"
)

var tableOnly bool

func init() {
	rootCmd.AddCommand(createTableCmd)
	rootCmd.AddCommand(seqOnlyCmd)
	rootCmd.AddCommand(idxOnlyCmd)
	rootCmd.AddCommand(viewOnlyCmd)
	rootCmd.AddCommand(onlyDataCmd)
	createTableCmd.Flags().BoolVarP(&tableOnly, "tableOnly", "t", false, "only create table true")
}

var createTableCmd = &cobra.Command{
	Use:   "createTable",
	Short: "Create meta table and no table data rows",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		// 获取配置文件中的数据库连接字符串
		connStr := getConn()
		// 每页的分页记录数,仅全库迁移时有效
		pageSize := viper.GetInt("pageSize")
		// 从配置文件中获取需要排除的表
		excludeTab := viper.GetStringSlice("exclude")
		PrepareSrc(connStr)
		PrepareDest(connStr)
		var tableMap map[string][]string
		// 以下是迁移数据前的准备工作，获取要迁移的表名以及该表查询源库的sql语句(如果有主键生成该表的分页查询切片集合，没有主键的统一是全表查询sql)
		if selFromYml { // 如果用了-s选项，从配置文件中获取表名以及sql语句
			tableMap = viper.GetStringMapStringSlice("tables")
		} else { // 不指定-s选项，查询源库所有表名
			tableMap = fetchTableMap(pageSize, excludeTab)
		}
		// 创建运行日志目录
		logDir, _ := filepath.Abs(CreateDateDir(""))
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
		// 实例初始化，调用接口中创建目标表的方法
		var db Database
		start := time.Now()
		db = new(Table)
		// 用于控制协程goroutine运行时候的并发数,例如3个一批，3个一批的goroutine并发运行
		maxParallel := viper.GetInt("maxParallel")
		if maxParallel == 0 {
			maxParallel = 20
		}
		ch := make(chan struct{}, maxParallel)
		//遍历tableMap
		for tableName := range tableMap { //获取单个表名
			ch <- struct{}{}
			wg2.Add(1)
			go db.TableCreate(logDir, tableName, ch)
		}
		// 这里等待上面所有迁移数据的goroutine协程任务完成才会接着运行下面的主程序，如果这里不wait，上面还在迁移行数据的goroutine会被强制中断
		wg2.Wait()
		cost := time.Since(start)
		log.Info("Table structure synced from MySQL to PostgreSQL ,Source Table Total ", tableCount, " Failed Total ", strconv.Itoa(failedCount))
		fmt.Println("Table Create finish elapsed time ", cost)
	},
}

var seqOnlyCmd = &cobra.Command{
	Use:   "seqOnly",
	Short: "Create sequence",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		// 获取配置文件中的数据库连接字符串
		connStr := getConn()
		PrepareSrc(connStr)
		PrepareDest(connStr)
		// 创建运行日志目录
		logDir, _ := filepath.Abs(CreateDateDir(""))
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
		// 实例初始化，调用接口中创建目标表的方法
		var db Database
		db = new(Table)
		db.SeqCreate(logDir)
	},
}

var idxOnlyCmd = &cobra.Command{
	Use:   "idxOnly",
	Short: "Create index",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		// 获取配置文件中的数据库连接字符串
		connStr := getConn()
		PrepareSrc(connStr)
		PrepareDest(connStr)
		// 从配置文件中获取需要排除的表
		excludeTab := viper.GetStringSlice("exclude")
		// 创建运行日志目录
		logDir, _ := filepath.Abs(CreateDateDir(""))
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
		// 实例初始化，调用接口中创建目标表的方法
		var db Database
		db = new(Table)
		db.IdxCreate(logDir, excludeTab)
	},
}

var viewOnlyCmd = &cobra.Command{
	Use:   "viewOnly",
	Short: "Create view",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		// 获取配置文件中的数据库连接字符串
		connStr := getConn()
		PrepareSrc(connStr)
		PrepareDest(connStr)
		// 创建运行日志目录
		logDir, _ := filepath.Abs(CreateDateDir(""))
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
		// 实例初始化，调用接口中创建目标表的方法
		var db Database
		db = new(Table)
		db.ViewCreate(logDir)
	},
}

var onlyDataCmd = &cobra.Command{
	Use:   "onlyData",
	Short: "only transfer table data rows",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		migDataStart := time.Now()
		// 获取配置文件中的数据库连接字符串
		connStr := getConn()
		// 每页的分页记录数,仅全库迁移时有效
		pageSize := viper.GetInt("pageSize")
		// 从配置文件中获取需要排除的表
		excludeTab := viper.GetStringSlice("exclude")
		PrepareSrc(connStr)
		PrepareDest(connStr)
		var tableMap map[string][]string
		// 以下是迁移数据前的准备工作，获取要迁移的表名以及该表查询源库的sql语句(如果有主键生成该表的分页查询切片集合，没有主键的统一是全表查询sql)
		if selFromYml { // 如果用了-s选项，从配置文件中获取表名以及sql语句
			tableMap = viper.GetStringMapStringSlice("tables")
		} else { // 不指定-s选项，查询源库所有表名
			tableMap = fetchTableMap(pageSize, excludeTab)
		}
		// 创建运行日志目录
		logDir, _ := filepath.Abs(CreateDateDir(""))
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
		// 创建表之后，开始准备迁移表行数据
		// 同时执行goroutine的数量，这里是每个表查询语句切片集合的长度
		var goroutineSize int
		//遍历每个表需要执行的切片查询SQL，累计起来获得总的goroutine并发大小
		for _, sqlList := range tableMap {
			goroutineSize += len(sqlList)
		}
		// 每个goroutine运行开始以及结束之后使用的通道，主要用于控制内层的goroutine任务与外层main线程的同步，即主线程需要等待子任务完成
		//ch := make(chan int, goroutineSize)  //v0.1.4及之前的版本通道使用的通道，配合下面for循环遍历行数据迁移失败的计数
		// 从yml配置文件中获取迁移数据时最大运行协程数
		maxParallel := viper.GetInt("maxParallel")
		if maxParallel == 0 {
			maxParallel = 20
		}
		// 用于控制协程goroutine运行时候的并发数,例如3个一批，3个一批的goroutine并发运行
		ch := make(chan struct{}, maxParallel)
		// 在协程里运行函数response，主要是从下面调用协程go runMigration的时候获取到里面迁移行数据失败的数量
		go response()
		//遍历tableMap，先遍历表，再遍历该表的sql切片集合
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
				log.Info("table not exists")
			}
		}
		// v0.1.4版本及之前通过循环获取ch通道里写的int数据判断是否有迁移行数据失败的表，如果通道里发送的数据是2说明copy失败了
		// 这里是等待上面所有goroutine任务完成，才会执行for循环下面的动作
		//migDataFailed := 0
		//for i := 0; i < goroutineSize; i++ {
		//	migDataRet := <-ch
		//	log.Info("goroutine[", i, "]", " finish ", time.Now().Format("2006-01-02 15:04:05.000000"))
		//	if migDataRet == 2 {
		//		migDataFailed += 1
		//	}
		//}
		// 这里等待上面所有迁移数据的goroutine协程任务完成才会接着运行下面的主程序，如果这里不wait，上面还在迁移行数据的goroutine会被强制中断
		wg.Wait()
		migDataEnd := time.Now()
		migCost := time.Since(migDataStart)
		tableDataRet := []string{"TableData", migDataStart.Format("2006-01-02 15:04:05.000000"), migDataEnd.Format("2006-01-02 15:04:05.000000"), strconv.Itoa(errDataCount), migCost.String()}
		// 输出配置文件信息
		tblConfig, err := gotable.Create("SourceDb", "DestDb", "MaxParallel", "PageSize")
		if err != nil {
			fmt.Println("Create tblConfig failed: ", err.Error())
			return
		}
		ymlConfig := []string{connStr.SrcHost + "-" + connStr.SrcDatabase, connStr.DestHost + "-" + connStr.DestDatabase, strconv.Itoa(maxParallel), strconv.Itoa(pageSize)}
		tblConfig.AddRow(ymlConfig)
		fmt.Println(tblConfig)
		// 数据库对象迁移后信息
		table, err := gotable.Create("Object", "BeginTime", "EndTime", "DataErrorCount", "ElapsedTime")
		if err != nil {
			fmt.Println("Create table failed: ", err.Error())
			return
		}
		table.AddRow(tableDataRet)
		table.Align("Object", 1)
		table.Align("DataErrorCount", 1)
		table.Align("ElapsedTime", 1)
		fmt.Println(table)
		log.Info(fmt.Sprintf("All Table Data Finish Total Elapsed TIme %s The Report Dir %s", migCost, logDir))
	},
}
