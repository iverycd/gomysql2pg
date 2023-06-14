package cmd

import (
	"github.com/spf13/viper"
	"io"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
)

var tableOnly bool

func init() {
	rootCmd.AddCommand(createTableCmd)
	rootCmd.AddCommand(seqOnlyCmd)
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
		db = new(Table)
		db.TableCreate(logDir, tableMap)
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
