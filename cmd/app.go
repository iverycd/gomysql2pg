package cmd

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"fmt"
	_ "gitee.com/opengauss/openGauss-connector-go-pq"
	"github.com/spf13/viper"
	"gomysql2pg/connect"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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
	srcDb.SetConnMaxLifetime(2 * time.Hour) // 一个连接被使用的最长时间，过一段时间之后会被强制回收
	srcDb.SetMaxIdleConns(0)                // 最大空闲连接数，0为不限制
	srcDb.SetMaxOpenConns(30)               // 设置连接池最大连接数
	log.Info("connect MySQL ", srcHost, " success")
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
	if strings.ToUpper(viper.GetString("dest.dbType")) == "GAUSS" {
		destDb, err = sql.Open("opengauss", conn)
	}
	if err != nil {
		log.Fatal("please check Postgres yml file", err)
	}
	c := destDb.Ping()
	if c != nil {
		log.Fatal("connect Postgres failed", c)
	}
	log.Info("connect Postgres ", destHost, " success")
}

func LogError(logDir string, logName string, strContent string, errInfo error) {
	f, errFile := os.OpenFile(logDir+"/"+logName+".log", os.O_CREATE|os.O_APPEND|os.O_RDWR, os.ModePerm)
	if errFile != nil {
		log.Fatal(errFile)
	}
	defer func() {
		if errFile := f.Close(); errFile != nil {
			log.Fatal(errFile) // 或设置到函数返回值中
		}
	}()
	// create new buffer
	buffer := bufio.NewWriter(f)
	_, errFile = buffer.WriteString(strContent + " -- ErrorInfo " + StrVal(errInfo) + "\n")
	if errFile != nil {
		log.Fatal(errFile)
	}
	// flush buffered data to the file
	if errFile := buffer.Flush(); errFile != nil {
		log.Fatal(errFile)
	}
}

// StrVal
// 获取变量的字符串值，目前用于interface类型转成字符串类型
// 浮点型 3.0将会转换成字符串3, "3"
// 非数值或字符类型的变量将会被转换成JSON格式字符串
func StrVal(value interface{}) string {
	var key string
	if value == nil {
		return key
	}

	switch value.(type) {
	case float64:
		ft := value.(float64)
		key = strconv.FormatFloat(ft, 'f', -1, 64)
	case float32:
		ft := value.(float32)
		key = strconv.FormatFloat(float64(ft), 'f', -1, 64)
	case int:
		it := value.(int)
		key = strconv.Itoa(it)
	case uint:
		it := value.(uint)
		key = strconv.Itoa(int(it))
	case int8:
		it := value.(int8)
		key = strconv.Itoa(int(it))
	case uint8:
		it := value.(uint8)
		key = strconv.Itoa(int(it))
	case int16:
		it := value.(int16)
		key = strconv.Itoa(int(it))
	case uint16:
		it := value.(uint16)
		key = strconv.Itoa(int(it))
	case int32:
		it := value.(int32)
		key = strconv.Itoa(int(it))
	case uint32:
		it := value.(uint32)
		key = strconv.Itoa(int(it))
	case int64:
		it := value.(int64)
		key = strconv.FormatInt(it, 10)
	case uint64:
		it := value.(uint64)
		key = strconv.FormatUint(it, 10)
	case string:
		key = value.(string)
	case []byte:
		key = string(value.([]byte))
	default:
		newValue, _ := json.Marshal(value)
		key = string(newValue)
	}

	return key
}

func cleanDBconn() {
	// 遍历正在执行gomysql2pg的客户端，使用kill query 命令kill所有查询id
	rows, err := srcDb.Query("select id from information_schema.PROCESSLIST where info like '/* gomysql2pg%';")
	if err != nil {
		log.Error(err)
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		err = rows.Scan(&id)
		if err != nil {
			log.Error("rows.Scan(&id) failed!", err)
		}
		srcDb.Exec("kill query " + id)
		log.Info("kill thread id ", id)
	}
}

// 监控来自终端的信号，如果按下了ctrl+c，断开数据库查询以及退出程序
func exitHandle(exitChan chan os.Signal) {

	for {
		select {
		case sig := <-exitChan:
			fmt.Println("receive system signal:", sig)
			cleanDBconn() // 调用清理数据库连接的方法
			os.Exit(1)    //如果ctrl+c 关不掉程序，使用os.Exit强行关掉
		}
	}

}

// CreateDateDir 根据当前日期来创建文件夹
func CreateDateDir(basePath string) string {
	folderName := "log/" + time.Now().Format("2006_01_02_15_04_05")
	folderPath := filepath.Join(basePath, folderName)
	if _, err := os.Stat(folderPath); os.IsNotExist(err) {
		// 必须分成两步
		// 先创建文件夹
		err := os.MkdirAll(folderPath, 0777) //级联创建目录
		if err != nil {
			fmt.Println("create directory log failed ", err)
		}
		// 再修改权限
		err = os.Chmod(folderPath, 0777)
		if err != nil {
			fmt.Println("chmod directory log failed ", err)
		}
	}
	return folderPath
}
