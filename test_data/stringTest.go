package main

import (
	"bytes"
	"fmt"
	"strings"
)

func strTest() {
	var sqlStr string
	var excludeSql string
	excludeTable := []string{"aa*", "bb*", "cc*", "dd"}
	if excludeTable != nil {
		//sqlStr = "select table_name from information_schema.tables where table_schema=database() and table_type='BASE TABLE' and table_name not in ("
		sqlStr = "select table_name from information_schema.tables where table_schema=database() and table_type='BASE TABLE'"
		buffer := bytes.NewBufferString("")

		for _, tabName := range excludeTable {
			//if index < len(excludeTable)-1 {
			//	buffer.WriteString("'" + tabName + "'" + ",")
			//} else {
			//	buffer.WriteString("'" + tabName + "'" + ")")
			//}
			if strings.Contains(tabName, "*") {
				tabName = strings.ReplaceAll(tabName, "*", "%")
				buffer.WriteString(" and table_name not like " + "'" + tabName + "'" + " ")
				excludeSql += " and table_name not like " + tabName
			} else {
				buffer.WriteString(" and table_name not like " + "'" + tabName + "'" + " ")
				excludeSql += " and table_name not like " + tabName
			}

		}
		sqlStr += buffer.String()
	} else {
		sqlStr = "select table_name from information_schema.tables where table_schema=database() and table_type='BASE TABLE';" // 获取库里全表名称
	}
	fmt.Println(sqlStr)
	fmt.Println(excludeSql)
}

func main() {
	strTest()
}
