# gomysql2pg
## change history
### v0.0.2
2023-05-24
增加排除表参数，以及config yml文件配置异常检查
### v0.0.1
2023-05-23
log方法打印调用文件以及方法源行数，增加日志重定向到平面文件

## 一、简介
迁移MySQL数据库表数据到目标postgresql数据库，迁移时会使用多个goroutine并发的读取以及写入数据

## 二、使用方法
### 2.1 编辑yml配置文件

编辑`example.cfg`文件，输入

```yaml
src:
  host: 192.168.1.3  # 源库ip
  database: test  # 源库数据库名
  username: root
  password: 11111
dest:
  host: 192.168.1.200  # 目标ip
  database: test  # 目标数据库名称
  username: test  # 目标用户名
  password: 11111
maxRows: 10000
tables:  # 要迁移的表名，按顺序列出
  test1:  # 要迁移的表名
    - select * from test1  # 查询源表的SQL语句
  test2:
    - select * from test2
```

### 2.2 迁移模式指定

模式1 全库行数据迁移

go run ./main.go  --config 配置文件

根据配置文件源库以及目标的信息(会忽略配置文件中自定义查询SQL语句)，查找源库的所有表，全表行数据迁移到pg目标库
```
go run ./main.go  --config example.yml
```

模式2 自定义SQL查询迁移

go run ./main.go  --config 配置文件 --selfromyml

不迁移全表数据，只根据配置文件中自定义查询语句迁移数据到目标库
```
go run ./main.go  --config example.yml --selfromyml
```
