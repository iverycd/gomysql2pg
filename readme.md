# gomysql2pg

## change history


### v0.1.1
2023-06-26

增加创建视图、外键、触发器到目标数据库


### v0.1.0
2023-06-16

增加创建索引、主键、等约束

### v0.0.9
2023-06-14

新增创建序列


### v0.0.8
2023-06-13

使用多个goroutine并发生成每个表的迁移任务、创建表，其余优化

### v0.0.7
2023-06-12

修复prepareSqlStr中没有行数据被漏掉创建的表,迁移数据前会查询下目标表是否存在,其余优化

### v0.0.6
2023-06-09

增加创建基本表的功能

### v0.0.5
2023-06-06

增加标题字符图，显示版本信息,彩色文字显示输出

### v0.0.4
2023-06-05

在遇到Ctrl+c输入后主动关闭数据库正在运行的sql,输出格式简化,转储迁移失败的表数据到日志目录

### v0.0.3
2023-06-02

config文件增加端口设定,自定义sql外面包了一层select * from (自定义sql) where 1=0 用于获取列字段，避免查询全表数据,在copy方法的exec刷buffer之前，再一次主动使用row.close关闭数据库连接

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
exclude:
```

### 2.2 迁移模式指定

模式1 全库行数据迁移

go run ./main.go  --config 配置文件

根据配置文件源库以及目标的信息(会忽略配置文件中自定义查询SQL语句)，查找源库的所有表，全表行数据迁移到pg目标库
```
go run ./main.go  --config example.yml
```

模式2 自定义SQL查询迁移

go run ./main.go  --config 配置文件 -s

不迁移全表数据，只根据配置文件中自定义查询语句迁移数据到目标库
```
go run ./main.go  --config example.yml -s
```

模式3 仅创建表(全库所有表)

`如果配置文件中exclude区域有配置排除表，该表也不会被创建`

go run ./main.go  --config 配置文件 -t

仅在目标库创建所有表
```
go run ./main.go  --config example.yml -t
```

模式4 仅创建配置文件内容的表

go run ./main.go  --config 配置文件 -s -t

仅在目标库创建所有表
```
go run ./main.go  --config example.yml -s -t
```
