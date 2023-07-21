# gomysql2pg

([CN](https://github.com/iverycd/gomysql2pg/blob/master/readme_cn.md))

## Features
  MySQL database migration to postgresql kernel database,such as postgresql(pgsql),vastbase,Huawei postgresql,GaussDB,telepg,Kingbase V8R6

* No need for cumbersome deployment, ready to use out of the box, compact and lightweight

* Online migration of MySQL to target database tables, views, indexes, foreign keys, self increasing columns, and other objects

* Multiple goroutines migrate data concurrently, fully utilizing CPU multi-core performance

* Migrate Partial Tables and row data

* Record migration logs, dump SQL statements for DDL object creation failures such as tables and views

* One click migration of MySQL to postgreSQL, convenient, fast, and easy to use


## Pre-requirement
The running client PC needs to be able to connect to both the source MySQL database and the target database simultaneously

run on Windows,Linux,macOS

## Installation

tar and run 

e.g.

`[root@localhost opt]# tar -zxvf gomysql2pg-linux64-0.1.7.tar.gz`

## How to use

The following is an example of a Windows platform, with the same command-line parameters as other operating systems

`Note`: Please run this tool in `CMD` on a `Windows` system, or in a directory with read and write permissions on `MacOS` or `Linux`

### 1 Edit yml configuration file

Edit the `example.cfg` file and input the source(src) and target(dest) database information separately

```yaml
src:
  host: 192.168.1.3
  port: 3306
  database: test
  username: root
  password: 11111
dest:
  host: 192.168.1.200
  port: 5432
  database: test
  username: test
  password: 11111
pageSize: 100000
maxParallel: 30
tables:
  test1:
    - select * from test1
  test2:
    - select * from test2
exclude:
  operalog1
  operalog2
  operalog3

```

pageSize: Number of records per page for pagination query
```
e.g.
pageSize:100000
SELECT t.* FROM (SELECT id FROM test  ORDER BY id LIMIT 0, 100000) temp LEFT JOIN test t ON temp.id = t.id;
```
maxParallel: The maximum number of concurrency that can run goroutine simultaneously

tables: Customized migrated tables and customized query source tables, indented in yml format

exclude: Tables that do not migrate to target database, indented in yml format


### 2 Full database migration

Migrate entire database table structure, row data, views, index constraints, and self increasing columns to target database

gomysql2pg.exe  --config file.yml
```
e.g.
gomysql2pg.exe --config example.yml

on Linux and MacOS you can run
./gomysql2pg --config example.yml
```

### 3 View Migration Summary

After the entire database migration is completed, a migration summary will be generated to observe if there are any failed objects. By querying the migration log, the failed objects can be analyzed

```bash
+-------------------------+---------------------+-------------+----------+
|        SourceDb         |       DestDb        | MaxParallel | PageSize |
+-------------------------+---------------------+-------------+----------+
| 192.168.149.37-sourcedb | 192.168.149.33-test |     30      |  100000  |
+-------------------------+---------------------+-------------+----------+

+------------+----------------------------+----------------------------+-------------+---------------+
|Object      |         BeginTime          |          EndTime           |FailedTotal  |ElapsedTime    |
+------------+----------------------------+----------------------------+-------------+---------------+
|TableData   | 2023-07-11 12:23:55.584092 | 2023-07-11 12:28:44.105372 |6            |4m48.5212802s  |
|Sequence    | 2023-07-11 12:30:04.697570 | 2023-07-11 12:30:12.549534 |1            |7.8519647s     |
|Index       | 2023-07-11 12:30:12.549534 | 2023-07-11 12:33:45.312366 |5            |3m32.7628317s  |
|ForeignKey  | 2023-07-11 12:33:45.312366 | 2023-07-11 12:34:00.413767 |0            |15.1014013s    |
|View        | 2023-07-11 12:34:00.413767 | 2023-07-11 12:34:01.240472 |14           |826.705ms      |
|Trigger     | 2023-07-11 12:34:01.240472 | 2023-07-11 12:34:01.339078 |1            |98.6061ms      |
+------------+----------------------------+----------------------------+-------------+---------------+

Table Create finish elapsed time  5.0256021s
time="2023-07-11T12:34:01+08:00" level=info msg="All complete totalTime 10m30.1667987s\nThe Report Dir C:\\go\\src\\gomysql2pg\\2023_07_11_12_23_31" func=gomysql2pg/cmd.mysql2pg file="C:/go/src/gomysql2pg/cmd/root.go:207"

```

### 4 Compare Source and Target database

After migration finish you can compare source table and target database table rows,displayed failed table only

`gomysql2pg.exe --config your_file.yml compareDb`

```
e.g.
gomysql2pg.exe --config example.yml compareDb

on Linux and MacOS you can run
./gomysql2pg --config example.yml compareDb
```

```bash
Table Compare Result (Only Not Ok Displayed)
+-----------------------+------------+----------+-------------+------+
|Table                  |SourceRows  |DestRows  |DestIsExist  |isOk  |
+-----------------------+------------+----------+-------------+------+
|abc_testinfo           |7458        |0         |YES          |NO    |
|log1_qweharddiskweqaz  |0           |0         |NO           |NO    |
|abcdef_jkiu_button     |4           |0         |YES          |NO    |
|abcdrf_yuio            |5           |0         |YES          |NO    |
|zzz_ss_idcard          |56639       |0         |YES          |NO    |
|asdxz_uiop             |290497      |190497    |YES          |NO    |
|abcd_info              |1052258     |700000    |YES          |NO    |
+-----------------------+------------+----------+-------------+------+ 
INFO[0040] Table Compare finish elapsed time 11.307881434s 
```







## Other migration modes

In addition to migrating the entire database, the tool also supports the migration of some database objects, such as partial table structures, views, self increasing columns, indexes, and so on


#### 1 Full database migration

Migrate entire database table structure, row data, views, index constraints, and self increasing columns to target database

gomysql2pg.exe  --config file.yml

```
e.g.
gomysql2pg.exe --config example.yml
```

#### 2 Custom SQL Query Migration

only migrate some tables not entire database, and migrate the table structure and table data to the target database according to the custom query statement in file.yml

gomysql2pg.exe  --config file.yml -s

```
e.g.
gomysql2pg.exe  --config example.yml -s
```

#### 3 Migrate all table structures in the entire database

Create all table structure(only table metadata not row data) to  target database

gomysql2pg.exe  --config file.yml createTable -t

```
e.g.
gomysql2pg.exe  --config example.yml createTable -t
```

#### 4 Migrate the table structure of custom tables

Read custom tables from yml file and create target table 

gomysql2pg.exe  --config file.yml createTable -s -t

```
e.g.
gomysql2pg.exe  --config example.yml createTable -s -t
```


#### 5 Migrate full database table data

Only migrate the entire database table row data to the target database, only row data, not contain table structure

gomysql2pg.exe  --config file.yml onlyData
```
e.g.
gomysql2pg.exe  --config example.yml onlyData
```

#### 6 Migrate custom table data

Only migrate custom query SQL from yml file, only row data, not contain table structure

gomysql2pg.exe  --config file.yml onlyData -s

```
e.g.
gomysql2pg.exe  --config example.yml onlyData -s
```

#### 7 Migrate self increasing columns to the target sequence

Only migrate MySQL's autoincrement columns to target database sequences

gomysql2pg.exe  --config file.yml seqOnly

```
e.g.
gomysql2pg.exe  --config example.yml seqOnly
```

#### 8 Migrate index and primary key

Only migrate MySQL primary keys, indexes, and other objects to the target database

gomysql2pg.exe  --config file.yml idxOnly

```
e.g.
gomysql2pg.exe  --config example.yml idxOnly
```

#### 9 Migration View

Only migrate MySQL views to the target database

gomysql2pg.exe  --config file.yml viewOnly

```
e.g.
gomysql2pg.exe  --config example.yml viewOnly
```

## change history

### v0.1.9
2023-07-21

modify compare full table result all data and readme modify


### v0.1.8
2023-07-14

add compare database,create table method add double quote

### v0.1.7
2023-07-11

Using multiple goroutines to create tables concurrently and optimize migration summary information

### v0.1.6
2023-07-10

Add Makefile and output config info


### v0.1.5
2023-07-07

Increase the count of the Global variable channel's failure to process the migration row data, which will be shown in the migration summary

### v0.1.4
2023-06-30

Fixed the issue of only migrating Linux pg libraries and failing to migrate under Windows. The method of creating tables has now been changed to single threaded

### v0.1.3
2023-06-28

Add commands to separately migrate table row data, optimize migration summary, and dump error information to log files for optimization

### v0.1.2
2023-06-27

Add migration summary and improve constraints for creating foreign keys

### v0.1.1
2023-06-26

Add migration summary and improve constraints for creating foreign keys


### v0.1.0
2023-06-16

Add constraints such as creating indexes, primary keys, etc

### v0.0.9
2023-06-14

Add Create Sequence


### v0.0.8
2023-06-13

Add a creation sequence that uses multiple goroutines to concurrently generate migration tasks for each table, create tables, and optimize the rest

### v0.0.7
2023-06-12

Fix the table created without missing row data in prepareSqlStr. Before migrating data, check if the target table exists, and optimize the rest

### v0.0.6
2023-06-09

Add create table metadata to target database

### v0.0.5
2023-06-06

Add a title character map, display version information, and output color text display

### v0.0.4
2023-06-05

After encountering Ctrl+c input, proactively close the running SQL of the database, simplify the output format, and dump the table data that failed the migration to the log directory

### v0.0.3
2023-06-02

The config file adds port settings, and a layer of select * from (custom SQL) is added outside the custom SQL, where 1=0 is used to obtain column fields to avoid querying the entire table data. Before using the exec buffer of the copy method, the database connection is actively closed again using row.close

### v0.0.2
2023-05-24

Add exclusion table parameters and check for abnormal configuration in the config yml file

### v0.0.1
2023-05-23

The log method prints the calling file and the number of method source lines, increasing the redirection of logs to a flat file