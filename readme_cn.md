# gomysql2pg

![logo.png](image/logo.png)

## 一、工具简介与特性

### 1.1 功能特性

支持 `MySQL` 数据库一键迁移到 `PostgreSQL` 内核类型的目标数据库，如 `PostgreSQL`、`海量数据库 Vastbase`、`华为 GaussDB`、`电信 TelePG`、`人大金仓 Kingbase V8R6`、`华高数据库 HighGo` 等。

- 无需繁琐部署，开箱即用，小巧轻量化
- 支持批量迁移多对数据库，比如100对数据库
- 在线迁移 MySQL 到目标数据库的表、视图、索引、外键、自增列等对象
- 多个 goroutine 并发迁移数据，充分利用 CPU 多核性能
- 支持迁移源库部分表功能
- 记录迁移日志，转储表、视图等 DDL 对象创建失败的 SQL 语句
- 一键迁移 MySQL 到 PostgreSQL，方便快捷，轻松使用


<img src="image/gomysql2pg_en_struct.png" width="500">


需要知道的是 MySQL 跟其他基于 pgsql 内核或者协议的国产数据库，无论是数据库架构还是在表结构以及列类型、自增列实现方式、函数、存储过程等多个对象存在很多差异，如果通过传统的 SQL 备份文件导入到目标数据库，这势必是不可取也是最没效率的一种方式。

通过此工具，可以降低异构数据库之间开发的人工成本，并尽可能地将 MySQL 绝大多数的表结构、自增列、视图、行数据等多对象并发迁移到目标数据库，尽可能地适配目标类型，提供了全库级别、表对象级、自定义查询 SQL 等多种迁移方式。

gomysql2pg 根据 MySQL 内部的数据字典获取到数据库各个对象的定义和属性信息，并适配到目标数据库。数据迁移本质上就是把表数据从一个数据库"搬家"到另一个数据库，其中主要涉及读表、传输、写表，而读表和写表占据迁移绝大多数时间。

### 1.2 环境要求

在运行的客户端 PC 需要同时能连通源端 MySQL 数据库以及目标数据库。

支持 Windows、Linux、MacOS。

### 1.3 安装方式

解压之后即可运行此工具。

若在 Linux 环境下请使用 tar 解压，例如：

```bash
[root@localhost opt]# tar -zxvf gomysql2pg-linux64-0.1.7.tar.gz
```

---

## 二、单对库迁移

以下为 Windows 平台示例，其余操作系统命令行参数一样。

> `注意：` 在 `Windows` 系统请在 `CMD` 运行此工具；如果是在 `MacOS` 或者 `Linux` 系统，请在有读写权限的目录运行。

### 2.1 编辑配置文件

编辑 `example.yml` 文件，分别输入源库与目标数据库信息：

```yaml
src:
  host: 192.168.1.3
  port: 3306
  database: test
  username: root
  password: 11111
dest:
  dbType: Gauss # 如果使用的是 openGauss、GaussDB 或 HighGo 类型请一定要添加此行（取值为 Gauss 或 Highgo），其他类型一定要注释本行
  host: 192.168.1.200
  port: 5432
  database: test
  username: test
  password: 11111
pageSize: 100000
maxParallel: 30
charInLength: false
useNvarchar2: false
Distributed: false
tables:
  test1:
    - select * from test1
  test2:
    - select * from test2
exclude:
  - 'log1'
  - 'log2'
  - '*_log'
  - '*_cswysk'
```

**配置参数说明：**

- `pageSize`：分页查询每页的记录数。
  ```sql
  -- e.g. pageSize: 100000
  SELECT t.* FROM (SELECT id FROM test ORDER BY id LIMIT 0, 100000) temp LEFT JOIN test t ON temp.id = t.id;
  ```
- `maxParallel`：最大能同时运行 goroutine 的并发数。
- `tables`：自定义迁移的表以及自定义查询源表，按 yml 格式缩进。
- `exclude`：不需要迁移的表，按 yml 格式缩进，支持通配符星号（`*`），例如 `test*`。
- `charInLength`：若为 `true`，varchar 类型存储字符长度而不是字节长度，仅兼容部分数据库（例如海量数据库）。
- `useNvarchar2`：若为 `true`，目标数据库使用 nvarchar2 类型（例如 GaussDB）。
- `Distributed`：默认为 `false`（非分布式数据库）；如果是分布式数据库（如 GaussDB 8.1.3）请设为 `true`，工具会在增加主键之前先更改表分布列为主键列。
- `schemaMapping`：可选。视图 SQL 中跨 schema 引用的映射规则（key = MySQL 源 schema 名，value = PG 目标 schema 名；value 为空字符串则删除该前缀）。仅影响视图迁移，不影响表/数据。

### 2.2 全库迁移

迁移全库表结构、行数据、视图、索引约束、自增列等所有对象。

> `注意：` 如果目标数据库是 openGauss 或者 GaussDB，有 2 种方案可以让 varchar 类型按字符作为长度（即与 MySQL 一样）：
>
> - **方案 1**：调整配置文件，确保 `useNvarchar2` 字段值为 `true`，这将在目标数据库使用 nvarchar2 类型（Gauss 系列此类型支持按字符长度存储而不是字节）。
> - **方案 2**：创建数据库时指定兼容模式为 MySQL 或 PG，例如：
>   ```sql
>   create database test owner test DBCOMPATIBILITY='B';
>   -- 或
>   create database test owner test DBCOMPATIBILITY='PG';
>   ```

```bash
# Windows
gomysql2pg.exe --config example.yml

# Linux / macOS
./gomysql2pg --config example.yml
```

### 2.3 迁移模式说明

除全库迁移外，工具还支持通过子命令迁移部分数据库对象。

| 子命令 / 参数 | 说明 |
|---|---|
| `（无子命令）` | 全库迁移：表结构、行数据、视图、索引、自增列等全部对象 |
| `-s` | 自定义 SQL 查询迁移：仅迁移配置文件 `tables` 中定义的表（含表结构和数据） |
| `createTable -t` | 仅在目标库创建全库所有表的表结构 |
| `createTable -s -t` | 仅在目标库创建配置文件中自定义的表结构 |
| `onlyData` | 仅迁移全库所有表的行数据（不含表结构） |
| `onlyData -s` | 仅迁移配置文件中自定义查询 SQL 对应的行数据（不含表结构） |
| `seqOnly` | 仅将 MySQL 自增列转换为目标数据库序列 |
| `idxOnly` | 仅迁移 MySQL 的主键、索引等约束对象 |
| `viewOnly` | 仅迁移 MySQL 的视图 |
| `dryRun` | 预检模式：只读检查，不创建任何对象，不迁移任何数据 |

**各模式示例（Windows）：**

```bash
gomysql2pg.exe --config example.yml              # 全库迁移
gomysql2pg.exe --config example.yml -s           # 自定义 SQL 迁移
gomysql2pg.exe --config example.yml createTable -t      # 仅建表结构（全库）
gomysql2pg.exe --config example.yml createTable -s -t   # 仅建表结构（自定义）
gomysql2pg.exe --config example.yml onlyData     # 仅迁移数据（全库）
gomysql2pg.exe --config example.yml onlyData -s  # 仅迁移数据（自定义）
gomysql2pg.exe --config example.yml seqOnly      # 仅迁移自增列/序列
gomysql2pg.exe --config example.yml idxOnly      # 仅迁移索引约束
gomysql2pg.exe --config example.yml viewOnly     # 仅迁移视图
gomysql2pg.exe --config example.yml dryRun       # 预检
```

Linux / macOS 将 `gomysql2pg.exe` 替换为 `./gomysql2pg` 即可。

**预检（dryRun）说明：**

预检模式仅执行以下三项只读检查，不创建任何对象，不迁移任何数据：

1. `SourcePing`：ping 源 MySQL；
2. `DestPing`：ping 目的库（自动按 `dest.dbType` 选择驱动）；
3. `SameNameSchema`：目的库是否存在与 `dest.username` 同名的 schema。

退出码：全部通过为 0；任一失败为 1，并把失败明细写入 `log/<时间>__src__to__dest/dryRunFailed.log`。

如果 `SameNameSchema` 报 `no schema named "<user>" in database "<db>"`，请到目的库以特权用户执行：

```sql
create user <user> with password '123456';
create schema <user> authorization <user>;
```

将 `<user>` 替换为 yml 里的 `dest.username`，并将 `'123456'` 换成真实密码。

---

## 三、多对库批量迁移

适用场景：一次迁移多个库 / 多对源-目标。仓库根目录提供两份脚本：`run_batch.sh`（Linux / macOS）和 `run_batch.ps1`（Windows），二者行为、提示、日志格式、退出码完全一致。

### 3.1 批量生成配置文件

编辑 `configs/` 目录下的 Excel 文件 `example.xlsx`，在各个表头下方输入正确的连接信息（`src` 开头的为源库，`dest` 开头的为目标库）。

必需的表头列（首行，大小写不敏感）：`src_host`、`src_port`、`src_database`、`src_username`、`src_password`、`dest_host`、`dest_port`、`dest_database`、`dest_username`、`dest_password`。

- 输出文件命名为 `NNN_<src_database>.yml`，按行顺序编号；
- 已存在的文件默认跳过（加 `--overwrite` 覆盖）；
- 发现重复行会直接中止。

使用 `tools/xlsx2yml` 工具从 xlsx 批量生成配置文件：

```bash
# 开发环境
go run ./tools/xlsx2yml                              # 默认读取 configs/example.xlsx，输出到 configs/
go run ./tools/xlsx2yml -f my.xlsx -o out_dir        # 指定输入文件与输出目录
go run ./tools/xlsx2yml --sheet Sheet2 --overwrite   # 指定 sheet、覆盖已存在的 yml
go run ./tools/xlsx2yml --schema-mapping             # 同时生成 schemaMapping: { src_db: dest_user }

# 二进制文件
xlsx2yml.exe                              # 默认读取 configs/example.xlsx，输出到 configs/
xlsx2yml.exe -f my.xlsx -o out_dir
xlsx2yml.exe --sheet Sheet2 --overwrite
xlsx2yml.exe --schema-mapping
```

执行后会在 `configs/` 目录下生成多个 yml 格式的配置文件：

```
configs/
├── 001_db_1.yml
├── 002_db_2.yml
└── 003_db_3.yml
```

### 3.2 迁移预检（dryrun）

建议在正式迁移前先跑一次 `dryrun`，把环境问题（连通性、schema 缺失）一次性筛掉。

**脚本行为：**

1. 对每个 yml 调用 `gomysql2pg --config <file> dryRun`，仅做三项只读检查：源 MySQL 连接 ping、目的库连接 ping、目的库是否存在与 `dest.username` 同名的 schema；
2. 不创建任何对象、不迁移任何数据，跳过 `yes` 交互确认；
3. 批次日志命名为 `batch-dryrun-YYYYMMDD-HHMMSS.log`；单条失败时会在 `log/<时间>__src__to__dest/` 下生成 `dryRunFailed.log`，可被 `check_log.sh` / `check_log.ps1` 捕获；
4. 当存在"目的端缺少同名 schema"类失败时，全部跑完后终端尾部会追加补救 SQL 提示（也同步写入 batch 日志），可直接拷贝到目的库执行：

```
=== missing same-name schema(s) detected ===
Run the following SQL on the destination database as a privileged user:

create user admin2 with password '123456';
create schema admin2 authorization admin2;

(Replace '123456' with a real password before executing.)
```

**Linux / macOS：**

```bash
bash run_batch.sh configs dryrun
```

**Windows（PowerShell 5.1+，系统自带）：**

首次使用需授权 PowerShell 脚本执行权限（以管理员运行 PowerShell）：

```powershell
Set-ExecutionPolicy RemoteSigned -Scope CurrentUser -Force
Get-ExecutionPolicy -List
```

进入工具目录后运行预检：

```powershell
cd C:\db_tool\gomysql2pg-win-x64-v0.2.10
.\run_batch.ps1 configs dryrun
```

预检通过示例：

```
=== batch done 2026-05-15 15:00:51 ===
success: 3
  OK   C:\db_tool\gomysql2pg-win-x64-v0.2.10\configs\01_a.yml
  OK   C:\db_tool\gomysql2pg-win-x64-v0.2.10\configs\02_b.yml
  OK   C:\db_tool\gomysql2pg-win-x64-v0.2.10\configs\03_c.yml
failed:  0
```

### 3.3 正式批量迁移

**脚本行为：**

1. 先列出预览：`[i] file | src=host:port/db -> dest=user@host:port/db`；
2. 等待用户输入 `yes` 才开始执行，输入其它内容则放弃并退出（退出码 1）；
3. 顺序逐个调用 `gomysql2pg --config <file>`，单条失败不中断后续；
4. 全程输出同时打到屏幕和批次日志 `batch-YYYYMMDD-HHMMSS.log`；
5. 全部结束后打印 success / failed 列表，**退出码等于失败条数**（全部成功为 0）。

**Linux / macOS：**
脚本参数:
```bash
bash run_batch.sh                       # 默认读取 configs/ 目录
bash run_batch.sh path/to/cfg_dir       # 指定其它目录
```

运行示例：

```
cd gomysql2pg-linux-x64-v0.2.10

bash run_batch.sh

Found 3 config(s) to migrate:
[1] configs/01_a.yml  |  src=192.168.1.86:3306/db_a101  ->  dest=admin@192.168.1.95:5432/test
[2] configs/02_b.yml  |  src=192.168.1.86:3306/db_a102  ->  dest=admin2@192.168.1.95:5432/test
[3] configs/03_c.yml  |  src=192.168.1.86:3306/db_a999  ->  dest=admin3@192.168.1.95:5432/test

Proceed with these 3 migration(s)? [yes/no]: -> 输入yes或者no选择继续或者退出
```

**Windows（PowerShell 5.1+）：**
脚本参数:
```powershell
.\run_batch.ps1                              # 默认读取 configs/ 目录
.\run_batch.ps1 -ConfigDir my_cfgs           # 指定其它目录
```

运行示例：

```
PS C:\db_tool\gomysql2pg-win-x64-v0.2.10> .\run_batch.ps1
Found 3 config(s) to migrate:
  [1] C:\db_tool\gomysql2pg-win-x64-v0.2.10\configs\01_a.yml  |  src=192.168.1.86:3306/db_a101  ->  dest=admin@192.168.1.95:5432/test
  [2] C:\db_tool\gomysql2pg-win-x64-v0.2.10\configs\02_b.yml  |  src=192.168.1.86:3306/db_a102  ->  dest=admin2@192.168.1.95:5432/test
  [3] C:\db_tool\gomysql2pg-win-x64-v0.2.10\configs\03_c.yml  |  src=192.168.1.86:3306/db_a999  ->  dest=admin3@192.168.1.95:5432/test

Proceed with these 3 migration(s)? [yes/no]: -> 输入yes或者no选择继续或者退出
```

### 3.4 检查迁移日志

批量迁移结束后，使用 `check_log.sh` / `check_log.ps1` 扫描 `log/` 目录，快速汇总哪些库迁移无误、哪些存在失败或警告，无需逐一翻看子目录。

**Linux / macOS：**
脚本参数:
```bash
bash check_log.sh           # 默认扫描 log/ 目录
bash check_log.sh other_log # 指定其它日志目录
```

**使用示例：**
```bash
bash check_log.sh
```

**Windows（PowerShell）：**
脚本参数:
```powershell
.\check_log.ps1               # 默认扫描 log/
.\check_log.ps1 -LogDir other_log  # 指定目录
```

**使用示例：**
```powershell
.\check_log.ps1
```


**输出示例：**

```
Scanning log directory: log/

[OK]   2026_05_10_09_00_00__192.168.1.86__to__192.168.1.95
[WARN] 2026_05_10_09_05_00__192.168.1.86__to__192.168.1.95
       invalidTableData.log               (3 lines)

[FAIL] 2026_05_10_09_10_00__192.168.1.86__to__192.168.1.95
       tableCreateFailed.log              (5 lines)
       viewCreateFailed.log               (2 lines)
       invalidTableData.log               (1 lines) [WARN]

--- Summary ---
Total:   3
Failed:  1
OK:      2
```

**标记含义：**

| 标记 | 含义 |
|---|---|
| `[OK]` | 无任何失败或警告文件，迁移完全正常 |
| `[WARN]` | 仅存在 `invalidTableData.log`，部分行数据无效（如含无法转换的字符），其余对象正常 |
| `[FAIL]` | 存在一个或多个失败日志文件，显示文件名及行数，需人工排查 |

**检测的失败日志文件：**

`tableCreateFailed.log`、`seqCreateFailed.log`、`idxCreateFailed.log`、`DistributedAlterFailed.log`、`FkCreateFailed.log`、`viewCreateFailed.log`、`TriggerCreateFailed.log`、`failedTable.log`、`errorTableData.log`、`dryRunFailed.log`

**退出码：** 等于 `[FAIL]` 的目录数（全部通过为 0），可直接用于 CI/CD 流水线判断批量迁移是否成功。

---

## 四、查看迁移摘要

全库迁移完成之后会生成迁移摘要，可观察是否有失败的对象，通过查询迁移日志可对迁移失败的对象进行分析。

```
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
time="2023-07-11T12:34:01+08:00" level=info msg="All complete totalTime 10m30.1667987s\nThe Report Dir C:\\go\\src\\gomysql2pg\\2023_07_11_12_23_31"
```

---

## 五、比对数据

迁移完成后比对源库和目标库，查看是否有迁移数据失败的表。

```bash
# Windows
gomysql2pg.exe --config example.yml compareDb

# Linux / macOS
./gomysql2pg --config example.yml compareDb
```

比对结果示例（仅显示不一致的表）：

```
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

---

## 六、版本更新说明

| 版本 | 日期 | 主要更新 |
|---|---|---|
| v0.2.7.1 | 2026-04-15 | 新增 binary/tinyblob/blob/mediumblob/longblob 类型到 `bytea` 的映射支持 |
| v0.2.8 | 2026-04-15~16 | 视图 SQL 自动适配转换，包括字符集处理、schema 前缀删除/重命名 |
| v0.2.8.1 | 2026-05-11 | 修复迁移摘要（Summary）输出 |
| v0.2.8.2 | 2026-05-11 | 优化日志打印格式与完整性 |
| v0.2.9 | 2026-05-11~12 | 新增批量迁移脚本 `run_batch.sh`（Linux/macOS）与 `run_batch.ps1`（Windows） |
| v0.2.10 | 2026-05-13~14 | 新增 Excel 配置生成工具（`tools/xlsx2yml`）；新增 `schemaMapping` 配置项；新增 `dryRun` 预检模式；新增日志扫描脚本 `check_log.sh` / `check_log.ps1`；修复目标库连接兼容性问题 |
