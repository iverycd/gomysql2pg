package cmd

import (
	"bufio"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "gitee.com/opengauss/openGauss-connector-go-pq"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"github.com/liushuochen/gotable"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"gomysql2pg/connect"
	_ "gomysql2pg/internal/highgopq"
)

func init() {
	rootCmd.AddCommand(dryRunCmd)
}

var dryRunCmd = &cobra.Command{
	Use:   "dryRun",
	Short: "Validate connections and target same-name schema without migrating",
	Long: `Only performs read-only pre-flight checks: pings the MySQL source, pings
the PostgreSQL target, and verifies a schema with the same name as
dest.username exists. No objects are created and no data is migrated.

Exits with code 0 when all checks pass. On any failure a log directory
containing dryRunFailed.log is created so check_log scripts can flag it.`,
	Run: func(cmd *cobra.Command, args []string) {
		connStr := getConn()
		exitCode := runDryRun(connStr)
		if exitCode != 0 {
			os.Exit(exitCode)
		}
	},
}

type dryRunResult struct {
	Check  string
	Status string
	Detail string
}

func runDryRun(connStr *connect.DbConnStr) int {
	destDriver := "postgres"
	if strings.ToUpper(viper.GetString("dest.dbType")) == "GAUSS" {
		destDriver = "opengauss"
	} else if strings.ToUpper(viper.GetString("dest.dbType")) == "HIGHGO" {
		destDriver = "highgo"
	}

	results := make([]dryRunResult, 0, 3)

	if err := pingSrc(connStr); err != nil {
		results = append(results, dryRunResult{"SourcePing", "FAIL", err.Error()})
	} else {
		results = append(results, dryRunResult{"SourcePing", "OK", fmt.Sprintf("mysql %s:%d/%s", connStr.SrcHost, connStr.SrcPort, connStr.SrcDatabase)})
	}

	destPingErr := pingDest(connStr, destDriver)
	if destPingErr != nil {
		results = append(results, dryRunResult{"DestPing", "FAIL", fmt.Sprintf("driver=%s %s", destDriver, destPingErr.Error())})
	} else {
		results = append(results, dryRunResult{"DestPing", "OK", fmt.Sprintf("driver=%s %s:%d/%s", destDriver, connStr.DestHost, connStr.DestPort, connStr.DestDatabase)})
	}

	switch {
	case strings.TrimSpace(connStr.DestUserName) == "":
		results = append(results, dryRunResult{"SameNameSchema", "FAIL", "dest.username is empty in config"})
	case destPingErr != nil:
		results = append(results, dryRunResult{"SameNameSchema", "SKIP", "skipped because DestPing failed"})
	default:
		if err := checkSameNameSchema(connStr, destDriver); err != nil {
			results = append(results, dryRunResult{"SameNameSchema", "FAIL", err.Error()})
		} else {
			results = append(results, dryRunResult{"SameNameSchema", "OK", fmt.Sprintf("schema %q exists", connStr.DestUserName)})
		}
	}

	printDryRunReport(connStr, destDriver, results)

	failures := make([]dryRunResult, 0, len(results))
	for _, r := range results {
		if r.Status == "FAIL" {
			failures = append(failures, r)
		}
	}
	if len(failures) == 0 {
		log.Info("dryRun passed, no objects created and no data migrated")
		return 0
	}

	writeDryRunFailedLog(failures)
	log.Errorf("dryRun failed with %d check(s) not ok", len(failures))
	return 1
}

func pingSrc(connStr *connect.DbConnStr) error {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%v)/%s?charset=utf8&maxAllowedPacket=0",
		connStr.SrcUserName, connStr.SrcPassword, connStr.SrcHost, connStr.SrcPort, connStr.SrcDatabase)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	db.SetConnMaxLifetime(5 * time.Second)
	return db.Ping()
}

func pingDest(connStr *connect.DbConnStr, driver string) error {
	dsn := fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%v sslmode=disable",
		connStr.DestHost, connStr.DestUserName, connStr.DestPassword, connStr.DestDatabase, connStr.DestPort)
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	db.SetConnMaxLifetime(5 * time.Second)
	return db.Ping()
}

func checkSameNameSchema(connStr *connect.DbConnStr, driver string) error {
	dsn := fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%v sslmode=disable",
		connStr.DestHost, connStr.DestUserName, connStr.DestPassword, connStr.DestDatabase, connStr.DestPort)
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return err
	}
	defer db.Close()

	var name string
	err = db.QueryRow(`SELECT schema_name FROM information_schema.schemata WHERE schema_name = $1`, connStr.DestUserName).Scan(&name)
	if err == sql.ErrNoRows {
		return fmt.Errorf("no schema named %q in database %q", connStr.DestUserName, connStr.DestDatabase)
	}
	if err != nil {
		return fmt.Errorf("query schemata: %v", err)
	}
	return nil
}

func printDryRunReport(connStr *connect.DbConnStr, driver string, results []dryRunResult) {
	cfgTable, err := gotable.Create("SourceDb", "DestDb", "DestUser", "DbType")
	if err == nil {
		_ = cfgTable.AddRow([]string{
			fmt.Sprintf("%s:%d/%s", connStr.SrcHost, connStr.SrcPort, connStr.SrcDatabase),
			fmt.Sprintf("%s:%d/%s", connStr.DestHost, connStr.DestPort, connStr.DestDatabase),
			connStr.DestUserName,
			driver,
		})
		fmt.Println("DryRun Config")
		fmt.Println(cfgTable)
	}

	resTable, err := gotable.Create("Check", "Status", "Detail")
	if err != nil {
		for _, r := range results {
			fmt.Printf("%-16s %-6s %s\n", r.Check, r.Status, r.Detail)
		}
		return
	}
	for _, r := range results {
		_ = resTable.AddRow([]string{r.Check, r.Status, r.Detail})
	}
	resTable.Align("Check", 1)
	resTable.Align("Status", 1)
	resTable.Align("Detail", 1)
	fmt.Println("DryRun Result")
	fmt.Println(resTable)
}

func writeDryRunFailedLog(failures []dryRunResult) {
	logDir, _ := filepath.Abs(CreateDateDir(""))
	p := logDir + "/dryRunFailed.log"
	f, err := os.OpenFile(p, os.O_CREATE|os.O_APPEND|os.O_RDWR, os.ModePerm)
	if err != nil {
		log.Error("open dryRunFailed.log: ", err)
		return
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	for _, r := range failures {
		_, _ = w.WriteString(fmt.Sprintf("%s -- %s\n", r.Check, r.Detail))
	}
	if err := w.Flush(); err != nil {
		log.Error("flush dryRunFailed.log: ", err)
		return
	}
	log.Info("dryRun failures written to ", p)
}
