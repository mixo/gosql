package gosql

import (
    "fmt"
    "time"
    "strings"
    "strconv"
    "database/sql"
    _ "github.com/go-sql-driver/mysql"
    _ "github.com/lib/pq"
    "github.com/mixo/goslice"
    "github.com/mixo/godt"
)

const (
    maxInsertRows = 1000
)

type DB struct {
    Driver, Host, Port, User, Password, Database string
}

func (this DB) Connect() *sql.DB {
    db, err := sql.Open(this.Driver, this.getConnectionString())
    if err != nil {
        panic(err)
    }

    return db
}

func (this DB) getConnectionString() string {
    switch this.Driver {
        case "mysql":
            return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", this.User, this.Password, this.Host, this.Port, this.Database)
        case "postgres":
            return fmt.Sprintf("host=%s port=%s user=%s " +
            "password=%s dbname=%s sslmode=disable", this.Host, this.Port, this.User, this.Password, this.Database)
        default:
            panic("Undefined the driver " + this.Driver)
    }
}

func (this DB) Quote(statement string) string {
    switch this.Driver {
        case "mysql":
            return fmt.Sprintf("`%s`", statement)
        case "postgres":
            return fmt.Sprintf("\"%s\"", statement)
        default:
            panic("Undefined the driver " + this.Driver)
    }
}

func (this DB) QuoteMultiple(statements []string) (quotedStatements []string) {
    for _, statement := range statements {
        quotedStatements = append(quotedStatements, this.Quote(statement))
    }
    return
}

func (this DB) Exec(sql string) {
    db := this.Connect()
    defer db.Close()

    _, err := db.Exec(sql)
    if err != nil {
        panic(err)
    }
}

func (this DB) CreateTable(tableName string, columns []string) {
    sql := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (" + strings.Join(columns, "\n") + ")", this.Quote(tableName))
    this.Exec(sql)
}

func (this DB) DropTable(tableName string) {
    sql := fmt.Sprintf("DROP TABLE IF EXISTS %s", this.Quote(tableName))
    this.Exec(sql)
}

func (this DB) GetAvgRowCountPerDay(
    tableName, dateColumn string,
    startDate, endDate time.Time,
    dayCount int) (avgRowCountPerDay int) {
    db := this.Connect()
    defer db.Close()

    tableName = this.Quote(tableName)
    dateColumn = this.Quote(dateColumn)
    placeholders := this.getPlaceholders(3)

    sql := "SELECT ROUND(SUM(rowCountPerDay) / %d) AS c FROM ("
    sql += "    SELECT %s, COUNT(*) AS rowCountPerDay "
    sql += "    FROM %s WHERE %s BETWEEN %s AND %s GROUP BY %s"
    sql += ") q"
    sql = fmt.Sprintf(sql, dayCount, dateColumn, tableName, dateColumn, placeholders[0], placeholders[1], dateColumn)
fmt.Println(sql)
    err := db.QueryRow(sql, godt.ToString(startDate), godt.ToString(endDate)).Scan(&avgRowCountPerDay)
    if err != nil {
        panic(err)
    }

    return
}

func (this DB) GetRowCountOnDate(tableName, dateColumn string, date time.Time) (yesterdayRowCount int) {
    db := this.Connect()
    defer db.Close()

    placeholders := this.getPlaceholders(1)
    sql := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s = %s", this.Quote(tableName), this.Quote(dateColumn), placeholders[0])

    err := db.QueryRow(sql, godt.ToString(date)).Scan(&yesterdayRowCount)
    if err != nil {
        panic(err)
    }

    return
}

func (this DB) InsertMultiple(tableName string, rows [][]interface{}, columns []string) {
    columnsString := strings.Join(this.QuoteMultiple(columns), ", ")
    queryBasis := fmt.Sprintf("INSERT INTO %s (%s) VALUES ", this.Quote(tableName), columnsString);
    db := this.Connect()
    defer db.Close()

    for _, rowsPart := range goslice.Split(rows, maxInsertRows) {
        placeholders := this.getRowsPlaceholders(rowsPart)
        values := this.getRowsValues(rowsPart)
        _, err := db.Exec(queryBasis + placeholders, values...)
        if err != nil {
            panic(err)
        }
    }
}

func (this DB) getRowsPlaceholders(rows [][]interface{}) (placeholders string) {
    rowPlaceholdersCount := 0
    if len(rows) > 0 {
        rowPlaceholdersCount = len(rows[0])
    }

    rowCount := len(rows)

    switch this.Driver {
        case "mysql":
            return this.getMysqlRowsPlaceholders(rowPlaceholdersCount, rowCount)
        case "postgres":
            return this.getPostgresRowsPlaceholders(rowPlaceholdersCount, rowCount)
        default:
            panic("Undefined the driver " + this.Driver)
    }

    return
}

func (this DB) getPlaceholders(quantity int) (placeholders []string) {
    switch this.Driver {
        case "mysql":
            return this.getMysqlPlaceholders(quantity)
        case "postgres":
            return this.getPostgresPlaceholders(quantity)
        default:
            panic("Undefined the driver " + this.Driver)
    }
}

func (this DB) getMysqlPlaceholders(quantity int) []string {
    return goslice.FillWithStrings([]string{}, "?", quantity)
}

func (this DB) getPostgresPlaceholders(quantity int) (placeholders []string) {
    for i := 1; i <= quantity; i++ {
        placeholders = append(placeholders, "$" + strconv.Itoa(i))
    }
    return
}

func (this DB) getRowsValues(rows [][]interface{}) (values []interface{}) {
    for _, row := range rows {
        values = append(values, row...)
    }

    return
}

func (this DB) getMysqlRowsPlaceholders(rowPlaceholdersCount, rowCount int) string {
    rowPlaceholdersString := "(" + strings.Join(goslice.FillWithStrings([]string{}, "?", rowPlaceholdersCount), ",") + ")"
    return strings.Join(goslice.FillWithStrings([]string{}, rowPlaceholdersString, rowCount), ",")
}

func (this DB) getPostgresRowsPlaceholders(rowPlaceholdersCount, rowCount int) string {
    var placeholders, rowPlaceholders []string
    index := 1
    for r := 0; r < rowCount; r++ {
        rowPlaceholders = make([]string, 0)
        for c := 0; c < rowPlaceholdersCount; c++ {
            rowPlaceholders = append(rowPlaceholders, "$" + strconv.Itoa(index))
            index++
        }
        placeholders = append(placeholders, "(" + strings.Join(rowPlaceholders, ",") + ")")
    }

    return strings.Join(placeholders, ",")
}

