package gosql

import (
    "fmt"
    "time"
    "strings"
    "strconv"
    "database/sql"
    _ "github.com/go-sql-driver/mysql"
    _ "github.com/lib/pq"
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

func (this DB) Exec(sqlQuery string) {
    db := this.Connect()
    defer db.Close()

    _, err := db.Exec(sqlQuery)
    if err != nil {
        panic(err)
    }
}

func (this DB) CreateTable(tableName string, columns []string) {
    sqlQuery := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (" + strings.Join(columns, ", ") + ")", this.Quote(tableName))
    this.Exec(sqlQuery)
}

func (this DB) DropTable(tableName string) {
    sqlQuery := fmt.Sprintf("DROP TABLE IF EXISTS %s", this.Quote(tableName))
    this.Exec(sqlQuery)
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

    sqlQuery := "SELECT ROUND(SUM(rowCountPerDay) / %d) AS c FROM ("
    sqlQuery += "    SELECT %s, COUNT(*) AS rowCountPerDay "
    sqlQuery += "    FROM %s WHERE %s BETWEEN %s AND %s GROUP BY %s"
    sqlQuery += ") q"
    sqlQuery = fmt.Sprintf(sqlQuery, dayCount, dateColumn, tableName, dateColumn, placeholders[0], placeholders[1], dateColumn)

    err := db.QueryRow(sqlQuery, godt.ToString(startDate), godt.ToString(endDate)).Scan(&avgRowCountPerDay)
    if err != nil {
        panic(err)
    }

    return
}

func (this DB) GetAvgRowParamsPerDay(
    tableName, dateColumn string,
    startDate, endDate time.Time,
    dayCount int,
    quantityColumn string,
    numericColumns []string,
    groupColumn string,
    filteredGroups []interface{}) []map[string]interface{} {

    tableName = this.Quote(tableName)
    dateColumn = this.Quote(dateColumn)
    groupColumn = this.Quote(groupColumn)
    placeholders := this.getPlaceholders(2 + len(filteredGroups))

    stmts := []string{}
    avgStmts := []string{}
    for _, numericColumn := range numericColumns {
    	numericColumn = this.Quote(numericColumn)

    	stmt := fmt.Sprintf("SUM(%s) AS %s", numericColumn, numericColumn)
    	stmts = append(stmts, stmt)

    	avgStmt := fmt.Sprintf("(SUM(%s) / %d) AS %s", numericColumn, dayCount, numericColumn)
    	avgStmts = append(avgStmts, avgStmt)
    }
    numericColumnStmts := strings.Join(stmts, ", ")
    if numericColumnStmts != "" {
    	numericColumnStmts = ", " + numericColumnStmts
    }

    avgNumericColumnStmts := strings.Join(avgStmts, ", ")
    if avgNumericColumnStmts != "" {
    	avgNumericColumnStmts = ", " + avgNumericColumnStmts
    }

    sqlQuery := fmt.Sprintf("SELECT %s, ROUND(SUM(_quantity) / %d) AS _quantity%s FROM (", groupColumn, dayCount, avgNumericColumnStmts)
    sqlQuery += fmt.Sprintf("    SELECT %s, %s, COUNT(*) AS _quantity%s ", dateColumn, groupColumn, numericColumnStmts)
    sqlQuery += fmt.Sprintf("    FROM %s", tableName)
    sqlQuery += fmt.Sprintf("    WHERE %s BETWEEN %s AND %s", dateColumn, placeholders[0], placeholders[1])
    if len(filteredGroups) > 0 {
		sqlQuery += fmt.Sprintf("AND %s IN (%s)", groupColumn, strings.Join(placeholders[2:], ", "))
    }
    sqlQuery += fmt.Sprintf("    GROUP BY %s, %s", dateColumn, groupColumn)
    sqlQuery += fmt.Sprintf(") q GROUP BY %s", groupColumn)

	values := []interface{}{godt.ToString(startDate), godt.ToString(endDate)}
	if len(filteredGroups) > 0 {
		values = append(values, filteredGroups...)
	}

	return this.QueryObjects(sqlQuery, values...)
}

func (this DB) GetRowParamsOnDate(
    tableName, dateColumn string,
    date time.Time,
    quantityColumn string,
    numericColumns []string,
    groupColumn string,
	filteredGroups []interface{}) []map[string]interface{} {

    tableName = this.Quote(tableName)
    dateColumn = this.Quote(dateColumn)
    groupColumn = this.Quote(groupColumn)
    placeholders := this.getPlaceholders(1 + len(filteredGroups))

    stmts := []string{}
    for _, numericColumn := range numericColumns {
    	numericColumn = this.Quote(numericColumn)

    	stmt := fmt.Sprintf("SUM(%s) AS %s", numericColumn, numericColumn)
    	stmts = append(stmts, stmt)
    }
    numericColumnStmts := strings.Join(stmts, ", ")
    if numericColumnStmts != "" {
    	numericColumnStmts = ", " + numericColumnStmts
    }

    sqlQuery := fmt.Sprintf("SELECT %s, COUNT(*) AS _quantity%s ", groupColumn, numericColumnStmts)
    sqlQuery += fmt.Sprintf("FROM %s", tableName)
    sqlQuery += fmt.Sprintf("WHERE %s = %s ", dateColumn, placeholders[0])
    if len(filteredGroups) > 0 {
		sqlQuery += fmt.Sprintf("AND %s IN (%s)", groupColumn, strings.Join(placeholders[1:], ", "))
    }
    sqlQuery += fmt.Sprintf("GROUP BY %s", groupColumn)

	values := []interface{}{godt.ToString(date)}
	if len(filteredGroups) > 0 {
		values = append(values, filteredGroups...)
	}

	return this.QueryObjects(sqlQuery, values...)
}

func (this DB) QueryObjects(sqlQuery string, values... interface{}) (objects []map[string]interface{}) {
    db := this.Connect()
    defer db.Close()

    rows, err := db.Query(sqlQuery, values...)
    if err != nil {
        panic(err)
    }
    defer rows.Close()

    for rows.Next() {
		columns, err := rows.ColumnTypes()
		if err != nil {
			panic(err)
		}

		values := make([]interface{}, len(columns))
		object := map[string]interface{}{}
		for i, column := range columns {
			var value interface{}
			databaseTypeName := column.DatabaseTypeName()
			switch databaseTypeName {
				case "VARCHAR":
					value = new(string)
				case "INT2":
					value = new(int)
				case "INT8":
				case "BIGINT":
					value = new(int64)
				case "NUMERIC":
				case "DECIMAL":
					value = new(float64)
				default:
					panic(fmt.Sprintf("Undefined database type name '%s'", databaseTypeName))
			}
			object[column.Name()] = value
			values[i] = value
		}

    	err = rows.Scan(values...)
    	if err != nil {
			panic(err)
    	}

		for name, value := range object {
			switch value.(type) {
				case *int:
					object[name] = *value.(*int)
				case *int8:
					object[name] = *value.(*int8)
				case *int16:
					object[name] = *value.(*int16)
				case *int32:
					object[name] = *value.(*int32)
				case *int64:
					object[name] = *value.(*int64)
				case *string:
					object[name] = *value.(*string)
				case *float64:
					object[name] = *value.(*float64)
				default:
					panic(fmt.Sprintf("Undefined type %T", value))
			}
		}

		objects = append(objects, object)
    }
    err = rows.Err()
    if err != nil {
        panic(err)
    }

    return
}

func (this DB) GetRowCountOnDate(tableName, dateColumn string, date time.Time) (yesterdayRowCount int) {
    db := this.Connect()
    defer db.Close()

    placeholders := this.getPlaceholders(1)
    sqlQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s = %s", this.Quote(tableName), this.Quote(dateColumn), placeholders[0])

    err := db.QueryRow(sqlQuery, godt.ToString(date)).Scan(&yesterdayRowCount)
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

    for _, rowsPart := range this.splitRows(rows, maxInsertRows) {
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
    return this.fillWithStrings([]string{}, "?", quantity)
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
    rowPlaceholdersString := "(" + strings.Join(this.fillWithStrings([]string{}, "?", rowPlaceholdersCount), ",") + ")"
    return strings.Join(this.fillWithStrings([]string{}, rowPlaceholdersString, rowCount), ",")
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

func (this DB) splitRows(slice [][]interface{}, size int) (slices [][][]interface{}) {
    for len(slice) > 0 {
        if size > len(slice) {
            size = len(slice)
        }
        slices = append(slices, slice[0:size])
        slice = slice[size:]
    }
    return
}

func (this DB) fillWithStrings(slice []string, value string, count int) []string {
    for i := 0; i < count; i++ {
        slice = append(slice, value)
    }
    return slice
}

func (this DB) toString(slice []interface{}) (stringSlice []string) {
    for _, item := range slice {
        stringSlice = append(stringSlice, item.(string))
    }
    return
}

