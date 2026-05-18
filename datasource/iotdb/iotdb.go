package iotdb

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/mitchellh/mapstructure"

	"github.com/ccfos/nightingale/v6/datasource"
	iot "github.com/ccfos/nightingale/v6/dskit/iotdb"
	"github.com/ccfos/nightingale/v6/dskit/sqlbase"
	"github.com/ccfos/nightingale/v6/dskit/types"
	"github.com/ccfos/nightingale/v6/models"
	"github.com/ccfos/nightingale/v6/pkg/logx"
	"github.com/ccfos/nightingale/v6/pkg/macros"
)

const (
	IoTDBType = "iotdb"
)

type IoTDB struct {
	iot.Iotdb `json:",inline" mapstructure:",squash"`
}

type QueryParam struct {
	Ref      string          `json:"ref" mapstructure:"ref"`
	Database string          `json:"database" mapstructure:"database"`
	Table    string          `json:"table" mapstructure:"table"`
	SQL      string          `json:"sql" mapstructure:"sql"`
	Query    string          `json:"query" mapstructure:"query"`
	Keys     datasource.Keys `json:"keys" mapstructure:"keys"`
	From     interface{}     `json:"from" mapstructure:"from"`
	To       interface{}     `json:"to" mapstructure:"to"`
	Interval int64           `json:"interval" mapstructure:"interval"`
	Limit    int             `json:"limit" mapstructure:"limit"`
}

func init() {
	datasource.RegisterDatasource(IoTDBType, new(IoTDB))
}

func (it *IoTDB) Init(settings map[string]interface{}) (datasource.Datasource, error) {
	newest := new(IoTDB)
	err := mapstructure.Decode(settings, newest)
	return newest, err
}

func (it *IoTDB) InitClient() error {
	it.InitCli()
	return nil
}

func (it *IoTDB) Equal(other datasource.Datasource) bool {
	otherIoTDB, ok := other.(*IoTDB)
	if !ok {
		return false
	}

	if it.Addr != otherIoTDB.Addr ||
		it.Timeout != otherIoTDB.Timeout ||
		it.DialTimeout != otherIoTDB.DialTimeout ||
		it.MaxIdleConnsPerHost != otherIoTDB.MaxIdleConnsPerHost ||
		it.SkipTlsVerify != otherIoTDB.SkipTlsVerify {
		return false
	}

	if len(it.Headers) != len(otherIoTDB.Headers) {
		return false
	}

	for k, v := range it.Headers {
		if otherV, ok := otherIoTDB.Headers[k]; !ok || otherV != v {
			return false
		}
	}

	if it.Basic == nil || otherIoTDB.Basic == nil {
		return it.Basic == nil && otherIoTDB.Basic == nil
	}

	return it.Basic.User == otherIoTDB.Basic.User && it.Basic.Password == otherIoTDB.Basic.Password
}

func (it *IoTDB) Validate(ctx context.Context) error {
	if strings.TrimSpace(it.Addr) == "" {
		return fmt.Errorf("iotdb addr is invalid, please check datasource setting")
	}
	return nil
}

func (it *IoTDB) ShowDatabases(ctx context.Context) ([]string, error) {
	return it.Iotdb.ShowDatabases(ctx)
}

func (it *IoTDB) ShowTables(ctx context.Context, database string) ([]string, error) {
	return it.Iotdb.ShowTables(ctx, database)
}

func (it *IoTDB) DescribeTable(ctx context.Context, query interface{}) ([]*types.ColumnProperty, error) {
	return it.Iotdb.DescribeTable(ctx, query)
}

func (it *IoTDB) MakeLogQuery(ctx context.Context, query interface{}, eventTags []string, start, end int64) (interface{}, error) {
	return nil, nil
}

func (it *IoTDB) MakeTSQuery(ctx context.Context, query interface{}, eventTags []string, start, end int64) (interface{}, error) {
	return nil, nil
}

func (it *IoTDB) QueryMapData(ctx context.Context, query interface{}) ([]map[string]string, error) {
	return nil, nil
}

func (it *IoTDB) QueryData(ctx context.Context, query interface{}) ([]models.DataResp, error) {
	queryParam, err := decodeQueryParam(query)
	if err != nil {
		return nil, err
	}

	rows, err := it.queryRows(ctx, queryParam)
	if err != nil {
		return nil, err
	}
	if normalizeRowsTime(rows, queryParam.Keys.TimeKey) {
		// After normalizing IoTDB epoch values to seconds, let the generic
		// timeseries parser treat them as unix timestamps instead of re-parsing
		// them with a datetime layout.
		queryParam.Keys.TimeFormat = ""
	}

	valueKey := strings.TrimSpace(queryParam.Keys.ValueKey)
	if valueKey == "" {
		valueKey = strings.Join(metricKeysFromRows(rows), " ")
	}
	if valueKey == "" {
		return nil, fmt.Errorf("valueKey is required")
	}

	items := sqlbase.FormatMetricValues(types.Keys{
		ValueKey:   valueKey,
		LabelKey:   queryParam.Keys.LabelKey,
		TimeKey:    queryParam.Keys.TimeKey,
		TimeFormat: queryParam.Keys.TimeFormat,
	}, rows)

	data := make([]models.DataResp, 0, len(items))
	for i := range items {
		data = append(data, models.DataResp{
			Ref:    queryParam.Ref,
			Metric: items[i].Metric,
			Values: items[i].Values,
		})
	}

	return data, nil
}

func (it *IoTDB) QueryLog(ctx context.Context, query interface{}) ([]interface{}, int64, error) {
	queryParam, err := decodeQueryParam(query)
	if err != nil {
		return nil, 0, err
	}

	rows, err := it.queryRows(ctx, queryParam)
	if err != nil {
		return nil, 0, err
	}

	logs := make([]interface{}, 0, len(rows))
	for _, row := range rows {
		logs = append(logs, row)
	}

	return logs, int64(len(logs)), nil
}

func (it *IoTDB) queryRows(ctx context.Context, queryParam *QueryParam) ([]map[string]interface{}, error) {
	sqlText := strings.TrimSpace(queryParam.SQL)
	if sqlText == "" {
		sqlText = strings.TrimSpace(queryParam.Query)
	}
	if sqlText == "" {
		return nil, fmt.Errorf("sql is required")
	}

	hasMacro := strings.Contains(sqlText, "$__") || strings.Contains(sqlText, "${__")
	if hasMacro {
		from, err := parseQueryTime(queryParam.From)
		if err != nil {
			return nil, fmt.Errorf("parse from failed: %w", err)
		}
		to, err := parseQueryTime(queryParam.To)
		if err != nil {
			return nil, fmt.Errorf("parse to failed: %w", err)
		}
		sqlText = replaceIoTDBMacros(sqlText, queryParam, from, to)
		sqlText, err = macros.Macro(sqlText, from, to)
		if err != nil {
			return nil, err
		}
	} else {
		var err error
		sqlText = autoDownsampleSQL(sqlText, queryParam)
		sqlText, err = appendTimeFilter(sqlText, queryParam)
		if err != nil {
			return nil, err
		}
	}

	timeout := time.Duration(it.Timeout) * time.Millisecond
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	resp, err := it.Iotdb.QueryTable(queryParam.Database, sqlText, queryParam.Limit)
	if err != nil {
		logx.Warningf(ctx, "query:%+v get data err:%v", queryParam, err)
		return nil, err
	}

	rows := responseToRows(resp)
	select {
	case <-timeoutCtx.Done():
		return nil, timeoutCtx.Err()
	default:
	}
	return rows, nil
}

func decodeQueryParam(query interface{}) (*QueryParam, error) {
	queryParam := new(QueryParam)
	if err := mapstructure.Decode(query, queryParam); err != nil {
		return nil, err
	}
	return queryParam, nil
}

func parseQueryTime(value interface{}) (int64, error) {
	switch v := value.(type) {
	case nil:
		return 0, nil
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case float64:
		return int64(v), nil
	case float32:
		return int64(v), nil
	case string:
		raw := strings.TrimSpace(v)
		if raw == "" {
			return 0, nil
		}
		if ts, err := strconv.ParseInt(raw, 10, 64); err == nil {
			return ts, nil
		}
		layouts := []string{
			time.RFC3339Nano,
			time.RFC3339,
			"2006-01-02 15:04:05",
			"2006-01-02T15:04:05",
		}
		for _, layout := range layouts {
			if parsed, err := time.Parse(layout, raw); err == nil {
				return parsed.Unix(), nil
			}
		}
		return 0, fmt.Errorf("unsupported time format: %s", raw)
	default:
		return 0, fmt.Errorf("unsupported time type: %T", value)
	}
}

func responseToRows(resp iot.APIResponse) []map[string]interface{} {
	if len(resp.Timestamps) > 0 && len(resp.Expressions) > 0 {
		rows := make([]map[string]interface{}, 0, len(resp.Timestamps))
		for rowIdx, ts := range resp.Timestamps {
			row := map[string]interface{}{
				"__time__": ts / 1000,
			}

			for colIdx, expr := range resp.Expressions {
				if colIdx >= len(resp.Values) || rowIdx >= len(resp.Values[colIdx]) {
					row[expr] = nil
					continue
				}
				row[expr] = resp.Values[colIdx][rowIdx]
			}
			rows = append(rows, row)
		}
		return rows
	}

	return iotColumnarToRows(resp)
}

func iotColumnarToRows(resp iot.APIResponse) []map[string]interface{} {
	columns := resp.ColumnNames
	if len(columns) == 0 {
		columns = resp.Expressions
	}

	if len(columns) == 0 || len(resp.Values) == 0 {
		return []map[string]interface{}{}
	}

	if len(resp.Values[0]) == len(columns) {
		rows := make([]map[string]interface{}, 0, len(resp.Values))
		for _, rawRow := range resp.Values {
			row := make(map[string]interface{}, len(columns))
			for colIdx, colName := range columns {
				if colIdx >= len(rawRow) {
					row[colName] = nil
					continue
				}
				row[colName] = rawRow[colIdx]
			}
			rows = append(rows, row)
		}
		return rows
	}

	rowCount := 0
	for _, col := range resp.Values {
		if len(col) > rowCount {
			rowCount = len(col)
		}
	}

	rows := make([]map[string]interface{}, 0, rowCount)
	for rowIdx := 0; rowIdx < rowCount; rowIdx++ {
		row := make(map[string]interface{}, len(columns))
		for colIdx, colName := range columns {
			if colIdx >= len(resp.Values) || rowIdx >= len(resp.Values[colIdx]) {
				row[colName] = nil
				continue
			}
			row[colName] = resp.Values[colIdx][rowIdx]
		}
		rows = append(rows, row)
	}
	return rows
}

func metricKeysFromRows(rows []map[string]interface{}) []string {
	if len(rows) == 0 {
		return nil
	}

	keys := make([]string, 0)
	for k := range rows[0] {
		if k == "__time__" {
			continue
		}
		keys = append(keys, k)
	}
	return keys
}

func normalizeRowsTime(rows []map[string]interface{}, timeKey string) bool {
	keys := []string{"__time__", "time"}
	if strings.TrimSpace(timeKey) != "" {
		keys = append([]string{timeKey}, keys...)
	}

	normalizedAny := false
	for _, row := range rows {
		for _, key := range keys {
			value, exists := row[key]
			if !exists || value == nil {
				continue
			}
			if normalized, ok := normalizeEpochToSeconds(value); ok {
				row[key] = normalized
				normalizedAny = true
				break
			}
		}
	}
	return normalizedAny
}

func normalizeEpochToSeconds(value interface{}) (interface{}, bool) {
	switch v := value.(type) {
	case int64:
		return scaleEpoch(v), true
	case int:
		return scaleEpoch(int64(v)), true
	case int32:
		return scaleEpoch(int64(v)), true
	case float64:
		return float64(scaleEpoch(int64(v))), true
	case float32:
		return float64(scaleEpoch(int64(v))), true
	case string:
		raw := strings.TrimSpace(v)
		if raw == "" {
			return value, false
		}
		ts, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return value, false
		}
		return strconv.FormatInt(scaleEpoch(ts), 10), true
	default:
		return value, false
	}
}

func scaleEpoch(ts int64) int64 {
	switch {
	case ts >= 1e18:
		return ts / 1e9
	case ts >= 1e15:
		return ts / 1e6
	case ts >= 1e12:
		return ts / 1e3
	default:
		return ts
	}
}

var (
	explicitTimeFilterOperators = []string{">=", "<=", "<>", "!=", ">", "<", "="}
	sqlTailClauses              = []string{"group by", "having", "fill", "order by", "offset", "limit"}
	sqlGroupInsertClauses       = []string{"having", "fill", "order by", "offset", "limit"}
	timeFilterMacroPattern      = regexp.MustCompile(`\$__timeFilter\(\s*([^)]+?)\s*\)`)
	timeGroupMacroPattern       = regexp.MustCompile(`\$__timeGroup\(\s*([^,)]+?)\s*(?:,\s*([^)]+?))?\s*\)`)
)

func replaceIoTDBMacros(sqlText string, queryParam *QueryParam, from, to int64) string {
	timeKey := strings.TrimSpace(queryParam.Keys.TimeKey)
	if timeKey == "" {
		timeKey = "time"
	}

	intervalSecondsValue := intervalSeconds(queryParam.Interval)
	if intervalSecondsValue <= 0 {
		intervalSecondsValue = defaultIntervalFromRange(from, to)
	}
	interval := formatIoTDBInterval(intervalSecondsValue)
	if interval == "" {
		interval = "60s"
		intervalSecondsValue = 60
	}

	sqlText = strings.ReplaceAll(sqlText, "$__interval_ms", strconv.FormatInt(intervalSecondsValue*1000, 10))
	sqlText = strings.ReplaceAll(sqlText, "${__interval_ms}", strconv.FormatInt(intervalSecondsValue*1000, 10))
	sqlText = strings.ReplaceAll(sqlText, "$__interval", interval)
	sqlText = strings.ReplaceAll(sqlText, "${__interval}", interval)
	sqlText = timeFilterMacroPattern.ReplaceAllStringFunc(sqlText, func(match string) string {
		parts := timeFilterMacroPattern.FindStringSubmatch(match)
		field := timeKey
		if len(parts) > 1 && strings.TrimSpace(parts[1]) != "" {
			field = strings.TrimSpace(parts[1])
		}
		condition, err := buildTimeFilterCondition(field, queryParam.From, queryParam.To)
		if err != nil || condition == "" {
			return "1=1"
		}
		return condition
	})
	sqlText = timeGroupMacroPattern.ReplaceAllStringFunc(sqlText, func(match string) string {
		parts := timeGroupMacroPattern.FindStringSubmatch(match)
		field := timeKey
		if len(parts) > 1 && strings.TrimSpace(parts[1]) != "" {
			field = strings.TrimSpace(parts[1])
		}
		bucket := interval
		if len(parts) > 2 && strings.TrimSpace(parts[2]) != "" {
			bucket = strings.TrimSpace(parts[2])
		}
		return fmt.Sprintf("date_bin(%s, %s)", bucket, field)
	})

	return sqlText
}

func autoDownsampleSQL(sqlText string, queryParam *QueryParam) string {
	if queryParam.Interval <= 0 {
		return sqlText
	}

	timeKey := strings.TrimSpace(queryParam.Keys.TimeKey)
	if timeKey == "" {
		timeKey = "time"
	}
	valueKeys := splitSQLKeys(queryParam.Keys.ValueKey)
	if len(valueKeys) == 0 || !allSafeSQLIdentifiers(valueKeys) {
		return sqlText
	}
	labelKeys := splitSQLKeys(queryParam.Keys.LabelKey)
	if !allSafeSQLIdentifiers(labelKeys) {
		return sqlText
	}

	trimmed := strings.TrimSpace(sqlText)
	suffix := ""
	if strings.HasSuffix(trimmed, ";") {
		trimmed = strings.TrimSpace(strings.TrimSuffix(trimmed, ";"))
		suffix = ";"
	}

	lower := strings.ToLower(trimmed)
	if strings.Contains(lower, "date_bin(") ||
		findTopLevelKeyword(trimmed, "group by") >= 0 ||
		findTopLevelKeyword(trimmed, "union") >= 0 ||
		findTopLevelKeyword(trimmed, "join") >= 0 {
		return sqlText
	}

	selectIdx := findTopLevelKeyword(trimmed, "select")
	fromIdx := findTopLevelKeyword(trimmed, "from")
	if selectIdx != 0 || fromIdx < 0 {
		return sqlText
	}

	selectList := strings.TrimSpace(trimmed[len("select"):fromIdx])
	if selectList == "*" || strings.Contains(selectList, "*") || hasTopLevelAggregate(selectList) {
		return sqlText
	}

	interval := formatIoTDBInterval(queryParam.Interval)
	if interval == "" {
		return sqlText
	}

	timeRef := sqlIdentifierRef(timeKey)
	timeAlias := sqlIdentifierAlias(timeKey)
	selectParts := []string{fmt.Sprintf("date_bin(%s, %s) as %s", interval, timeRef, sqlIdentifierRef(timeAlias))}
	for _, labelKey := range labelKeys {
		selectParts = append(selectParts, sqlIdentifierRef(labelKey))
	}
	for _, valueKey := range valueKeys {
		selectParts = append(selectParts, fmt.Sprintf("avg(%s) as %s", sqlIdentifierRef(valueKey), sqlIdentifierRef(sqlIdentifierAlias(valueKey))))
	}

	downsampled := "select " + strings.Join(selectParts, ", ") + " " + strings.TrimSpace(trimmed[fromIdx:])
	groupParts := []string{fmt.Sprintf("date_bin(%s, %s)", interval, timeRef)}
	for _, labelKey := range labelKeys {
		groupParts = append(groupParts, sqlIdentifierRef(labelKey))
	}
	downsampled = insertGroupByCondition(downsampled, strings.Join(groupParts, ", "))
	if findTopLevelKeyword(downsampled, "order by") < 0 {
		downsampled = insertOrderByTime(downsampled, sqlIdentifierRef(timeAlias))
	}

	return downsampled + suffix
}

func appendTimeFilter(sqlText string, queryParam *QueryParam) (string, error) {
	timeKey := strings.TrimSpace(queryParam.Keys.TimeKey)
	if timeKey == "" {
		timeKey = "time"
	}

	if hasExplicitTimeFilter(sqlText, timeKey) {
		return sqlText, nil
	}

	condition, err := buildTimeFilterCondition(timeKey, queryParam.From, queryParam.To)
	if err != nil {
		return "", err
	}
	if condition == "" {
		return sqlText, nil
	}

	return insertWhereCondition(sqlText, condition), nil
}

func buildTimeFilterCondition(timeKey string, fromValue, toValue interface{}) (string, error) {
	conditions := make([]string, 0, 2)

	if from, ok, err := queryTimeToMillis(fromValue); err != nil {
		return "", fmt.Errorf("parse from failed: %w", err)
	} else if ok {
		conditions = append(conditions, fmt.Sprintf("%s >= %d", timeKey, from))
	}

	if to, ok, err := queryTimeToMillis(toValue); err != nil {
		return "", fmt.Errorf("parse to failed: %w", err)
	} else if ok {
		conditions = append(conditions, fmt.Sprintf("%s <= %d", timeKey, to))
	}

	return strings.Join(conditions, " AND "), nil
}

func queryTimeToMillis(value interface{}) (int64, bool, error) {
	switch v := value.(type) {
	case nil:
		return 0, false, nil
	case int:
		return epochToMillis(int64(v)), true, nil
	case int32:
		return epochToMillis(int64(v)), true, nil
	case int64:
		return epochToMillis(v), true, nil
	case float32:
		return epochToMillis(int64(v)), true, nil
	case float64:
		return epochToMillis(int64(v)), true, nil
	case string:
		raw := strings.TrimSpace(v)
		if raw == "" {
			return 0, false, nil
		}
		if ts, err := strconv.ParseInt(raw, 10, 64); err == nil {
			return epochToMillis(ts), true, nil
		}

		layouts := []string{
			time.RFC3339Nano,
			time.RFC3339,
			"2006-01-02 15:04:05.000",
			"2006-01-02 15:04:05",
			"2006-01-02T15:04:05.000",
			"2006-01-02T15:04:05",
		}
		for _, layout := range layouts {
			if ts, err := time.Parse(layout, raw); err == nil {
				return ts.UnixMilli(), true, nil
			}
		}
		return 0, false, fmt.Errorf("unsupported time format: %s", raw)
	default:
		return 0, false, fmt.Errorf("unsupported time type: %T", value)
	}
}

func epochToMillis(ts int64) int64 {
	switch {
	case ts >= 1e18:
		return ts / 1e6
	case ts >= 1e15:
		return ts / 1e3
	case ts >= 1e12:
		return ts
	default:
		return ts * 1e3
	}
}

func hasExplicitTimeFilter(sqlText, timeKey string) bool {
	token := timeKeyRegexp(timeKey)
	for _, op := range explicitTimeFilterOperators {
		pattern := regexp.MustCompile(`(?i)(^|[^\w.])` + token + `\s*` + regexp.QuoteMeta(op))
		if pattern.MatchString(sqlText) {
			return true
		}
	}

	patterns := []*regexp.Regexp{
		regexp.MustCompile(`(?i)(^|[^\w.])` + token + `\s+between\b`),
		regexp.MustCompile(`(?i)(^|[^\w.])` + token + `\s+in\b`),
		regexp.MustCompile(`(?i)(^|[^\w.])` + token + `\s+is\s+(not\s+)?null\b`),
	}
	for _, pattern := range patterns {
		if pattern.MatchString(sqlText) {
			return true
		}
	}
	return false
}

func timeKeyRegexp(timeKey string) string {
	if strings.Contains(timeKey, ".") {
		return regexp.QuoteMeta(timeKey)
	}

	identifier := regexp.QuoteMeta(strings.Trim(timeKey, "`"))
	return `(?:` + "`?" + `[A-Za-z_][\w]*` + "`?" + `\.)*` + "`?" + identifier + "`?"
}

func insertWhereCondition(sqlText, condition string) string {
	trimmed := strings.TrimSpace(sqlText)
	if trimmed == "" || condition == "" {
		return sqlText
	}

	suffix := ""
	if strings.HasSuffix(trimmed, ";") {
		trimmed = strings.TrimSpace(strings.TrimSuffix(trimmed, ";"))
		suffix = ";"
	}

	insertAt := findInsertBeforeClause(trimmed)
	head := strings.TrimRightFunc(trimmed[:insertAt], unicode.IsSpace)
	tail := strings.TrimLeftFunc(trimmed[insertAt:], unicode.IsSpace)

	joiner := " WHERE "
	if findTopLevelKeyword(head, "where") >= 0 {
		joiner = " AND "
	}

	result := head + joiner + condition
	if tail != "" {
		result += " " + tail
	}
	return result + suffix
}

func insertGroupByCondition(sqlText, groupBy string) string {
	if groupBy == "" || findTopLevelKeyword(sqlText, "group by") >= 0 {
		return sqlText
	}
	insertAt := findInsertBeforeAnyClause(sqlText, sqlGroupInsertClauses)
	head := strings.TrimRightFunc(sqlText[:insertAt], unicode.IsSpace)
	tail := strings.TrimLeftFunc(sqlText[insertAt:], unicode.IsSpace)

	result := head + " GROUP BY " + groupBy
	if tail != "" {
		result += " " + tail
	}
	return result
}

func insertOrderByTime(sqlText, timeKey string) string {
	insertAt := findInsertBeforeAnyClause(sqlText, []string{"offset", "limit"})
	head := strings.TrimRightFunc(sqlText[:insertAt], unicode.IsSpace)
	tail := strings.TrimLeftFunc(sqlText[insertAt:], unicode.IsSpace)

	result := head + " ORDER BY " + timeKey
	if tail != "" {
		result += " " + tail
	}
	return result
}

func findInsertBeforeClause(sqlText string) int {
	return findInsertBeforeAnyClause(sqlText, sqlTailClauses)
}

func findInsertBeforeAnyClause(sqlText string, clauses []string) int {
	insertAt := len(sqlText)
	for _, clause := range clauses {
		if idx := findTopLevelKeyword(sqlText, clause); idx >= 0 && idx < insertAt {
			insertAt = idx
		}
	}
	return insertAt
}

func splitSQLKeys(value string) []string {
	keys := strings.Fields(strings.TrimSpace(value))
	if len(keys) == 1 && strings.Contains(keys[0], ",") {
		keys = strings.Split(keys[0], ",")
	}

	normalized := make([]string, 0, len(keys))
	for _, key := range keys {
		key = strings.Trim(strings.TrimSpace(key), ",")
		if key != "" {
			normalized = append(normalized, key)
		}
	}
	return normalized
}

func allSafeSQLIdentifiers(values []string) bool {
	for _, value := range values {
		if !isSafeSQLIdentifier(value) {
			return false
		}
	}
	return true
}

func isSafeSQLIdentifier(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" {
		return false
	}
	parts := strings.Split(value, ".")
	for _, part := range parts {
		part = strings.Trim(part, "`\"")
		if part == "" {
			return false
		}
		for i, r := range part {
			if i == 0 && !(r == '_' || unicode.IsLetter(r)) {
				return false
			}
			if !(r == '_' || r == ':' || unicode.IsLetter(r) || unicode.IsDigit(r)) {
				return false
			}
		}
	}
	return true
}

func sqlIdentifierRef(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return value
	}
	parts := strings.Split(value, ".")
	quoted := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.Trim(strings.TrimSpace(part), "`\"")
		if isSimpleSQLIdentifierPart(part) {
			quoted = append(quoted, part)
			continue
		}
		quoted = append(quoted, `"`+strings.ReplaceAll(part, `"`, `""`)+`"`)
	}
	return strings.Join(quoted, ".")
}

func sqlIdentifierAlias(value string) string {
	parts := strings.Split(strings.TrimSpace(value), ".")
	alias := strings.Trim(parts[len(parts)-1], "`\"")
	if alias == "" {
		return value
	}
	return alias
}

func isSimpleSQLIdentifierPart(value string) bool {
	if value == "" {
		return false
	}
	for i, r := range value {
		if i == 0 && !(r == '_' || unicode.IsLetter(r)) {
			return false
		}
		if !(r == '_' || unicode.IsLetter(r) || unicode.IsDigit(r)) {
			return false
		}
	}
	return true
}

func hasTopLevelAggregate(selectList string) bool {
	for _, fn := range []string{"avg", "count", "sum", "min", "max", "first", "last", "date_bin", "diff"} {
		if findTopLevelKeyword(selectList, fn) >= 0 {
			return true
		}
	}
	return false
}

func formatIoTDBInterval(seconds int64) string {
	seconds = intervalSeconds(seconds)
	if seconds <= 0 {
		return ""
	}
	if seconds%86400 == 0 {
		return fmt.Sprintf("%dd", seconds/86400)
	}
	if seconds%3600 == 0 {
		return fmt.Sprintf("%dh", seconds/3600)
	}
	if seconds%60 == 0 {
		return fmt.Sprintf("%dm", seconds/60)
	}
	return fmt.Sprintf("%ds", seconds)
}

func intervalSeconds(interval int64) int64 {
	if interval >= 1e12 {
		return interval / 1e3
	}
	return interval
}

func defaultIntervalFromRange(from, to int64) int64 {
	if from <= 0 || to <= from {
		return 60
	}
	interval := (to - from) / 240
	if interval < 1 {
		return 1
	}
	return interval
}

func findTopLevelKeyword(sqlText, keyword string) int {
	lowerSQL := strings.ToLower(sqlText)
	lowerKeyword := strings.ToLower(keyword)
	depth := 0
	quote := rune(0)

	for i, r := range lowerSQL {
		if quote != 0 {
			if r == quote {
				quote = 0
			}
			continue
		}

		switch r {
		case '\'', '"', '`':
			quote = r
			continue
		case '(':
			depth++
			continue
		case ')':
			if depth > 0 {
				depth--
			}
			continue
		}

		if depth == 0 && strings.HasPrefix(lowerSQL[i:], lowerKeyword) && isSQLKeywordBoundary(lowerSQL, i, len(lowerKeyword)) {
			return i
		}
	}

	return -1
}

func isSQLKeywordBoundary(sqlText string, start, length int) bool {
	before := start == 0 || !isSQLIdentifierRune(rune(sqlText[start-1]))
	afterIdx := start + length
	after := afterIdx >= len(sqlText) || !isSQLIdentifierRune(rune(sqlText[afterIdx]))
	return before && after
}

func isSQLIdentifierRune(r rune) bool {
	return r == '_' || unicode.IsLetter(r) || unicode.IsDigit(r)
}
