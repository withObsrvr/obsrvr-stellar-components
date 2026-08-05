package columnar

import (
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/withObsrvr/obsrvr-stellar-components/pkg/bronze"
)

// TypedTableLayout is the physical column contract shared by Arrow builders,
// Parquet writers, DuckDB readers, and DuckLake registration.
type TypedTableLayout struct {
	Schema   *arrow.Schema
	SQLTypes []string
}

var (
	typedLayoutsOnce sync.Once
	typedLayouts     map[string]TypedTableLayout
	typedLayoutsErr  error
)

// LayoutFor resolves a Bronze table from the embedded authoritative DDL. The
// parser deliberately accepts only the small SQL type vocabulary in that DDL;
// a new type therefore fails closed instead of silently changing Parquet.
func LayoutFor(spec bronze.TypedTableSpec) (TypedTableLayout, error) {
	typedLayoutsOnce.Do(func() {
		typedLayouts, typedLayoutsErr = parseTypedLayouts(bronze.SchemaSQL)
	})
	if typedLayoutsErr != nil {
		return TypedTableLayout{}, typedLayoutsErr
	}
	layout, ok := typedLayouts[spec.TableName]
	if !ok {
		return TypedTableLayout{}, fmt.Errorf("Bronze DDL has no typed table %q", spec.TableName)
	}
	fields := layout.Schema.Fields()
	if len(fields) != len(spec.Columns) {
		return TypedTableLayout{}, fmt.Errorf("Bronze DDL table %s has %d columns, typed spec has %d", spec.TableName, len(fields), len(spec.Columns))
	}
	for index, column := range spec.Columns {
		if fields[index].Name != column {
			return TypedTableLayout{}, fmt.Errorf("Bronze DDL table %s column %d is %q, typed spec has %q", spec.TableName, index, fields[index].Name, column)
		}
	}
	return layout, nil
}

func parseTypedLayouts(schemaSQL string) (map[string]TypedTableLayout, error) {
	const prefix = "CREATE TABLE IF NOT EXISTS bronze."
	layouts := make(map[string]TypedTableLayout)
	for _, statement := range bronze.SplitSQLStatements(schemaSQL) {
		if !strings.HasPrefix(statement, prefix) {
			continue
		}
		open := strings.IndexByte(statement, '(')
		close := strings.LastIndexByte(statement, ')')
		if open < len(prefix)+1 || close <= open {
			return nil, fmt.Errorf("parse Bronze table DDL: malformed CREATE TABLE")
		}
		tableName := strings.TrimSpace(statement[len(prefix):open])
		var fields []arrow.Field
		var sqlTypes []string
		for _, rawLine := range strings.Split(statement[open+1:close], "\n") {
			line := strings.TrimSpace(strings.SplitN(rawLine, "--", 2)[0])
			line = strings.TrimSpace(strings.TrimSuffix(line, ","))
			if line == "" {
				continue
			}
			name, sqlType, err := parseColumnDefinition(line)
			if err != nil {
				return nil, fmt.Errorf("parse Bronze DDL %s: %w", tableName, err)
			}
			arrowType, canonicalType, err := arrowTypeForSQL(sqlType)
			if err != nil {
				return nil, fmt.Errorf("parse Bronze DDL %s.%s: %w", tableName, name, err)
			}
			fields = append(fields, arrow.Field{Name: name, Type: arrowType, Nullable: true})
			sqlTypes = append(sqlTypes, canonicalType)
		}
		if len(fields) == 0 {
			return nil, fmt.Errorf("Bronze DDL table %s has no columns", tableName)
		}
		if _, exists := layouts[tableName]; exists {
			return nil, fmt.Errorf("Bronze DDL defines table %s more than once", tableName)
		}
		layouts[tableName] = TypedTableLayout{Schema: arrow.NewSchema(fields, nil), SQLTypes: sqlTypes}
	}
	return layouts, nil
}

func parseColumnDefinition(line string) (string, string, error) {
	var name, remainder string
	if strings.HasPrefix(line, `"`) {
		end := strings.Index(line[1:], `"`)
		if end < 0 {
			return "", "", fmt.Errorf("unterminated quoted column in %q", line)
		}
		end++
		quoted := line[:end+1]
		unquoted, err := strconv.Unquote(quoted)
		if err != nil {
			return "", "", fmt.Errorf("unquote column %q: %w", quoted, err)
		}
		name = unquoted
		remainder = strings.TrimSpace(line[end+1:])
	} else {
		parts := strings.Fields(line)
		if len(parts) < 2 {
			return "", "", fmt.Errorf("invalid column definition %q", line)
		}
		name = parts[0]
		remainder = strings.TrimSpace(line[len(name):])
	}
	parts := strings.Fields(remainder)
	if len(parts) == 0 {
		return "", "", fmt.Errorf("column %s has no SQL type", name)
	}
	return name, strings.ToUpper(parts[0]), nil
}

func arrowTypeForSQL(sqlType string) (arrow.DataType, string, error) {
	switch strings.ToUpper(sqlType) {
	case "TEXT", "VARCHAR":
		return arrow.BinaryTypes.String, "VARCHAR", nil
	case "BIGINT":
		return arrow.PrimitiveTypes.Int64, "BIGINT", nil
	case "INTEGER":
		return arrow.PrimitiveTypes.Int32, "INTEGER", nil
	case "DOUBLE":
		return arrow.PrimitiveTypes.Float64, "DOUBLE", nil
	case "BOOLEAN":
		return arrow.FixedWidthTypes.Boolean, "BOOLEAN", nil
	case "TIMESTAMP":
		return timestampWithoutTimeZone, "TIMESTAMP", nil
	default:
		return nil, "", fmt.Errorf("unsupported SQL type %q", sqlType)
	}
}
