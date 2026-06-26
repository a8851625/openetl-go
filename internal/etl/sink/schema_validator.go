package sink

import (
	"fmt"
	"sort"
	"strings"

	"github.com/a8851625/openetl-go/internal/etl/core"
)

type schemaValidationOptions struct {
	targetName     string
	allowMissing   bool
	allowTypeSync  bool
	missingRemedy  string
	typeSyncRemedy string
}

func validateSchemaCompatibility(source core.SchemaInfo, target []core.ColumnInfo, opts schemaValidationOptions) error {
	if len(source.Columns) == 0 || len(target) == 0 {
		return nil
	}

	targetByName := make(map[string]core.ColumnInfo, len(target))
	for _, col := range target {
		targetByName[strings.ToLower(col.Name)] = col
	}

	var missing []string
	var incompatible []string
	for _, src := range source.Columns {
		targetCol, ok := targetByName[strings.ToLower(src.Name)]
		if !ok {
			missing = append(missing, src.Name)
			continue
		}
		if schemaTypesCompatible(src.DataType, targetCol.DataType) {
			continue
		}
		incompatible = append(incompatible, fmt.Sprintf("%s source=%s target=%s", src.Name, src.DataType, targetCol.DataType))
	}

	var problems []string
	if len(missing) > 0 && !opts.allowMissing {
		sort.Strings(missing)
		remedy := opts.missingRemedy
		if remedy == "" {
			remedy = "enable schema_drift=add_columns or add the columns manually"
		}
		problems = append(problems, fmt.Sprintf("missing target columns [%s] (%s)", strings.Join(missing, ", "), remedy))
	}
	if len(incompatible) > 0 && !opts.allowTypeSync {
		sort.Strings(incompatible)
		remedy := opts.typeSyncRemedy
		if remedy == "" {
			remedy = "adjust the target column type or transform the source field"
		}
		problems = append(problems, fmt.Sprintf("incompatible target column types [%s] (%s)", strings.Join(incompatible, "; "), remedy))
	}
	if len(problems) == 0 {
		return nil
	}
	targetName := opts.targetName
	if targetName == "" {
		targetName = "target"
	}
	return fmt.Errorf("schema validation failed for %s: %s", targetName, strings.Join(problems, "; "))
}

func schemaTypesCompatible(sourceType, targetType string) bool {
	source := schemaTypeFamily(sourceType)
	target := schemaTypeFamily(targetType)
	if source == "" || target == "" || source == "unknown" || target == "unknown" {
		return true
	}
	if source == target {
		return true
	}
	switch source {
	case "int", "uint", "float", "decimal":
		return target == "int" || target == "uint" || target == "float" || target == "decimal" || target == "string"
	case "bool":
		return target == "bool" || target == "int" || target == "uint" || target == "string"
	case "date":
		return target == "date" || target == "time" || target == "string"
	case "time":
		return target == "time" || target == "string"
	case "json":
		return target == "json" || target == "string"
	case "bytes":
		return target == "bytes" || target == "string"
	case "string":
		return target == "string"
	default:
		return false
	}
}

func schemaTypeFamily(t string) string {
	t = strings.ToLower(strings.TrimSpace(t))
	if t == "" {
		return ""
	}
	t = unwrapSchemaType(t, "nullable")
	t = unwrapSchemaType(t, "lowcardinality")
	if strings.HasPrefix(t, "array(") || strings.HasPrefix(t, "map(") || strings.HasPrefix(t, "tuple(") {
		return "json"
	}
	base := t
	if idx := strings.IndexAny(base, "( "); idx >= 0 {
		base = base[:idx]
	}
	base = strings.Trim(base, "`\"")

	switch base {
	case "tinyint", "smallint", "mediumint", "int", "integer", "bigint",
		"serial", "bigserial", "smallserial", "int1", "int2", "int4", "int8",
		"int16", "int32", "int64":
		return "int"
	case "uint8", "uint16", "uint32", "uint64":
		return "uint"
	case "float", "double", "real", "float4", "float8", "float32", "float64":
		return "float"
	case "decimal", "numeric", "number", "decimal32", "decimal64", "decimal128", "decimal256":
		return "decimal"
	case "bool", "boolean":
		return "bool"
	case "date", "date32":
		return "date"
	case "datetime", "timestamp", "timestamptz", "time", "timetz", "datetime64":
		return "time"
	case "char", "character", "varchar", "text", "tinytext", "mediumtext", "longtext",
		"enum", "set", "uuid", "string", "fixedstring", "inet", "cidr", "macaddr":
		return "string"
	case "binary", "varbinary", "blob", "tinyblob", "mediumblob", "longblob", "bytea":
		return "bytes"
	case "json", "jsonb", "object":
		return "json"
	default:
		if strings.Contains(t, "unsigned") && strings.Contains(t, "int") {
			return "uint"
		}
		return "unknown"
	}
}

func unwrapSchemaType(t, wrapper string) string {
	prefix := wrapper + "("
	for strings.HasPrefix(t, prefix) && strings.HasSuffix(t, ")") {
		t = strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(t, prefix), ")"))
	}
	return t
}
