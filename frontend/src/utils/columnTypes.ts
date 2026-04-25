export type ColumnTypeProperty = "length" | "precision" | "scale";
type ColumnSemantic = "TEXT" | "ID" | "INTEGER" | "DECIMAL" | "BOOLEAN" | "DATETIME" | "UUID";

export interface ColumnTypeOption {
  value: string;
  label: string;
  supports: ColumnTypeProperty[];
  defaultLength?: number | null;
  defaultPrecision?: number | null;
  defaultScale?: number | null;
}

export interface ColumnTypeDraft {
  dbType?: string | null;
  lengthValue?: number | null;
  precisionValue?: number | null;
  scaleValue?: number | null;
  generatorType?: string | null;
  primaryKeyFlag?: boolean;
}

const defaultLengthType = 255;
const defaultPrecisionType = 18;
const defaultScaleType = 2;

const mysqlOptions: ColumnTypeOption[] = [
  { value: "BIGINT", label: "长整数", supports: [] },
  { value: "INT", label: "整数", supports: [] },
  { value: "DECIMAL", label: "小数", supports: ["precision", "scale"], defaultPrecision: defaultPrecisionType, defaultScale: defaultScaleType },
  { value: "VARCHAR", label: "文本", supports: ["length"], defaultLength: defaultLengthType },
  { value: "CHAR", label: "定长文本", supports: ["length"], defaultLength: 1 },
  { value: "TEXT", label: "长文本", supports: [] },
  { value: "BOOLEAN", label: "布尔值", supports: [] },
  { value: "DATE", label: "日期", supports: [] },
  { value: "DATETIME", label: "日期时间", supports: [] },
  { value: "TIMESTAMP", label: "时间戳", supports: [] },
  { value: "UUID", label: "UUID", supports: [] }
];

const postgresqlOptions: ColumnTypeOption[] = [
  { value: "BIGINT", label: "长整数", supports: [] },
  { value: "INTEGER", label: "整数", supports: [] },
  { value: "NUMERIC", label: "小数", supports: ["precision", "scale"], defaultPrecision: defaultPrecisionType, defaultScale: defaultScaleType },
  { value: "VARCHAR", label: "文本", supports: ["length"], defaultLength: defaultLengthType },
  { value: "TEXT", label: "长文本", supports: [] },
  { value: "BOOLEAN", label: "布尔值", supports: [] },
  { value: "DATE", label: "日期", supports: [] },
  { value: "TIMESTAMP", label: "日期时间", supports: [] },
  { value: "JSON", label: "JSON", supports: [] },
  { value: "JSONB", label: "JSONB", supports: [] },
  { value: "UUID", label: "UUID", supports: [] }
];

const sqlServerOptions: ColumnTypeOption[] = [
  { value: "BIGINT", label: "长整数", supports: [] },
  { value: "INT", label: "整数", supports: [] },
  { value: "DECIMAL", label: "小数", supports: ["precision", "scale"], defaultPrecision: defaultPrecisionType, defaultScale: defaultScaleType },
  { value: "NVARCHAR", label: "文本", supports: ["length"], defaultLength: defaultLengthType },
  { value: "NCHAR", label: "定长文本", supports: ["length"], defaultLength: 1 },
  { value: "BIT", label: "布尔值", supports: [] },
  { value: "DATE", label: "日期", supports: [] },
  { value: "DATETIME2", label: "日期时间", supports: [] },
  { value: "UNIQUEIDENTIFIER", label: "UUID", supports: [] }
];

const oracleOptions: ColumnTypeOption[] = [
  { value: "BIGINT", label: "长整数", supports: [] },
  { value: "INT", label: "整数", supports: [] },
  { value: "NUMBER", label: "数字", supports: ["precision", "scale"], defaultPrecision: defaultPrecisionType, defaultScale: defaultScaleType },
  { value: "VARCHAR2", label: "文本", supports: ["length"], defaultLength: defaultLengthType },
  { value: "NVARCHAR2", label: "多语言文本", supports: ["length"], defaultLength: defaultLengthType },
  { value: "CHAR", label: "定长文本", supports: ["length"], defaultLength: 1 },
  { value: "CLOB", label: "长文本", supports: [] },
  { value: "BOOLEAN", label: "布尔值", supports: [] },
  { value: "DATE", label: "日期", supports: [] },
  { value: "TIMESTAMP", label: "日期时间", supports: [] },
  { value: "UUID", label: "UUID", supports: [] }
];

const kafkaOptions: ColumnTypeOption[] = [
  { value: "BIGINT", label: "长整数", supports: [] },
  { value: "INT", label: "整数", supports: [] },
  { value: "DECIMAL", label: "小数", supports: ["precision", "scale"], defaultPrecision: defaultPrecisionType, defaultScale: defaultScaleType },
  { value: "VARCHAR", label: "文本", supports: ["length"], defaultLength: defaultLengthType },
  { value: "TEXT", label: "长文本", supports: [] },
  { value: "BOOLEAN", label: "布尔值", supports: [] },
  { value: "DATETIME", label: "日期时间", supports: [] },
  { value: "UUID", label: "UUID", supports: [] }
];

const optionsByDatabaseType: Record<string, ColumnTypeOption[]> = {
  MYSQL: mysqlOptions,
  POSTGRESQL: postgresqlOptions,
  SQLSERVER: sqlServerOptions,
  ORACLE: oracleOptions,
  KAFKA: kafkaOptions
};

export function getColumnTypeOptions(databaseType: string | null | undefined) {
  return optionsByDatabaseType[normalizeDatabaseType(databaseType)] ?? mysqlOptions;
}

export function getPreferredColumnType(databaseType: string | null | undefined, semantic: ColumnSemantic) {
  const mapping: Record<string, Record<ColumnSemantic, string>> = {
    MYSQL: {
      TEXT: "VARCHAR",
      ID: "BIGINT",
      INTEGER: "INT",
      DECIMAL: "DECIMAL",
      BOOLEAN: "BOOLEAN",
      DATETIME: "DATETIME",
      UUID: "UUID"
    },
    POSTGRESQL: {
      TEXT: "VARCHAR",
      ID: "BIGINT",
      INTEGER: "INTEGER",
      DECIMAL: "NUMERIC",
      BOOLEAN: "BOOLEAN",
      DATETIME: "TIMESTAMP",
      UUID: "UUID"
    },
    SQLSERVER: {
      TEXT: "NVARCHAR",
      ID: "BIGINT",
      INTEGER: "INT",
      DECIMAL: "DECIMAL",
      BOOLEAN: "BIT",
      DATETIME: "DATETIME2",
      UUID: "UNIQUEIDENTIFIER"
    },
    ORACLE: {
      TEXT: "VARCHAR2",
      ID: "BIGINT",
      INTEGER: "INT",
      DECIMAL: "NUMBER",
      BOOLEAN: "BOOLEAN",
      DATETIME: "TIMESTAMP",
      UUID: "UUID"
    },
    KAFKA: {
      TEXT: "VARCHAR",
      ID: "BIGINT",
      INTEGER: "INT",
      DECIMAL: "DECIMAL",
      BOOLEAN: "BOOLEAN",
      DATETIME: "DATETIME",
      UUID: "UUID"
    }
  };
  return mapping[normalizeDatabaseType(databaseType)]?.[semantic] ?? "VARCHAR";
}

export function withColumnTypeDefaults(databaseType: string | null | undefined, draft: ColumnTypeDraft) {
  const normalizedType = resolveSupportedColumnType(databaseType, draft);
  const definition = resolveColumnTypeOption(databaseType, normalizedType);

  const lengthValue = definition.supports.includes("length")
    ? coalescePositiveInteger(draft.lengthValue, definition.defaultLength ?? null)
    : null;

  const precisionValue = definition.supports.includes("precision")
    ? coalescePositiveInteger(draft.precisionValue, definition.defaultPrecision ?? null)
    : null;

  const scaleSeed = definition.supports.includes("scale")
    ? coalesceScaleValue(draft.scaleValue, definition.defaultScale ?? null)
    : null;

  const scaleValue = definition.supports.includes("scale") ? scaleSeed : null;
  const normalizedPrecision = definition.supports.includes("precision") && scaleValue !== null && precisionValue === null
    ? (definition.defaultPrecision ?? defaultPrecisionType)
    : precisionValue;

  return {
    dbType: normalizedType,
    lengthValue,
    precisionValue: normalizedPrecision,
    scaleValue
  };
}

export function labelColumnDbType(databaseType: string | null | undefined, dbType: string | null | undefined) {
  return resolveColumnTypeOption(databaseType, resolveSupportedColumnType(databaseType, { dbType })).label;
}

export function supportsColumnTypeProperty(
  databaseType: string | null | undefined,
  dbType: string | null | undefined,
  property: ColumnTypeProperty
) {
  return resolveColumnTypeOption(databaseType, resolveSupportedColumnType(databaseType, { dbType })).supports.includes(property);
}

function resolveSupportedColumnType(databaseType: string | null | undefined, draft: ColumnTypeDraft) {
  const options = getColumnTypeOptions(databaseType);
  const normalizedType = normalizeTypeName(draft.dbType);
  const aliasedType = normalizeColumnTypeAlias(databaseType, normalizedType);
  if (aliasedType && options.some((option) => option.value === aliasedType)) {
    return aliasedType;
  }
  if (normalizedType) {
    return normalizedType;
  }
  return getPreferredColumnType(databaseType, inferSemantic(draft));
}

function normalizeColumnTypeAlias(databaseType: string | null | undefined, dbType: string) {
  const normalizedDatabaseType = normalizeDatabaseType(databaseType);
  if (normalizedDatabaseType === "POSTGRESQL") {
    switch (dbType) {
      case "INT8":
        return "BIGINT";
      case "INT4":
      case "INT":
        return "INTEGER";
      case "BOOL":
      case "BIT":
        return "BOOLEAN";
      case "DECIMAL":
      case "NUMBER":
        return "NUMERIC";
      case "DATETIME":
      case "DATETIME2":
        return "TIMESTAMP";
      default:
        return dbType;
    }
  }
  return dbType;
}

function resolveColumnTypeOption(databaseType: string | null | undefined, dbType: string) {
  const options = getColumnTypeOptions(databaseType);
  const normalizedType = normalizeTypeName(dbType);
  return options.find((option) => option.value === normalizedType)
    ?? {
      value: normalizedType || getPreferredColumnType(databaseType, "TEXT"),
      label: normalizedType || "文本",
      supports: inferSupportedProperties(normalizedType)
    };
}

function inferSemantic(draft: ColumnTypeDraft): ColumnSemantic {
  const normalizedType = normalizeTypeName(draft.dbType);
  if (draft.primaryKeyFlag || draft.generatorType === "SEQUENCE" || normalizedType === "BIGINT") {
    return "ID";
  }
  if (draft.generatorType === "UUID" || normalizedType.includes("UUID") || normalizedType.includes("UNIQUEIDENTIFIER")) {
    return "UUID";
  }
  if (draft.generatorType === "BOOLEAN" || normalizedType.includes("BOOL") || normalizedType === "BIT") {
    return "BOOLEAN";
  }
  if (draft.generatorType === "DATETIME" || normalizedType.includes("DATE") || normalizedType.includes("TIME")) {
    return "DATETIME";
  }
  if (draft.generatorType === "RANDOM_DECIMAL"
    || normalizedType.includes("DECIMAL")
    || normalizedType.includes("NUMERIC")
    || normalizedType.includes("NUMBER")
    || normalizedType.includes("FLOAT")
    || normalizedType.includes("DOUBLE")
    || normalizedType.includes("REAL")
    || (draft.scaleValue ?? 0) > 0) {
    return "DECIMAL";
  }
  if (draft.generatorType === "RANDOM_INT"
    || normalizedType.includes("INT")
    || normalizedType === "NUMBER"
    || normalizedType === "INTEGER"
    || normalizedType === "SMALLINT"
    || normalizedType === "TINYINT") {
    return "INTEGER";
  }
  return "TEXT";
}

function inferSupportedProperties(dbType: string | null | undefined): ColumnTypeProperty[] {
  const normalizedType = normalizeTypeName(dbType);
  if (normalizedType.includes("CHAR") || normalizedType.includes("BINARY")) {
    return ["length"];
  }
  if (normalizedType.includes("DECIMAL") || normalizedType.includes("NUMERIC") || normalizedType.includes("NUMBER")) {
    return ["precision", "scale"];
  }
  return [];
}

function normalizeDatabaseType(databaseType: string | null | undefined) {
  const normalized = normalizeTypeName(databaseType);
  return normalized || "MYSQL";
}

function normalizeTypeName(value: string | null | undefined) {
  return typeof value === "string" ? value.trim().toUpperCase() : "";
}

function coalescePositiveInteger(value: number | null | undefined, fallback: number | null) {
  const normalizedValue = normalizePositiveInteger(value);
  return normalizedValue ?? normalizePositiveInteger(fallback);
}

function normalizePositiveInteger(value: number | null | undefined) {
  if (typeof value !== "number" || !Number.isFinite(value) || value <= 0) {
    return null;
  }
  return Math.floor(value);
}

function coalesceScaleValue(value: number | null | undefined, fallback: number | null) {
  const normalizedValue = normalizeScaleValue(value);
  return normalizedValue ?? normalizeScaleValue(fallback);
}

function normalizeScaleValue(value: number | null | undefined) {
  if (typeof value !== "number" || !Number.isFinite(value) || value < 0) {
    return null;
  }
  return Math.floor(value);
}
