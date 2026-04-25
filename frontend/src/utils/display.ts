const connectorTypeLabels: Record<string, string> = {
  FILE: "\u6587\u4ef6",
  HTTP: "HTTP",
  MYSQL: "MySQL",
  POSTGRESQL: "PostgreSQL",
  KAFKA: "Kafka"
};

const connectorStatusLabels: Record<string, string> = {
  READY: "\u5c31\u7eea",
  DRAFT: "\u8349\u7a3f",
  DISABLED: "\u5df2\u7981\u7528"
};

const datasetStatusLabels: Record<string, string> = {
  READY: "\u5c31\u7eea",
  DRAFT: "\u8349\u7a3f",
  ARCHIVED: "\u5df2\u5f52\u6863"
};

const jobStatusLabels: Record<string, string> = {
  READY: "\u5c31\u7eea",
  DRAFT: "\u8349\u7a3f",
  PAUSED: "\u5df2\u6682\u505c",
  DISABLED: "\u5df2\u7981\u7528",
  RUNNING: "\u8fd0\u884c\u4e2d"
};

const jobScheduleTypeLabels: Record<string, string> = {
  MANUAL: "\u624b\u52a8",
  ONCE: "\u5355\u6b21",
  CRON: "\u5b9a\u65f6"
};

const jobWriteStrategyLabels: Record<string, string> = {
  APPEND: "\u8ffd\u52a0",
  OVERWRITE: "\u8986\u76d6",
  STREAM: "\u6d41\u5f0f",
  UPSERT: "\u66f4\u65b0\u63d2\u5165"
};

const schedulerStateLabels: Record<string, string> = {
  MANUAL: "\u624b\u52a8\u89e6\u53d1",
  DISABLED: "\u5df2\u7981\u7528",
  COMPLETED: "\u5df2\u5b8c\u6210",
  COMPLETE: "\u5df2\u5b8c\u6210",
  UNSCHEDULED: "\u672a\u8c03\u5ea6",
  NORMAL: "\u6b63\u5e38",
  PAUSED: "\u5df2\u6682\u505c",
  BLOCKED: "\u963b\u585e",
  ERROR: "\u5f02\u5e38",
  READY: "\u5c31\u7eea",
  DRAFT: "\u8349\u7a3f",
  NONE: "\u65e0",
  STOPPED: "\u5df2\u505c\u6b62"
};

const executionStatusLabels: Record<string, string> = {
  PENDING: "\u5f85\u6267\u884c",
  RUNNING: "\u6267\u884c\u4e2d",
  PARTIAL_SUCCESS: "\u90e8\u5206\u6210\u529f",
  SUCCESS: "\u6210\u529f",
  FAILED: "\u5931\u8d25",
  CANCELED: "\u5df2\u53d6\u6d88"
};

const triggerTypeLabels: Record<string, string> = {
  MANUAL: "\u624b\u52a8",
  SCHEDULED: "\u8c03\u5ea6",
  API: "API",
  CONTINUOUS: "\u6301\u7eed\u5199\u5165"
};

const logLevelLabels: Record<string, string> = {
  INFO: "\u4fe1\u606f",
  WARN: "\u8b66\u544a",
  ERROR: "\u9519\u8bef"
};

const connectorProbeStatusLabels: Record<string, string> = {
  READY: "\u5c31\u7eea",
  UNREACHABLE: "\u4e0d\u53ef\u8fbe",
  INVALID_URL: "\u5730\u5740\u65e0\u6548",
  NOT_WRITABLE: "\u4e0d\u53ef\u5199",
  INVALID_PATH: "\u8def\u5f84\u65e0\u6548",
  PATH_CREATE_FAILED: "\u76ee\u5f55\u521b\u5efa\u5931\u8d25",
  SUCCESS: "\u6210\u529f",
  FAILED: "\u5931\u8d25"
};

const databaseTypeLabels: Record<string, string> = {
  MYSQL: "MySQL",
  POSTGRESQL: "PostgreSQL",
  SQLSERVER: "SQL Server",
  ORACLE: "Oracle",
  KAFKA: "Kafka"
};

export const supportedDatabaseTypes = Object.freeze(Object.keys(databaseTypeLabels));

const writeTaskStatusLabels: Record<string, string> = {
  READY: "\u5c31\u7eea",
  DRAFT: "\u8349\u7a3f",
  PAUSED: "\u5df2\u6682\u505c",
  DISABLED: "\u5df2\u7981\u7528",
  RUNNING: "\u8fd0\u884c\u4e2d"
};

const writeModeLabels: Record<string, string> = {
  APPEND: "\u8ffd\u52a0",
  OVERWRITE: "\u8986\u76d6"
};

const writeTaskScheduleTypeLabels: Record<string, string> = {
  MANUAL: "\u624b\u52a8\u6267\u884c",
  ONCE: "\u5355\u6b21\u5b9a\u65f6",
  CRON: "\u5468\u671f\u5b9a\u65f6",
  INTERVAL: "\u6301\u7eed\u5199\u5165"
};

const tableModeLabels: Record<string, string> = {
  USE_EXISTING: "\u4f7f\u7528\u5df2\u6709\u8868",
  CREATE_IF_MISSING: "\u4e0d\u5b58\u5728\u5219\u521b\u5efa"
};

const columnGeneratorTypeLabels: Record<string, string> = {
  SEQUENCE: "\u9012\u589e\u5e8f\u5217",
  RANDOM_INT: "\u968f\u673a\u6574\u6570",
  RANDOM_DECIMAL: "\u968f\u673a\u5c0f\u6570",
  STRING: "\u5b57\u7b26\u4e32",
  ENUM: "\u679a\u4e3e",
  BOOLEAN: "\u5e03\u5c14",
  DATETIME: "\u65e5\u671f\u65f6\u95f4",
  UUID: "UUID"
};

function labelOf(labels: Record<string, string>, value: string | null | undefined, fallback = "-") {
  if (!value) {
    return fallback;
  }
  return labels[value] ?? value;
}

export function labelConnectorType(value: string | null | undefined) {
  return labelOf(connectorTypeLabels, value);
}

export function labelConnectorStatus(value: string | null | undefined) {
  return labelOf(connectorStatusLabels, value);
}

export function labelDatasetStatus(value: string | null | undefined) {
  return labelOf(datasetStatusLabels, value);
}

export function labelJobStatus(value: string | null | undefined) {
  return labelOf(jobStatusLabels, value);
}

export function labelJobScheduleType(value: string | null | undefined) {
  return labelOf(jobScheduleTypeLabels, value);
}

export function labelJobWriteStrategy(value: string | null | undefined) {
  return labelOf(jobWriteStrategyLabels, value);
}

export function labelSchedulerState(value: string | null | undefined) {
  return labelOf(schedulerStateLabels, value, "\u672a\u8bbe\u7f6e");
}

export function labelExecutionStatus(value: string | null | undefined) {
  return labelOf(executionStatusLabels, value);
}

export function labelTriggerType(value: string | null | undefined) {
  return labelOf(triggerTypeLabels, value);
}

export function labelLogLevel(value: string | null | undefined) {
  return labelOf(logLevelLabels, value);
}

export function labelConnectorProbeStatus(value: string | null | undefined) {
  return labelOf(connectorProbeStatusLabels, value, "\u672a\u6267\u884c");
}

export function labelDatabaseType(value: string | null | undefined) {
  return labelOf(databaseTypeLabels, value);
}

export function labelWriteTaskStatus(value: string | null | undefined) {
  return labelOf(writeTaskStatusLabels, value);
}

export function labelWriteMode(value: string | null | undefined) {
  return labelOf(writeModeLabels, value);
}

export function labelWriteTaskScheduleType(value: string | null | undefined) {
  return labelOf(writeTaskScheduleTypeLabels, value);
}

export function labelTableMode(value: string | null | undefined) {
  return labelOf(tableModeLabels, value);
}

export function labelColumnGeneratorType(value: string | null | undefined) {
  return labelOf(columnGeneratorTypeLabels, value);
}

export function formatDisplayDate(value: string) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return date.toLocaleString("zh-CN", { hour12: false });
}
