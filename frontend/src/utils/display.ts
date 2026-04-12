const connectorTypeLabels: Record<string, string> = {
  FILE: "文件",
  HTTP: "HTTP",
  MYSQL: "MySQL",
  POSTGRESQL: "PostgreSQL",
  KAFKA: "Kafka"
};

const connectorRoleLabels: Record<string, string> = {
  SOURCE: "源端",
  TARGET: "目标端",
  BOTH: "双向"
};

const connectorStatusLabels: Record<string, string> = {
  READY: "就绪",
  DRAFT: "草稿",
  DISABLED: "已禁用"
};

const datasetStatusLabels: Record<string, string> = {
  READY: "就绪",
  DRAFT: "草稿",
  ARCHIVED: "已归档"
};

const jobStatusLabels: Record<string, string> = {
  READY: "就绪",
  DRAFT: "草稿",
  PAUSED: "已暂停",
  DISABLED: "已禁用",
  RUNNING: "运行中"
};

const jobScheduleTypeLabels: Record<string, string> = {
  MANUAL: "手动",
  ONCE: "单次",
  CRON: "定时"
};

const jobWriteStrategyLabels: Record<string, string> = {
  APPEND: "追加",
  OVERWRITE: "覆盖",
  STREAM: "流式",
  UPSERT: "更新插入"
};

const schedulerStateLabels: Record<string, string> = {
  MANUAL: "手动触发",
  DISABLED: "已禁用",
  COMPLETED: "已完成",
  COMPLETE: "已完成",
  UNSCHEDULED: "未调度",
  NORMAL: "正常",
  PAUSED: "已暂停",
  BLOCKED: "阻塞",
  ERROR: "异常",
  READY: "就绪",
  DRAFT: "草稿",
  NONE: "无"
};

const executionStatusLabels: Record<string, string> = {
  PENDING: "待执行",
  RUNNING: "执行中",
  PARTIAL_SUCCESS: "部分成功",
  SUCCESS: "成功",
  FAILED: "失败",
  CANCELED: "已取消"
};

const triggerTypeLabels: Record<string, string> = {
  MANUAL: "手动",
  SCHEDULED: "调度",
  API: "接口"
};

const logLevelLabels: Record<string, string> = {
  INFO: "信息",
  WARN: "警告",
  ERROR: "错误"
};

const connectorProbeStatusLabels: Record<string, string> = {
  READY: "就绪",
  UNREACHABLE: "不可达",
  INVALID_URL: "地址无效",
  NOT_WRITABLE: "不可写",
  INVALID_PATH: "路径无效",
  PATH_CREATE_FAILED: "目录创建失败",
  SUCCESS: "成功",
  FAILED: "失败"
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

export function labelConnectorRole(value: string | null | undefined) {
  return labelOf(connectorRoleLabels, value);
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
  return labelOf(schedulerStateLabels, value, "未设置");
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
  return labelOf(connectorProbeStatusLabels, value, "未执行");
}

export function formatDisplayDate(value: string) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return date.toLocaleString("zh-CN", { hour12: false });
}
