<template>
  <section class="page">
    <header class="page-header page-header--stacked">
      <div class="page-header__content">
        <h2>写入任务</h2>
      </div>
      <div class="page-actions">
        <button class="button button--ghost" type="button" @click="loadPageData">刷新</button>
        <button
          v-if="isTaskListPage"
          class="button"
          type="button"
          :disabled="isSaving || isPreviewing || hasTaskActionInFlight"
          @click="startNewTask"
        >
          新建任务
        </button>
        <button
          v-else
          class="button button--ghost"
          type="button"
          :disabled="isSaving || isPreviewing || hasTaskActionInFlight"
          @click="goToTaskList"
        >
          返回任务列表
        </button>
      </div>
    </header>

    <section v-if="isTaskListPage" class="stats-grid stats-grid--compact">
      <article class="stat-card">
        <span>任务总数</span>
        <strong>{{ tasks.length }}</strong>
      </article>
      <article class="stat-card">
        <span>就绪任务</span>
        <strong>{{ readyTaskCount }}</strong>
      </article>
      <article class="stat-card">
        <span>运行中</span>
        <strong>{{ runningTaskCount }}</strong>
      </article>
      <article class="stat-card">
        <span>持续写入</span>
        <strong>{{ continuousTaskCount }}</strong>
      </article>
    </section>

    <div
      v-if="feedback.message && isTaskListPage"
      class="status-banner"
      :class="feedback.kind === 'error' ? 'status-banner--error' : 'status-banner--success'"
    >
      {{ feedback.message }}
    </div>

    <section class="workspace-grid jobs-workspace jobs-workspace--single">
      <aside v-if="isTaskListPage && selectedTaskId === null" class="stack jobs-workspace__sidebar">
        <section class="panel task-list-panel">
          <div class="panel__row panel__row--divider">
            <div>
              <p class="list-caption">任务较多时按页切换，列表和编辑区不会再互相挤压。</p>
              <h3>任务列表</h3>
            </div>
            <div class="panel__actions panel__actions--tight">
              <button class="button button--ghost" type="button" :disabled="isSaving || isPreviewing || hasTaskActionInFlight" @click="startNewTask">新建任务</button>
            </div>
          </div>

          <div v-if="tasks.length" class="task-list">
            <article v-for="task in paginatedTasks" :key="task.id" class="task-list__item" :class="{ 'panel--selected': selectedTaskId === task.id }">
              <div class="task-list__main">
                <div class="panel__row">
                  <h3>{{ displayTaskName(task) }}</h3>
                  <span class="pill">{{ labelWriteTaskStatus(task.status) }}</span>
                </div>

                <div class="task-list__meta">
                  <span>{{ resolveConnectionName(task.connectionId) }}</span>
                  <span>{{ task.tableName }}</span>
                  <span>{{ labelTableMode(task.tableMode) }} / {{ labelWriteMode(task.writeMode) }}</span>
                  <span>{{ labelWriteTaskScheduleType(task.scheduleType) }} / {{ labelSchedulerState(task.schedulerState) }}</span>
                  <span>{{ describeSchedule(task) }}</span>
                  <span>{{ task.rowCount }} 条 / {{ taskFieldCount(task) }} 字段</span>
                  <span v-if="task.scheduleType === 'INTERVAL'">{{ describeContinuousLimit(task) }}</span>
                  <span v-if="task.nextFireAt">下次执行：{{ formatOptionalDate(task.nextFireAt) }}</span>
                </div>
              </div>

              <div class="task-list__actions">
                <button class="button" type="button" :disabled="isSaving || isPreviewing || hasTaskActionInFlight" @click="selectTask(task)">编辑</button>
                <button class="button button--ghost" type="button" :disabled="isSaving || isPreviewing || hasTaskActionInFlight" @click="openTaskPreview(task.id)">预览</button>
                <button class="button button--ghost" type="button" :disabled="isSaving || isPreviewing || hasTaskActionInFlight" @click="openTaskExecutions(task.id)">实例</button>
                <button
                  v-if="task.scheduleType !== 'INTERVAL' || task.status !== 'RUNNING'"
                  class="button button--ghost"
                  type="button"
                  :disabled="isSaving || isPreviewing || hasTaskActionInFlight"
                  @click="runTask(task.id)"
                >
                  {{ executingTaskId === task.id ? '执行中...' : (task.scheduleType === 'INTERVAL' ? '执行一轮' : '执行') }}
                </button>
                <button
                  v-if="canStartContinuous(task)"
                  class="button button--ghost"
                  type="button"
                  :disabled="isSaving || isPreviewing || hasTaskActionInFlight"
                  @click="startContinuousTask(task.id)"
                >
                  {{ mutatingTaskId === task.id ? '处理中...' : '启动持续写入' }}
                </button>
                <button
                  v-if="canPauseTask(task)"
                  class="button button--ghost"
                  type="button"
                  :disabled="isSaving || isPreviewing || hasTaskActionInFlight"
                  @click="pauseTaskSchedule(task.id)"
                >
                  {{ mutatingTaskId === task.id ? '处理中...' : '暂停' }}
                </button>
                <button
                  v-if="canResumeTask(task)"
                  class="button button--ghost"
                  type="button"
                  :disabled="isSaving || isPreviewing || hasTaskActionInFlight"
                  @click="resumeTaskSchedule(task.id)"
                >
                  {{ mutatingTaskId === task.id ? '处理中...' : '恢复' }}
                </button>
                <button
                  v-if="canStopContinuous(task)"
                  class="button button--ghost"
                  type="button"
                  :disabled="isSaving || isPreviewing || hasTaskActionInFlight"
                  @click="stopContinuousTask(task.id)"
                >
                  {{ mutatingTaskId === task.id ? '处理中...' : '停止' }}
                </button>
                <button class="button button--danger" type="button" :disabled="isSaving || isPreviewing || hasTaskActionInFlight" @click="removeTask(task.id)">删除</button>
              </div>
            </article>
          </div>

          <PaginationBar
            v-if="tasks.length"
            :page="taskPage"
            :page-size="taskPageSize"
            :total="tasks.length"
            noun="个任务"
            @update:page="taskPage = $event"
            @update:page-size="taskPageSize = $event"
          />

          <section v-else class="empty-state">
            <h3>暂无任务</h3>
          </section>
        </section>
      </aside>

      <article v-if="isTaskEditorPage || selectedTaskId !== null" ref="builderPanelRef" class="panel builder-layout jobs-workspace__primary">
        <div class="panel__actions panel__actions--tight">
          <button class="button button--ghost" type="button" :disabled="isSaving || isPreviewing || hasTaskActionInFlight" @click="goToTaskList">
            返回任务列表
          </button>
        </div>
        <div>
          <h3>{{ selectedTaskId ? '编辑任务' : '新建任务' }}</h3>
        </div>

        <div
          v-if="feedback.message"
          class="status-banner"
          :class="feedback.kind === 'error' ? 'status-banner--error' : 'status-banner--success'"
        >
          <div class="status-banner__content">
            <span>{{ feedback.message }}</span>
            <button
              v-if="feedback.kind === 'error' && validationIssue"
              class="button button--ghost status-banner__action"
              type="button"
              @click="revealValidationIssue()"
            >
              定位问题
            </button>
          </div>
        </div>

        <section v-if="lastRunSummary" ref="runSummaryPanelRef" class="panel panel--embedded">
          <h3>最近一次执行结果</h3>
          <div class="meta-grid">
            <p>
              <strong>任务：</strong>{{ lastRunSummary.taskName }} /
              <strong>状态：</strong>{{ lastRunSummary.statusText }}
            </p>
            <p>
              <template v-if="lastRunSummary.deliveryType === 'KAFKA'">
                <strong>本次写入：</strong>{{ lastRunSummary.writtenRowCount }} 条消息 /
                <strong>Topic：</strong>{{ lastRunSummary.topic || "-" }} /
                <strong>Payload：</strong>{{ lastRunSummary.payloadFormat || "JSON" }}
              </template>
              <template v-else>
                <strong>本次写入：</strong>{{ lastRunSummary.writtenRowCount }} 条 /
                <strong>写入前：</strong>{{ lastRunSummary.beforeWriteRowCount }} 条 /
                <strong>写入后：</strong>{{ lastRunSummary.afterWriteRowCount }} 条 /
                <strong>净变化：</strong>{{ lastRunSummary.rowDelta }} 条
              </template>
            </p>
            <p v-if="lastRunSummary.deliveryType === 'KAFKA'">
              <strong>Key 模式：</strong>{{ lastRunSummary.keyMode || "NONE" }} /
              <strong>Key 路径：</strong>{{ lastRunSummary.keyPath || lastRunSummary.keyField || "-" }} /
              <strong>固定 Key：</strong>{{ lastRunSummary.fixedKey || "-" }} /
              <strong>Headers：</strong>{{ lastRunSummary.headerCount }}
            </p>
            <p v-else-if="lastRunSummary.hasValidation">
              <strong>非空校验：</strong>{{ lastRunSummary.validationPassed ? '通过' : '未通过' }} /
              <strong>空值：</strong>{{ lastRunSummary.nullValueCount }} /
              <strong>空字符串：</strong>{{ lastRunSummary.blankStringCount }}
            </p>
            <p v-if="lastRunSummary.errorSummary">
              <strong>错误：</strong>{{ lastRunSummary.errorSummary }}
            </p>
          </div>
          <div class="panel__actions panel__actions--tight">
            <button class="button button--ghost" type="button" @click="openTaskExecutionDetail(lastRunSummary.taskId, lastRunSummary.executionId)">
              查看本次实例
            </button>
            <button class="button button--ghost" type="button" @click="openTaskExecutions(lastRunSummary.taskId)">
              查看全部实例
            </button>
          </div>
        </section>

        <form class="form-grid" @submit.prevent="saveTask">
          <section class="builder-section">
            <div class="section-title">
              <span class="section-index">01</span>
              <div>
                <h3>目标表信息</h3>
              </div>
            </div>

            <div v-if="!isKafkaTarget" class="mode-switch">
              <button
                class="mode-switch__item"
                :class="{ 'mode-switch__item--active': sourceMode === 'EXISTING' }"
                type="button"
                @click="switchSourceMode('EXISTING')"
              >
                使用已有表
              </button>
              <button
                class="mode-switch__item"
                :class="{ 'mode-switch__item--active': sourceMode === 'MANUAL' }"
                type="button"
                @click="switchSourceMode('MANUAL')"
              >
                新建表
              </button>
            </div>

            <div class="field">
              <label>
                <span>任务名称</span>
                <input v-model.trim="form.name" name="taskName" type="text" placeholder="示例：向订单表写入 1000 条模拟数据" />
              </label>
            </div>

            <div class="field field--half">
              <label>
                <span>目标连接</span>
                <select v-model.number="form.connectionId" name="connectionId" @change="handleConnectionChange">
                  <option :value="null">请选择连接</option>
                  <option v-for="connection in connections" :key="connection.id" :value="connection.id">
                    {{ connection.name }}（{{ labelDatabaseType(connection.dbType) }}）
                  </option>
                </select>
              </label>

              <label v-if="sourceMode === 'EXISTING' && !isKafkaTarget">
                <span>选择已有表</span>
                <select v-model="form.tableName" name="tableName" @change="handleExistingTableSelection">
                  <option value="">请选择表</option>
                  <option v-for="table in tables" :key="tableOptionValue(table)" :value="tableOptionValue(table)">
                    {{ formatTableLabel(table) }}
                  </option>
                </select>
              </label>

              <label v-else>
                <span>{{ isKafkaTarget ? "Topic 名称" : "新表名" }}</span>
                <input v-model.trim="form.tableName" name="tableName" type="text" :placeholder="manualTargetPlaceholder" />
              </label>
            </div>

            <template v-if="sourceMode === 'EXISTING' && !isKafkaTarget">
              <div class="button-row button-row--compact">
                <button class="button button--ghost" type="button" @click="loadTables(undefined, { clearFeedbackOnSuccess: true })">刷新表列表</button>
              </div>
            </template>
          </section>

          <section class="builder-section">
            <div class="section-title">
              <span class="section-index">02</span>
              <div>
                <h3>写入计划</h3>
              </div>
            </div>

            <div class="panel panel--embedded">
              <div class="panel__row">
                <div>
                  <h3>执行方式</h3>
                </div>
                <span class="pill pill--soft">{{ labelWriteTaskScheduleType(form.scheduleType) }}</span>
              </div>

              <div class="field field--half">
                <label>
                  <select
                    v-if="!isKafkaComplexMode"
                    v-model="form.keyField"
                    name="keyField"
                  >
                    <option value="">请选择字段</option>
                    <option v-for="path in availableKafkaFieldPaths" :key="`simple-${path}`" :value="path">{{ path }}</option>
                  </select>
                  <select
                    v-else
                    v-model="form.keyPath"
                    name="keyPath"
                  >
                    <option value="">请选择字段路径</option>
                    <option v-for="path in availableKafkaFieldPaths" :key="`complex-${path}`" :value="path">{{ path }}</option>
                  </select>
                  <span>写入模式</span>
                  <select v-model="form.writeMode" name="writeMode">
                    <option v-for="mode in availableWriteModes" :key="mode" :value="mode">{{ labelWriteMode(mode) }}</option>
                  </select>
                </label>

                <label>
                  <span>调度方式</span>
                  <select v-model="form.scheduleType" name="scheduleType" @change="handleScheduleTypeChange">
                    <option value="MANUAL">手动执行</option>
                    <option value="ONCE">单次定时</option>
                    <option value="CRON">周期定时</option>
                    <option value="INTERVAL">持续写入</option>
                  </select>
                </label>
              </div>

              <div class="field field--half">
                <label>
                  <span>每批生成条数</span>
                  <input v-model.number="form.rowCount" name="rowCount" type="number" min="1" max="100000" />
                </label>

                <label>
                  <span>单批提交大小</span>
                  <input v-model.number="form.batchSize" name="batchSize" type="number" min="1" max="5000" />
                </label>
              </div>

              <div v-if="form.scheduleType === 'ONCE'" class="field field--half">
                <label>
                  <span>执行时间</span>
                  <input v-model="form.triggerAt" name="triggerAt" type="datetime-local" />
                </label>
              </div>

              <div v-else-if="form.scheduleType === 'CRON'" class="field">
                <div class="field field--half">
                  <label>
                    <span>计划类型</span>
                    <select v-model="form.cronBuilderMode" name="cronBuilderMode" @change="handleCronBuilderChange">
                      <option v-for="option in cronBuilderModeOptions" :key="option.value" :value="option.value">{{ option.label }}</option>
                    </select>
                  </label>

                  <label v-if="form.cronBuilderMode === 'EVERY_MINUTES'">
                    <span>间隔分钟</span>
                    <select v-model.number="form.cronMinuteStep" name="cronMinuteStep" @change="handleCronBuilderChange">
                      <option v-for="value in cronMinuteStepOptions" :key="value" :value="value">每 {{ value }} 分钟</option>
                    </select>
                  </label>

                  <label v-else-if="form.cronBuilderMode === 'EVERY_HOURS'">
                    <span>间隔小时</span>
                    <select v-model.number="form.cronHourStep" name="cronHourStep" @change="handleCronBuilderChange">
                      <option v-for="value in cronHourStepOptions" :key="value" :value="value">每 {{ value }} 小时</option>
                    </select>
                  </label>

                  <label v-else-if="form.cronBuilderMode === 'WEEKLY'">
                    <span>执行星期</span>
                    <select v-model="form.cronWeekDay" name="cronWeekDay" @change="handleCronBuilderChange">
                      <option v-for="option in cronWeekDayOptions" :key="option.value" :value="option.value">{{ option.label }}</option>
                    </select>
                  </label>

                  <label v-else>
                    <span>执行说明</span>
                    <input type="text" :value="'按固定时间执行'" readonly />
                  </label>
                </div>

                <div v-if="form.cronBuilderMode === 'EVERY_HOURS' || form.cronBuilderMode === 'DAILY' || form.cronBuilderMode === 'WEEKLY'" class="field field--half">
                  <label>
                    <span>执行小时</span>
                    <select v-model.number="form.cronAtHour" name="cronAtHour" @change="handleCronBuilderChange">
                      <option v-for="value in cronHourOptions" :key="value" :value="value">{{ value.toString().padStart(2, '0') }} 点</option>
                    </select>
                  </label>

                  <label>
                    <span>执行分钟</span>
                    <select v-model.number="form.cronAtMinute" name="cronAtMinute" @change="handleCronBuilderChange">
                      <option v-for="value in cronMinuteOptions" :key="value" :value="value">{{ value.toString().padStart(2, '0') }} 分</option>
                    </select>
                  </label>
                </div>

                <div class="cron-builder-summary">
                  <strong>当前计划：</strong>{{ describeCronExpression(cronPreviewExpression) }}
                </div>

                <div v-if="cronExpressionMode === 'LEGACY'" class="status-banner">
                  当前任务沿用旧 Cron：{{ form.cronExpression }}。只要修改上面的选项，系统就会自动切换为新的选择式计划。
                </div>
              </div>

              <template v-else-if="form.scheduleType === 'INTERVAL'">
                <div class="field field--half">
                  <label>
                    <span>间隔秒数</span>
                    <input v-model.number="form.intervalSeconds" name="intervalSeconds" type="number" min="1" />
                  </label>

                  <label>
                    <span>最多执行次数</span>
                    <input v-model.number="form.maxRuns" name="maxRuns" type="number" min="1" placeholder="可选" />
                  </label>
                </div>

                <div class="field field--half">
                  <label>
                    <span>累计最多写入条数</span>
                    <input v-model.number="form.maxRowsTotal" name="maxRowsTotal" type="number" min="1" placeholder="可选" />
                  </label>
                </div>
              </template>

              <details class="details-panel">
                <summary>高级参数</summary>
                <div class="details-panel__content">
                  <div class="field field--half">
                    <label>
                      <span>随机种子</span>
                      <input v-model.number="form.seed" name="seed" type="number" placeholder="可选，用于复现数据" />
                    </label>

                    <label>
                      <span>任务状态</span>
                      <select v-model="form.status">
                        <option v-for="status in availableStatuses" :key="status" :value="status">{{ labelWriteTaskStatus(status) }}</option>
                      </select>
                    </label>
                  </div>

                  <div class="field">
                    <label>
                      <span>说明</span>
                      <textarea v-model.trim="form.description" rows="3" placeholder="描述这个任务的用途和目标表含义。"></textarea>
                    </label>
                  </div>
                </div>
              </details>
            </div>

            <section v-if="selectedTask" class="panel panel--embedded">
              <h3>任务状态</h3>
              <div class="meta-grid">
                <p>
                  <strong>调度方式：</strong>{{ labelWriteTaskScheduleType(selectedTask.scheduleType) }} /
                  <strong>任务状态：</strong>{{ labelWriteTaskStatus(selectedTask.status) }} /
                  <strong>调度状态：</strong>{{ labelSchedulerState(selectedTask.schedulerState) }}
                </p>
                <p>
                  <strong>当前计划：</strong>{{ describeSchedule(selectedTask) }}
                </p>
                <p>
                  <strong>下次执行：</strong>{{ formatOptionalDate(selectedTask.nextFireAt) }} /
                  <strong>上次调度：</strong>{{ formatOptionalDate(selectedTask.previousFireAt) }} /
                  <strong>最近触发：</strong>{{ formatOptionalDate(selectedTask.lastTriggeredAt) }}
                </p>
                <p v-if="selectedTask.scheduleType === 'INTERVAL'">
                  <strong>停止条件：</strong>{{ describeContinuousLimit(selectedTask) }}
                </p>
              </div>
            </section>
          </section>

          <section v-if="isKafkaTarget" class="builder-section">
            <div class="section-title">
              <span class="section-index">03</span>
              <div>
                <h3>Kafka 投递设置</h3>
              </div>
            </div>

            <div class="panel panel--embedded kafka-panel" data-section="kafka-settings">
              <div class="kafka-panel__top">
                <div class="kafka-panel__mode">
                  <div class="mode-switch">
                    <button
                      class="mode-switch__item"
                      :class="{ 'mode-switch__item--active': form.kafkaMessageMode === 'SIMPLE' }"
                      type="button"
                      @click="switchKafkaMessageMode('SIMPLE')"
                    >
                      简单字段模式
                    </button>
                    <button
                      class="mode-switch__item"
                      :class="{ 'mode-switch__item--active': form.kafkaMessageMode === 'COMPLEX' }"
                      type="button"
                      @click="switchKafkaMessageMode('COMPLEX')"
                    >
                      复杂 JSON 模式
                    </button>
                  </div>
                  <p class="kafka-panel__hint">
                    {{ form.kafkaMessageMode === 'COMPLEX' ? '适合对象、数组这类嵌套 JSON 消息。' : '适合字段较少、结构扁平的消息。' }}
                  </p>
                </div>

                <div class="kafka-panel__summary">
                  <div class="kafka-panel__summary-item">
                    <span>Payload</span>
                    <strong>JSON</strong>
                  </div>
                  <div class="kafka-panel__summary-item">
                    <span>Key 模式</span>
                    <strong>{{ labelKafkaKeyMode(form.keyMode) }}</strong>
                  </div>
                  <div class="kafka-panel__summary-item">
                    <span>可用字段</span>
                    <strong>{{ kafkaFieldCount }}</strong>
                  </div>
                </div>
              </div>

              <KafkaSchemaImportPanel
                v-if="isKafkaComplexMode"
                @imported="applyImportedKafkaSchema"
              />

              <div class="field field--half kafka-panel__controls">
                <label>
                  <span>Payload 格式</span>
                  <input type="text" value="JSON" readonly />
                </label>

                <label>
                  <span>Key 模式</span>
                  <select v-model="form.keyMode" name="keyMode">
                    <option value="NONE">不设置 Key</option>
                    <option value="FIELD">使用字段值</option>
                    <option value="FIXED">使用固定值</option>
                  </select>
                </label>
              </div>

              <div v-if="form.keyMode === 'FIELD'" class="field">
                <label>
                  <span>{{ isKafkaComplexMode ? "Key 路径" : "Key 字段名" }}</span>
                  <select
                    v-if="!isKafkaComplexMode"
                    v-model="form.keyField"
                    name="keyField"
                  >
                    <option value="">请选择字段</option>
                    <option v-for="path in availableKafkaFieldPaths" :key="`simple-${path}`" :value="path">{{ path }}</option>
                  </select>
                  <select
                    v-else
                    v-model="form.keyPath"
                    name="keyPath"
                  >
                    <option value="">请选择字段路径</option>
                    <option v-for="path in availableKafkaFieldPaths" :key="`complex-${path}`" :value="path">{{ path }}</option>
                  </select>
                </label>
              </div>

              <div v-if="form.keyMode === 'FIXED'" class="field">
                <label>
                  <span>固定 Key</span>
                  <input v-model.trim="form.fixedKey" name="fixedKey" type="text" placeholder="例如：demo-key" />
                </label>
              </div>

              <div class="field field--half">
                <label>
                  <span>指定分区</span>
                  <input v-model.number="form.partition" name="partition" type="number" min="0" placeholder="可选" />
                </label>
              </div>

              <KafkaHeadersEditor
                v-model="form.headerEntries"
                :available-paths="availableKafkaFieldPaths"
              />
            </div>
          </section>

          <section class="builder-section">
            <div class="section-title">
              <span class="section-index">{{ isKafkaTarget ? "04" : "03" }}</span>
              <div>
                <h3>字段与规则</h3>
              </div>
            </div>

            <div v-if="isKafkaComplexMode" class="panel panel--embedded kafka-schema-panel" data-section="payload-schema">
              <div class="panel__row kafka-schema-panel__header">
                <div>
                  <h3>消息结构</h3>
                </div>
                <div class="kafka-schema-panel__meta">
                  <span class="pill pill--soft">标量字段 {{ kafkaFieldCount }}</span>
                  <span class="pill pill--soft">Key 路径 {{ availableKafkaKeyPaths.length }}</span>
                </div>
              </div>

              <KafkaPayloadSchemaEditor
                v-if="form.payloadSchemaRoot"
                :active-selector="validationIssue?.selector ?? null"
                :node="form.payloadSchemaRoot"
              />
            </div>

            <div v-else class="panel panel--embedded" data-section="field-rules">
              <div class="panel__row">
                <div>
                  <h3>{{ sourceMode === 'EXISTING' ? '字段已按表结构自动导入' : '手工配置字段与生成规则' }}</h3>
                </div>
                <div v-if="sourceMode === 'MANUAL'" class="panel__actions panel__actions--tight">
                  <button class="button button--ghost" data-action="add-column" type="button" @click="addColumn">新增字段</button>
                </div>
              </div>

              <div class="stack">
                <article v-for="(column, index) in form.columns" :key="column.localId" class="panel panel--subtle column-card" :data-column-id="column.localId">
                  <div class="panel__row column-card__header">
                    <strong class="column-card__title">字段 {{ index + 1 }}</strong>
                    <button v-if="sourceMode === 'MANUAL'" class="button button--danger" type="button" @click="removeColumn(index)">删除</button>
                  </div>

                  <div class="field field--half">
                    <label>
                      <span>字段名</span>
                      <input v-model.trim="column.columnName" data-column-field="name" :readonly="sourceMode === 'EXISTING'" type="text" placeholder="例如：id" />
                    </label>
                    <label>
                      <span>数据类型</span>
                      <input
                        v-if="sourceMode === 'EXISTING'"
                        v-model.trim="column.dbType"
                        data-column-field="dbType"
                        readonly
                        type="text"
                        placeholder="例如：BIGINT / VARCHAR / TIMESTAMP"
                      />
                      <select
                        v-else
                        v-model="column.dbType"
                        data-column-field="dbType"
                        class="column-type-select"
                        @change="handleColumnTypeChange(column)"
                      >
                        <option v-for="option in manualColumnTypeOptions" :key="option.value" :value="option.value">
                          {{ option.label }}
                        </option>
                      </select>
                    </label>
                  </div>

                  <div v-if="supportsLength(column) || supportsPrecision(column)" class="field field--half">
                    <label v-if="supportsLength(column)">
                      <span>长度</span>
                      <input v-model.number="column.lengthValue" class="column-length-input" :disabled="sourceMode === 'EXISTING'" type="number" min="1" placeholder="可选" />
                    </label>
                    <label v-if="supportsPrecision(column)">
                      <span>精度</span>
                      <input v-model.number="column.precisionValue" class="column-precision-input" :disabled="sourceMode === 'EXISTING'" type="number" min="1" placeholder="可选" />
                    </label>
                  </div>

                  <div class="field field--half">
                    <label v-if="supportsScale(column)">
                      <span>小数位</span>
                      <input v-model.number="column.scaleValue" class="column-scale-input" :disabled="sourceMode === 'EXISTING'" type="number" min="0" placeholder="可选" />
                    </label>
                    <label>
                      <span>生成规则</span>
                      <select v-model="column.generatorType" @change="resetGeneratorConfig(column)">
                        <option v-for="type in generatorTypes" :key="type" :value="type">{{ labelColumnGeneratorType(type) }}</option>
                      </select>
                    </label>
                  </div>

                  <div class="column-card__flags">
                    <label class="checkbox-chip">
                      <input v-model="column.nullableFlag" type="checkbox" />
                      <span>可为空</span>
                    </label>
                    <label class="checkbox-chip">
                      <input v-model="column.primaryKeyFlag" type="checkbox" @change="handlePrimaryKeyFlagChange(column)" />
                      <span>主键</span>
                    </label>
                  </div>

                  <section class="generator-section">
                    <h4>{{ labelColumnGeneratorType(column.generatorType) }}</h4>

                    <div v-if="column.generatorType === 'SEQUENCE'" class="field field--half">
                      <label>
                        <span>起始值</span>
                        <input :value="readGeneratorNumber(column, 'start', 1)" type="number" @input="updateGeneratorNumber(column, 'start', $event)" />
                      </label>
                      <label>
                        <span>步长</span>
                        <input :value="readGeneratorNumber(column, 'step', 1)" type="number" @input="updateGeneratorNumber(column, 'step', $event)" />
                      </label>
                    </div>

                    <div v-else-if="column.generatorType === 'RANDOM_INT'" class="field field--half">
                      <label>
                        <span>最小值</span>
                        <input :value="readGeneratorNumber(column, 'min', 0)" type="number" @input="updateGeneratorNumber(column, 'min', $event)" />
                      </label>
                      <label>
                        <span>最大值</span>
                        <input :value="readGeneratorNumber(column, 'max', 1000)" type="number" @input="updateGeneratorNumber(column, 'max', $event)" />
                      </label>
                    </div>

                    <div v-else-if="column.generatorType === 'RANDOM_DECIMAL'" class="field field--half">
                      <label>
                        <span>最小值</span>
                        <input :value="readGeneratorNumber(column, 'min', 0)" type="number" step="0.01" @input="updateGeneratorNumber(column, 'min', $event)" />
                      </label>
                      <label>
                        <span>最大值</span>
                        <input :value="readGeneratorNumber(column, 'max', 1000)" type="number" step="0.01" @input="updateGeneratorNumber(column, 'max', $event)" />
                      </label>
                      <label>
                        <span>小数位</span>
                        <input :value="readGeneratorNumber(column, 'scale', 2)" type="number" min="0" @input="updateGeneratorNumber(column, 'scale', $event)" />
                      </label>
                    </div>

                    <template v-else-if="column.generatorType === 'STRING'">
                      <div class="field field--half">
                        <label>
                          <span>模式</span>
                          <select :value="readGeneratorMode(column)" @change="updateGeneratorMode(column, $event)">
                            <option value="random">随机字符串</option>
                            <option value="email">邮箱地址</option>
                          </select>
                        </label>
                        <label>
                          <span>前缀</span>
                          <input :value="readGeneratorText(column, 'prefix', '')" type="text" placeholder="可选" @input="updateGeneratorText(column, 'prefix', $event)" />
                        </label>
                      </div>

                      <div class="field field--half">
                        <label v-if="readGeneratorMode(column) === 'random'">
                          <span>随机长度</span>
                          <input :value="readGeneratorNumber(column, 'length', 12)" type="number" min="1" @input="updateGeneratorNumber(column, 'length', $event)" />
                        </label>
                        <label v-else>
                          <span>邮箱域名</span>
                          <input :value="readGeneratorText(column, 'domain', 'demo.local')" type="text" placeholder="demo.local" @input="updateGeneratorText(column, 'domain', $event)" />
                        </label>

                        <label>
                          <span>后缀</span>
                          <input :value="readGeneratorText(column, 'suffix', '')" type="text" placeholder="可选" @input="updateGeneratorText(column, 'suffix', $event)" />
                        </label>
                      </div>

                      <div v-if="readGeneratorMode(column) === 'random'" class="field field--half">
                        <label>
                          <span>字符集</span>
                          <select
                            :value="readGeneratorCharsetPreset(column)"
                            name="charsetPreset"
                            @change="updateGeneratorCharsetPreset(column, $event)"
                          >
                            <option v-for="option in randomStringCharsetOptions" :key="option.value" :value="option.value">
                              {{ option.label }}
                            </option>
                            <option value="CUSTOM">自定义</option>
                          </select>
                        </label>
                        <label v-if="readGeneratorCharsetPreset(column) === 'CUSTOM'">
                          <span>自定义字符</span>
                          <input
                            :value="readGeneratorCharset(column)"
                            name="charsetCustom"
                            type="text"
                            placeholder="例如：abc123_-"
                            @input="updateGeneratorText(column, 'charset', $event)"
                          />
                        </label>
                      </div>
                    </template>

                    <div v-else-if="column.generatorType === 'ENUM'" class="field">
                      <label>
                        <span>可选值</span>
                        <textarea
                          :value="readGeneratorValues(column)"
                          data-column-field="enumValues"
                          rows="4"
                          placeholder="每行一个值，或使用逗号分隔"
                          @input="updateGeneratorValues(column, $event)"
                        ></textarea>
                      </label>
                    </div>

                    <div v-else-if="column.generatorType === 'BOOLEAN'" class="field field--half">
                      <label>
                        <span>true 概率</span>
                        <input :value="readGeneratorNumber(column, 'trueRate', 0.5)" type="number" min="0" max="1" step="0.01" @input="updateGeneratorNumber(column, 'trueRate', $event)" />
                      </label>
                    </div>

                    <div v-else-if="column.generatorType === 'DATETIME'" class="field field--half">
                      <label>
                        <span>开始时间</span>
                        <input :value="readGeneratorDateTime(column, 'from', defaultGeneratorFrom())" data-column-field="datetime-from" type="datetime-local" @input="updateGeneratorText(column, 'from', $event)" />
                      </label>
                      <label>
                        <span>结束时间</span>
                        <input :value="readGeneratorDateTime(column, 'to', defaultGeneratorTo())" data-column-field="datetime-to" type="datetime-local" @input="updateGeneratorText(column, 'to', $event)" />
                      </label>
                    </div>

                  </section>
                </article>
              </div>
            </div>
          </section>

          <div class="button-row">
            <button class="button" type="submit" :disabled="isSaving || isPreviewing || hasTaskActionInFlight">
              {{ isSaving ? (selectedTaskId ? '保存中...' : '创建中...') : (selectedTaskId ? '保存任务' : '创建任务') }}
            </button>
            <button class="button button--ghost" type="button" :disabled="isSaving || isPreviewing || hasTaskActionInFlight" @click="previewTask">
              {{ isPreviewing ? '预览中...' : '预览数据' }}
            </button>
            <button
              v-if="selectedTaskId && (!selectedTask || selectedTask.scheduleType !== 'INTERVAL' || selectedTask.status !== 'RUNNING')"
              class="button button--ghost"
              type="button"
              :disabled="isSaving || isPreviewing || hasTaskActionInFlight"
              @click="runTask(selectedTaskId)"
            >
              {{ executingTaskId === selectedTaskId ? '执行中...' : (form.scheduleType === 'INTERVAL' ? '立即执行一轮' : '执行写入') }}
            </button>
            <button
              v-if="selectedTask && canStartContinuous(selectedTask)"
              class="button button--ghost"
              type="button"
              :disabled="isSaving || isPreviewing || hasTaskActionInFlight"
              @click="startContinuousTask(selectedTask.id)"
            >
              {{ mutatingTaskId === selectedTask.id ? '处理中...' : '启动持续写入' }}
            </button>
            <button
              v-if="selectedTask && canPauseTask(selectedTask)"
              class="button button--ghost"
              type="button"
              :disabled="isSaving || isPreviewing || hasTaskActionInFlight"
              @click="pauseTaskSchedule(selectedTask.id)"
            >
              {{ mutatingTaskId === selectedTask.id ? '处理中...' : '暂停调度' }}
            </button>
            <button
              v-if="selectedTask && canResumeTask(selectedTask)"
              class="button button--ghost"
              type="button"
              :disabled="isSaving || isPreviewing || hasTaskActionInFlight"
              @click="resumeTaskSchedule(selectedTask.id)"
            >
              {{ mutatingTaskId === selectedTask.id ? '处理中...' : '恢复调度' }}
            </button>
            <button
              v-if="selectedTask && canStopContinuous(selectedTask)"
              class="button button--ghost"
              type="button"
              :disabled="isSaving || isPreviewing || hasTaskActionInFlight"
              @click="stopContinuousTask(selectedTask.id)"
            >
              {{ mutatingTaskId === selectedTask.id ? '处理中...' : '停止持续写入' }}
            </button>
            <button class="button button--ghost" type="button" :disabled="isSaving || isPreviewing || hasTaskActionInFlight" @click="resetForm">重置</button>
          </div>
        </form>
      </article>
    </section>

    <section v-if="isTaskEditorPage && previewRows.length" ref="previewPanelRef" class="panel">
      <h3>预览数据</h3>
      <pre class="code-block">{{ formatJson(previewRows) }}</pre>
    </section>
  </section>
</template>
<script setup lang="ts">
import { computed, nextTick, onMounted, reactive, ref, watch } from "vue";
import { useRoute, useRouter } from "vue-router";
import KafkaHeadersEditor from "../components/KafkaHeadersEditor.vue";
import KafkaPayloadSchemaEditor from "../components/KafkaPayloadSchemaEditor.vue";
import KafkaSchemaImportPanel from "../components/KafkaSchemaImportPanel.vue";
import PaginationBar from "../components/PaginationBar.vue";
import { apiClient, readApiError, type ApiResponse } from "../api/client";
import { clampPage, paginateItems } from "../utils/pagination";
import {
  getColumnTypeOptions,
  getPreferredColumnType,
  supportsColumnTypeProperty,
  withColumnTypeDefaults
} from "../utils/columnTypes";
import {
  formatDisplayDate,
  labelColumnGeneratorType,
  labelDatabaseType,
  labelExecutionStatus,
  labelSchedulerState,
  labelTableMode,
  labelWriteMode,
  labelWriteTaskScheduleType,
  labelWriteTaskStatus
} from "../utils/display";
import type { KafkaHeaderEntryDraft, KafkaSchemaImportResult } from "../utils/kafka";

type FeedbackKind = "success" | "error";
type DatabaseType = "MYSQL" | "POSTGRESQL" | "SQLSERVER" | "ORACLE" | "KAFKA";
type WriteTaskStatus = "READY" | "DRAFT" | "PAUSED" | "DISABLED" | "RUNNING";
type TableMode = "USE_EXISTING" | "CREATE_IF_MISSING";
type WriteMode = "APPEND" | "OVERWRITE";
type WriteTaskScheduleType = "MANUAL" | "ONCE" | "CRON" | "INTERVAL";
type GeneratorType = "SEQUENCE" | "RANDOM_INT" | "RANDOM_DECIMAL" | "STRING" | "ENUM" | "BOOLEAN" | "DATETIME" | "UUID";
type TaskSourceMode = "EXISTING" | "MANUAL";
type KafkaMessageMode = "SIMPLE" | "COMPLEX";
type PayloadNodeType = "OBJECT" | "ARRAY" | "SCALAR";
type PayloadValueType = "STRING" | "INT" | "LONG" | "DECIMAL" | "BOOLEAN" | "DATETIME" | "UUID";
type CronBuilderMode = "EVERY_MINUTES" | "EVERY_HOURS" | "DAILY" | "WEEKLY";
type CronWeekDay = "MON" | "TUE" | "WED" | "THU" | "FRI" | "SAT" | "SUN";
type RandomStringCharsetPreset = "LOWERCASE" | "LOWERCASE_DIGITS" | "LETTERS_DIGITS" | "DIGITS" | "CUSTOM";

interface ConnectionOption {
  id: number;
  name: string;
  dbType: DatabaseType;
  configJson?: string | null;
}

interface DatabaseTable {
  schemaName: string | null;
  tableName: string;
}

interface DatabaseColumn {
  columnName: string;
  dbType: string;
  length: number | null;
  precision: number | null;
  scale: number | null;
  nullable: boolean;
  primaryKey: boolean;
  autoIncrement: boolean;
}

interface TaskColumn {
  id?: number;
  localId: number;
  columnName: string;
  dbType: string;
  lengthValue: number | null;
  precisionValue: number | null;
  scaleValue: number | null;
  nullableFlag: boolean;
  primaryKeyFlag: boolean;
  generatorType: GeneratorType;
  generatorConfigJson: string;
  stringCharsetPreset?: RandomStringCharsetPreset | null;
  sortOrder: number;
}

interface PayloadSchemaNodeDraft {
  id: string;
  name: string;
  type: PayloadNodeType;
  nullable: boolean;
  valueType: PayloadValueType;
  generatorType: GeneratorType;
  generatorConfigJson: string;
  children: PayloadSchemaNodeDraft[];
  itemSchema: PayloadSchemaNodeDraft | null;
  minItems: number | null;
  maxItems: number | null;
}

interface WriteTask {
  id: number;
  createdAt: string;
  updatedAt: string;
  name: string;
  connectionId: number;
  tableName: string;
  tableMode: TableMode;
  writeMode: WriteMode;
  rowCount: number;
  batchSize: number;
  seed: number | null;
  status: WriteTaskStatus;
  scheduleType: WriteTaskScheduleType;
  cronExpression: string | null;
  triggerAt: string | null;
  intervalSeconds: number | null;
  maxRuns: number | null;
  maxRowsTotal: number | null;
  description: string | null;
  targetConfigJson: string | null;
  payloadSchemaJson: string | null;
  lastTriggeredAt: string | null;
  schedulerState: string | null;
  nextFireAt: string | null;
  previousFireAt: string | null;
  columns: Array<{
    id?: number;
    columnName: string;
    dbType: string;
    lengthValue: number | null;
    precisionValue: number | null;
    scaleValue: number | null;
    nullableFlag: boolean;
    primaryKeyFlag: boolean;
    generatorType: GeneratorType;
    generatorConfig: Record<string, unknown>;
    sortOrder: number;
  }>;
}

interface PreviewResponse {
  count: number;
  seed: number;
  rows: Array<Record<string, unknown>>;
}

interface RunExecutionResponse {
  id: number;
  writeTaskId: number;
  status: string;
  successCount: number;
  errorCount: number;
  errorSummary: string | null;
  deliveryDetailsJson: string | null;
}

interface RunSummary {
  executionId: number;
  taskId: number;
  taskName: string;
  statusText: string;
  deliveryType: string;
  writtenRowCount: number;
  beforeWriteRowCount: number;
  afterWriteRowCount: number;
  rowDelta: number;
  hasBeforeAfterMetrics: boolean;
  hasValidation: boolean;
  validationPassed: boolean;
  nullValueCount: number;
  blankStringCount: number;
  topic: string | null;
  payloadFormat: string | null;
  keyMode: string | null;
  keyField: string | null;
  keyPath: string | null;
  fixedKey: string | null;
  partition: number | null;
  headerCount: number;
  errorSummary: string | null;
}

interface ValidationIssue {
  message: string;
  selector: string;
  focus?: boolean;
}

type TaskValidationError = Error & ValidationIssue;

const writeModes: WriteMode[] = ["APPEND", "OVERWRITE"];
const generatorTypes: GeneratorType[] = ["SEQUENCE", "RANDOM_INT", "RANDOM_DECIMAL", "STRING", "ENUM", "BOOLEAN", "DATETIME", "UUID"];
const cronBuilderModeOptions: Array<{ value: CronBuilderMode; label: string }> = [
  { value: "EVERY_MINUTES", label: "每隔几分钟" },
  { value: "EVERY_HOURS", label: "每隔几小时" },
  { value: "DAILY", label: "每天固定时间" },
  { value: "WEEKLY", label: "每周固定时间" }
];
const cronMinuteStepOptions = [5, 10, 15, 30];
const cronHourStepOptions = [1, 2, 4, 6, 12];
const cronWeekDayOptions: Array<{ value: CronWeekDay; label: string }> = [
  { value: "MON", label: "周一" },
  { value: "TUE", label: "周二" },
  { value: "WED", label: "周三" },
  { value: "THU", label: "周四" },
  { value: "FRI", label: "周五" },
  { value: "SAT", label: "周六" },
  { value: "SUN", label: "周日" }
];
const cronHourOptions = Array.from({ length: 24 }, (_, value) => value);
const cronMinuteOptions = Array.from({ length: 60 }, (_, value) => value);
const randomStringCharsetOptions: Array<{ value: Exclude<RandomStringCharsetPreset, "CUSTOM">; label: string; charset: string }> = [
  { value: "LOWERCASE_DIGITS", label: "小写字母 + 数字", charset: "abcdefghijklmnopqrstuvwxyz0123456789" },
  { value: "LOWERCASE", label: "仅小写字母", charset: "abcdefghijklmnopqrstuvwxyz" },
  { value: "LETTERS_DIGITS", label: "大小写字母 + 数字", charset: "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789" },
  { value: "DIGITS", label: "仅数字", charset: "0123456789" }
];

const tasks = ref<WriteTask[]>([]);
const taskPage = ref(1);
const taskPageSize = ref(8);
const connections = ref<ConnectionOption[]>([]);
const tables = ref<DatabaseTable[]>([]);
const previewRows = ref<Array<Record<string, unknown>>>([]);
const selectedTaskId = ref<number | null>(null);
const lastImportedTable = ref("");
const lastRunSummary = ref<RunSummary | null>(null);
const sourceMode = ref<TaskSourceMode>("EXISTING");
const builderPanelRef = ref<HTMLElement | null>(null);
const previewPanelRef = ref<HTMLElement | null>(null);
const runSummaryPanelRef = ref<HTMLElement | null>(null);
const isSaving = ref(false);
const isPreviewing = ref(false);
const executingTaskId = ref<number | null>(null);
const mutatingTaskId = ref<number | null>(null);
const feedback = reactive<{ kind: FeedbackKind; message: string }>({
  kind: "success",
  message: ""
});
const validationIssue = ref<ValidationIssue | null>(null);

let nextLocalId = 1;
const route = useRoute();
const router = useRouter();
let highlightedValidationBlock: HTMLElement | null = null;
let highlightedValidationField: HTMLElement | null = null;

const form = reactive({
  name: "",
  connectionId: null as number | null,
  tableName: "",
  tableMode: "USE_EXISTING" as TableMode,
  writeMode: "APPEND" as WriteMode,
  rowCount: 100,
  batchSize: 500,
  scheduleType: "MANUAL" as WriteTaskScheduleType,
  cronExpression: "",
  cronBuilderMode: "DAILY" as CronBuilderMode,
  cronMinuteStep: 5,
  cronHourStep: 1,
  cronAtHour: 9,
  cronAtMinute: 0,
  cronWeekDay: "MON" as CronWeekDay,
  triggerAt: "",
  intervalSeconds: 10,
  maxRuns: null as number | null,
  maxRowsTotal: null as number | null,
  seed: null as number | null,
  status: "READY" as WriteTaskStatus,
  description: "",
  kafkaMessageMode: "SIMPLE" as KafkaMessageMode,
  keyMode: "NONE",
  keyField: "",
  keyPath: "",
  fixedKey: "",
  partition: null as number | null,
  headerEntries: [] as KafkaHeaderEntryDraft[],
  payloadSchemaRoot: null as PayloadSchemaNodeDraft | null,
  columns: [] as TaskColumn[]
});
const cronExpressionMode = ref<"BUILDER" | "LEGACY">("BUILDER");

const hasTaskActionInFlight = computed(() => executingTaskId.value !== null || mutatingTaskId.value !== null);
const isTaskListPage = computed(() => route.name === "write-tasks");
const isTaskCreatePage = computed(() => route.name === "write-task-create");
const routedTaskId = computed(() => {
  const raw = Number(route.params.id);
  return Number.isInteger(raw) && raw > 0 ? raw : null;
});
const routedPrefillConnectionId = computed(() => {
  const raw = Number(route.query.connectionId);
  return Number.isInteger(raw) && raw > 0 ? raw : null;
});
const routedPrefillTableName = computed(() =>
  typeof route.query.tableName === "string" ? route.query.tableName.trim() : ""
);
const isTaskEditPage = computed(() => route.name === "write-task-edit" || routedTaskId.value !== null);
const isTaskEditorPage = computed(() => isTaskCreatePage.value || isTaskEditPage.value);
const readyTaskCount = computed(() => tasks.value.filter((task) => task.status === "READY").length);
const runningTaskCount = computed(() => tasks.value.filter((task) => task.status === "RUNNING").length);
const continuousTaskCount = computed(() => tasks.value.filter((task) => task.scheduleType === "INTERVAL").length);
const paginatedTasks = computed(() =>
  paginateItems(tasks.value, taskPage.value, taskPageSize.value)
);
const selectedTask = computed(() =>
  selectedTaskId.value === null
    ? null
    : tasks.value.find((item) => item.id === selectedTaskId.value) ?? null
);
const selectedConnection = computed(() =>
  form.connectionId === null
    ? null
    : connections.value.find((item) => item.id === form.connectionId) ?? null
);
const isKafkaTarget = computed(() => selectedConnection.value?.dbType === "KAFKA");
const availableWriteModes = computed<WriteMode[]>(() => (isKafkaTarget.value ? ["APPEND"] : writeModes));
const manualColumnTypeOptions = computed(() => getColumnTypeOptions(selectedConnection.value?.dbType));
const targetEntityName = computed(() => (isKafkaTarget.value ? "Topic" : "表"));
const manualTargetPlaceholder = computed(() => (isKafkaTarget.value ? "例如：synthetic.user.activity" : "例如：synthetic_orders"));
const availableStatuses = computed<WriteTaskStatus[]>(() => {
  switch (form.scheduleType) {
    case "MANUAL":
      return ["READY", "DRAFT", "DISABLED"];
    case "ONCE":
    case "CRON":
      return ["READY", "DRAFT", "PAUSED", "DISABLED"];
    case "INTERVAL":
      return ["READY", "DRAFT", "PAUSED", "DISABLED", "RUNNING"];
    default:
      return ["READY", "DRAFT", "DISABLED"];
  }
});

const isKafkaComplexMode = computed(() => isKafkaTarget.value && form.kafkaMessageMode === "COMPLEX");
const availableKafkaKeyPaths = computed(() => collectScalarPaths(form.payloadSchemaRoot));
const availableKafkaFieldPaths = computed(() => {
  if (isKafkaComplexMode.value) {
    return availableKafkaKeyPaths.value;
  }
  return Array.from(new Set(
    form.columns
      .map((column) => column.columnName.trim())
      .filter(Boolean)
  ));
});
const kafkaFieldCount = computed(() => {
  if (!isKafkaComplexMode.value) {
    return form.columns.length;
  }
  return availableKafkaKeyPaths.value.length;
});

function clampInteger(value: number | null | undefined, min: number, max: number, fallback: number) {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return fallback;
  }
  return Math.min(Math.max(Math.floor(value), min), max);
}

function resetCronBuilder() {
  form.cronBuilderMode = "DAILY";
  form.cronMinuteStep = 5;
  form.cronHourStep = 1;
  form.cronAtHour = 9;
  form.cronAtMinute = 0;
  form.cronWeekDay = "MON";
  cronExpressionMode.value = "BUILDER";
}

function readNormalizedCronBuilder() {
  return {
    mode: form.cronBuilderMode,
    minuteStep: cronMinuteStepOptions.includes(form.cronMinuteStep) ? form.cronMinuteStep : 5,
    hourStep: cronHourStepOptions.includes(form.cronHourStep) ? form.cronHourStep : 1,
    hour: clampInteger(form.cronAtHour, 0, 23, 9),
    minute: clampInteger(form.cronAtMinute, 0, 59, 0),
    weekDay: cronWeekDayOptions.some((option) => option.value === form.cronWeekDay) ? form.cronWeekDay : "MON"
  };
}

function normalizeCronBuilder() {
  const normalized = readNormalizedCronBuilder();
  form.cronBuilderMode = normalized.mode;
  form.cronMinuteStep = normalized.minuteStep;
  form.cronHourStep = normalized.hourStep;
  form.cronAtHour = normalized.hour;
  form.cronAtMinute = normalized.minute;
  form.cronWeekDay = normalized.weekDay;
  return normalized;
}

function buildCronExpressionFromBuilder() {
  const normalized = readNormalizedCronBuilder();
  switch (normalized.mode) {
    case "EVERY_MINUTES":
      return `0 0/${normalized.minuteStep} * * * ?`;
    case "EVERY_HOURS":
      return `0 ${normalized.minute} 0/${normalized.hourStep} * * ?`;
    case "DAILY":
      return `0 ${normalized.minute} ${normalized.hour} * * ?`;
    case "WEEKLY":
      return `0 ${normalized.minute} ${normalized.hour} ? * ${normalized.weekDay}`;
  }
}

function handleCronBuilderChange() {
  normalizeCronBuilder();
  cronExpressionMode.value = "BUILDER";
  form.cronExpression = buildCronExpressionFromBuilder();
}

function parseCronExpression(expression: string | null | undefined) {
  const normalized = typeof expression === "string" ? expression.trim().toUpperCase() : "";
  if (!normalized) {
    return null;
  }

  const everyMinutes = normalized.match(/^0\s+0\/(\d+)\s+\*\s+\*\s+\*\s+\?$/);
  if (everyMinutes) {
    const interval = Number(everyMinutes[1]);
    if (cronMinuteStepOptions.includes(interval)) {
      return {
        mode: "EVERY_MINUTES" as CronBuilderMode,
        minuteStep: interval,
        hourStep: 1,
        hour: 9,
        minute: 0,
        weekDay: "MON" as CronWeekDay
      };
    }
  }

  const everyHours = normalized.match(/^0\s+(\d{1,2})\s+0\/(\d+)\s+\*\s+\*\s+\?$/);
  if (everyHours) {
    const minute = Number(everyHours[1]);
    const interval = Number(everyHours[2]);
    if (minute >= 0 && minute <= 59 && cronHourStepOptions.includes(interval)) {
      return {
        mode: "EVERY_HOURS" as CronBuilderMode,
        minuteStep: 5,
        hourStep: interval,
        hour: 9,
        minute,
        weekDay: "MON" as CronWeekDay
      };
    }
  }

  const daily = normalized.match(/^0\s+(\d{1,2})\s+(\d{1,2})\s+\*\s+\*\s+\?$/);
  if (daily) {
    const minute = Number(daily[1]);
    const hour = Number(daily[2]);
    if (hour >= 0 && hour <= 23 && minute >= 0 && minute <= 59) {
      return {
        mode: "DAILY" as CronBuilderMode,
        minuteStep: 5,
        hourStep: 1,
        hour,
        minute,
        weekDay: "MON" as CronWeekDay
      };
    }
  }

  const weekly = normalized.match(/^0\s+(\d{1,2})\s+(\d{1,2})\s+\?\s+\*\s+(MON|TUE|WED|THU|FRI|SAT|SUN)$/);
  if (weekly) {
    const minute = Number(weekly[1]);
    const hour = Number(weekly[2]);
    const weekDay = weekly[3] as CronWeekDay;
    if (hour >= 0 && hour <= 23 && minute >= 0 && minute <= 59) {
      return {
        mode: "WEEKLY" as CronBuilderMode,
        minuteStep: 5,
        hourStep: 1,
        hour,
        minute,
        weekDay
      };
    }
  }

  return null;
}

function applyCronExpressionToBuilder(expression: string | null | undefined) {
  const parsed = parseCronExpression(expression);
  if (!expression || !expression.trim()) {
    resetCronBuilder();
    form.cronExpression = "";
    return;
  }

  if (!parsed) {
    resetCronBuilder();
    form.cronExpression = expression.trim();
    cronExpressionMode.value = "LEGACY";
    return;
  }

  form.cronBuilderMode = parsed.mode;
  form.cronMinuteStep = parsed.minuteStep;
  form.cronHourStep = parsed.hourStep;
  form.cronAtHour = parsed.hour;
  form.cronAtMinute = parsed.minute;
  form.cronWeekDay = parsed.weekDay;
  cronExpressionMode.value = "BUILDER";
  form.cronExpression = expression.trim();
}

const cronPreviewExpression = computed(() => (
  form.scheduleType !== "CRON"
    ? ""
    : (cronExpressionMode.value === "LEGACY" ? form.cronExpression.trim() : buildCronExpressionFromBuilder())
));

function createColumn(partial?: Partial<TaskColumn>): TaskColumn {
  const normalizedType = withColumnTypeDefaults(selectedConnection.value?.dbType, {
    dbType: partial?.dbType,
    lengthValue: partial?.lengthValue,
    precisionValue: partial?.precisionValue,
    scaleValue: partial?.scaleValue,
    generatorType: partial?.generatorType,
    primaryKeyFlag: partial?.primaryKeyFlag
  });
  const generatorType = partial?.generatorType ?? inferGeneratorType(
    normalizedType.dbType,
    partial?.columnName ?? "",
    partial?.primaryKeyFlag ?? false,
    false,
    normalizedType.precisionValue,
    normalizedType.scaleValue
  );
  return {
    localId: nextLocalId++,
    columnName: partial?.columnName ?? "",
    dbType: normalizedType.dbType,
    lengthValue: normalizedType.lengthValue,
    precisionValue: normalizedType.precisionValue,
    scaleValue: normalizedType.scaleValue,
    nullableFlag: partial?.nullableFlag ?? true,
    primaryKeyFlag: partial?.primaryKeyFlag ?? false,
    generatorType,
    generatorConfigJson: partial?.generatorConfigJson ?? JSON.stringify(defaultGeneratorConfig(generatorType, partial?.columnName ?? ""), null, 2),
    stringCharsetPreset: partial?.stringCharsetPreset ?? null,
    sortOrder: partial?.sortOrder ?? form.columns.length
  };
}

function createPayloadSchemaNode(type: PayloadNodeType, partial?: Partial<PayloadSchemaNodeDraft>): PayloadSchemaNodeDraft {
  const id = partial?.id ?? `schema-${nextLocalId++}`;
  if (type === "OBJECT") {
    return {
      id,
      name: partial?.name ?? "",
      type,
      nullable: partial?.nullable ?? false,
      valueType: partial?.valueType ?? "STRING",
      generatorType: partial?.generatorType ?? "STRING",
      generatorConfigJson: partial?.generatorConfigJson ?? "{}",
      children: partial?.children ?? [createPayloadSchemaNode("SCALAR")],
      itemSchema: null,
      minItems: partial?.minItems ?? null,
      maxItems: partial?.maxItems ?? null
    };
  }

  if (type === "ARRAY") {
    return {
      id,
      name: partial?.name ?? "",
      type,
      nullable: partial?.nullable ?? false,
      valueType: partial?.valueType ?? "STRING",
      generatorType: partial?.generatorType ?? "STRING",
      generatorConfigJson: partial?.generatorConfigJson ?? "{}",
      children: [],
      itemSchema: partial?.itemSchema ?? createPayloadSchemaNode("SCALAR"),
      minItems: partial?.minItems ?? 1,
      maxItems: partial?.maxItems ?? 3
    };
  }

  const generatorType = partial?.generatorType ?? "STRING";
  return {
    id,
    name: partial?.name ?? "",
    type,
    nullable: partial?.nullable ?? false,
    valueType: partial?.valueType ?? "STRING",
    generatorType,
    generatorConfigJson: partial?.generatorConfigJson ?? JSON.stringify(defaultGeneratorConfig(generatorType), null, 2),
    children: [],
    itemSchema: null,
    minItems: null,
    maxItems: null
  };
}

function createDefaultPayloadSchemaRoot() {
  return createPayloadSchemaNode("OBJECT", {
    children: [createPayloadSchemaNode("SCALAR", { name: "id", valueType: "LONG", generatorType: "SEQUENCE", generatorConfigJson: JSON.stringify({ start: 1, step: 1 }, null, 2) })]
  });
}

function createColumnFromDatabase(column: DatabaseColumn, index: number): TaskColumn {
  const generatorType = inferGeneratorType(
    column.dbType,
    column.columnName,
    column.primaryKey,
    column.autoIncrement,
    column.precision,
    column.scale
  );
  return createColumn({
    columnName: column.columnName,
    dbType: column.dbType,
    lengthValue: column.length,
    precisionValue: column.precision,
    scaleValue: column.scale,
    nullableFlag: column.nullable,
    primaryKeyFlag: column.primaryKey,
    generatorType,
    generatorConfigJson: JSON.stringify(defaultGeneratorConfig(generatorType, column.columnName), null, 2),
    sortOrder: index
  });
}

function setFeedback(kind: FeedbackKind, message: string) {
  feedback.kind = kind;
  feedback.message = message;
}

function clearFeedback() {
  feedback.message = "";
}

function createValidationError(message: string, selector: string, options?: { focus?: boolean }): TaskValidationError {
  const error = new Error(message) as TaskValidationError;
  error.name = "TaskValidationError";
  error.message = message;
  error.selector = selector;
  error.focus = options?.focus ?? true;
  return error;
}

function isValidationError(error: unknown): error is TaskValidationError {
  return error instanceof Error
    && error.name === "TaskValidationError"
    && "selector" in error
    && typeof (error as TaskValidationError).selector === "string";
}

function clearValidationHighlight() {
  highlightedValidationBlock?.classList.remove("validation-block--error");
  highlightedValidationField?.classList.remove("field-control--error");
  highlightedValidationBlock = null;
  highlightedValidationField = null;
}

function resetValidationIssue() {
  validationIssue.value = null;
  clearValidationHighlight();
}

function resolveFocusableValidationTarget(element: HTMLElement | null) {
  if (!element) {
    return null;
  }
  if (element.matches("input, select, textarea, button")) {
    return element;
  }
  return element.querySelector<HTMLElement>("input, select, textarea, button");
}

async function revealValidationIssue(issue = validationIssue.value) {
  if (!issue) {
    return;
  }

  await nextTick();
  clearValidationHighlight();

  const target = document.querySelector<HTMLElement>(issue.selector);
  if (!target) {
    scrollBuilderIntoView();
    return;
  }

  const block = target.closest<HTMLElement>(".payload-node, .column-card, .field, .panel--embedded, .builder-section") ?? target;
  const focusTarget = resolveFocusableValidationTarget(target);

  highlightedValidationBlock = block;
  highlightedValidationBlock.classList.add("validation-block--error");

  if (focusTarget) {
    highlightedValidationField = focusTarget;
    highlightedValidationField.classList.add("field-control--error");
  }

  scrollElementIntoViewport(block, { force: true });
  if (issue.focus !== false) {
    focusTarget?.focus();
  }
}

function applyValidationError(error: unknown, fallbackMessage: string) {
  if (isValidationError(error)) {
    validationIssue.value = {
      message: error.message,
      selector: error.selector,
      focus: error.focus
    };
    setFeedback("error", error.message);
    void revealValidationIssue(validationIssue.value);
    return;
  }

  validationIssue.value = null;
  clearValidationHighlight();
  setFeedback("error", readApiError(error, fallbackMessage));
}

function stickyViewportOffset() {
  const topbar = document.querySelector<HTMLElement>(".shell__topbar");
  return Math.ceil((topbar?.getBoundingClientRect().height ?? 0) + 18);
}

function isElementComfortablyVisible(element: HTMLElement, topOffset: number) {
  const rect = element.getBoundingClientRect();
  const viewportHeight = window.innerHeight || document.documentElement.clientHeight || 0;
  if (viewportHeight <= 0) {
    return false;
  }
  return rect.top >= topOffset && rect.bottom <= viewportHeight - 24;
}

function scrollElementIntoViewport(element: HTMLElement | null, options?: { force?: boolean; focusSelector?: string }) {
  if (!element) {
    return;
  }

  const topOffset = stickyViewportOffset();
  if (options?.force || !isElementComfortablyVisible(element, topOffset)) {
    const nextTop = window.scrollY + element.getBoundingClientRect().top - topOffset;
    window.scrollTo({
      top: Math.max(nextTop, 0),
      behavior: "smooth"
    });
  }

  if (options?.focusSelector) {
    element.querySelector<HTMLInputElement>(options.focusSelector)?.focus();
  }
}

function scrollBuilderIntoView() {
  scrollElementIntoViewport(builderPanelRef.value, {
    force: true,
    focusSelector: "input[name='taskName']"
  });
}

function scrollPreviewIntoView() {
  scrollElementIntoViewport(previewPanelRef.value);
}

function scrollRunSummaryIntoView() {
  scrollElementIntoViewport(runSummaryPanelRef.value);
}

function labelKafkaKeyMode(mode: string) {
  switch (mode) {
    case "FIELD":
      return "使用字段";
    case "FIXED":
      return "固定值";
    default:
      return "不设置";
  }
}

function columnFieldSelector(column: TaskColumn, field: string) {
  return `[data-column-id="${column.localId}"] [data-column-field="${field}"]`;
}

function headerFieldSelector(index: number, field: string) {
  return `[data-header-index="${index}"] [data-header-field="${field}"]`;
}

function payloadNodeSelector(node: PayloadSchemaNodeDraft) {
  return `[data-node-id="${node.id}"]`;
}

function payloadNodeFieldSelector(node: PayloadSchemaNodeDraft, field: string) {
  return `${payloadNodeSelector(node)} [data-node-field="${field}"]`;
}

function payloadNodeActionSelector(node: PayloadSchemaNodeDraft, action: string) {
  return `${payloadNodeSelector(node)} [data-node-action="${action}"]`;
}

function payloadNodeSectionSelector(node: PayloadSchemaNodeDraft, section: string) {
  return `${payloadNodeSelector(node)} [data-node-section="${section}"]`;
}

function formatDateTimeLocal(value: Date) {
  const year = value.getFullYear();
  const month = `${value.getMonth() + 1}`.padStart(2, "0");
  const day = `${value.getDate()}`.padStart(2, "0");
  const hours = `${value.getHours()}`.padStart(2, "0");
  const minutes = `${value.getMinutes()}`.padStart(2, "0");
  return `${year}-${month}-${day}T${hours}:${minutes}`;
}

function defaultGeneratorFrom() {
  const value = new Date();
  value.setDate(value.getDate() - 30);
  return formatDateTimeLocal(value);
}

function defaultGeneratorTo() {
  return formatDateTimeLocal(new Date());
}

function readGeneratorConfig(column: TaskColumn) {
  try {
    return column.generatorConfigJson ? JSON.parse(column.generatorConfigJson) : {};
  } catch {
    return {};
  }
}

function writeGeneratorConfig(column: TaskColumn, config: Record<string, unknown>) {
  column.generatorConfigJson = JSON.stringify(config, null, 2);
}

function defaultRandomCharset() {
  return randomStringCharsetOptions[0].charset;
}

function readGeneratorText(column: TaskColumn, key: string, fallback = "") {
  const value = readGeneratorConfig(column)[key];
  return typeof value === "string" && value.trim() ? value : fallback;
}

function readGeneratorNumber(column: TaskColumn, key: string, fallback: number) {
  const value = readGeneratorConfig(column)[key];
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string" && value.trim()) {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }
  return fallback;
}

function readGeneratorValues(column: TaskColumn) {
  const values = readGeneratorConfig(column).values;
  if (!Array.isArray(values)) {
    return "";
  }
  return values.map((value) => String(value)).join("\n");
}

function readGeneratorMode(column: TaskColumn) {
  return readGeneratorText(column, "mode", column.columnName.toLowerCase().includes("email") ? "email" : "random");
}

function readGeneratorCharset(column: TaskColumn) {
  return readGeneratorText(column, "charset", defaultRandomCharset());
}

function readGeneratorCharsetPreset(column: TaskColumn): RandomStringCharsetPreset {
  if (column.stringCharsetPreset === "CUSTOM") {
    return "CUSTOM";
  }
  const matchedOption = randomStringCharsetOptions.find((option) => option.charset === readGeneratorCharset(column));
  if (matchedOption) {
    return matchedOption.value;
  }
  return column.stringCharsetPreset ?? "CUSTOM";
}

function readGeneratorDateTime(column: TaskColumn, key: string, fallback: string) {
  const value = readGeneratorText(column, key, fallback);
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return fallback;
  }
  return formatDateTimeLocal(parsed);
}

function updateGeneratorConfig(column: TaskColumn, patch: Record<string, unknown>) {
  writeGeneratorConfig(column, { ...readGeneratorConfig(column), ...patch });
}

function updateGeneratorText(column: TaskColumn, key: string, event: Event) {
  if (key === "charset") {
    column.stringCharsetPreset = "CUSTOM";
  }
  updateGeneratorConfig(column, { [key]: (event.target as HTMLInputElement | HTMLTextAreaElement).value });
}

function updateGeneratorNumber(column: TaskColumn, key: string, event: Event) {
  const rawValue = (event.target as HTMLInputElement).value;
  updateGeneratorConfig(column, { [key]: rawValue === "" ? null : Number(rawValue) });
}

function updateGeneratorValues(column: TaskColumn, event: Event) {
  const value = (event.target as HTMLTextAreaElement).value;
  updateGeneratorConfig(column, {
    values: value
      .split(/[\n,，]+/)
      .map((item) => item.trim())
      .filter(Boolean)
  });
}

function updateGeneratorMode(column: TaskColumn, event: Event) {
  const mode = (event.target as HTMLSelectElement).value;
  if (mode === "email") {
    writeGeneratorConfig(column, {
      mode: "email",
      prefix: readGeneratorText(column, "prefix"),
      suffix: readGeneratorText(column, "suffix"),
      domain: readGeneratorText(column, "domain", "demo.local")
    });
    return;
  }

  writeGeneratorConfig(column, {
    mode: "random",
    prefix: readGeneratorText(column, "prefix"),
    suffix: readGeneratorText(column, "suffix"),
    length: readGeneratorNumber(column, "length", 12),
    charset: readGeneratorCharset(column)
  });
}

function updateGeneratorCharsetPreset(column: TaskColumn, event: Event) {
  const preset = (event.target as HTMLSelectElement).value as RandomStringCharsetPreset;
  const matchedOption = randomStringCharsetOptions.find((option) => option.value === preset);
  if (matchedOption) {
    column.stringCharsetPreset = matchedOption.value;
    updateGeneratorConfig(column, { charset: matchedOption.charset });
    return;
  }
  column.stringCharsetPreset = "CUSTOM";
}

function defaultGeneratorConfig(generatorType: GeneratorType, columnName = "") {
  switch (generatorType) {
    case "SEQUENCE":
      return { start: 1, step: 1 };
    case "RANDOM_INT":
      return { min: 0, max: 1000 };
    case "RANDOM_DECIMAL":
      return { min: 0, max: 1000, scale: 2 };
    case "STRING":
      if (columnName.toLowerCase().includes("email")) {
        return { mode: "email", domain: "demo.local", prefix: "", suffix: "" };
      }
      return { mode: "random", length: 12, charset: defaultRandomCharset(), prefix: "", suffix: "" };
    case "ENUM":
      return { values: ["值1", "值2"] };
    case "BOOLEAN":
      return { trueRate: 0.5 };
    case "DATETIME":
      return { from: defaultGeneratorFrom(), to: defaultGeneratorTo() };
    case "UUID":
      return {};
  }
}

function resetGeneratorConfig(column: TaskColumn) {
  column.stringCharsetPreset = null;
  writeGeneratorConfig(column, defaultGeneratorConfig(column.generatorType, column.columnName));
}

function supportsLength(column: TaskColumn) {
  return supportsColumnTypeProperty(selectedConnection.value?.dbType, column.dbType, "length");
}

function supportsPrecision(column: TaskColumn) {
  return supportsColumnTypeProperty(selectedConnection.value?.dbType, column.dbType, "precision");
}

function supportsScale(column: TaskColumn) {
  return supportsColumnTypeProperty(selectedConnection.value?.dbType, column.dbType, "scale");
}

function applyColumnTypeDefaults(column: TaskColumn) {
  const normalized = withColumnTypeDefaults(selectedConnection.value?.dbType, {
    dbType: column.dbType,
    lengthValue: column.lengthValue,
    precisionValue: column.precisionValue,
    scaleValue: column.scaleValue,
    generatorType: column.generatorType,
    primaryKeyFlag: column.primaryKeyFlag
  });
  column.dbType = normalized.dbType;
  column.lengthValue = normalized.lengthValue;
  column.precisionValue = normalized.precisionValue;
  column.scaleValue = normalized.scaleValue;
}

function normalizeTaskColumn(column: TaskColumn): TaskColumn {
  const normalized = withColumnTypeDefaults(selectedConnection.value?.dbType, {
    dbType: column.dbType,
    lengthValue: column.lengthValue,
    precisionValue: column.precisionValue,
    scaleValue: column.scaleValue,
    generatorType: column.generatorType,
    primaryKeyFlag: column.primaryKeyFlag
  });
  return {
    ...column,
    dbType: normalized.dbType,
    lengthValue: normalized.lengthValue,
    precisionValue: normalized.precisionValue,
    scaleValue: normalized.scaleValue
  };
}

function handleColumnTypeChange(column: TaskColumn) {
  applyColumnTypeDefaults(column);
  const generatorType = inferGeneratorType(
    column.dbType,
    column.columnName,
    column.primaryKeyFlag,
    false,
    column.precisionValue,
    column.scaleValue
  );
  if (column.generatorType !== generatorType) {
    column.generatorType = generatorType;
  }
  resetGeneratorConfig(column);
}

function handlePrimaryKeyFlagChange(column: TaskColumn) {
  if (column.primaryKeyFlag) {
    column.nullableFlag = false;
  }
  const generatorType = inferGeneratorType(
    column.dbType,
    column.columnName,
    column.primaryKeyFlag,
    false,
    column.precisionValue,
    column.scaleValue
  );
  if (column.generatorType !== generatorType) {
    column.generatorType = generatorType;
    resetGeneratorConfig(column);
  }
}

function createDefaultPrimaryKeyColumn() {
  return createColumn({
    columnName: "id",
    dbType: getPreferredColumnType(selectedConnection.value?.dbType, "ID"),
    nullableFlag: false,
    primaryKeyFlag: true,
    generatorType: "SEQUENCE"
  });
}

function nextManualColumnName() {
  const normalizedNames = new Set(
    form.columns
      .map((column) => column.columnName.trim().toLowerCase())
      .filter(Boolean)
  );
  if (!normalizedNames.has("name")) {
    return "name";
  }
  let index = 1;
  while (normalizedNames.has(`field_${index}`)) {
    index += 1;
  }
  return `field_${index}`;
}

function createDefaultTextColumn() {
  return createColumn({
    columnName: nextManualColumnName(),
    dbType: getPreferredColumnType(selectedConnection.value?.dbType, "TEXT"),
    nullableFlag: true,
    primaryKeyFlag: false,
    generatorType: "STRING"
  });
}

function initializeManualColumns() {
  form.columns = [createDefaultPrimaryKeyColumn()];
}

function normalizeManualColumnsForConnection() {
  form.columns.forEach((column) => {
    applyColumnTypeDefaults(column);
  });
}

function shouldUseQualifiedTableName(dbType: DatabaseType | null | undefined) {
  return dbType === "POSTGRESQL" || dbType === "SQLSERVER" || dbType === "ORACLE";
}

function formatTableLabel(table: DatabaseTable) {
  if (table.schemaName && table.schemaName.trim()) {
    return `${table.schemaName}.${table.tableName}`;
  }
  return table.tableName;
}

function tableOptionValue(table: DatabaseTable) {
  if (shouldUseQualifiedTableName(selectedConnection.value?.dbType) && table.schemaName && table.schemaName.trim()) {
    return `${table.schemaName}.${table.tableName}`;
  }
  return table.tableName;
}

function normalizeSelectedTableName() {
  if (sourceMode.value !== "EXISTING" || !form.tableName.trim()) {
    return;
  }
  const matchedTable = tables.value.find((table) =>
    tableOptionValue(table) === form.tableName.trim()
    || formatTableLabel(table) === form.tableName.trim()
    || table.tableName === form.tableName.trim()
  );
  if (matchedTable) {
    form.tableName = tableOptionValue(matchedTable);
  }
}

function isIntegerLikeColumn(primaryKey: boolean, autoIncrement: boolean, normalizedName: string) {
  return primaryKey || autoIncrement || normalizedName === "id";
}

function inferGeneratorType(
  dbType: string,
  columnName: string,
  primaryKey = false,
  autoIncrement = false,
  _precision: number | null = null,
  scale: number | null = null
): GeneratorType {
  const normalizedType = dbType.toUpperCase();
  const normalizedName = columnName.toLowerCase();

  if (normalizedType.includes("UUID") || normalizedType.includes("UNIQUEIDENTIFIER")) {
    return "UUID";
  }
  if (normalizedType.includes("BOOL") || normalizedType === "BIT") {
    return "BOOLEAN";
  }
  if (normalizedType.includes("DATE") || normalizedType.includes("TIME")) {
    return "DATETIME";
  }
  if (normalizedType.includes("NUMBER")) {
    return scale === null || scale === 0
      ? (isIntegerLikeColumn(primaryKey, autoIncrement, normalizedName) ? "SEQUENCE" : "RANDOM_INT")
      : "RANDOM_DECIMAL";
  }
  if (normalizedType.includes("DECIMAL") || normalizedType.includes("NUMERIC") || normalizedType.includes("FLOAT") || normalizedType.includes("DOUBLE")) {
    return "RANDOM_DECIMAL";
  }
  if (normalizedType.includes("INT") || normalizedType === "BIGINT" || normalizedType === "SMALLINT" || normalizedType === "TINYINT") {
    return isIntegerLikeColumn(primaryKey, autoIncrement, normalizedName) ? "SEQUENCE" : "RANDOM_INT";
  }
  return "STRING";
}

function resetForm() {
  selectedTaskId.value = null;
  resetValidationIssue();
  sourceMode.value = "EXISTING";
  form.name = "";
  form.connectionId = null;
  form.tableName = "";
  form.tableMode = "USE_EXISTING";
  form.writeMode = "APPEND";
  form.rowCount = 100;
  form.batchSize = 500;
  form.scheduleType = "MANUAL";
  form.cronExpression = "";
  resetCronBuilder();
  form.triggerAt = "";
  form.intervalSeconds = 10;
  form.maxRuns = null;
  form.maxRowsTotal = null;
  form.seed = null;
  form.status = "READY";
  form.description = "";
  form.kafkaMessageMode = "SIMPLE";
  form.keyMode = "NONE";
  form.keyField = "";
  form.keyPath = "";
  form.fixedKey = "";
  form.partition = null;
  form.headerEntries = [];
  form.payloadSchemaRoot = createDefaultPayloadSchemaRoot();
  form.columns = [];
  previewRows.value = [];
  tables.value = [];
  lastImportedTable.value = "";
  lastRunSummary.value = null;
  clearFeedback();
}

function addColumn() {
  if (sourceMode.value !== "MANUAL") {
    return;
  }
  form.columns.push(createDefaultTextColumn());
}

function resetTargetConfigFields() {
  form.kafkaMessageMode = "SIMPLE";
  form.keyMode = "NONE";
  form.keyField = "";
  form.keyPath = "";
  form.fixedKey = "";
  form.partition = null;
  form.headerEntries = [];
  form.payloadSchemaRoot = createDefaultPayloadSchemaRoot();
}

function applyTargetConfigToForm(value: string | null | undefined) {
  const config = parseJson(value);
  form.keyMode = typeof config.keyMode === "string" ? config.keyMode : "NONE";
  form.keyField = typeof config.keyField === "string" ? config.keyField : "";
  form.keyPath = typeof config.keyPath === "string"
    ? config.keyPath
    : (typeof config.keyField === "string" ? config.keyField : "");
  form.fixedKey = typeof config.fixedKey === "string" ? config.fixedKey : "";
  form.partition = typeof config.partition === "number" && Number.isFinite(config.partition) ? config.partition : null;
  if (Array.isArray(config.headerDefinitions)) {
    form.headerEntries = config.headerDefinitions
      .filter((entry): entry is Record<string, unknown> => Boolean(entry) && typeof entry === "object")
      .map((entry) => ({
        name: typeof entry.name === "string" ? entry.name : "",
        mode: entry.mode === "FIELD" ? "FIELD" : "FIXED",
        value: typeof entry.value === "string" ? entry.value : "",
        path: typeof entry.path === "string" ? entry.path : ""
      }));
    return;
  }
  form.headerEntries = config.headers && typeof config.headers === "object"
    ? Object.entries(config.headers as Record<string, unknown>).map(([name, headerValue]) => ({
      name,
      mode: "FIXED",
      value: String(headerValue),
      path: ""
    }))
    : [];
}

function removeColumn(index: number) {
  form.columns.splice(index, 1);
  form.columns.forEach((column, columnIndex) => {
    column.sortOrder = columnIndex;
  });
}

function startNewTask() {
  resetForm();
  clearFeedback();
  previewRows.value = [];
  lastRunSummary.value = null;
  void router.push({ name: "write-task-create" });
}

function goToTaskList() {
  clearFeedback();
  void router.push({ name: "write-tasks" });
}

function toLocalDateTimeInput(value: string | null | undefined) {
  if (!value) {
    return "";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "";
  }
  return formatDateTimeLocal(date);
}

function toInstantString(value: string, label: string, selector?: string) {
  if (!value.trim()) {
    throw selector ? createValidationError(`请输入${label}`, selector) : new Error(`请输入${label}`);
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    throw selector ? createValidationError(`${label}格式不正确`, selector) : new Error(`${label}格式不正确`);
  }
  return date.toISOString();
}

function formatOptionalDate(value: string | null | undefined) {
  return value ? formatDisplayDate(value) : "未设置";
}

function sanitizeOptionalNumber(value: number | null | undefined) {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function ensureStatusMatchesScheduleType() {
  if (!availableStatuses.value.includes(form.status)) {
    form.status = "READY";
  }
}

function handleScheduleTypeChange() {
  if (form.scheduleType !== "ONCE") {
    form.triggerAt = "";
  }
  if (form.scheduleType !== "CRON") {
    form.cronExpression = "";
    resetCronBuilder();
  } else {
    handleCronBuilderChange();
  }
  if (form.scheduleType !== "INTERVAL") {
    form.intervalSeconds = 10;
    form.maxRuns = null;
    form.maxRowsTotal = null;
  }
  if (form.scheduleType === "INTERVAL" && !sanitizeOptionalNumber(form.intervalSeconds)) {
    form.intervalSeconds = 10;
  }
  ensureStatusMatchesScheduleType();
}

function ensureWriteModeMatchesTarget() {
  if (isKafkaTarget.value) {
    form.writeMode = "APPEND";
  }
}

function ensureSourceModeMatchesTarget() {
  if (!isKafkaTarget.value) {
    return;
  }
  sourceMode.value = "MANUAL";
  form.tableMode = "CREATE_IF_MISSING";
}

function applyImportedKafkaSchema(result: KafkaSchemaImportResult) {
  form.kafkaMessageMode = "COMPLEX";
  form.payloadSchemaRoot = parsePayloadSchemaJson(result.payloadSchemaJson);
  form.keyField = "";
  form.keyPath = "";
  setFeedback("success", result.warnings.length ? `消息结构已导入，包含 ${result.warnings.length} 条提示` : "消息结构已导入");
}

function normalizeKafkaHeaderEntriesConfig() {
  const normalizedEntries = form.headerEntries
    .map((entry, index) => ({
      index,
      name: entry.name.trim(),
      mode: entry.mode,
      value: entry.value.trim(),
      path: entry.path.trim()
    }))
    .filter((entry) => entry.name || entry.value || entry.path);

  if (!normalizedEntries.length) {
    return {};
  }

  const headerNames = new Set<string>();
  const fixedHeaders: Record<string, string> = {};
  const headerDefinitions: Array<Record<string, string>> = [];
  const hasFieldHeader = normalizedEntries.some((entry) => entry.mode === "FIELD");

  for (const entry of normalizedEntries) {
    if (!entry.name) {
      throw createValidationError("Header 名称不能为空", headerFieldSelector(entry.index, "name"));
    }
    const normalizedName = entry.name.toLowerCase();
    if (headerNames.has(normalizedName)) {
      throw createValidationError("Header 名称不能重复", headerFieldSelector(entry.index, "name"));
    }
    headerNames.add(normalizedName);

    if (entry.mode === "FIELD") {
      if (!entry.path) {
        throw createValidationError("请选择 Header 字段路径", headerFieldSelector(entry.index, "path"));
      }
      if (!availableKafkaFieldPaths.value.includes(entry.path)) {
        throw createValidationError("Header 字段路径必须来自当前字段列表或消息结构", headerFieldSelector(entry.index, "path"));
      }
      headerDefinitions.push({
        name: entry.name,
        mode: "FIELD",
        path: entry.path
      });
      continue;
    }

    if (!entry.value) {
      throw createValidationError("Header 固定值不能为空", headerFieldSelector(entry.index, "value"));
    }

    fixedHeaders[entry.name] = entry.value;
    if (hasFieldHeader) {
      headerDefinitions.push({
        name: entry.name,
        mode: "FIXED",
        value: entry.value
      });
    }
  }

  if (hasFieldHeader) {
    return {
      headerDefinitions
    };
  }

  return {
    headers: fixedHeaders
  };
}

function buildTargetConfigJson() {
  if (!isKafkaTarget.value) {
    return null;
  }

  if (form.writeMode !== "APPEND") {
    throw createValidationError("Kafka 目标端仅支持追加写入", "select[name='writeMode']");
  }

  if (form.keyMode === "FIELD" && !resolveKafkaKeyPath().trim()) {
    throw createValidationError(
      isKafkaComplexMode.value ? "复杂消息模式下必须填写 Key 路径" : "选择字段 Key 时必须填写 Key 字段名",
      isKafkaComplexMode.value ? "select[name='keyPath']" : "select[name='keyField']"
    );
  }

  if (form.keyMode === "FIXED" && !form.fixedKey.trim()) {
    throw createValidationError("选择固定 Key 时必须填写固定 Key", "input[name='fixedKey']");
  }

  const payload: Record<string, unknown> = {
    payloadFormat: "JSON",
    keyMode: form.keyMode
  };

  if (form.keyMode === "FIELD") {
    const keyPath = resolveKafkaKeyPath().trim();
    if (keyPath && !availableKafkaFieldPaths.value.includes(keyPath)) {
      throw createValidationError(
        "Key 路径必须来自当前字段列表或消息结构",
        isKafkaComplexMode.value ? "select[name='keyPath']" : "select[name='keyField']"
      );
    }
    if (isKafkaComplexMode.value) {
      payload.keyPath = keyPath;
    } else {
      payload.keyField = keyPath;
      payload.keyPath = keyPath;
    }
  }

  if (form.keyMode === "FIXED") {
    payload.fixedKey = form.fixedKey.trim();
  }

  const partition = sanitizeOptionalNumber(form.partition);
  if (partition !== null) {
    payload.partition = partition;
  }

  const headers = normalizeKafkaHeaderEntriesConfig();
  if ("headers" in headers && headers.headers && Object.keys(headers.headers).length) {
    payload.headers = headers.headers;
  }
  if ("headerDefinitions" in headers && headers.headerDefinitions?.length) {
    payload.headerDefinitions = headers.headerDefinitions;
  }

  return JSON.stringify(payload);
}

function normalizeJson(value: string, label: string, selector?: string) {
  try {
    return JSON.stringify(JSON.parse(value), null, 2);
  } catch {
    throw selector ? createValidationError(`${label} 必须是合法的 JSON`, selector) : new Error(`${label} 必须是合法的 JSON`);
  }
}

function normalizeGeneratorConfig(column: TaskColumn, index: number) {
  const label = column.columnName || `字段 ${index + 1}`;
  const config = JSON.parse(normalizeJson(column.generatorConfigJson, `${label} 生成规则`));

  if (column.generatorType === "ENUM") {
    const values = Array.isArray(config.values) ? config.values.map((item: unknown) => String(item).trim()).filter(Boolean) : [];
    if (!values.length) {
      throw createValidationError(`${label} 至少需要一个可选值`, columnFieldSelector(column, "enumValues"));
    }
    return { values };
  }

  if (column.generatorType === "DATETIME") {
    const from = typeof config.from === "string" && config.from.trim() ? new Date(config.from) : null;
    const to = typeof config.to === "string" && config.to.trim() ? new Date(config.to) : null;
    if (!from || Number.isNaN(from.getTime())) {
      throw createValidationError(`${label} 开始时间格式不正确`, columnFieldSelector(column, "datetime-from"));
    }
    if (!to || Number.isNaN(to.getTime())) {
      throw createValidationError(`${label} 结束时间格式不正确`, columnFieldSelector(column, "datetime-to"));
    }
    return {
      from: from.toISOString(),
      to: to.toISOString()
    };
  }

  if (column.generatorType === "STRING" && config.mode === "email") {
    return {
      mode: "email",
      prefix: typeof config.prefix === "string" ? config.prefix : "",
      suffix: typeof config.suffix === "string" ? config.suffix : "",
      domain: typeof config.domain === "string" && config.domain.trim() ? config.domain : "demo.local"
    };
  }

  return config;
}

function normalizeSchemaGeneratorConfig(node: PayloadSchemaNodeDraft, label: string) {
  const configText = node.generatorConfigJson?.trim() ? node.generatorConfigJson : "{}";
  return JSON.parse(normalizeJson(configText, `${label} 生成规则`, payloadNodeFieldSelector(node, "generatorConfig")));
}

function buildPayloadSchemaJson() {
  if (!form.payloadSchemaRoot) {
    throw createValidationError("未定义 Kafka 消息结构", "[data-section='payload-schema']", { focus: false });
  }
  const schema = serializePayloadSchemaNode(form.payloadSchemaRoot, true);
  return JSON.stringify(schema);
}

function serializePayloadSchemaNode(
  node: PayloadSchemaNodeDraft,
  isRoot = false,
  isArrayItem = false
): Record<string, unknown> {
  if (!isRoot && !isArrayItem && !node.name.trim()) {
    throw createValidationError("复杂消息模式下，每个字段都必须填写名称", payloadNodeFieldSelector(node, "name"));
  }

  if (node.type === "OBJECT") {
    if (!node.children.length) {
      throw createValidationError(
        `${node.name || "消息结构"} 至少需要一个子字段`,
        payloadNodeActionSelector(node, "add-scalar")
      );
    }
    return {
      ...(isRoot || isArrayItem ? {} : { name: node.name.trim() }),
      type: "OBJECT",
      ...(isRoot || isArrayItem ? {} : { nullable: node.nullable }),
      children: node.children.map((child) => serializePayloadSchemaNode(child))
    };
  }

  if (node.type === "ARRAY") {
    if (!node.itemSchema) {
      throw createValidationError(
        `${node.name || "数组字段"} 必须定义元素结构`,
        payloadNodeActionSelector(node, "set-item-scalar")
      );
    }
    const minItems = sanitizeOptionalNumber(node.minItems);
    const maxItems = sanitizeOptionalNumber(node.maxItems);
    if (minItems !== null && minItems < 0) {
      throw createValidationError(`${node.name || "数组字段"} 的最少元素数不能小于 0`, payloadNodeFieldSelector(node, "minItems"));
    }
    if (minItems !== null && maxItems !== null && maxItems < minItems) {
      throw createValidationError(`${node.name || "数组字段"} 的最多元素数不能小于最少元素数`, payloadNodeFieldSelector(node, "maxItems"));
    }
    return {
      ...(isRoot || isArrayItem ? {} : { name: node.name.trim() }),
      type: "ARRAY",
      nullable: node.nullable,
      minItems,
      maxItems,
      itemSchema: serializePayloadSchemaNode(node.itemSchema, false, true)
    };
  }

  const fieldLabel = node.name.trim() || "标量字段";
  return {
    ...(isRoot || isArrayItem ? {} : { name: node.name.trim() }),
    type: "SCALAR",
    nullable: node.nullable,
    valueType: node.valueType,
    generatorType: node.generatorType,
    generatorConfig: normalizeSchemaGeneratorConfig(node, fieldLabel)
  };
}

function parsePayloadSchemaJson(value: string | null | undefined) {
  if (!value) {
    return createDefaultPayloadSchemaRoot();
  }
  try {
    const parsed = JSON.parse(value) as Record<string, unknown>;
    return parsePayloadSchemaNode(parsed, true);
  } catch {
    return createDefaultPayloadSchemaRoot();
  }
}

function parsePayloadSchemaNode(raw: Record<string, unknown>, isRoot = false): PayloadSchemaNodeDraft {
  const type = String(raw.type ?? "OBJECT").toUpperCase() as PayloadNodeType;
  if (type === "OBJECT") {
    const rawChildren = Array.isArray(raw.children) ? raw.children as Array<Record<string, unknown>> : [];
    return createPayloadSchemaNode("OBJECT", {
      name: isRoot ? "" : String(raw.name ?? ""),
      nullable: Boolean(raw.nullable ?? false),
      children: rawChildren.length
        ? rawChildren.map((child) => parsePayloadSchemaNode(child))
        : [createPayloadSchemaNode("SCALAR")]
    });
  }
  if (type === "ARRAY") {
    return createPayloadSchemaNode("ARRAY", {
      name: String(raw.name ?? ""),
      nullable: Boolean(raw.nullable ?? false),
      minItems: typeof raw.minItems === "number" ? raw.minItems : 1,
      maxItems: typeof raw.maxItems === "number" ? raw.maxItems : 3,
      itemSchema: raw.itemSchema && typeof raw.itemSchema === "object"
        ? parsePayloadSchemaNode(raw.itemSchema as Record<string, unknown>)
        : createPayloadSchemaNode("SCALAR")
    });
  }
  return createPayloadSchemaNode("SCALAR", {
    name: String(raw.name ?? ""),
    nullable: Boolean(raw.nullable ?? false),
    valueType: String(raw.valueType ?? "STRING").toUpperCase() as PayloadValueType,
    generatorType: String(raw.generatorType ?? "STRING").toUpperCase() as GeneratorType,
    generatorConfigJson: JSON.stringify(raw.generatorConfig ?? {}, null, 2)
  });
}

function collectScalarPaths(node: PayloadSchemaNodeDraft | null, parentPath = ""): string[] {
  if (!node) {
    return [];
  }
  if (node.type === "OBJECT") {
    return node.children.flatMap((child) => collectScalarPaths(child, joinPath(parentPath, child.name)));
  }
  if (node.type === "ARRAY") {
    return collectScalarPaths(node.itemSchema, `${parentPath}[]`);
  }
  return parentPath ? [parentPath] : [];
}

function joinPath(parentPath: string, name: string) {
  const segment = name.trim();
  if (!segment) {
    return "";
  }
  if (!parentPath) {
    return segment;
  }
  return `${parentPath}.${segment}`;
}

function resolveKafkaKeyPath() {
  return isKafkaComplexMode.value ? form.keyPath : form.keyField;
}

function buildPayload() {
  const tableMode = isKafkaTarget.value
    ? "CREATE_IF_MISSING"
    : (sourceMode.value === "EXISTING" ? "USE_EXISTING" : "CREATE_IF_MISSING");
  if (!form.name.trim()) {
    throw createValidationError("请输入任务名称", "input[name='taskName']");
  }
  if (!form.connectionId) {
    throw createValidationError("请选择目标连接", "select[name='connectionId']");
  }
  if (!form.tableName.trim()) {
    throw createValidationError(
      isKafkaTarget.value ? "请输入 Topic 名称" : (sourceMode.value === "EXISTING" ? "请选择已有表" : "请输入新表名"),
      isKafkaTarget.value || sourceMode.value === "MANUAL" ? "input[name='tableName']" : "select[name='tableName']"
    );
  }
  if (!isKafkaComplexMode.value && !form.columns.length) {
    throw createValidationError(
      isKafkaTarget.value ? "请至少定义一个消息字段" : (sourceMode.value === "EXISTING" ? "请选择表并导入字段" : "请至少添加一个字段"),
      isKafkaTarget.value || sourceMode.value === "MANUAL" ? "[data-action='add-column']" : "select[name='tableName']"
    );
  }
  if (isKafkaComplexMode.value && !form.payloadSchemaRoot) {
    throw createValidationError("未定义 Kafka 复杂消息结构", "[data-section='payload-schema']", { focus: false });
  }

  const rowCount = sanitizeOptionalNumber(form.rowCount);
  const batchSize = sanitizeOptionalNumber(form.batchSize);
  if (!rowCount || rowCount < 1) {
    throw createValidationError("每批生成条数必须大于 0", "input[name='rowCount']");
  }
  if (!batchSize || batchSize < 1) {
    throw createValidationError("单批提交大小必须大于 0", "input[name='batchSize']");
  }

  let cronExpression: string | null = null;
  let triggerAt: string | null = null;
  let intervalSeconds: number | null = null;
  let maxRuns: number | null = null;
  let maxRowsTotal: number | null = null;

  switch (form.scheduleType) {
    case "MANUAL":
      break;
    case "ONCE":
      triggerAt = toInstantString(form.triggerAt, "执行时间", "input[name='triggerAt']");
      break;
    case "CRON":
      cronExpression = cronExpressionMode.value === "LEGACY"
        ? form.cronExpression.trim()
        : buildCronExpressionFromBuilder();
      if (!cronExpression) {
        throw createValidationError("请选择周期定时计划", "select[name='cronBuilderMode']");
      }
      break;
    case "INTERVAL":
      intervalSeconds = sanitizeOptionalNumber(form.intervalSeconds);
      if (!intervalSeconds || intervalSeconds < 1) {
        throw createValidationError("持续写入必须设置大于 0 的间隔秒数", "input[name='intervalSeconds']");
      }
      maxRuns = sanitizeOptionalNumber(form.maxRuns);
      maxRowsTotal = sanitizeOptionalNumber(form.maxRowsTotal);
      break;
  }

  return {
    name: form.name.trim(),
    connectionId: form.connectionId,
    tableName: form.tableName.trim(),
    tableMode,
    writeMode: form.writeMode,
    rowCount,
    batchSize,
    seed: sanitizeOptionalNumber(form.seed),
    status: form.status,
    scheduleType: form.scheduleType,
    cronExpression,
    triggerAt,
    intervalSeconds,
    maxRuns,
    maxRowsTotal,
    description: form.description || null,
    targetConfigJson: buildTargetConfigJson(),
    payloadSchemaJson: isKafkaComplexMode.value ? buildPayloadSchemaJson() : null,
    columns: isKafkaComplexMode.value
      ? []
      : form.columns.map((column, index) => {
        const normalizedColumn = normalizeTaskColumn(column);
        const columnName = normalizedColumn.columnName.trim();
        const dbType = normalizedColumn.dbType.trim();
        if (!columnName) {
          throw createValidationError(`字段 ${index + 1} 必须填写字段名`, columnFieldSelector(column, "name"));
        }
        if (!dbType) {
          throw createValidationError(`${columnName || `字段 ${index + 1}`} 必须填写数据类型`, columnFieldSelector(column, "dbType"));
        }
        return {
          columnName,
          dbType,
          lengthValue: normalizedColumn.lengthValue,
          precisionValue: normalizedColumn.precisionValue,
          scaleValue: normalizedColumn.scaleValue,
          nullableFlag: normalizedColumn.nullableFlag,
          primaryKeyFlag: normalizedColumn.primaryKeyFlag,
          generatorType: normalizedColumn.generatorType,
          generatorConfig: normalizeGeneratorConfig(normalizedColumn, index),
          sortOrder: index
        };
      })
  };
}

function applyTaskToForm(task: WriteTask, options?: { clearOutputs?: boolean; silentTableLoad?: boolean }) {
  resetValidationIssue();
  const connection = connections.value.find((item) => item.id === task.connectionId) ?? null;
  const kafkaTask = connection?.dbType === "KAFKA";
  const kafkaComplexTask = kafkaTask && Boolean(task.payloadSchemaJson);
  selectedTaskId.value = task.id;
  sourceMode.value = kafkaTask ? "MANUAL" : (task.tableMode === "USE_EXISTING" ? "EXISTING" : "MANUAL");
  form.name = task.name ?? "";
  form.connectionId = task.connectionId;
  form.tableName = task.tableName ?? "";
  form.tableMode = kafkaTask ? "CREATE_IF_MISSING" : task.tableMode;
  form.writeMode = kafkaTask ? "APPEND" : (task.writeMode ?? "APPEND");
  form.rowCount = task.rowCount ?? 100;
  form.batchSize = task.batchSize ?? 500;
  form.scheduleType = task.scheduleType ?? "MANUAL";
  form.cronExpression = task.cronExpression ?? "";
  applyCronExpressionToBuilder(task.cronExpression);
  form.triggerAt = toLocalDateTimeInput(task.triggerAt);
  form.intervalSeconds = task.intervalSeconds ?? 10;
  form.maxRuns = task.maxRuns ?? null;
  form.maxRowsTotal = task.maxRowsTotal ?? null;
  form.seed = task.seed;
  form.status = task.status ?? "READY";
  form.description = task.description ?? "";
  form.kafkaMessageMode = kafkaComplexTask ? "COMPLEX" : "SIMPLE";
  applyTargetConfigToForm(task.targetConfigJson);
  form.payloadSchemaRoot = kafkaComplexTask ? parsePayloadSchemaJson(task.payloadSchemaJson) : createDefaultPayloadSchemaRoot();
  form.columns = (task.columns ?? []).map((column, index) =>
    createColumn({
      columnName: column.columnName,
      dbType: column.dbType,
      lengthValue: column.lengthValue,
      precisionValue: column.precisionValue,
      scaleValue: column.scaleValue,
      nullableFlag: column.nullableFlag,
      primaryKeyFlag: column.primaryKeyFlag,
      generatorType: column.generatorType,
      generatorConfigJson: JSON.stringify(column.generatorConfig, null, 2),
      sortOrder: column.sortOrder ?? index
    })
  );
  if (kafkaTask && !kafkaComplexTask && !form.columns.length) {
    initializeManualColumns();
  }
  if (options?.clearOutputs !== false) {
    previewRows.value = [];
    lastRunSummary.value = null;
  }
  if (sourceMode.value === "EXISTING" && form.connectionId && !kafkaTask) {
    void loadTables(form.connectionId, { silent: options?.silentTableLoad ?? true });
  }
  ensureSourceModeMatchesTarget();
  ensureWriteModeMatchesTarget();
  ensureStatusMatchesScheduleType();
}

function syncSelectedTaskFromList(clearOutputs = false) {
  if (selectedTaskId.value === null) {
    return;
  }
  const task = tasks.value.find((item) => item.id === selectedTaskId.value);
  if (task) {
    applyTaskToForm(task, {
      clearOutputs,
      silentTableLoad: true
    });
  }
}

function selectTask(task: WriteTask) {
  applyTaskToForm(task, {
    clearOutputs: true,
    silentTableLoad: true
  });
  clearFeedback();
  void router.push({ name: "write-task-edit", params: { id: task.id } });
}

function openTaskPreview(taskId: number) {
  clearFeedback();
  void router.push({ name: "write-task-edit", params: { id: taskId }, query: { preview: "1" } });
}

function openTaskExecutions(taskId: number) {
  clearFeedback();
  void router.push({ name: "write-task-executions", params: { taskId } });
}

function openTaskExecutionDetail(taskId: number, executionId: number) {
  clearFeedback();
  void router.push({ name: "write-task-execution-detail", params: { taskId, executionId } });
}

async function applyCreateRoutePrefill() {
  if (routedPrefillConnectionId.value === null) {
    return;
  }

  const connection = connections.value.find((item) => item.id === routedPrefillConnectionId.value);
  if (!connection) {
    return;
  }

  form.connectionId = connection.id;
  ensureSourceModeMatchesTarget();
  ensureWriteModeMatchesTarget();

  if (connection.dbType === "KAFKA") {
    form.tableName = routedPrefillTableName.value;
    return;
  }

  sourceMode.value = "EXISTING";
  form.tableMode = "USE_EXISTING";
  await loadTables(connection.id, { silent: true, clearFeedbackOnSuccess: true });

  if (!routedPrefillTableName.value) {
    return;
  }

  form.tableName = routedPrefillTableName.value;
  normalizeSelectedTableName();
  if (form.tableName.trim()) {
    await importColumnsFromTable();
  }
}

async function syncTaskRouteState() {
  if (isTaskListPage.value) {
    selectedTaskId.value = null;
    previewRows.value = [];
    resetValidationIssue();
    return;
  }

  if (isTaskCreatePage.value) {
    resetForm();
    await applyCreateRoutePrefill();
    previewRows.value = [];
    lastRunSummary.value = null;
    await nextTick();
    scrollBuilderIntoView();
    return;
  }

  if (!isTaskEditPage.value || routedTaskId.value === null) {
    return;
  }

  const task = tasks.value.find((item) => item.id === routedTaskId.value);
  if (!task) {
    setFeedback("error", "未找到写入任务");
    await router.replace({ name: "write-tasks" });
    return;
  }

  const keepOutputs = selectedTaskId.value === task.id;
  applyTaskToForm(task, {
    clearOutputs: !keepOutputs,
    silentTableLoad: true
  });

  if (route.query.preview === "1") {
    await previewExistingTask(task.id);
    await router.replace({ name: "write-task-edit", params: { id: task.id } });
  } else {
    await nextTick();
    scrollBuilderIntoView();
  }
}

async function loadPageData() {
  try {
    const [taskResponse, connectionResponse] = await Promise.all([
      apiClient.get<ApiResponse<WriteTask[]>>("/write-tasks"),
      apiClient.get<ApiResponse<ConnectionOption[]>>("/connections")
    ]);
    tasks.value = taskResponse.data.success ? taskResponse.data.data : [];
    connections.value = connectionResponse.data.success ? connectionResponse.data.data : [];
    await syncTaskRouteState();
    if (feedback.kind === "error" && feedback.message) {
      clearFeedback();
    }
  } catch (error) {
    tasks.value = [];
    connections.value = [];
    setFeedback("error", readApiError(error, "加载写入任务失败"));
  }
}

async function loadTables(connectionId = form.connectionId, options?: { silent?: boolean; clearFeedbackOnSuccess?: boolean }) {
  if (!connectionId) {
    setFeedback("error", "请选择目标连接");
    return;
  }

  const connection = connections.value.find((item) => item.id === connectionId);
  if (connection?.dbType === "KAFKA") {
    tables.value = [];
    return;
  }

  try {
    const response = await apiClient.get<ApiResponse<DatabaseTable[]>>(`/connections/${connectionId}/tables`);
    tables.value = response.data.success ? response.data.data : [];
    normalizeSelectedTableName();
    if (options?.clearFeedbackOnSuccess) {
      clearFeedback();
    }
  } catch (error) {
    tables.value = [];
    setFeedback("error", readApiError(error, "读取数据表失败"));
  }
}

async function handleConnectionChange() {
  tables.value = [];
  lastImportedTable.value = "";
  if (sourceMode.value === "EXISTING") {
    form.tableName = "";
    form.columns = [];
  }

  ensureSourceModeMatchesTarget();
  ensureWriteModeMatchesTarget();

  if (form.connectionId && !isKafkaTarget.value) {
    if (sourceMode.value === "MANUAL" && form.columns.length) {
      normalizeManualColumnsForConnection();
    }
    await loadTables(form.connectionId, { clearFeedbackOnSuccess: true });
    return;
  }

  if (isKafkaTarget.value) {
    form.tableName = "";
    if (!form.payloadSchemaRoot) {
      form.payloadSchemaRoot = createDefaultPayloadSchemaRoot();
    }
    if (form.kafkaMessageMode !== "COMPLEX" && !form.columns.length) {
      initializeManualColumns();
    } else if (form.kafkaMessageMode !== "COMPLEX") {
      normalizeManualColumnsForConnection();
    }
    return;
  }

  form.kafkaMessageMode = "SIMPLE";
  form.keyPath = "";
  form.payloadSchemaRoot = createDefaultPayloadSchemaRoot();
}

function switchSourceMode(mode: TaskSourceMode) {
  if (mode === "EXISTING" && isKafkaTarget.value) {
    setFeedback("error", "Kafka 不支持读取表结构");
    return;
  }
  if (sourceMode.value === mode) {
    return;
  }
  sourceMode.value = mode;
  form.tableMode = mode === "EXISTING" ? "USE_EXISTING" : "CREATE_IF_MISSING";
  form.tableName = "";
  lastImportedTable.value = "";
  if (mode === "EXISTING") {
    form.columns = [];
    if (form.connectionId) {
      void loadTables(form.connectionId, { clearFeedbackOnSuccess: true });
    } else {
      tables.value = [];
    }
    return;
  }

  tables.value = [];
  initializeManualColumns();
}

function switchKafkaMessageMode(mode: KafkaMessageMode) {
  if (!isKafkaTarget.value) {
    return;
  }
  if (form.kafkaMessageMode === mode) {
    return;
  }
  form.kafkaMessageMode = mode;
  form.keyField = "";
  form.keyPath = "";
  if (mode === "COMPLEX") {
    form.payloadSchemaRoot = form.payloadSchemaRoot ?? createDefaultPayloadSchemaRoot();
    return;
  }
  if (!form.columns.length) {
    initializeManualColumns();
    return;
  }
  normalizeManualColumnsForConnection();
}

async function handleExistingTableSelection() {
  if (isKafkaTarget.value) {
    return;
  }
  if (!form.tableName.trim()) {
    form.columns = [];
    lastImportedTable.value = "";
    return;
  }
  await importColumnsFromTable();
}

async function importColumnsFromTable() {
  if (!form.connectionId) {
    setFeedback("error", "请选择目标连接");
    return;
  }
  if (isKafkaTarget.value) {
    setFeedback("error", "Kafka 不支持导入表字段");
    return;
  }
  if (!form.tableName.trim()) {
    setFeedback("error", "请选择目标表");
    return;
  }

  try {
    const response = await apiClient.get<ApiResponse<DatabaseColumn[]>>(`/connections/${form.connectionId}/table-columns`, {
      params: { tableName: form.tableName.trim() }
    });
    const columns = response.data.success ? response.data.data : [];
    form.columns = columns.map((column, index) => createColumnFromDatabase(column, index));
    if (!form.columns.length) {
      form.columns = [createDefaultTextColumn()];
    }
    lastImportedTable.value = form.tableName.trim();
    clearFeedback();
  } catch (error) {
    setFeedback("error", readApiError(error, "导入表字段失败"));
  }
}

async function saveTask() {
  if (false) {
    setFeedback("error", "请完成连接、目标表和字段配置");
    return;
  }

  const isEditing = selectedTaskId.value !== null;
  isSaving.value = true;
  try {
    resetValidationIssue();
    const payload = buildPayload();
    const fallbackMessage = isEditing ? "写入任务已更新" : "写入任务已创建";
    const response = selectedTaskId.value
      ? await apiClient.put<ApiResponse<WriteTask>>(`/write-tasks/${selectedTaskId.value}`, payload)
      : await apiClient.post<ApiResponse<WriteTask>>("/write-tasks", payload);

    const savedTask = response.data.success ? response.data.data : null;
    if (savedTask) {
      applyTaskToForm(savedTask, {
        clearOutputs: false,
        silentTableLoad: true
      });
      if (route.name !== "write-task-edit" || routedTaskId.value !== savedTask.id) {
        await router.replace({ name: "write-task-edit", params: { id: savedTask.id } });
      }
    }
    await loadPageData();
    syncSelectedTaskFromList(false);
    resetValidationIssue();
    setFeedback("success", response.data.message ?? fallbackMessage);
    await nextTick();
    scrollBuilderIntoView();
  } catch (error) {
    applyValidationError(error, "保存写入任务失败");
    return;
    setFeedback("error", readApiError(error, "保存写入任务失败"));
  } finally {
    isSaving.value = false;
  }
}

async function previewTask() {
  if (false) {
    setFeedback("error", "请完成连接、目标表和字段配置");
    return;
  }

  isPreviewing.value = true;
  try {
    resetValidationIssue();
    const response = await apiClient.post<ApiResponse<PreviewResponse>>("/write-tasks/preview", {
      task: buildPayload(),
      count: Math.min(form.rowCount, 5),
      seed: form.seed
    });
    previewRows.value = response.data.success ? response.data.data.rows : [];
    resetValidationIssue();
    setFeedback("success", "预览完成");
    await nextTick();
    scrollPreviewIntoView();
  } catch (error) {
    applyValidationError(error, "预览数据失败");
    return;
    setFeedback("error", readApiError(error, "预览数据失败"));
  } finally {
    isPreviewing.value = false;
  }
}

async function previewExistingTask(taskId: number) {
  isPreviewing.value = true;
  try {
    const response = await apiClient.get<ApiResponse<PreviewResponse>>(`/write-tasks/${taskId}/preview`, {
      params: { count: 5 }
    });
    previewRows.value = response.data.success ? response.data.data.rows : [];
    setFeedback("success", "预览完成");
    await nextTick();
    scrollPreviewIntoView();
  } catch (error) {
    setFeedback("error", readApiError(error, "读取预览数据失败"));
  } finally {
    isPreviewing.value = false;
  }
}

async function runTask(taskId: number) {
  executingTaskId.value = taskId;
  try {
    const response = await apiClient.post<ApiResponse<RunExecutionResponse>>(`/write-tasks/${taskId}/run`);
    const execution = response.data.success ? response.data.data : null;
    lastRunSummary.value = execution ? createRunSummary(taskId, execution) : null;
    await loadPageData();
    syncSelectedTaskFromList(false);
    if (lastRunSummary.value) {
      setFeedback(...buildRunFeedback(lastRunSummary.value, execution));
      await nextTick();
      scrollRunSummaryIntoView();
    } else {
      setFeedback("success", "写入任务已执行");
    }
  } catch (error) {
    lastRunSummary.value = null;
    setFeedback("error", readApiError(error, "执行写入任务失败"));
  } finally {
    executingTaskId.value = null;
  }
}

async function removeTask(taskId: number) {
  mutatingTaskId.value = taskId;
  try {
    await apiClient.delete(`/write-tasks/${taskId}`);
    const removingSelectedTask = selectedTaskId.value === taskId;
    if (selectedTaskId.value === taskId) {
      resetForm();
    }
    if (removingSelectedTask && isTaskEditorPage.value) {
      await router.replace({ name: "write-tasks" });
    }
    await loadPageData();
    setFeedback("success", "写入任务已删除");
  } catch (error) {
    setFeedback("error", readApiError(error, "删除写入任务失败"));
  } finally {
    mutatingTaskId.value = null;
  }
}

async function mutateTaskSchedule(taskId: number, action: "start" | "pause" | "resume" | "stop", fallbackMessage: string) {
  mutatingTaskId.value = taskId;
  try {
    const response = await apiClient.post<ApiResponse<WriteTask>>(`/write-tasks/${taskId}/${action}`);
    await loadPageData();
    syncSelectedTaskFromList(false);
    setFeedback("success", response.data.message ?? fallbackMessage);
  } catch (error) {
    setFeedback("error", readApiError(error, "更新任务调度状态失败"));
  } finally {
    mutatingTaskId.value = null;
  }
}

async function startContinuousTask(taskId: number) {
  await mutateTaskSchedule(taskId, "start", "持续写入已启动");
}

async function pauseTaskSchedule(taskId: number) {
  await mutateTaskSchedule(taskId, "pause", "任务已暂停");
}

async function resumeTaskSchedule(taskId: number) {
  await mutateTaskSchedule(taskId, "resume", "任务已恢复");
}

async function stopContinuousTask(taskId: number) {
  await mutateTaskSchedule(taskId, "stop", "持续写入已停止");
}

function isTaskBusy(taskId: number) {
  return executingTaskId.value === taskId || mutatingTaskId.value === taskId;
}

function canStartContinuous(task: WriteTask | null) {
  return Boolean(task && task.scheduleType === "INTERVAL" && task.status === "READY");
}

function canPauseTask(task: WriteTask | null) {
  if (!task) {
    return false;
  }
  if (task.scheduleType === "INTERVAL") {
    return task.status === "RUNNING";
  }
  return (task.scheduleType === "ONCE" || task.scheduleType === "CRON") && task.status === "READY";
}

function canResumeTask(task: WriteTask | null) {
  if (!task) {
    return false;
  }
  return (task.scheduleType === "ONCE" || task.scheduleType === "CRON" || task.scheduleType === "INTERVAL")
    && task.status === "PAUSED";
}

function canStopContinuous(task: WriteTask | null) {
  return Boolean(task && task.scheduleType === "INTERVAL" && (task.status === "RUNNING" || task.status === "PAUSED"));
}

function looksCorrupted(value: string | null | undefined) {
  if (!value) {
    return true;
  }
  return value.includes("??") || /[\u951F\u95BF\uFFFD]/.test(value);
}

function displayTaskName(task: WriteTask) {
  if (looksCorrupted(task.name)) {
    return `${task.tableName} 写入任务`;
  }
  return task.name;
}

function taskFieldCount(task: WriteTask) {
  if (task.payloadSchemaJson) {
    return collectScalarPaths(parsePayloadSchemaJson(task.payloadSchemaJson)).length;
  }
  return task.columns.length;
}

function resolveConnectionName(connectionId: number) {
  const connection = connections.value.find((item) => item.id === connectionId);
  if (!connection) {
    return `连接 #${connectionId}`;
  }
  if (looksCorrupted(connection.name)) {
    return `连接 #${connectionId}（${labelDatabaseType(connection.dbType)}）`;
  }
  return `${connection.name}（${labelDatabaseType(connection.dbType)}）`;
}

function formatJson(value: unknown) {
  return JSON.stringify(value, null, 2);
}

function describeCronExpression(expression: string | null | undefined) {
  const parsed = parseCronExpression(expression);
  if (!expression || !expression.trim()) {
    return "未设置周期定时计划";
  }
  if (!parsed) {
    return `Cron：${expression}`;
  }
  switch (parsed.mode) {
    case "EVERY_MINUTES":
      return `每 ${parsed.minuteStep} 分钟执行一次`;
    case "EVERY_HOURS":
      return `每 ${parsed.hourStep} 小时在 ${String(parsed.minute).padStart(2, "0")} 分执行一次`;
    case "DAILY":
      return `每天 ${String(parsed.hour).padStart(2, "0")}:${String(parsed.minute).padStart(2, "0")} 执行`;
    case "WEEKLY":
      return `${cronWeekDayOptions.find((option) => option.value === parsed.weekDay)?.label ?? parsed.weekDay} ${String(parsed.hour).padStart(2, "0")}:${String(parsed.minute).padStart(2, "0")} 执行`;
  }
}

function describeSchedule(task: WriteTask) {
  switch (task.scheduleType) {
    case "MANUAL":
      return "仅在手动执行时写入";
    case "ONCE":
      return task.triggerAt ? `${formatOptionalDate(task.triggerAt)} 执行一次` : "未设置执行时间";
    case "CRON":
      return describeCronExpression(task.cronExpression);
    case "INTERVAL":
      return `每 ${task.intervalSeconds ?? "-"} 秒写入一批`;
  }
}

function describeContinuousLimit(task: WriteTask) {
  if (task.scheduleType !== "INTERVAL") {
    return "";
  }
  const limits: string[] = [];
  if (task.maxRuns) {
    limits.push(`最多执行 ${task.maxRuns} 次`);
  }
  if (task.maxRowsTotal) {
    limits.push(`累计写入 ${task.maxRowsTotal} 条后停止`);
  }
  return limits.length ? limits.join(" / ") : "未设置自动停止条件";
}

function createRunSummary(taskId: number, execution: RunExecutionResponse): RunSummary {
  const task = tasks.value.find((item) => item.id === taskId);
  const delivery = parseJson(execution.deliveryDetailsJson);
  const validation = delivery.nonNullValidation && typeof delivery.nonNullValidation === "object"
    ? delivery.nonNullValidation as Record<string, unknown>
    : null;
  const headers = delivery.headers && typeof delivery.headers === "object"
    ? delivery.headers as Record<string, unknown>
    : null;
  const headerDefinitions = Array.isArray(delivery.headerDefinitions)
    ? delivery.headerDefinitions
    : [];
  const deliveryType = String(delivery.deliveryType ?? delivery.targetType ?? "JDBC").toUpperCase();
  const hasBeforeAfterMetrics = deliveryType !== "KAFKA"
    && (
      delivery.beforeWriteRowCount !== undefined
      || delivery.afterWriteRowCount !== undefined
      || delivery.rowDelta !== undefined
    );

  return {
    executionId: execution.id,
    taskId,
    taskName: task ? displayTaskName(task) : `任务 #${taskId}`,
    statusText: labelExecutionStatus(execution.status),
    deliveryType,
    writtenRowCount: Number(delivery.writtenRowCount ?? execution.successCount ?? 0),
    beforeWriteRowCount: hasBeforeAfterMetrics ? Number(delivery.beforeWriteRowCount ?? 0) : 0,
    afterWriteRowCount: hasBeforeAfterMetrics ? Number(delivery.afterWriteRowCount ?? 0) : 0,
    rowDelta: hasBeforeAfterMetrics ? Number(delivery.rowDelta ?? 0) : 0,
    hasBeforeAfterMetrics,
    hasValidation: validation !== null,
    validationPassed: Boolean(validation?.passed ?? false),
    nullValueCount: Number(validation?.nullValueCount ?? 0),
    blankStringCount: Number(validation?.blankStringCount ?? 0),
    topic: typeof delivery.topic === "string" ? delivery.topic : null,
    payloadFormat: typeof delivery.payloadFormat === "string" ? delivery.payloadFormat : null,
    keyMode: typeof delivery.keyMode === "string" ? delivery.keyMode : null,
    keyField: typeof delivery.keyField === "string" ? delivery.keyField : null,
    keyPath: typeof delivery.keyPath === "string" ? delivery.keyPath : null,
    fixedKey: typeof delivery.fixedKey === "string" ? delivery.fixedKey : null,
    partition: typeof delivery.partition === "number" && Number.isFinite(delivery.partition) ? delivery.partition : null,
    headerCount: typeof delivery.headerCount === "number" && Number.isFinite(delivery.headerCount)
      ? delivery.headerCount
      : (headerDefinitions.length ? headerDefinitions.length : (headers ? Object.keys(headers).length : 0)),
    errorSummary: execution.errorSummary
  };
}

function buildRunFeedback(summary: RunSummary, execution: RunExecutionResponse | null): [FeedbackKind, string] {
  if (!execution) {
    return ["success", "写入任务已执行"];
  }

  if (execution.status === "FAILED") {
    return ["error", summary.errorSummary || "执行失败"];
  }

  if (execution.status === "PARTIAL_SUCCESS") {
    return [
      "error",
      summary.errorSummary || `部分成功：写入 ${summary.writtenRowCount} 条，失败 ${execution.errorCount} 条`
    ];
  }

  if (summary.deliveryType === "KAFKA") {
    return [
      "success",
      `执行完成：已向 Topic ${summary.topic || form.tableName} 写入 ${summary.writtenRowCount} 条消息`
    ];
  }

  return [
    "success",
    `执行完成：本次写入 ${summary.writtenRowCount} 条，写入后共 ${summary.afterWriteRowCount} 条`
  ];
}

function parseJson(value: string | null | undefined) {
  if (!value) {
    return {};
  }
  try {
    return JSON.parse(value) as Record<string, unknown>;
  } catch {
    return {};
  }
}

onMounted(async () => {
  resetForm();
  await loadPageData();
});

watch(
  () => [route.name, route.params.id, route.query.preview],
  () => {
    void syncTaskRouteState();
  }
);

watch(
  () => tasks.value.length,
  () => {
    taskPage.value = clampPage(taskPage.value, tasks.value.length, taskPageSize.value);
  }
);

watch(taskPageSize, () => {
  taskPage.value = 1;
});
</script>
