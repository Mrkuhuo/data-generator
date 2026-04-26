<template>
  <section class="page">
    <header class="page-header page-header--stacked">
      <div class="page-header__content">
        <h2>{{ text.pageTitle }}</h2>
        <p class="muted">{{ isListPage ? text.pageSummary : text.editorSummary }}</p>
      </div>
      <div class="page-actions">
        <button class="button button--ghost" type="button" :disabled="loading" @click="loadPageData">
          {{ text.refresh }}
        </button>
        <button
          v-if="isListPage"
          class="button"
          type="button"
          :disabled="saving || running"
          data-testid="create-group"
          @click="startCreate"
        >
          {{ text.createGroup }}
        </button>
        <button
          v-else
          class="button button--ghost"
          type="button"
          :disabled="saving || running"
          data-testid="back-to-group-list"
          @click="goToGroupList"
        >
          {{ text.backToList }}
        </button>
      </div>
    </header>

    <div
      v-if="feedback.message"
      class="status-banner"
      :class="feedback.kind === 'error' ? 'status-banner--error' : 'status-banner--success'"
    >
      {{ feedback.message }}
    </div>

    <section v-if="!isListPage" class="action-dock" data-testid="editor-action-dock">
      <div class="action-dock__summary">
        <span>{{ text.currentMode }}</span>
        <strong>{{ labelScheduleType(form.scheduleType) }}</strong>
        <div class="action-dock__meta">
          <span>{{ form.tasks.length }} {{ text.taskUnit }}</span>
          <span>{{ form.relations.length }} {{ text.relationUnit }}</span>
          <span>{{ text.scheduleSummary }} {{ describeSchedule(formScheduleSummary) }}</span>
        </div>
      </div>
      <div class="action-dock__actions">
        <button class="button" data-testid="save-group" type="button" :disabled="saving" @click="saveGroup">
          {{ saving ? text.saving : text.saveGroup }}
        </button>
        <button
          class="button button--ghost"
          data-testid="preview-current"
          type="button"
          :disabled="previewing"
          @click="previewCurrent"
        >
          {{ previewing ? text.previewing : text.preview }}
        </button>
        <button
          class="button button--ghost"
          data-testid="run-current"
          type="button"
          :disabled="running"
          @click="runCurrent"
        >
          {{ running ? text.running : text.runNow }}
        </button>
      </div>
    </section>

    <section v-if="!isListPage && (lastExecution || isContinuousRuntimeVisible)" class="panel runtime-card" data-testid="execution-summary">
      <div class="panel__row panel__row--divider">
        <div>
          <h3>{{ text.executionResultTitle }}</h3>
          <p class="muted">
            <span v-if="lastExecution">
              {{ text.executionStatus }} {{ labelExecutionStatus(lastExecution.status) }}
              / {{ text.triggerType }} {{ labelTriggerType(lastExecution.triggerType) }}
              / {{ text.insertedRows }} {{ lastExecution.insertedRowCount ?? 0 }}
            </span>
            <span v-else>{{ text.waitingContinuousResult }}</span>
          </p>
        </div>
        <div v-if="showRuntimeRefreshControls" class="runtime-card__refresh">
          <span class="pill pill--soft">{{ autoRefreshLabel }}</span>
          <button
            class="button button--ghost"
            data-testid="refresh-runtime"
            type="button"
            :disabled="refreshingRuntime"
            @click="refreshRuntimeStatus"
          >
            {{ text.refreshRuntime }}
          </button>
          <button class="button button--ghost" type="button" @click="toggleAutoRefresh">
            {{ autoRefreshEnabled ? text.pauseAutoRefresh : text.resumeAutoRefresh }}
          </button>
        </div>
      </div>

      <div v-if="form.id !== null && lastExecution" class="panel__actions panel__actions--tight">
        <button class="button button--ghost" type="button" @click="openGroupExecutionDetail(form.id, lastExecution.id)">
          查看本次实例
        </button>
        <button class="button button--ghost" type="button" @click="openGroupExecutions(form.id)">
          查看全部实例
        </button>
      </div>

      <div class="metric-grid">
        <div class="metric-card">
          <span>{{ text.insertedRows }}</span>
          <strong>{{ lastExecution?.insertedRowCount ?? 0 }}</strong>
        </div>
        <div class="metric-card">
          <span>{{ text.successTables }}</span>
          <strong>{{ lastExecution ? `${lastExecution.successTableCount} / ${lastExecution.plannedTableCount}` : "-" }}</strong>
        </div>
        <div class="metric-card">
          <span>{{ text.failedTables }}</span>
          <strong>{{ lastExecution?.failureTableCount ?? 0 }}</strong>
        </div>
        <div class="metric-card">
          <span>{{ text.lastRefreshAt }}</span>
          <strong>{{ lastRuntimeRefreshAt ? formatOptionalDate(lastRuntimeRefreshAt) : "-" }}</strong>
        </div>
      </div>

      <div v-if="lastExecution?.errorSummary" class="execution-result-table__errors">
        <strong>{{ text.executionErrorLabel }}</strong>
        <span>{{ lastExecution.errorSummary }}</span>
      </div>

      <div v-if="lastExecution?.tables?.length" class="execution-result-table">
        <div class="execution-result-table__head">
          <span>{{ text.table }}</span>
          <span>{{ text.status }}</span>
          <span>{{ text.beforeCount }}</span>
          <span>{{ text.afterCount }}</span>
          <span>{{ text.insertedShort }}</span>
          <span>{{ text.validationResult }}</span>
        </div>
        <div v-for="table in lastExecution.tables" :key="table.id" class="execution-result-table__row">
          <strong class="execution-result-table__table-name">{{ table.tableName }}</strong>
          <span>{{ labelExecutionStatus(table.status) }}</span>
          <span>{{ table.beforeWriteRowCount ?? "-" }}</span>
          <span>{{ table.afterWriteRowCount ?? "-" }}</span>
          <span>{{ table.insertedCount }}</span>
          <div class="execution-result-table__validation">
            <span>
              {{ text.nullShort }} {{ table.nullViolationCount }} /
              {{ text.blankShort }} {{ table.blankStringCount }} /
              {{ text.fkShort }} {{ table.fkMissCount }} /
              {{ text.pkShort }} {{ table.pkDuplicateCount }}
            </span>
            <div v-if="executionTableErrors(table).length" class="execution-result-table__errors">
              <strong>{{ text.executionErrorLabel }}</strong>
              <span v-for="(error, errorIndex) in executionTableErrors(table)" :key="`${table.id}-error-${errorIndex}`">
                {{ error }}
              </span>
            </div>
          </div>
        </div>
      </div>

      <div v-else-if="lastExecution && isActiveExecutionStatus(lastExecution.status)" class="runtime-card__empty muted">
        {{ text.executionPendingHint }}
      </div>
    </section>

    <section v-if="!isListPage && previewData" class="panel preview-summary" data-testid="preview-summary">
      <div class="panel__row panel__row--divider">
        <div>
          <h3>{{ text.previewResultTitle }}</h3>
          <p class="muted">{{ text.previewResultHint }}</p>
        </div>
      </div>
      <div class="stack">
        <section v-for="table in previewData.tables" :key="table.taskKey" class="panel panel--embedded">
          <div class="panel__row panel__row--divider">
            <div>
              <h3>{{ table.tableName }}</h3>
              <p class="muted">
                {{ text.generatedRows }} {{ table.generatedRowCount }}
                {{ text.previewRows }} {{ table.previewRowCount }}
                {{ text.foreignKeyMisses }} {{ table.foreignKeyMissCount }}
                {{ text.nullViolations }} {{ table.nullViolationCount }}
              </p>
            </div>
          </div>

          <div class="preview-table">
            <table>
              <thead>
                <tr>
                  <th v-for="header in previewHeaders(table.rows)" :key="header">{{ header }}</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="(row, rowIndex) in table.rows" :key="rowIndex">
                  <td v-for="header in previewHeaders(table.rows)" :key="`${rowIndex}-${header}`">
                    {{ displayPreviewValue(row[header]) }}
                  </td>
                </tr>
              </tbody>
            </table>
          </div>
        </section>
      </div>
    </section>

    <section v-if="isListPage" class="panel">
      <div class="panel__row panel__row--divider">
        <div>
          <h3>{{ text.groupListTitle }}</h3>
          <p class="muted">{{ text.groupListHint }}</p>
        </div>
        <span class="pill pill--soft">{{ groups.length }} {{ text.groupUnit }}</span>
      </div>

      <div v-if="groups.length" class="task-list" data-testid="group-list">
        <article v-for="group in paginatedGroups" :key="group.id" class="task-list__item">
          <div class="task-list__main">
            <div class="panel__row">
              <h3>{{ group.name }}</h3>
              <span class="pill">{{ labelStatus(group.status) }}</span>
            </div>
            <div class="task-list__meta">
              <span>{{ resolveConnectionName(group.connectionId) }}</span>
              <span>{{ group.tasks.length }} {{ text.taskUnit }}</span>
              <span>{{ group.relations.length }} {{ text.relationUnit }}</span>
              <span>{{ labelScheduleType(group.scheduleType) }}</span>
              <span>{{ labelSchedulerState(group.schedulerState) }}</span>
              <span>{{ describeSchedule(group) }}</span>
              <span v-if="group.nextFireAt">{{ text.nextFireAt }} {{ formatOptionalDate(group.nextFireAt) }}</span>
            </div>
          </div>
          <div class="task-list__actions">
            <button class="button" type="button" @click="openGroupEditor(group.id)">{{ text.edit }}</button>
            <button class="button button--ghost" type="button" @click="openGroupExecutions(group.id)">实例</button>
            <button
              class="button button--ghost"
              type="button"
              :data-testid="`run-group-${group.id}`"
              :disabled="running"
              @click="runExisting(group.id)"
            >
              {{ text.runNow }}
            </button>
            <button
              v-if="canStartContinuous(group)"
              class="button button--ghost"
              type="button"
              :data-testid="`start-group-${group.id}`"
              :disabled="!canMutateScheduling"
              @click="startGroup(group.id)"
            >
              {{ text.startContinuous }}
            </button>
            <button
              v-if="canPauseGroup(group)"
              class="button button--ghost"
              type="button"
              :data-testid="`pause-group-${group.id}`"
              :disabled="!canMutateScheduling"
              @click="pauseGroup(group.id)"
            >
              {{ text.pauseSchedule }}
            </button>
            <button
              v-if="canResumeGroup(group)"
              class="button button--ghost"
              type="button"
              :data-testid="`resume-group-${group.id}`"
              :disabled="!canMutateScheduling"
              @click="resumeGroup(group.id)"
            >
              {{ text.resumeSchedule }}
            </button>
            <button
              v-if="canStopContinuous(group)"
              class="button button--ghost"
              type="button"
              :data-testid="`stop-group-${group.id}`"
              :disabled="!canMutateScheduling"
              @click="stopGroup(group.id)"
            >
              {{ text.stopContinuous }}
            </button>
          </div>
          <div v-if="listExecutionByGroup[group.id]" class="task-list__runtime">
            <strong>{{ text.latestRun }}</strong>
            <span>{{ labelExecutionStatus(listExecutionByGroup[group.id].status) }}</span>
            <span>{{ text.insertedRows }} {{ listExecutionByGroup[group.id].insertedRowCount ?? 0 }}</span>
            <span>{{ text.successTables }} {{ listExecutionByGroup[group.id].successTableCount }} / {{ listExecutionByGroup[group.id].plannedTableCount }}</span>
            <button class="button button--ghost" type="button" @click="openGroupExecutionDetail(group.id, listExecutionByGroup[group.id].id)">查看最近实例</button>
          </div>
        </article>
      </div>

      <PaginationBar
        v-if="groups.length"
        :page="groupPage"
        :page-size="groupPageSize"
        :total="groups.length"
        :noun="text.groupUnit"
        @update:page="groupPage = $event"
        @update:page-size="groupPageSize = $event"
      />

      <section v-else class="empty-state">
        <h3>{{ text.emptyGroupTitle }}</h3>
        <p>{{ text.emptyGroupHint }}</p>
      </section>
    </section>

    <section v-else class="workspace-grid relational-layout relational-layout--editor">
      <article class="panel builder-layout">
        <form class="form-grid" data-testid="group-form" @submit.prevent="saveGroup">
          <section class="builder-section">
            <div class="section-title">
              <span class="section-index">01</span>
              <div>
                <h3>{{ text.basicInfoTitle }}</h3>
                <p class="section-lead">{{ text.basicInfoHint }}</p>
              </div>
            </div>

            <div class="field">
              <label>
                <span>{{ text.groupName }}</span>
                <input v-model.trim="form.name" type="text" :placeholder="text.groupNamePlaceholder" />
              </label>
            </div>

            <div class="field">
              <label>
                <span>{{ text.connection }}</span>
                <select v-model.number="form.connectionId" @change="handleConnectionChange">
                  <option :value="null">{{ text.selectConnection }}</option>
                  <option v-for="connection in connections" :key="connection.id" :value="connection.id">
                    {{ connection.name }} ({{ labelDatabaseType(connection.dbType) }})
                  </option>
                </select>
              </label>
            </div>

            <div class="field field--half">
              <label>
                <span>{{ text.seed }}</span>
                <input v-model.number="form.seed" type="number" min="1" :placeholder="text.seedPlaceholder" />
              </label>

              <label>
                <span>{{ text.description }}</span>
                <input v-model.trim="form.description" type="text" :placeholder="text.descriptionPlaceholder" />
              </label>
            </div>
          </section>

          <section class="builder-section">
            <div class="section-title">
              <span class="section-index">02</span>
              <div>
                <h3>{{ text.scheduleTitle }}</h3>
                <p class="section-lead">{{ text.scheduleHint }}</p>
              </div>
            </div>

            <div class="schedule-mode-panel">
              <div class="schedule-mode-grid" data-testid="schedule-type">
                <button
                  v-for="option in scheduleOptions"
                  :key="option"
                  class="schedule-mode-card"
                  :class="{ 'schedule-mode-card--active': form.scheduleType === option }"
                  :data-testid="`schedule-type-${option}`"
                  type="button"
                  @click="setScheduleType(option)"
                >
                  <strong>{{ labelScheduleType(option) }}</strong>
                  <span>{{ describeScheduleMode(option) }}</span>
                </button>
              </div>

              <div class="schedule-current">
                <span>{{ text.scheduleSummary }}</span>
                <strong>{{ describeSchedule(formScheduleSummary) }}</strong>
              </div>
            </div>

            <div v-if="form.scheduleType === 'CRON'" class="schedule-builder">
              <div class="field field--half">
                <label>
                  <span>{{ text.cronPlanType }}</span>
                  <select
                    v-model="form.cronBuilderMode"
                    name="cronBuilderMode"
                    data-testid="cron-builder-mode"
                    @change="handleCronBuilderChange"
                  >
                    <option v-for="option in cronBuilderModeOptions" :key="option.value" :value="option.value">
                      {{ option.label }}
                    </option>
                  </select>
                </label>

                <label v-if="form.cronBuilderMode === 'EVERY_MINUTES'">
                  <span>{{ text.cronMinuteStep }}</span>
                  <select
                    v-model.number="form.cronMinuteStep"
                    name="cronMinuteStep"
                    data-testid="cron-minute-step"
                    @change="handleCronBuilderChange"
                  >
                    <option v-for="value in cronMinuteStepOptions" :key="value" :value="value">
                      {{ text.every }} {{ value }} {{ text.minuteUnit }}
                    </option>
                  </select>
                </label>

                <label v-else-if="form.cronBuilderMode === 'EVERY_HOURS'">
                  <span>{{ text.cronHourStep }}</span>
                  <select
                    v-model.number="form.cronHourStep"
                    name="cronHourStep"
                    data-testid="cron-hour-step"
                    @change="handleCronBuilderChange"
                  >
                    <option v-for="value in cronHourStepOptions" :key="value" :value="value">
                      {{ text.every }} {{ value }} {{ text.hourUnit }}
                    </option>
                  </select>
                </label>

                <label v-else-if="form.cronBuilderMode === 'WEEKLY'">
                  <span>{{ text.cronWeekDay }}</span>
                  <select
                    v-model="form.cronWeekDay"
                    name="cronWeekDay"
                    data-testid="cron-week-day"
                    @change="handleCronBuilderChange"
                  >
                    <option v-for="option in cronWeekDayOptions" :key="option.value" :value="option.value">
                      {{ option.label }}
                    </option>
                  </select>
                </label>

                <label v-else>
                  <span>{{ text.cronExecutionHint }}</span>
                  <input :value="text.dailyExecutionHint" readonly type="text" />
                </label>
              </div>

              <div
                v-if="form.cronBuilderMode === 'EVERY_HOURS' || form.cronBuilderMode === 'DAILY' || form.cronBuilderMode === 'WEEKLY'"
                class="field field--half"
              >
                <label>
                  <span>{{ text.cronAtHour }}</span>
                  <select
                    v-model.number="form.cronAtHour"
                    name="cronAtHour"
                    data-testid="cron-at-hour"
                    @change="handleCronBuilderChange"
                  >
                    <option v-for="value in cronHourOptions" :key="value" :value="value">
                      {{ value.toString().padStart(2, "0") }} {{ text.oClock }}
                    </option>
                  </select>
                </label>

                <label>
                  <span>{{ text.cronAtMinute }}</span>
                  <select
                    v-model.number="form.cronAtMinute"
                    name="cronAtMinute"
                    data-testid="cron-at-minute"
                    @change="handleCronBuilderChange"
                  >
                    <option v-for="value in cronMinuteOptions" :key="value" :value="value">
                      {{ value.toString().padStart(2, "0") }} {{ text.minuteLabel }}
                    </option>
                  </select>
                </label>
              </div>

              <div class="cron-builder-summary">
                <strong>{{ text.scheduleSummary }}：</strong>{{ describeCronExpression(cronPreviewExpression) }}
              </div>

              <div v-if="cronExpressionMode === 'LEGACY'" class="status-banner">
                {{ text.legacyCronNotice }} {{ form.cronExpression }}{{ text.legacyCronSuffix }}
              </div>
            </div>

            <div v-else-if="form.scheduleType === 'ONCE'" class="field field--half">
              <label>
                <span>{{ text.triggerAt }}</span>
                <input v-model="form.triggerAt" type="datetime-local" />
              </label>
            </div>

            <div v-else-if="form.scheduleType === 'INTERVAL'" class="field field--half">
              <label>
                <span>{{ text.intervalSeconds }}</span>
                <input v-model.number="form.intervalSeconds" type="number" min="1" />
              </label>

              <label>
                <span>{{ text.maxRuns }}</span>
                <input v-model.number="form.maxRuns" type="number" min="1" :placeholder="text.optionalPlaceholder" />
              </label>

              <label>
                <span>{{ text.maxRowsTotal }}</span>
                <input v-model.number="form.maxRowsTotal" type="number" min="1" :placeholder="text.optionalPlaceholder" />
              </label>
            </div>
          </section>

          <section class="builder-section">
            <div class="section-title">
              <span class="section-index">03</span>
              <div>
                <h3>{{ isKafkaGroup ? text.kafkaSetupTitle : text.schemaTitle }}</h3>
                <p class="section-lead">{{ isKafkaGroup ? text.kafkaSetupHint : text.schemaHint }}</p>
              </div>
            </div>

            <section v-if="!isKafkaGroup" class="panel panel--embedded">
              <div class="panel__row panel__row--divider">
                <div>
                  <h3>{{ text.tableSelectionTitle }}</h3>
                  <p class="muted">{{ text.tableSelectionHint }}</p>
                </div>
                <div class="panel__actions">
                  <button
                    class="button button--ghost"
                    type="button"
                    :disabled="!form.connectionId"
                    @click="loadTables"
                  >
                    {{ text.refreshTables }}
                  </button>
                  <button
                    class="button button--ghost"
                    type="button"
                    data-testid="import-schema"
                    :disabled="importingSchema || !form.connectionId || !selectedTableNames.length"
                    @click="importSchema"
                  >
                    {{ importingSchema ? text.importingSchema : text.importSchema }}
                  </button>
                </div>
              </div>

              <div v-if="availableTables.length" class="table-selector">
                <label v-for="table in availableTables" :key="tableKey(table)" class="selector-chip">
                  <input v-model="selectedTableNames" type="checkbox" :value="tableKey(table)" />
                  <span>{{ formatTableName(table) }}</span>
                </label>
              </div>

              <section v-else class="empty-state empty-state--embedded">
                <h3>{{ text.emptyTablesTitle }}</h3>
                <p>{{ form.connectionId ? text.emptyTablesHint : text.chooseConnectionFirst }}</p>
              </section>
            </section>

            <section v-else class="panel panel--embedded">
              <div class="panel__row panel__row--divider">
                <div>
                  <h3>{{ text.kafkaTaskListTitle }}</h3>
                  <p class="muted">{{ text.kafkaTaskListHint }}</p>
                </div>
                <div class="panel__actions">
                  <button
                    class="button button--ghost"
                    type="button"
                    data-testid="add-kafka-task"
                    :disabled="!form.connectionId"
                    @click="addTask"
                  >
                    {{ text.addTask }}
                  </button>
                </div>
              </div>

              <div v-if="form.tasks.length" class="task-list__meta">
                <span>{{ text.kafkaTaskSummary }} {{ form.tasks.length }}</span>
                <span>{{ text.relationUnit }} {{ form.relations.length }}</span>
              </div>

              <section v-else class="empty-state empty-state--embedded">
                <h3>{{ text.emptyKafkaTasksTitle }}</h3>
                <p>{{ text.emptyKafkaTasksHint }}</p>
              </section>
            </section>
          </section>

          <section class="builder-section">
            <div class="section-title">
              <span class="section-index">04</span>
              <div>
                <h3>{{ text.tasksTitle }}</h3>
                <p class="section-lead">{{ isKafkaGroup ? text.kafkaTasksHint : text.tasksHint }}</p>
              </div>
            </div>

            <div v-if="form.tasks.length" class="stack">
              <section v-for="task in form.tasks" :key="task.taskKey" class="panel panel--embedded task-editor-card">
                <div class="panel__row panel__row--divider">
                  <div>
                    <h3>{{ displayTaskLabel(task) }}</h3>
                    <p class="muted">{{ task.taskKey }}</p>
                  </div>
                  <div class="panel__actions">
                    <button class="button button--ghost" type="button" @click="removeTask(task.taskKey)">
                      {{ isKafkaGroup ? text.removeKafkaTask : text.removeTask }}
                    </button>
                  </div>
                </div>

                <div class="field task-editor-row">
                  <label class="task-editor-row__item">
                    <span>{{ text.taskName }}</span>
                    <input v-model.trim="task.name" type="text" />
                  </label>

                  <label v-if="isKafkaGroup" class="task-editor-row__item">
                    <span>{{ text.kafkaTopicName }}</span>
                    <input v-model.trim="task.tableName" type="text" :placeholder="text.kafkaTopicPlaceholder" />
                  </label>

                  <label v-else class="task-editor-row__item">
                    <span>{{ text.targetTableName }}</span>
                    <input :value="task.tableName" readonly type="text" />
                  </label>

                  <label class="task-editor-row__item">
                    <span>{{ text.batchSize }}</span>
                    <input v-model.number="task.batchSize" type="number" min="1" />
                  </label>
                </div>

                <div class="field task-editor-row">
                  <label v-if="!isKafkaGroup" class="task-editor-row__item">
                    <span>{{ text.writeMode }}</span>
                    <select v-model="task.writeMode">
                      <option value="APPEND">{{ text.writeModeAppend }}</option>
                      <option value="OVERWRITE">{{ text.writeModeOverwrite }}</option>
                    </select>
                  </label>

                  <label v-else class="task-editor-row__item">
                    <span>{{ text.writeMode }}</span>
                    <input :value="text.kafkaWriteModeFixed" readonly type="text" />
                  </label>

                  <label class="task-editor-row__item">
                    <span>{{ text.rowPlanMode }}</span>
                    <select v-model="task.rowPlan.mode">
                      <option value="FIXED">{{ text.rowPlanFixed }}</option>
                      <option value="CHILD_PER_PARENT">{{ text.rowPlanChild }}</option>
                    </select>
                  </label>
                  <div class="task-editor-row__spacer" aria-hidden="true"></div>
                </div>

                <div v-if="task.rowPlan.mode === 'FIXED'" class="field task-editor-row">
                  <label class="task-editor-row__item">
                    <span>{{ text.rowCount }}</span>
                    <input v-model.number="task.rowPlan.rowCount" type="number" min="1" />
                  </label>
                  <div class="task-editor-row__spacer" aria-hidden="true"></div>
                  <div class="task-editor-row__spacer" aria-hidden="true"></div>
                </div>

                <div v-else class="field task-editor-row">
                  <label class="task-editor-row__item">
                    <span>{{ text.driverTask }}</span>
                    <select v-model="task.rowPlan.driverTaskKey">
                      <option value="">{{ text.selectDriverTask }}</option>
                      <option v-for="driver in driverTaskOptions(task.taskKey)" :key="driver.taskKey" :value="driver.taskKey">
                        {{ displayTaskLabel(driver) }}
                      </option>
                    </select>
                  </label>

                  <label class="task-editor-row__item">
                    <span>{{ text.minChildren }}</span>
                    <input v-model.number="task.rowPlan.minChildrenPerParent" type="number" min="1" />
                  </label>

                  <label class="task-editor-row__item">
                    <span>{{ text.maxChildren }}</span>
                    <input v-model.number="task.rowPlan.maxChildrenPerParent" type="number" min="1" />
                  </label>
                </div>

                <template v-if="isKafkaGroup">
                  <section class="panel panel--subtle kafka-task-panel">
                    <div class="panel__row panel__row--divider">
                      <div>
                        <h3>{{ text.kafkaMessageTitle }}</h3>
                        <p class="muted">{{ text.kafkaMessageHint }}</p>
                      </div>
                      <span class="pill pill--soft">{{ task.kafkaMessageMode === "COMPLEX" ? text.kafkaComplexMode : text.kafkaSimpleMode }}</span>
                    </div>

                    <div class="mode-switch">
                      <button
                        class="mode-switch__item"
                        :class="{ 'mode-switch__item--active': task.kafkaMessageMode === 'SIMPLE' }"
                        type="button"
                        @click="switchTaskKafkaMessageMode(task, 'SIMPLE')"
                      >
                        {{ text.kafkaSimpleMode }}
                      </button>
                      <button
                        class="mode-switch__item"
                        :class="{ 'mode-switch__item--active': task.kafkaMessageMode === 'COMPLEX' }"
                        type="button"
                        @click="switchTaskKafkaMessageMode(task, 'COMPLEX')"
                      >
                        {{ text.kafkaComplexMode }}
                      </button>
                    </div>

                    <KafkaSchemaImportPanel
                      v-if="task.kafkaMessageMode === 'COMPLEX'"
                      @imported="applyImportedKafkaSchemaToTask(task, $event)"
                    />

                    <div class="field field--half">
                      <label>
                        <span>{{ text.kafkaKeyMode }}</span>
                        <select v-model="task.keyMode">
                          <option value="NONE">{{ text.kafkaNoKey }}</option>
                          <option value="FIELD">{{ text.kafkaFieldKey }}</option>
                          <option value="FIXED">{{ text.kafkaFixedKey }}</option>
                        </select>
                      </label>

                      <label>
                        <span>{{ text.kafkaPartition }}</span>
                        <input v-model.number="task.partition" type="number" min="0" :placeholder="text.optionalPlaceholder" />
                      </label>
                    </div>

                    <div v-if="task.keyMode === 'FIELD'" class="field">
                      <label>
                        <span>{{ task.kafkaMessageMode === "COMPLEX" ? text.kafkaKeyPath : text.kafkaKeyField }}</span>
                        <select v-if="task.kafkaMessageMode === 'COMPLEX'" v-model="task.keyPath">
                          <option value="">{{ text.selectField }}</option>
                          <option v-for="path in taskAvailableFieldPaths(task)" :key="`${task.taskKey}-${path}`" :value="path">{{ path }}</option>
                        </select>
                        <select v-else v-model="task.keyField">
                          <option value="">{{ text.selectField }}</option>
                          <option v-for="path in taskAvailableFieldPaths(task)" :key="`${task.taskKey}-${path}`" :value="path">{{ path }}</option>
                        </select>
                      </label>
                    </div>

                    <div v-else-if="task.keyMode === 'FIXED'" class="field">
                      <label>
                        <span>{{ text.kafkaFixedKeyValue }}</span>
                        <input v-model.trim="task.fixedKey" type="text" :placeholder="text.kafkaFixedKeyPlaceholder" />
                      </label>
                    </div>

                    <KafkaHeadersEditor
                      v-model="task.headerEntries"
                      :available-paths="taskAvailableFieldPaths(task)"
                    />
                  </section>

                  <section v-if="task.kafkaMessageMode === 'COMPLEX'" class="panel panel--subtle kafka-task-panel">
                    <div class="panel__row panel__row--divider">
                      <div>
                        <h3>{{ text.kafkaPayloadTitle }}</h3>
                        <p class="muted">{{ text.kafkaPayloadHint }}</p>
                      </div>
                      <span class="pill pill--soft">{{ taskAvailableFieldPaths(task).length }} {{ text.availableFieldUnit }}</span>
                    </div>

                    <KafkaPayloadSchemaEditor
                      v-if="task.payloadSchemaRoot"
                      :node="task.payloadSchemaRoot"
                    />
                  </section>

                  <section v-else class="panel panel--subtle kafka-task-panel">
                    <div class="panel__row panel__row--divider">
                      <div>
                        <h3>{{ text.kafkaFieldsTitle }}</h3>
                        <p class="muted">{{ text.kafkaFieldsHint }}</p>
                      </div>
                      <div class="panel__actions">
                        <button class="button button--ghost" type="button" @click="addTaskField(task)">
                          {{ text.addField }}
                        </button>
                      </div>
                    </div>

                    <div class="field-grid field-grid--editable field-grid--kafka-simple">
                      <div class="field-grid__head">
                        <span>{{ text.columnName }}</span>
                        <span>{{ text.columnType }}</span>
                        <span>{{ text.columnFlags }}</span>
                        <span>{{ text.generatorType }}</span>
                        <span>{{ text.generatorRule }}</span>
                        <span>{{ text.actionColumn }}</span>
                      </div>
                      <article
                        v-for="(column, columnIndex) in task.columns"
                        :key="column.localId ?? `${task.taskKey}-${columnIndex}`"
                        class="field-grid__row field-grid__row--editable"
                      >
                        <div class="field-grid__cell field-grid__cell--name">
                          <div class="field-name-shell">
                            <div class="field-name-display" :title="fieldDisplayName(column, columnIndex)">
                              <span class="field-name-display__value">{{ fieldDisplayName(column, columnIndex) }}</span>
                              <button
                                class="button button--ghost field-name-display__action"
                                type="button"
                                @click="startTaskFieldNameEdit(column, columnIndex)"
                              >
                                {{ text.edit }}
                              </button>
                            </div>
                            <div v-if="isTaskFieldNameEditing(column)" class="field-name-editor">
                              <input
                                :ref="(element) => setTaskFieldNameInputRef(column, element)"
                                v-model.trim="editingTaskFieldNameDraft"
                                type="text"
                                :placeholder="text.fieldNamePlaceholder"
                                @keydown.enter.prevent="confirmTaskFieldNameEdit(column)"
                                @keydown.esc.prevent="cancelTaskFieldNameEdit"
                              />
                              <div class="field-name-editor__actions">
                                <button class="button button--ghost field-name-editor__action" type="button" @click="confirmTaskFieldNameEdit(column)">
                                  {{ text.saveAction }}
                                </button>
                                <button class="button button--ghost field-name-editor__action" type="button" @click="cancelTaskFieldNameEdit">
                                  {{ text.cancelAction }}
                                </button>
                              </div>
                            </div>
                          </div>
                        </div>
                        <label class="field-grid__cell field-grid__cell--type compact-control">
                          <select v-model="column.dbType">
                            <option v-for="option in kafkaSimpleFieldTypes" :key="option" :value="option">{{ option }}</option>
                          </select>
                        </label>
                        <div class="field-grid__cell field-grid__cell--flags">
                          <label class="checkbox-chip">
                            <input v-model="column.primaryKeyFlag" type="checkbox" />
                            <span>{{ text.primaryKey }}</span>
                          </label>
                          <label class="checkbox-chip">
                            <input v-model="column.nullableFlag" type="checkbox" />
                            <span>{{ text.nullable }}</span>
                          </label>
                        </div>
                        <label class="field-grid__cell field-grid__cell--generator compact-control field-grid__generator">
                          <select v-model="column.generatorType" @change="handleTaskGeneratorTypeChange(task, column)">
                            <option v-for="generator in generatorOptions" :key="generator" :value="generator">
                              {{ labelGeneratorType(generator) }}
                            </option>
                          </select>
                        </label>
                        <div class="field-grid__cell field-grid__cell--rule generator-rule field-grid__rule">
                          <span class="generator-rule__summary" :title="summarizeGeneratorConfig(column)">{{ summarizeGeneratorConfig(column) }}</span>
                          <details class="config-details">
                            <summary>{{ text.configAction }}</summary>
                            <textarea
                              v-model="column.generatorConfigText"
                              class="code-input"
                              rows="4"
                              @blur="commitGeneratorConfig(column, task)"
                            ></textarea>
                          </details>
                        </div>
                        <div class="field-grid__cell field-grid__cell--actions">
                          <button class="button button--ghost field-grid__remove" type="button" @click="removeTaskField(task, columnIndex)">
                            {{ text.removeAction }}
                          </button>
                        </div>
                      </article>
                    </div>
                  </section>
                </template>

                <div v-else class="field-grid">
                  <div class="field-grid__head">
                    <span>{{ text.columnName }}</span>
                    <span>{{ text.columnType }}</span>
                    <span>{{ text.columnFlags }}</span>
                    <span>{{ text.generatorType }}</span>
                    <span>{{ text.generatorRule }}</span>
                  </div>
                  <article v-for="column in task.columns" :key="`${task.taskKey}-${column.columnName}`" class="field-grid__row">
                    <div class="field-grid__cell field-grid__name">
                      <strong>{{ column.columnName }}</strong>
                    </div>
                    <div class="field-grid__cell field-grid__type">
                      <span class="badge">{{ displayColumnType(column) }}</span>
                    </div>
                    <div class="field-grid__cell column-summary field-grid__flags">
                      <span v-if="column.primaryKeyFlag" class="badge badge--accent">{{ text.primaryKey }}</span>
                      <span v-if="column.foreignKeyFlag" class="badge">{{ text.foreignKey }}</span>
                      <span v-if="!column.nullableFlag" class="badge">{{ text.notNull }}</span>
                      <span v-if="column.nullableFlag" class="badge badge--muted">{{ text.nullable }}</span>
                    </div>
                    <label class="field-grid__cell compact-control field-grid__generator">
                      <select v-model="column.generatorType" @change="handleGeneratorTypeChange(column)">
                        <option v-for="generator in generatorOptions" :key="generator" :value="generator">
                          {{ labelGeneratorType(generator) }}
                        </option>
                      </select>
                    </label>
                    <div class="field-grid__cell generator-rule field-grid__rule">
                      <span class="generator-rule__summary">{{ summarizeGeneratorConfig(column) }}</span>
                      <details class="config-details">
                        <summary>{{ text.advancedConfig }}</summary>
                        <textarea
                          v-model="column.generatorConfigText"
                          class="code-input"
                          rows="4"
                          @blur="commitGeneratorConfig(column, task)"
                        ></textarea>
                      </details>
                    </div>
                  </article>
                </div>
              </section>
            </div>

            <section v-else class="empty-state">
              <h3>{{ isKafkaGroup ? text.emptyKafkaTasksTitle : text.emptyTasksTitle }}</h3>
              <p>{{ isKafkaGroup ? text.emptyKafkaTasksHint : text.emptyTasksHint }}</p>
            </section>
          </section>

          <section class="builder-section">
            <div class="section-title">
              <span class="section-index">05</span>
              <div>
                <h3>{{ text.relationsTitle }}</h3>
                <p class="section-lead">{{ isKafkaGroup ? text.kafkaRelationsHint : text.relationsHint }}</p>
              </div>
            </div>

            <div class="panel panel--embedded">
              <div class="panel__row panel__row--divider">
                <div>
                  <h3>{{ text.relationConfigTitle }}</h3>
                  <p class="muted">{{ text.relationConfigHint }}</p>
                </div>
                <div class="panel__actions">
                  <button class="button button--ghost" type="button" :disabled="form.tasks.length < 2" @click="addRelation">
                    {{ text.addRelation }}
                  </button>
                </div>
              </div>

              <div v-if="form.relations.length" class="relation-grid">
                <section v-for="(relation, index) in form.relations" :key="`${relation.relationName}-${index}`" class="relation-card">
                  <div class="panel__row">
                    <div>
                      <h3>{{ relation.relationName }}</h3>
                      <p class="muted">{{ relationTaskLabel(relation.parentTaskKey) }} -> {{ relationTaskLabel(relation.childTaskKey) }}</p>
                    </div>
                    <button class="button button--ghost" type="button" @click="removeRelation(index)">
                      {{ text.removeRelation }}
                    </button>
                  </div>

                  <div class="field field--half">
                    <label>
                      <span>{{ text.relationName }}</span>
                      <input v-model.trim="relation.relationName" type="text" />
                    </label>

                    <label>
                      <span>{{ text.relationType }}</span>
                      <select v-model="relation.relationType">
                        <option value="ONE_TO_ONE">{{ text.oneToOne }}</option>
                        <option value="ONE_TO_MANY">{{ text.oneToMany }}</option>
                        <option value="MANY_TO_ONE">{{ text.manyToOne }}</option>
                      </select>
                    </label>
                  </div>

                  <div class="field field--half">
                    <label>
                      <span>{{ text.parentTask }}</span>
                      <select v-model="relation.parentTaskKey" @change="handleRelationParentChange(relation)">
                        <option value="">{{ text.selectParentTask }}</option>
                        <option v-for="task in form.tasks" :key="task.taskKey" :value="task.taskKey">{{ displayTaskLabel(task) }}</option>
                      </select>
                    </label>

                    <label>
                      <span>{{ text.childTask }}</span>
                      <select v-model="relation.childTaskKey" @change="handleRelationChildChange(relation)">
                        <option value="">{{ text.selectChildTask }}</option>
                        <option
                          v-for="task in form.tasks.filter((item) => item.taskKey !== relation.parentTaskKey)"
                          :key="task.taskKey"
                          :value="task.taskKey"
                        >
                          {{ displayTaskLabel(task) }}
                        </option>
                      </select>
                    </label>
                  </div>

                  <div v-if="!isKafkaGroup" class="field field--half">
                    <label>
                      <span>{{ text.parentColumn }}</span>
                      <select :value="firstParentColumn(relation)" @change="setParentColumn(relation, eventTargetValue($event))">
                        <option value="">{{ text.selectParentColumn }}</option>
                        <option v-for="column in relationParentColumnOptions(relation)" :key="column" :value="column">{{ column }}</option>
                      </select>
                    </label>

                    <label>
                      <span>{{ text.childColumn }}</span>
                      <select :value="firstChildColumn(relation)" @change="setChildColumn(relation, eventTargetValue($event))">
                        <option value="">{{ text.selectChildColumn }}</option>
                        <option v-for="column in relationChildColumnOptions(relation)" :key="column" :value="column">{{ column }}</option>
                      </select>
                    </label>
                  </div>

                  <div class="field field--half">
                    <label>
                      <span>{{ text.sourceMode }}</span>
                      <select v-model="relation.sourceMode">
                        <option value="CURRENT_BATCH">{{ text.sourceCurrentBatch }}</option>
                        <option v-if="!isKafkaGroup" value="TARGET_TABLE">{{ text.sourceTargetTable }}</option>
                        <option v-if="!isKafkaGroup" value="MIXED">{{ text.sourceMixed }}</option>
                      </select>
                    </label>

                    <label>
                      <span>{{ text.selectionStrategy }}</span>
                      <select v-model="relation.selectionStrategy">
                        <option value="RANDOM_UNIFORM">{{ text.selectionRandom }}</option>
                        <option value="PARENT_DRIVEN">{{ text.selectionParentDriven }}</option>
                      </select>
                    </label>

                    <label>
                      <span>{{ text.reusePolicy }}</span>
                      <select v-model="relation.reusePolicy">
                        <option value="ALLOW_REPEAT">{{ text.reuseRepeat }}</option>
                        <option value="UNIQUE_ONCE">{{ text.reuseUnique }}</option>
                      </select>
                    </label>
                  </div>

                  <div class="field field--half">
                    <label>
                      <span>{{ text.nullRate }}</span>
                      <input v-model.number="relation.nullRate" type="number" min="0" max="1" step="0.01" />
                    </label>

                    <label v-if="!isKafkaGroup && relation.sourceMode === 'MIXED'">
                      <span>{{ text.mixedRatio }}</span>
                      <input v-model.number="relation.mixedExistingRatio" type="number" min="0" max="1" step="0.01" />
                    </label>

                    <label>
                      <span>{{ text.minChildren }}</span>
                      <input v-model.number="relation.minChildrenPerParent" type="number" min="1" />
                    </label>

                    <label>
                      <span>{{ text.maxChildren }}</span>
                      <input v-model.number="relation.maxChildrenPerParent" type="number" min="1" />
                    </label>
                  </div>

                  <KafkaRelationMappingEditor
                    v-if="isKafkaGroup"
                    v-model="relation.mappingEntries"
                    :source-paths="relationSourceFieldPaths(relation)"
                    :target-paths="relationTargetFieldPaths(relation)"
                  />
                </section>
              </div>

              <section v-else class="empty-state empty-state--embedded">
                <h3>{{ text.emptyRelationsTitle }}</h3>
                <p>{{ text.emptyRelationsHint }}</p>
              </section>
            </div>
          </section>

          <section class="builder-section">
            <div class="section-title">
              <span class="section-index">06</span>
              <div>
                <h3>{{ text.executionHistoryTitle }}</h3>
                <p class="section-lead">{{ text.executionHistoryHint }}</p>
              </div>
            </div>

            <section v-if="executions.length" class="panel panel--embedded">
              <div class="panel__row panel__row--divider">
                <div>
                  <h3>{{ text.executionHistoryTitle }}</h3>
                </div>
              </div>

              <div class="task-list">
                <article v-for="execution in paginatedExecutions" :key="execution.id" class="task-list__item">
                  <div class="task-list__main">
                    <div class="panel__row">
                      <h3>#{{ execution.id }}</h3>
                      <span class="pill">{{ labelExecutionStatus(execution.status) }}</span>
                    </div>
                    <div class="task-list__meta">
                      <span>{{ labelTriggerType(execution.triggerType) }}</span>
                      <span>{{ text.successTables }} {{ execution.successTableCount }} / {{ execution.plannedTableCount }}</span>
                      <span v-if="execution.insertedRowCount !== null">{{ text.insertedRows }} {{ execution.insertedRowCount }}</span>
                      <span>{{ formatDateTime(execution.startedAt) }}</span>
                    </div>
                  </div>
                  <div class="task-list__actions">
                    <button class="button button--ghost" type="button" @click="openGroupExecutionDetail(form.id!, execution.id)">
                      查看实例详情
                    </button>
                  </div>
                </article>
              </div>

              <PaginationBar
                :page="executionPage"
                :page-size="executionPageSize"
                :total="executions.length"
                :noun="text.executionUnit"
                @update:page="executionPage = $event"
                @update:page-size="executionPageSize = $event"
              />
            </section>
          </section>
        </form>
      </article>
    </section>
  </section>
</template>

<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, reactive, ref, watch } from "vue";
import { useRoute, useRouter } from "vue-router";
import { apiClient, isApiTimeoutError, readApiError, REQUEST_TIMEOUT, type ApiResponse } from "../api/client";
import KafkaHeadersEditor from "../components/KafkaHeadersEditor.vue";
import KafkaPayloadSchemaEditor from "../components/KafkaPayloadSchemaEditor.vue";
import KafkaRelationMappingEditor from "../components/KafkaRelationMappingEditor.vue";
import KafkaSchemaImportPanel from "../components/KafkaSchemaImportPanel.vue";
import PaginationBar from "../components/PaginationBar.vue";
import type { KafkaHeaderEntryDraft, KafkaSchemaImportResult } from "../utils/kafka";
import { clampPage } from "../utils/pagination";

type TaskStatus = "DRAFT" | "READY" | "RUNNING" | "PAUSED" | "DISABLED";
type TableMode = "USE_EXISTING" | "CREATE_IF_MISSING";
type WriteMode = "APPEND" | "OVERWRITE";
type GeneratorType = "SEQUENCE" | "RANDOM_INT" | "RANDOM_DECIMAL" | "STRING" | "ENUM" | "BOOLEAN" | "DATETIME" | "UUID";
type RowPlanMode = "FIXED" | "CHILD_PER_PARENT";
type SourceMode = "CURRENT_BATCH" | "TARGET_TABLE" | "MIXED";
type SelectionStrategy = "RANDOM_UNIFORM" | "PARENT_DRIVEN";
type ReusePolicy = "ALLOW_REPEAT" | "UNIQUE_ONCE";
type GroupScheduleType = "MANUAL" | "ONCE" | "CRON" | "INTERVAL";
type RelationType = "ONE_TO_ONE" | "ONE_TO_MANY" | "MANY_TO_ONE";
type RelationMode = "DATABASE_COLUMNS" | "KAFKA_EVENT";
type CronBuilderMode = "EVERY_MINUTES" | "EVERY_HOURS" | "DAILY" | "WEEKLY";
type CronWeekDay = "MON" | "TUE" | "WED" | "THU" | "FRI" | "SAT" | "SUN";
type KafkaMessageMode = "SIMPLE" | "COMPLEX";
type KafkaKeyMode = "NONE" | "FIELD" | "FIXED";
type PayloadNodeType = "OBJECT" | "ARRAY" | "SCALAR";
type PayloadValueType = "STRING" | "INT" | "LONG" | "DECIMAL" | "BOOLEAN" | "DATETIME" | "UUID";

interface ConnectionItem {
  id: number;
  name: string;
  dbType: string;
}

interface TableItem {
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
  enumValues?: string[] | null;
}

interface DatabaseForeignKey {
  constraintName: string;
  parentTable: string;
  parentColumns: string[];
  childTable: string;
  childColumns: string[];
}

interface DatabaseTableSchema {
  tableName: string;
  columns: DatabaseColumn[];
  foreignKeys: DatabaseForeignKey[];
}

interface DatabaseModel {
  tables: DatabaseTableSchema[];
  relations: DatabaseForeignKey[];
}

interface TaskColumnForm {
  localId?: string;
  columnName: string;
  dbType: string;
  lengthValue: number | null;
  precisionValue: number | null;
  scaleValue: number | null;
  nullableFlag: boolean;
  primaryKeyFlag: boolean;
  foreignKeyFlag: boolean;
  generatorType: GeneratorType;
  generatorConfig: Record<string, unknown>;
  generatorConfigText: string;
  enumValues: string[] | null;
  sortOrder: number;
}

interface TaskRowPlanForm {
  mode: RowPlanMode;
  rowCount: number | null;
  driverTaskKey: string;
  minChildrenPerParent: number | null;
  maxChildrenPerParent: number | null;
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

interface TaskForm {
  id: number | null;
  taskKey: string;
  name: string;
  tableName: string;
  tableMode: TableMode;
  writeMode: WriteMode;
  batchSize: number;
  seed: number | null;
  description: string;
  status: TaskStatus;
  rowPlan: TaskRowPlanForm;
  kafkaMessageMode: KafkaMessageMode;
  keyMode: KafkaKeyMode;
  keyField: string;
  keyPath: string;
  fixedKey: string;
  partition: number | null;
  headerEntries: KafkaHeaderEntryDraft[];
  payloadSchemaRoot: PayloadSchemaNodeDraft | null;
  columns: TaskColumnForm[];
}

interface KafkaRelationFieldMappingForm {
  localId: string;
  from: string;
  to: string;
  required: boolean;
}

interface RelationForm {
  id: number | null;
  relationName: string;
  parentTaskKey: string;
  childTaskKey: string;
  relationMode: RelationMode;
  relationType: RelationType;
  sourceMode: SourceMode;
  selectionStrategy: SelectionStrategy;
  reusePolicy: ReusePolicy;
  parentColumns: string[];
  childColumns: string[];
  nullRate: number;
  mixedExistingRatio: number | null;
  minChildrenPerParent: number | null;
  maxChildrenPerParent: number | null;
  mappingEntries: KafkaRelationFieldMappingForm[];
  sortOrder: number;
}

interface GroupForm {
  id: number | null;
  name: string;
  connectionId: number | null;
  description: string;
  seed: number | null;
  status: TaskStatus;
  scheduleType: GroupScheduleType;
  cronExpression: string;
  cronBuilderMode: CronBuilderMode;
  cronMinuteStep: number;
  cronHourStep: number;
  cronAtHour: number;
  cronAtMinute: number;
  cronWeekDay: CronWeekDay;
  triggerAt: string;
  intervalSeconds: number | null;
  maxRuns: number | null;
  maxRowsTotal: number | null;
  tasks: TaskForm[];
  relations: RelationForm[];
}

interface GroupResponse {
  id: number;
  name: string;
  connectionId: number;
  description: string | null;
  seed: number | null;
  status: TaskStatus;
  scheduleType: GroupScheduleType;
  cronExpression: string | null;
  triggerAt: string | null;
  intervalSeconds: number | null;
  maxRuns: number | null;
  maxRowsTotal: number | null;
  lastTriggeredAt: string | null;
  schedulerState: string | null;
  nextFireAt: string | null;
  previousFireAt: string | null;
  tasks: Array<{
    id: number | null;
    taskKey: string;
    name: string;
    tableName: string;
    tableMode: TableMode;
    writeMode: WriteMode;
    batchSize: number;
    seed: number | null;
    description: string | null;
    status: TaskStatus;
    rowPlan: {
      mode: RowPlanMode;
      rowCount: number | null;
      driverTaskKey: string | null;
      minChildrenPerParent: number | null;
      maxChildrenPerParent: number | null;
    };
    targetConfigJson?: string | null;
    payloadSchemaJson?: string | null;
    columns: Array<{
      columnName: string;
      dbType: string;
      lengthValue: number | null;
      precisionValue: number | null;
      scaleValue: number | null;
      nullableFlag: boolean;
      primaryKeyFlag: boolean;
      foreignKeyFlag: boolean;
      generatorType: GeneratorType;
      generatorConfig: Record<string, unknown>;
      sortOrder: number;
    }>;
  }>;
  relations: Array<{
    id: number | null;
    relationName: string;
    parentTaskKey: string;
    childTaskKey: string;
    relationMode?: RelationMode | null;
    relationType: RelationType;
    sourceMode: SourceMode;
    selectionStrategy: SelectionStrategy;
    reusePolicy: ReusePolicy;
    parentColumns: string[];
    childColumns: string[];
    nullRate: number;
    mixedExistingRatio: number | null;
    minChildrenPerParent: number | null;
    maxChildrenPerParent: number | null;
    mappingConfigJson?: string | null;
    sortOrder: number;
  }>;
}

interface PreviewTable {
  taskId: number | null;
  taskKey: string;
  taskName: string;
  tableName: string;
  generatedRowCount: number;
  previewRowCount: number;
  foreignKeyMissCount: number;
  nullViolationCount: number;
  rows: Record<string, unknown>[];
}

interface PreviewResponse {
  seed: number;
  tables: PreviewTable[];
}

interface GroupExecutionTable {
  id: number;
  tableName: string;
  status: string;
  beforeWriteRowCount: number | null;
  afterWriteRowCount: number | null;
  insertedCount: number;
  nullViolationCount: number;
  blankStringCount: number;
  fkMissCount: number;
  pkDuplicateCount: number;
  summary?: Record<string, unknown>;
}

interface GroupExecution {
  id: number;
  writeTaskGroupId?: number;
  triggerType: string;
  status: string;
  startedAt: string;
  finishedAt: string | null;
  plannedTableCount: number;
  completedTableCount?: number;
  successTableCount: number;
  failureTableCount: number;
  insertedRowCount: number | null;
  errorSummary?: string | null;
  summary: Record<string, unknown>;
  tables: GroupExecutionTable[];
}

const text = {
  pageTitle: "\u5173\u7cfb\u4efb\u52a1",
  pageSummary: "\u9009\u62e9\u8868\u7ed3\u6784\u540e\u81ea\u52a8\u751f\u6210\u7236\u5b50\u8868\u4efb\u52a1\uff0c\u652f\u6301\u9884\u89c8\u3001\u624b\u52a8\u6267\u884c\u3001\u6301\u7eed\u5199\u5165\u548c\u5b9a\u65f6\u8c03\u5ea6\u3002",
  editorSummary: "\u5148\u9009\u76ee\u6807\u8fde\u63a5\u548c\u8868\uff0c\u518d\u8c03\u6574\u5b57\u6bb5\u751f\u6210\u89c4\u5219\u3001\u5173\u7cfb\u6620\u5c04\u4e0e\u6267\u884c\u8ba1\u5212\u3002",
  refresh: "\u5237\u65b0",
  createGroup: "\u65b0\u5efa\u4efb\u52a1\u7ec4",
  backToList: "\u8fd4\u56de\u5217\u8868",
  groupListTitle: "\u4efb\u52a1\u7ec4\u5217\u8868",
  groupListHint: "\u5217\u8868\u548c\u7f16\u8f91\u9875\u5df2\u62c6\u5206\u6210\u5355\u72ec\u8def\u7531\uff0c\u67e5\u770b\u548c\u7f16\u8f91\u4e0d\u4f1a\u518d\u76f8\u4e92\u6324\u538b\u3002",
  groupUnit: "\u4e2a\u4efb\u52a1\u7ec4",
  taskUnit: "\u5f20\u8868\u4efb\u52a1",
  relationUnit: "\u6761\u5173\u7cfb",
  nextFireAt: "\u4e0b\u6b21\u89e6\u53d1\uff1a",
  edit: "\u7f16\u8f91",
  preview: "\u9884\u89c8",
  runNow: "\u7acb\u5373\u6267\u884c",
  currentMode: "\u5f53\u524d\u6a21\u5f0f",
  startContinuous: "\u542f\u52a8\u6301\u7eed\u5199\u5165",
  pauseSchedule: "\u6682\u505c",
  resumeSchedule: "\u6062\u590d",
  stopContinuous: "\u505c\u6b62",
  latestRun: "\u6700\u8fd1\u6267\u884c",
  emptyGroupTitle: "\u8fd8\u6ca1\u6709\u5173\u7cfb\u4efb\u52a1\u7ec4",
  emptyGroupHint: "\u53ef\u4ee5\u4ece\u65b0\u5efa\u4efb\u52a1\u7ec4\u5f00\u59cb\uff0c\u9009\u62e9\u76ee\u6807\u8868\u540e\u81ea\u52a8\u5bfc\u5165\u7ed3\u6784\u3002",
  basicInfoTitle: "\u57fa\u7840\u4fe1\u606f",
  basicInfoHint: "\u5148\u786e\u5b9a\u8fd9\u7ec4\u6570\u636e\u8981\u5199\u5230\u54ea\u4e2a\u76ee\u6807\u8fde\u63a5\uff0c\u4efb\u52a1\u540d\u79f0\u548c\u79cd\u5b50\u53ea\u7528\u4e8e\u8bc6\u522b\u548c\u590d\u73b0\u3002",
  groupName: "\u4efb\u52a1\u7ec4\u540d\u79f0",
  groupNamePlaceholder: "\u4f8b\u5982\uff1a\u8ba2\u5355\u4e0e\u660e\u7ec6\u6f14\u7ec3\u6570\u636e",
  connection: "\u76ee\u6807\u8fde\u63a5",
  selectConnection: "\u8bf7\u9009\u62e9\u8fde\u63a5",
  status: "\u72b6\u6001",
  seed: "\u968f\u673a\u79cd\u5b50",
  seedPlaceholder: "\u4e0d\u586b\u5219\u7531\u7cfb\u7edf\u81ea\u52a8\u751f\u6210",
  description: "\u5907\u6ce8",
  descriptionPlaceholder: "\u53ef\u9009\uff0c\u7528\u4e8e\u8bf4\u660e\u8fd9\u7ec4\u4efb\u52a1\u7684\u7528\u9014",
  scheduleTitle: "\u5199\u5165\u8ba1\u5212",
  scheduleHint: "\u53ea\u9700\u9009\u62e9\u8fd9\u7ec4\u4efb\u52a1\u662f\u624b\u52a8\u6267\u884c\u3001\u5468\u671f\u5b9a\u65f6\uff0c\u8fd8\u662f\u6301\u7eed\u5199\u5165\uff0c\u72b6\u6001\u7531\u7cfb\u7edf\u81ea\u52a8\u63a7\u5236\u3002",
  scheduleType: "\u8c03\u5ea6\u65b9\u5f0f",
  scheduleSummary: "\u5f53\u524d\u8ba1\u5212",
  cronPlanType: "\u8ba1\u5212\u7c7b\u578b",
  cronMinuteStep: "\u95f4\u9694\u5206\u949f",
  cronHourStep: "\u95f4\u9694\u5c0f\u65f6",
  cronWeekDay: "\u6267\u884c\u661f\u671f",
  cronAtHour: "\u6267\u884c\u5c0f\u65f6",
  cronAtMinute: "\u6267\u884c\u5206\u949f",
  cronExecutionHint: "\u6267\u884c\u8bf4\u660e",
  dailyExecutionHint: "\u6309\u56fa\u5b9a\u65f6\u95f4\u6267\u884c",
  minuteLabel: "\u5206",
  minuteUnit: "\u5206\u949f",
  hourUnit: "\u5c0f\u65f6",
  every: "\u6bcf",
  oClock: "\u70b9",
  legacyCronNotice: "\u5f53\u524d\u4efb\u52a1\u6cbf\u7528\u65e7\u7684 Cron \u8ba1\u5212\uff1a",
  legacyCronSuffix: "\u3002\u53ea\u8981\u4fee\u6539\u4e0a\u9762\u7684\u9009\u9879\uff0c\u7cfb\u7edf\u5c31\u4f1a\u81ea\u52a8\u5207\u6362\u4e3a\u65b0\u7684\u9009\u62e9\u5f0f\u8ba1\u5212\u3002",
  triggerAt: "\u6267\u884c\u65f6\u95f4",
  intervalSeconds: "\u95f4\u9694\u79d2\u6570",
  maxRuns: "\u6700\u5927\u6267\u884c\u6b21\u6570",
  maxRowsTotal: "\u6700\u5927\u5199\u5165\u884c\u6570",
  optionalPlaceholder: "\u53ef\u9009",
  kafkaSetupTitle: "Kafka \u4efb\u52a1\u914d\u7f6e",
  kafkaSetupHint: "\u5148\u65b0\u589e Topic \u4efb\u52a1\uff0c\u518d\u4e3a\u6bcf\u4e2a\u4efb\u52a1\u914d\u7f6e\u6d88\u606f\u5b57\u6bb5\u3001JSON \u7ed3\u6784\u548c Key / Header\u3002",
  schemaTitle: "\u5bfc\u5165\u8868\u7ed3\u6784",
  schemaHint: "\u9009\u4e2d\u591a\u5f20\u8868\u540e\u5bfc\u5165\u7ed3\u6784\uff0c\u7cfb\u7edf\u4f1a\u81ea\u52a8\u4e3a\u5b50\u8868\u751f\u6210\u5173\u7cfb\u548c\u884c\u6570\u8ba1\u5212\u9ed8\u8ba4\u503c\u3002",
  tableSelectionTitle: "\u8868\u5217\u8868",
  tableSelectionHint: "\u5df2\u9009\u62e9\u7684\u8868\u4f1a\u5728\u4e0b\u65b9\u751f\u6210\u4efb\u52a1\u4e0e\u5173\u7cfb\u3002",
  refreshTables: "\u5237\u65b0\u8868\u5217\u8868",
  importSchema: "\u5bfc\u5165\u8868\u7ed3\u6784",
  importingSchema: "\u6b63\u5728\u5bfc\u5165...",
  emptyTablesTitle: "\u6682\u65e0\u53ef\u9009\u8868",
  emptyTablesHint: "\u8bf7\u786e\u8ba4\u76ee\u6807\u8fde\u63a5\u5df2\u6b63\u5e38\u8fde\u901a\uff0c\u7136\u540e\u70b9\u51fb\u5237\u65b0\u8868\u5217\u8868\u3002",
  chooseConnectionFirst: "\u8bf7\u5148\u9009\u62e9\u76ee\u6807\u8fde\u63a5\u3002",
  kafkaTaskListTitle: "Kafka \u4efb\u52a1\u5217\u8868",
  kafkaTaskListHint: "\u4e00\u4e2a\u5173\u7cfb\u4efb\u52a1\u7ec4\u53ef\u4ee5\u5305\u542b\u591a\u4e2a Topic \u4efb\u52a1\uff0c\u4f8b\u5982\u7236\u4e8b\u4ef6\u548c\u5b50\u4e8b\u4ef6\u3002",
  kafkaTaskSummary: "Kafka \u4efb\u52a1\u6570\uff1a",
  addTask: "\u65b0\u589e\u4efb\u52a1",
  tasksTitle: "\u8868\u4efb\u52a1\u4e0e\u5b57\u6bb5\u6620\u5c04",
  tasksHint: "\u5982\u679c\u9009\u4e86\u8868\uff0c\u4e0b\u65b9\u4f1a\u6309\u7167\u8868\u7ed3\u6784\u81ea\u52a8\u6620\u5c04\u51fa\u5b57\u6bb5\u548c\u9ed8\u8ba4\u751f\u6210\u5668\u3002",
  kafkaTasksHint: "\u5982\u679c\u662f Kafka \u76ee\u6807\u7aef\uff0c\u4e0b\u65b9\u53ef\u4ee5\u76f4\u63a5\u7ef4\u62a4 Topic \u3001\u6d88\u606f\u5b57\u6bb5\u3001JSON \u7ed3\u6784\u548c\u5199\u5165\u89c4\u5219\u3002",
  emptyTasksTitle: "\u8fd8\u6ca1\u6709\u8868\u4efb\u52a1",
  emptyTasksHint: "\u5148\u9009\u62e9\u8868\uff0c\u7136\u540e\u70b9\u51fb\u201c\u5bfc\u5165\u8868\u7ed3\u6784\u201d\u3002",
  emptyKafkaTasksTitle: "\u8fd8\u6ca1\u6709 Kafka \u4efb\u52a1",
  emptyKafkaTasksHint: "\u5148\u70b9\u51fb\u201c\u65b0\u589e\u4efb\u52a1\u201d\uff0c\u7136\u540e\u4e3a Topic \u914d\u7f6e\u6d88\u606f\u5185\u5bb9\u548c\u5173\u8054\u89c4\u5219\u3002",
  removeTask: "\u79fb\u9664\u8fd9\u5f20\u8868",
  removeKafkaTask: "\u79fb\u9664\u4efb\u52a1",
  taskName: "\u4efb\u52a1\u540d\u79f0",
  targetTableName: "\u76ee\u6807\u8868",
  kafkaTopicName: "Topic \u540d\u79f0",
  kafkaTopicPlaceholder: "\u4f8b\u5982\uff1asynthetic.order.items",
  batchSize: "\u5355\u6279\u5199\u5165\u5927\u5c0f",
  writeMode: "\u5199\u5165\u6a21\u5f0f",
  writeModeAppend: "\u8ffd\u52a0",
  writeModeOverwrite: "\u8986\u76d6",
  kafkaWriteModeFixed: "Kafka \u76ee\u6807\u7aef\u56fa\u5b9a\u4e3a\u8ffd\u52a0",
  rowPlanMode: "\u884c\u6570\u8ba1\u5212",
  rowPlanFixed: "\u56fa\u5b9a\u884c\u6570",
  rowPlanChild: "\u6309\u7236\u8868\u884d\u751f",
  rowCount: "\u5199\u5165\u884c\u6570",
  driverTask: "\u7236\u8868\u4efb\u52a1",
  selectDriverTask: "\u8bf7\u9009\u62e9\u7236\u8868\u4efb\u52a1",
  kafkaMessageTitle: "Kafka \u6d88\u606f\u8bbe\u5b9a",
  kafkaMessageHint: "\u9009\u62e9\u7b80\u5355\u5b57\u6bb5\u6216\u590d\u6742 JSON \u6a21\u5f0f\uff0c\u540c\u65f6\u914d\u7f6e Key\u3001Header \u548c\u5206\u533a\u3002",
  kafkaSimpleMode: "\u7b80\u5355\u5b57\u6bb5",
  kafkaComplexMode: "\u590d\u6742 JSON",
  kafkaKeyMode: "Key \u6a21\u5f0f",
  kafkaNoKey: "\u4e0d\u8bbe\u7f6e Key",
  kafkaFieldKey: "\u4f7f\u7528\u5b57\u6bb5",
  kafkaFixedKey: "\u4f7f\u7528\u56fa\u5b9a\u503c",
  kafkaPartition: "\u6307\u5b9a\u5206\u533a",
  kafkaKeyField: "Key \u5b57\u6bb5",
  kafkaKeyPath: "Key \u8def\u5f84",
  selectField: "\u8bf7\u9009\u62e9\u5b57\u6bb5",
  kafkaFixedKeyValue: "\u56fa\u5b9a Key",
  kafkaFixedKeyPlaceholder: "\u4f8b\u5982\uff1aorder-stream",
  kafkaPayloadTitle: "\u6d88\u606f\u7ed3\u6784",
  kafkaPayloadHint: "\u53ef\u4ee5\u76f4\u63a5\u7f16\u6392 JSON \u7ed3\u6784\uff0c\u4e5f\u53ef\u4ee5\u7ed8\u5165\u793a\u4f8b JSON / JSON Schema \u5feb\u901f\u751f\u6210\u3002",
  availableFieldUnit: "\u4e2a\u53ef\u7528\u8def\u5f84",
  kafkaFieldsTitle: "\u6d88\u606f\u5b57\u6bb5",
  kafkaFieldsHint: "\u9002\u5408\u5e73\u94fa\u7ed3\u6784\u7684\u6d88\u606f\uff0c\u53ef\u76f4\u63a5\u4e3a\u6bcf\u4e2a\u5b57\u6bb5\u6307\u5b9a\u751f\u6210\u89c4\u5219\u3002",
  addField: "\u65b0\u589e\u5b57\u6bb5",
  removeField: "\u5220\u9664\u5b57\u6bb5",
  fieldNamePlaceholder: "\u4f8b\u5982\uff1aorderId",
  minChildren: "\u6bcf\u4e2a\u7236\u8bb0\u5f55\u6700\u5c11\u5b50\u884c",
  maxChildren: "\u6bcf\u4e2a\u7236\u8bb0\u5f55\u6700\u591a\u5b50\u884c",
  primaryKey: "\u4e3b\u952e",
  foreignKey: "\u5916\u952e",
  notNull: "\u975e\u7a7a",
  nullable: "\u53ef\u4e3a\u7a7a",
  columnName: "\u5b57\u6bb5\u540d",
  columnType: "\u6570\u636e\u7c7b\u578b",
  columnFlags: "\u7ea6\u675f\u6807\u8bb0",
  actionColumn: "\u64cd\u4f5c",
  generatorType: "\u751f\u6210\u5668",
  generatorConfig: "\u751f\u6210\u914d\u7f6e (JSON)",
  generatorRule: "\u751f\u6210\u89c4\u5219",
  advancedConfig: "\u9ad8\u7ea7\u914d\u7f6e",
  configAction: "\u914d\u7f6e",
  saveAction: "\u4fdd\u5b58",
  cancelAction: "\u53d6\u6d88",
  removeAction: "\u5220\u9664",
  unnamedField: "\u672a\u547d\u540d\u5b57\u6bb5",
  relationsTitle: "\u5173\u7cfb\u914d\u7f6e",
  relationsHint: "\u5982\u679c\u81ea\u52a8\u8bc6\u522b\u7684\u5173\u7cfb\u4e0d\u591f\uff0c\u53ef\u4ee5\u5728\u8fd9\u91cc\u589e\u8865\u6216\u4fee\u6539\u3002",
  kafkaRelationsHint: "Kafka \u5173\u7cfb\u4efb\u52a1\u4f7f\u7528\u4e8b\u4ef6\u5b57\u6bb5\u6620\u5c04\uff0c\u628a\u7236\u4efb\u52a1\u751f\u6210\u7684\u5b57\u6bb5\u5199\u5165\u5230\u5b50\u4efb\u52a1\u6d88\u606f\u8def\u5f84\u3002",
  relationConfigTitle: "\u7236\u5b50\u8868\u5173\u7cfb",
  relationConfigHint: "\u76ee\u524d\u754c\u9762\u4ee5\u5355\u5217\u5bf9\u5355\u5217\u7684\u5173\u7cfb\u6620\u5c04\u4e3a\u4e3b\uff0c\u80fd\u6ee1\u8db3\u5927\u591a\u6570\u5e38\u89c1\u573a\u666f\u3002",
  addRelation: "\u65b0\u589e\u5173\u7cfb",
  removeRelation: "\u5220\u9664\u5173\u7cfb",
  emptyRelationsTitle: "\u6682\u65e0\u5173\u7cfb",
  emptyRelationsHint: "\u5982\u679c\u9009\u4e2d\u7684\u8868\u4e4b\u95f4\u6709\u5916\u952e\uff0c\u7cfb\u7edf\u5df2\u5c3d\u91cf\u81ea\u52a8\u8bc6\u522b\u3002",
  relationName: "\u5173\u7cfb\u540d\u79f0",
  relationType: "\u5173\u7cfb\u7c7b\u578b",
  oneToOne: "\u4e00\u5bf9\u4e00",
  oneToMany: "\u4e00\u5bf9\u591a",
  manyToOne: "\u591a\u5bf9\u4e00",
  parentTask: "\u7236\u8868\u4efb\u52a1",
  childTask: "\u5b50\u8868\u4efb\u52a1",
  selectParentTask: "\u8bf7\u9009\u62e9\u7236\u8868\u4efb\u52a1",
  selectChildTask: "\u8bf7\u9009\u62e9\u5b50\u8868\u4efb\u52a1",
  parentColumn: "\u7236\u8868\u5173\u8054\u5217",
  childColumn: "\u5b50\u8868\u5173\u8054\u5217",
  selectParentColumn: "\u8bf7\u9009\u62e9\u7236\u8868\u5217",
  selectChildColumn: "\u8bf7\u9009\u62e9\u5b50\u8868\u5217",
  sourceMode: "\u5173\u8054\u6570\u636e\u6765\u6e90",
  sourceCurrentBatch: "\u5f53\u524d\u6279\u6b21",
  sourceTargetTable: "\u76ee\u6807\u8868\u5df2\u6709\u6570\u636e",
  sourceMixed: "\u6df7\u5408",
  selectionStrategy: "\u5339\u914d\u7b56\u7565",
  selectionRandom: "\u968f\u673a\u5747\u5300",
  selectionParentDriven: "\u6309\u7236\u8868\u9a71\u52a8",
  reusePolicy: "\u590d\u7528\u7b56\u7565",
  reuseRepeat: "\u5141\u8bb8\u91cd\u590d",
  reuseUnique: "\u552f\u4e00\u4e00\u6b21",
  nullRate: "\u7a7a\u503c\u6bd4\u4f8b",
  mixedRatio: "\u4ece\u5df2\u6709\u6570\u636e\u4e2d\u53d6\u503c\u7684\u6bd4\u4f8b",
  executionTitle: "\u9884\u89c8\u4e0e\u6267\u884c",
  executionHint: "\u4fdd\u5b58\u540e\u53ef\u4ee5\u7acb\u5373\u6267\u884c\uff0c\u4e5f\u53ef\u4ee5\u5148\u9884\u89c8\u6570\u636e\u7ed3\u679c\u548c\u5173\u7cfb\u6574\u4f53\u6548\u679c\u3002",
  previewResultTitle: "\u9884\u89c8\u7ed3\u679c",
  previewResultHint: "\u9884\u89c8\u53ea\u5c55\u793a\u524d\u51e0\u884c\u548c\u5173\u952e\u6821\u9a8c\u7ed3\u679c\uff0c\u4e0d\u4f1a\u771f\u6b63\u5199\u5165\u5230\u76ee\u6807\u7aef\u3002",
  executionResultTitle: "\u672c\u6b21\u5199\u5165\u7ed3\u679c",
  waitingContinuousResult: "\u6301\u7eed\u5199\u5165\u542f\u52a8\u540e\uff0c\u8fd9\u91cc\u4f1a\u81ea\u52a8\u5237\u65b0\u6700\u65b0\u5199\u5165\u60c5\u51b5\u3002",
  executionPendingHint: "\u4efb\u52a1\u5df2\u63d0\u4ea4\uff0c\u540e\u53f0\u6b63\u5728\u5199\u5165\uff0c\u660e\u7ec6\u7ed3\u679c\u4f1a\u81ea\u52a8\u5237\u65b0\u3002",
  executionSubmitted: "\u4efb\u52a1\u5df2\u63d0\u4ea4\uff0c\u540e\u53f0\u4ecd\u5728\u6267\u884c\uff0c\u6b63\u5728\u81ea\u52a8\u5237\u65b0\u7ed3\u679c\u3002",
  executionFinished: "\u6267\u884c\u5df2\u5b8c\u6210\uff0c\u7ed3\u679c\u5df2\u5237\u65b0\u3002",
  executionFailedPrefix: "\u6267\u884c\u5931\u8d25\uff1a",
  saving: "\u6b63\u5728\u4fdd\u5b58...",
  saveGroup: "\u4fdd\u5b58\u4efb\u52a1\u7ec4",
  previewing: "\u6b63\u5728\u9884\u89c8...",
  running: "\u6b63\u5728\u6267\u884c...",
  generatedRows: "\u751f\u6210\u884c\u6570\uff1a",
  previewRows: "\uff0c\u9884\u89c8\u884c\u6570\uff1a",
  foreignKeyMisses: "\uff0c\u5916\u952e\u672a\u547d\u4e2d\uff1a",
  nullViolations: "\uff0c\u975e\u7a7a\u7ea6\u675f\u51b2\u7a81\uff1a",
  lastExecutionTitle: "\u6700\u8fd1\u4e00\u6b21\u6267\u884c",
  executionStatus: "\u6267\u884c\u72b6\u6001\uff1a",
  triggerType: "\u89e6\u53d1\u65b9\u5f0f\uff1a",
  insertedRows: "\u5199\u5165\u884c\u6570\uff1a",
  insertedShort: "\u63d2\u5165",
  beforeCount: "\u5199\u5165\u524d",
  afterCount: "\u5199\u5165\u540e",
  nullShort: "\u7a7a\u503c",
  blankShort: "\u7a7a\u4e32",
  fkShort: "\u5916\u952e",
  pkShort: "\u4e3b\u952e",
  validationResult: "\u6821\u9a8c\u7ed3\u679c",
  executionErrorLabel: "\u9519\u8bef\uff1a",
  table: "\u8868",
  beforeAfter: "\u5199\u5165\u524d / \u5199\u5165\u540e",
  executionMetrics: "\u6821\u9a8c\u6307\u6807",
  executionHistoryTitle: "\u6267\u884c\u8bb0\u5f55",
  executionHistoryHint: "\u624b\u52a8\u6267\u884c\u3001\u5468\u671f\u8c03\u5ea6\u548c\u6301\u7eed\u5199\u5165\u7684\u7ed3\u679c\u90fd\u4f1a\u5728\u8fd9\u91cc\u7559\u75d5\u3002",
  successTables: "\u6210\u529f\u8868\uff1a",
  failedTables: "\u5931\u8d25\u8868\uff1a",
  refreshRuntime: "\u7acb\u5373\u5237\u65b0",
  pauseAutoRefresh: "\u6682\u505c\u81ea\u52a8\u5237\u65b0",
  resumeAutoRefresh: "\u6062\u590d\u81ea\u52a8\u5237\u65b0",
  lastRefreshAt: "\u6700\u8fd1\u5237\u65b0",
  executionUnit: "\u6761\u6267\u884c\u8bb0\u5f55"
} as const;

const scheduleTypeLabels: Record<GroupScheduleType, string> = {
  MANUAL: "\u624b\u52a8\u6267\u884c",
  ONCE: "\u5355\u6b21\u5b9a\u65f6",
  CRON: "\u5468\u671f\u5b9a\u65f6",
  INTERVAL: "\u6301\u7eed\u5199\u5165"
};

const schedulerStateLabels: Record<string, string> = {
  MANUAL: "\u624b\u52a8",
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
  RUNNING: "\u8fd0\u884c\u4e2d",
  STOPPED: "\u5df2\u505c\u6b62"
};

const statusLabels: Record<TaskStatus, string> = {
  DRAFT: "\u8349\u7a3f",
  READY: "\u5c31\u7eea",
  RUNNING: "\u8fd0\u884c\u4e2d",
  PAUSED: "\u5df2\u6682\u505c",
  DISABLED: "\u5df2\u7981\u7528"
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

const databaseTypeLabels: Record<string, string> = {
  MYSQL: "MySQL",
  POSTGRESQL: "PostgreSQL",
  SQLSERVER: "SQL Server",
  ORACLE: "Oracle",
  KAFKA: "Kafka"
};

const generatorTypeLabels: Record<GeneratorType, string> = {
  SEQUENCE: "\u9012\u589e\u5e8f\u5217",
  RANDOM_INT: "\u968f\u673a\u6574\u6570",
  RANDOM_DECIMAL: "\u968f\u673a\u5c0f\u6570",
  STRING: "\u5b57\u7b26\u4e32",
  ENUM: "\u679a\u4e3e",
  BOOLEAN: "\u5e03\u5c14",
  DATETIME: "\u65e5\u671f\u65f6\u95f4",
  UUID: "UUID"
};

const scheduleOptions: GroupScheduleType[] = ["MANUAL", "CRON", "INTERVAL"];
const cronBuilderModeOptions: Array<{ value: CronBuilderMode; label: string }> = [
  { value: "EVERY_MINUTES", label: "\u6bcf\u9694\u51e0\u5206\u949f" },
  { value: "EVERY_HOURS", label: "\u6bcf\u9694\u51e0\u5c0f\u65f6" },
  { value: "DAILY", label: "\u6bcf\u5929\u56fa\u5b9a\u65f6\u95f4" },
  { value: "WEEKLY", label: "\u6bcf\u5468\u56fa\u5b9a\u65f6\u95f4" }
];
const cronMinuteStepOptions = [5, 10, 15, 30];
const cronHourStepOptions = [1, 2, 4, 6, 12];
const cronWeekDayOptions: Array<{ value: CronWeekDay; label: string }> = [
  { value: "MON", label: "\u5468\u4e00" },
  { value: "TUE", label: "\u5468\u4e8c" },
  { value: "WED", label: "\u5468\u4e09" },
  { value: "THU", label: "\u5468\u56db" },
  { value: "FRI", label: "\u5468\u4e94" },
  { value: "SAT", label: "\u5468\u516d" },
  { value: "SUN", label: "\u5468\u65e5" }
];
const cronHourOptions = Array.from({ length: 24 }, (_, value) => value);
const cronMinuteOptions = Array.from({ length: 60 }, (_, value) => value);
const generatorOptions: GeneratorType[] = [
  "SEQUENCE",
  "RANDOM_INT",
  "RANDOM_DECIMAL",
  "STRING",
  "ENUM",
  "BOOLEAN",
  "DATETIME",
  "UUID"
];
const kafkaSimpleFieldTypes = ["BIGINT", "INT", "DECIMAL", "VARCHAR", "BOOLEAN", "TIMESTAMP", "UUID"] as const;

const route = useRoute();
const router = useRouter();

const loading = ref(false);
const saving = ref(false);
const previewing = ref(false);
const running = ref(false);
const mutatingGroupId = ref<number | null>(null);
const importingSchema = ref(false);
const refreshingRuntime = ref(false);
const autoRefreshEnabled = ref(true);
const connections = ref<ConnectionItem[]>([]);
const availableTables = ref<TableItem[]>([]);
const selectedTableNames = ref<string[]>([]);
const groups = ref<GroupResponse[]>([]);
const previewData = ref<PreviewResponse | null>(null);
const executions = ref<GroupExecution[]>([]);
const lastExecution = ref<GroupExecution | null>(null);
const lastRuntimeRefreshAt = ref<string | null>(null);
const listExecutionByGroup = ref<Record<number, GroupExecution>>({});
const optimisticExecution = ref<GroupExecution | null>(null);
const optimisticExecutionGroupId = ref<number | null>(null);
const feedback = reactive({ kind: "success" as "success" | "error", message: "" });
const editingTaskFieldLocalId = ref<string | null>(null);
const editingTaskFieldNameDraft = ref("");
const taskFieldNameInputRefs = reactive<Record<string, HTMLInputElement | null>>({});
const groupPage = ref(1);
const groupPageSize = ref(8);
const executionPage = ref(1);
const executionPageSize = ref(5);
const cronExpressionMode = ref<"BUILDER" | "LEGACY">("BUILDER");

const form = reactive<GroupForm>(createEmptyForm());

const isListPage = computed(() => route.name === "relational-write-tasks");
const isCreatePage = computed(() => route.name === "relational-write-task-create");
const isEditPage = computed(() => route.name === "relational-write-task-edit");
const selectedConnection = computed(() => (
  form.connectionId === null
    ? null
    : connections.value.find((item) => item.id === form.connectionId) ?? null
));
const isKafkaGroup = computed(() => selectedConnection.value?.dbType === "KAFKA");
const routedGroupId = computed(() => {
  const raw = Number(route.params.id);
  return Number.isInteger(raw) && raw > 0 ? raw : null;
});
const paginatedGroups = computed(() => {
  const start = (groupPage.value - 1) * groupPageSize.value;
  return groups.value.slice(start, start + groupPageSize.value);
});
const paginatedExecutions = computed(() => {
  const start = (executionPage.value - 1) * executionPageSize.value;
  return executions.value.slice(start, start + executionPageSize.value);
});
const canMutateScheduling = computed(() => !saving.value && !previewing.value && !running.value && mutatingGroupId.value === null);
const hasActiveExecution = computed(() => isActiveExecutionStatus(lastExecution.value?.status));
const runtimeRefreshIntervalMs = computed(() => (hasActiveExecution.value || optimisticExecutionGroupId.value === form.id ? 3000 : 5000));
const showRuntimeRefreshControls = computed(() => (
  !isListPage.value && form.id !== null && (form.scheduleType === "INTERVAL" || hasActiveExecution.value || optimisticExecutionGroupId.value === form.id)
));
const autoRefreshLabel = computed(() => {
  if (!showRuntimeRefreshControls.value) {
    return "\u624b\u52a8\u5237\u65b0";
  }
  return autoRefreshEnabled.value
    ? `\u81ea\u52a8\u5237\u65b0 ${Math.max(1, Math.floor(runtimeRefreshIntervalMs.value / 1000))} \u79d2`
    : "\u81ea\u52a8\u5237\u65b0\u5df2\u6682\u505c";
});
const isContinuousRuntimeVisible = computed(
  () => form.scheduleType === "INTERVAL" && form.id !== null && (form.status === "RUNNING" || executions.value.length > 0)
);
const cronPreviewExpression = computed(() => (
  form.scheduleType !== "CRON"
    ? ""
    : (cronExpressionMode.value === "LEGACY" ? form.cronExpression.trim() : buildCronExpressionFromBuilder())
));
const formScheduleSummary = computed(() => ({
  scheduleType: form.scheduleType,
  triggerAt: form.triggerAt ? toIsoDateTime(form.triggerAt) : null,
  cronExpression: cronPreviewExpression.value,
  intervalSeconds: form.intervalSeconds,
  maxRuns: form.maxRuns,
  maxRowsTotal: form.maxRowsTotal
}));

let runtimeRefreshTimer: number | null = null;
let nextLocalId = 1;

function createEmptyForm(): GroupForm {
  return {
    id: null,
    name: "",
    connectionId: null,
    description: "",
    seed: null,
    status: "READY",
    scheduleType: "MANUAL",
    cronExpression: "",
    cronBuilderMode: "DAILY",
    cronMinuteStep: 5,
    cronHourStep: 1,
    cronAtHour: 9,
    cronAtMinute: 0,
    cronWeekDay: "MON",
    triggerAt: "",
    intervalSeconds: 10,
    maxRuns: null,
    maxRowsTotal: null,
    tasks: [],
    relations: []
  };
}

function resetForm() {
  Object.assign(form, createEmptyForm());
  cronExpressionMode.value = "BUILDER";
  selectedTableNames.value = [];
  availableTables.value = [];
  previewData.value = null;
  executions.value = [];
  lastExecution.value = null;
  lastRuntimeRefreshAt.value = null;
  clearOptimisticExecution();
  stopAutoRefresh();
  clearFeedback();
}

function clearFeedback() {
  feedback.message = "";
}

function setFeedback(kind: "success" | "error", message: string) {
  feedback.kind = kind;
  feedback.message = message;
}

function ensureTaskFieldLocalId(column: TaskColumnForm) {
  if (!column.localId) {
    column.localId = `task-column-${nextLocalId++}`;
  }
  return column.localId;
}

function fieldDisplayName(column: TaskColumnForm, index: number) {
  const value = column.columnName.trim();
  return value || `field_${index + 1}`;
}

function isTaskFieldNameEditing(column: TaskColumnForm) {
  return editingTaskFieldLocalId.value !== null && editingTaskFieldLocalId.value === column.localId;
}

function setTaskFieldNameInputRef(column: TaskColumnForm, element: unknown) {
  const localId = ensureTaskFieldLocalId(column);
  taskFieldNameInputRefs[localId] = element instanceof HTMLInputElement ? element : null;
}

function cancelTaskFieldNameEdit() {
  editingTaskFieldLocalId.value = null;
  editingTaskFieldNameDraft.value = "";
}

function startTaskFieldNameEdit(column: TaskColumnForm, index: number) {
  const localId = ensureTaskFieldLocalId(column);
  editingTaskFieldLocalId.value = localId;
  editingTaskFieldNameDraft.value = fieldDisplayName(column, index);
  void nextTick(() => {
    taskFieldNameInputRefs[localId]?.focus();
    taskFieldNameInputRefs[localId]?.select();
  });
}

function confirmTaskFieldNameEdit(column: TaskColumnForm) {
  const nextName = editingTaskFieldNameDraft.value.trim();
  if (!nextName) {
    setFeedback("error", "字段名不能为空");
    return;
  }
  const previousName = column.columnName.trim() || "field";
  column.columnName = nextName;
  if (column.generatorType === "STRING") {
    const currentPrefix = typeof column.generatorConfig?.prefix === "string" ? column.generatorConfig.prefix : "";
    if (!currentPrefix || currentPrefix === `${previousName}_`) {
      column.generatorConfig = {
        ...column.generatorConfig,
        prefix: `${nextName}_`
      };
      column.generatorConfigText = serializeConfig(column.generatorConfig);
    }
  }
  cancelTaskFieldNameEdit();
}

function labelStatus(value: TaskStatus) {
  return statusLabels[value] ?? value;
}

function labelExecutionStatus(value: string | null | undefined) {
  if (!value) {
    return "-";
  }
  return executionStatusLabels[value] ?? value;
}

function isActiveExecutionStatus(value: string | null | undefined) {
  return value === "PENDING" || value === "RUNNING";
}

function isTerminalExecutionStatus(value: string | null | undefined) {
  return Boolean(value) && !isActiveExecutionStatus(value);
}

function clearOptimisticExecution(groupId?: number) {
  if (groupId !== undefined && optimisticExecutionGroupId.value !== groupId) {
    return;
  }
  optimisticExecution.value = null;
  optimisticExecutionGroupId.value = null;
}

function parseExecutionTimestamp(value: string | null | undefined) {
  if (!value) {
    return null;
  }
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function matchesOptimisticExecution(remote: GroupExecution, preferred: GroupExecution) {
  if (remote.id === preferred.id) {
    return true;
  }
  const preferredAt = parseExecutionTimestamp(preferred.startedAt);
  const remoteAt = parseExecutionTimestamp(remote.startedAt);
  if (preferredAt === null || remoteAt === null) {
    return false;
  }
  return remote.triggerType === preferred.triggerType && remoteAt >= preferredAt - 2000;
}

function createOptimisticExecution(groupId: number, startedAt: string, plannedTableCount: number) {
  return {
    id: -Date.now(),
    writeTaskGroupId: groupId,
    triggerType: "MANUAL",
    status: "PENDING",
    startedAt,
    finishedAt: null,
    plannedTableCount,
    completedTableCount: 0,
    successTableCount: 0,
    failureTableCount: 0,
    insertedRowCount: 0,
    errorSummary: null,
    summary: { optimistic: true },
    tables: []
  } as GroupExecution;
}

function labelTriggerType(value: string | null | undefined) {
  if (!value) {
    return "-";
  }
  return triggerTypeLabels[value] ?? value;
}

function labelScheduleType(value: GroupScheduleType) {
  return scheduleTypeLabels[value] ?? value;
}

function labelSchedulerState(value: string | null | undefined) {
  if (!value) {
    return "-";
  }
  return schedulerStateLabels[value] ?? value;
}

function labelDatabaseType(value: string | null | undefined) {
  if (!value) {
    return "-";
  }
  return databaseTypeLabels[value] ?? value;
}

function labelGeneratorType(value: GeneratorType) {
  return generatorTypeLabels[value] ?? value;
}

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

function formatOptionalDate(value: string | null | undefined) {
  if (!value) {
    return "-";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return date.toLocaleString("zh-CN", { hour12: false });
}

function formatDateTime(value: string) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return date.toLocaleString("zh-CN", { hour12: false });
}

function toDateTimeLocal(value: string | null | undefined) {
  if (!value) {
    return "";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "";
  }
  const offset = date.getTimezoneOffset();
  const localDate = new Date(date.getTime() - offset * 60_000);
  return localDate.toISOString().slice(0, 16);
}

function toIsoDateTime(value: string) {
  if (!value) {
    return null;
  }
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? null : date.toISOString();
}

function describeSchedule(group: Pick<GroupResponse | GroupForm, "scheduleType" | "triggerAt" | "cronExpression" | "intervalSeconds" | "maxRuns" | "maxRowsTotal">) {
  if (group.scheduleType === "ONCE") {
    return group.triggerAt ? `${text.triggerAt} ${formatOptionalDate(group.triggerAt)}` : "\u672a\u8bbe\u7f6e";
  }
  if (group.scheduleType === "CRON") {
    return describeCronExpression(group.cronExpression);
  }
  if (group.scheduleType === "INTERVAL") {
    const limits: string[] = [];
    if (group.maxRuns) {
      limits.push(`\u6700\u591a ${group.maxRuns} \u6b21`);
    }
    if (group.maxRowsTotal) {
      limits.push(`\u7d2f\u8ba1 ${group.maxRowsTotal} \u884c`);
    }
    return `\u6bcf ${group.intervalSeconds ?? 0} \u79d2\u4e00\u6b21${limits.length ? ` / ${limits.join(" / ")}` : ""}`;
  }
  return "\u624b\u52a8\u89e6\u53d1";
}

function describeCronExpression(expression: string | null | undefined) {
  const parsed = parseCronExpression(expression);
  if (!expression || !expression.trim()) {
    return "\u672a\u8bbe\u7f6e";
  }
  if (!parsed) {
    return `Cron\uff1a${expression}`;
  }
  switch (parsed.mode) {
    case "EVERY_MINUTES":
      return `\u6bcf ${parsed.minuteStep} \u5206\u949f\u6267\u884c\u4e00\u6b21`;
    case "EVERY_HOURS":
      return `\u6bcf ${parsed.hourStep} \u5c0f\u65f6\u5728 ${String(parsed.minute).padStart(2, "0")} \u5206\u6267\u884c\u4e00\u6b21`;
    case "DAILY":
      return `\u6bcf\u5929 ${String(parsed.hour).padStart(2, "0")}:${String(parsed.minute).padStart(2, "0")} \u6267\u884c`;
    case "WEEKLY":
      return `${cronWeekDayOptions.find((option) => option.value === parsed.weekDay)?.label ?? parsed.weekDay} ${String(parsed.hour).padStart(2, "0")}:${String(parsed.minute).padStart(2, "0")} \u6267\u884c`;
  }
}

function describeScheduleMode(value: GroupScheduleType) {
  switch (value) {
    case "MANUAL":
      return "\u9700\u8981\u65f6\u624b\u52a8\u70b9\u51fb\u6267\u884c\uff0c\u7ed3\u679c\u7acb\u5373\u8fd4\u56de";
    case "CRON":
      return "\u6309\u9009\u62e9\u7684\u5468\u671f\u81ea\u52a8\u89e6\u53d1\uff0c\u4e5f\u53ef\u4ee5\u968f\u65f6\u8865\u6267\u884c";
    case "INTERVAL":
      return "\u6309\u56fa\u5b9a\u95f4\u9694\u6301\u7eed\u5199\u5165\uff0c\u5e76\u81ea\u52a8\u5237\u65b0\u5199\u5165\u60c5\u51b5";
    case "ONCE":
      return "\u5355\u6b21\u5b9a\u65f6\u8fd0\u884c";
  }
}

function setScheduleType(value: GroupScheduleType) {
  form.scheduleType = value;
  form.status = value === "INTERVAL" ? "READY" : "READY";
  if (value !== "CRON") {
    form.cronExpression = "";
    resetCronBuilder();
  } else if (!form.cronExpression.trim()) {
    handleCronBuilderChange();
  }
  if (value !== "ONCE") {
    form.triggerAt = "";
  }
  if (value !== "INTERVAL") {
    form.intervalSeconds = 10;
    form.maxRuns = null;
    form.maxRowsTotal = null;
  }
}

function canStartContinuous(group: GroupResponse) {
  return group.scheduleType === "INTERVAL" && (group.status === "READY" || group.schedulerState === "STOPPED");
}

function canPauseGroup(group: GroupResponse) {
  if (group.scheduleType === "MANUAL") {
    return false;
  }
  if (group.schedulerState === "STOPPED") {
    return false;
  }
  return group.status === "READY" || group.status === "RUNNING";
}

function canResumeGroup(group: GroupResponse) {
  return group.scheduleType !== "MANUAL" && group.status === "PAUSED";
}

function canStopContinuous(group: GroupResponse) {
  return group.scheduleType === "INTERVAL" && (group.status === "RUNNING" || group.status === "PAUSED");
}

function tableKey(table: TableItem) {
  return table.schemaName ? `${table.schemaName}.${table.tableName}` : table.tableName;
}

function formatTableName(table: TableItem) {
  return tableKey(table);
}

function resolveModelTableName(tableName: string, availableTableNames: string[]) {
  if (availableTableNames.includes(tableName)) {
    return tableName;
  }
  const suffix = `.${tableName}`;
  const matches = availableTableNames.filter((item) => item === tableName || item.endsWith(suffix));
  return matches.length === 1 ? matches[0] : tableName;
}

function normalizeTaskKey(tableName: string) {
  return tableName.replace(/[^a-zA-Z0-9_]/g, "_").toLowerCase();
}

function displayColumnType(column: Pick<TaskColumnForm, "dbType" | "lengthValue" | "precisionValue" | "scaleValue">) {
  if (column.lengthValue !== null && column.lengthValue !== undefined) {
    return `${column.dbType}(${column.lengthValue})`;
  }
  if (
    column.precisionValue !== null &&
    column.precisionValue !== undefined &&
    column.scaleValue !== null &&
    column.scaleValue !== undefined
  ) {
    return `${column.dbType}(${column.precisionValue}, ${column.scaleValue})`;
  }
  if (column.precisionValue !== null && column.precisionValue !== undefined) {
    return `${column.dbType}(${column.precisionValue})`;
  }
  return column.dbType;
}

function summarizeGeneratorConfig(column: TaskColumnForm) {
  const config = column.generatorConfig ?? {};
  switch (column.generatorType) {
    case "SEQUENCE":
      return `start=${config.start ?? 1}, step=${config.step ?? 1}`;
    case "RANDOM_INT":
      return `${config.min ?? 1} ~ ${config.max ?? 1000}`;
    case "RANDOM_DECIMAL":
      return `${config.min ?? 1} ~ ${config.max ?? 1000}, scale=${config.scale ?? column.scaleValue ?? 2}`;
    case "STRING":
      return `prefix=${config.prefix ?? `${column.columnName}_`}, length=${config.length ?? 12}`;
    case "ENUM":
      return Array.isArray(config.values) ? `values=${(config.values as unknown[]).join(", ")}` : "values=option_1, option_2";
    case "BOOLEAN":
      return `trueRate=${config.trueRate ?? 0.5}`;
    case "DATETIME":
      return "\u6309\u65f6\u95f4\u8303\u56f4\u968f\u673a\u751f\u6210";
    case "UUID":
      return "UUID \u81ea\u52a8\u751f\u6210";
    default:
      return "\u4f7f\u7528\u9ed8\u8ba4\u89c4\u5219";
  }
}

function serializeConfig(config: Record<string, unknown>) {
  return JSON.stringify(config, null, 2);
}

function buildDefaultGeneratorConfig(
  columnName: string,
  generatorType: GeneratorType,
  scale: number | null,
  enumValues: string[] | null,
  dbType = ""
) {
  switch (generatorType) {
    case "SEQUENCE":
      return { start: 1, step: 1 };
    case "ENUM":
      return { values: enumValues && enumValues.length ? enumValues : ["option_1", "option_2"] };
    case "RANDOM_INT":
      return { min: 1, max: 1000 };
    case "RANDOM_DECIMAL":
      return { min: 1, max: 1000, scale: scale ?? 2 };
    case "BOOLEAN":
      return { trueRate: 0.5 };
    case "DATETIME":
      if (dbType.trim().toUpperCase() === "DATE") {
        return { dateOnly: true };
      }
      return {};
    case "UUID":
      return {};
    case "STRING":
    default:
      return { prefix: `${columnName}_`, length: 12 };
  }
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
    generatorConfigJson: partial?.generatorConfigJson ?? JSON.stringify(buildDefaultGeneratorConfig(partial?.name ?? "field", generatorType, null, null), null, 2),
    children: [],
    itemSchema: null,
    minItems: null,
    maxItems: null
  };
}

function createDefaultPayloadSchemaRoot() {
  return createPayloadSchemaNode("OBJECT", {
    children: [
      createPayloadSchemaNode("SCALAR", {
        name: "id",
        valueType: "LONG",
        generatorType: "SEQUENCE",
        generatorConfigJson: JSON.stringify({ start: 1, step: 1 }, null, 2)
      })
    ]
  });
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

function parsePayloadSchemaJson(value: string | null | undefined) {
  if (!value) {
    return createDefaultPayloadSchemaRoot();
  }
  try {
    return parsePayloadSchemaNode(JSON.parse(value) as Record<string, unknown>, true);
  } catch {
    return createDefaultPayloadSchemaRoot();
  }
}

function normalizeJson(value: string, label: string) {
  try {
    return JSON.stringify(JSON.parse(value), null, 2);
  } catch {
    throw new Error(`${label} 必须是合法的 JSON`);
  }
}

function normalizePayloadGeneratorConfig(node: PayloadSchemaNodeDraft, label: string) {
  const configText = node.generatorConfigJson?.trim() ? node.generatorConfigJson : "{}";
  return JSON.parse(normalizeJson(configText, `${label} 生成规则`));
}

function serializePayloadSchemaNode(
  node: PayloadSchemaNodeDraft,
  isRoot = false,
  isArrayItem = false
): Record<string, unknown> {
  if (!isRoot && !isArrayItem && !node.name.trim()) {
    throw new Error("复杂消息模式下，每个字段都必须填写名称");
  }
  if (node.type === "OBJECT") {
    if (!node.children.length) {
      throw new Error(`${node.name || "消息结构"} 至少需要一个子字段`);
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
      throw new Error(`${node.name || "数组字段"} 必须定义元素结构`);
    }
    const minItems = sanitizeOptionalNumber(node.minItems);
    const maxItems = sanitizeOptionalNumber(node.maxItems);
    if (minItems !== null && minItems < 0) {
      throw new Error(`${node.name || "数组字段"} 的最少元素数不能小于 0`);
    }
    if (minItems !== null && maxItems !== null && maxItems < minItems) {
      throw new Error(`${node.name || "数组字段"} 的最多元素数不能小于最少元素数`);
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
  return {
    ...(isRoot || isArrayItem ? {} : { name: node.name.trim() }),
    type: "SCALAR",
    nullable: node.nullable,
    valueType: node.valueType,
    generatorType: node.generatorType,
    generatorConfig: normalizePayloadGeneratorConfig(node, node.name || "标量字段")
  };
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

function sanitizeOptionalNumber(value: number | null | undefined) {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function defaultGeneratorType(column: DatabaseColumn): GeneratorType {
  const type = column.dbType.toUpperCase();
  if (column.primaryKey || column.autoIncrement) {
    return "SEQUENCE";
  }
  if (type === "ENUM") {
    return "ENUM";
  }
  if (type.includes("UUID")) {
    return "UUID";
  }
  if (type.includes("DATE") || type.includes("TIME")) {
    return "DATETIME";
  }
  if (type.includes("BOOL") || type === "BIT") {
    return "BOOLEAN";
  }
  if (type.includes("DECIMAL") || type.includes("NUMERIC") || (type.includes("NUMBER") && (column.scale ?? 0) > 0)) {
    return "RANDOM_DECIMAL";
  }
  if (type.includes("INT") || type.includes("NUMBER")) {
    return "RANDOM_INT";
  }
  return "STRING";
}

function buildTaskColumn(column: DatabaseColumn, index: number): TaskColumnForm {
  const generatorType = defaultGeneratorType(column);
  const generatorConfig = buildDefaultGeneratorConfig(column.columnName, generatorType, column.scale, column.enumValues ?? null, column.dbType);
  return {
    localId: `task-column-${nextLocalId++}`,
    columnName: column.columnName,
    dbType: column.dbType,
    lengthValue: column.length,
    precisionValue: column.precision,
    scaleValue: column.scale,
    nullableFlag: column.nullable,
    primaryKeyFlag: column.primaryKey,
    foreignKeyFlag: false,
    generatorType,
    generatorConfig,
    generatorConfigText: serializeConfig(generatorConfig),
    enumValues: column.enumValues ?? null,
    sortOrder: index
  };
}

function createKafkaField(index: number): TaskColumnForm {
  const generatorType: GeneratorType = "STRING";
  const generatorConfig = buildDefaultGeneratorConfig(`field_${index + 1}`, generatorType, null, null);
  return {
    localId: `task-column-${nextLocalId++}`,
    columnName: `field_${index + 1}`,
    dbType: "VARCHAR",
    lengthValue: null,
    precisionValue: null,
    scaleValue: null,
    nullableFlag: false,
    primaryKeyFlag: false,
    foreignKeyFlag: false,
    generatorType,
    generatorConfig,
    generatorConfigText: serializeConfig(generatorConfig),
    enumValues: null,
    sortOrder: index
  };
}

function createKafkaTask(index: number): TaskForm {
  return {
    id: null,
    taskKey: `kafka_task_${index + 1}`,
    name: `Kafka 任务 ${index + 1}`,
    tableName: "",
    tableMode: "CREATE_IF_MISSING",
    writeMode: "APPEND",
    batchSize: 500,
    seed: null,
    description: "",
    status: "READY",
    rowPlan: index === 0
      ? { mode: "FIXED", rowCount: 100, driverTaskKey: "", minChildrenPerParent: null, maxChildrenPerParent: null }
      : { mode: "CHILD_PER_PARENT", rowCount: null, driverTaskKey: form.tasks[0]?.taskKey ?? "", minChildrenPerParent: 1, maxChildrenPerParent: 3 },
    kafkaMessageMode: "SIMPLE",
    keyMode: "NONE",
    keyField: "",
    keyPath: "",
    fixedKey: "",
    partition: null,
    headerEntries: [],
    payloadSchemaRoot: createDefaultPayloadSchemaRoot(),
    columns: [createKafkaField(0)]
  };
}

function buildTaskFromSchema(schema: DatabaseTableSchema, hasIncomingRelation: boolean, driverTaskKey = ""): TaskForm {
  return {
    id: null,
    taskKey: normalizeTaskKey(schema.tableName),
    name: schema.tableName,
    tableName: schema.tableName,
    tableMode: "USE_EXISTING",
    writeMode: "APPEND",
    batchSize: 500,
    seed: null,
    description: "",
    status: "READY",
    rowPlan: hasIncomingRelation
      ? { mode: "CHILD_PER_PARENT", rowCount: null, driverTaskKey, minChildrenPerParent: 1, maxChildrenPerParent: 3 }
      : { mode: "FIXED", rowCount: 100, driverTaskKey: "", minChildrenPerParent: null, maxChildrenPerParent: null },
    kafkaMessageMode: "SIMPLE",
    keyMode: "NONE",
    keyField: "",
    keyPath: "",
    fixedKey: "",
    partition: null,
    headerEntries: [],
    payloadSchemaRoot: createDefaultPayloadSchemaRoot(),
    columns: schema.columns.map((column, index) => buildTaskColumn(column, index))
  };
}

function displayTaskLabel(task: TaskForm) {
  return task.name.trim() || task.tableName.trim() || task.taskKey;
}

function relationTaskLabel(taskKey: string) {
  const task = taskByKey(taskKey);
  return task ? displayTaskLabel(task) : taskKey;
}

function normalizeTaskColumnsForSubmit(task: TaskForm) {
  return task.columns.map((column, index) => {
    const columnName = column.columnName.trim();
    const dbType = column.dbType.trim();
    if (!columnName) {
      throw new Error(`${displayTaskLabel(task)} 存在未填写字段名的消息字段`);
    }
    if (!dbType) {
      throw new Error(`${displayTaskLabel(task)} / ${columnName} 必须填写数据类型`);
    }
    const generatorConfig = parseGeneratorConfigText(column.generatorConfigText, task.tableName, column.columnName);
    column.generatorConfig = generatorConfig;
    column.generatorConfigText = serializeConfig(generatorConfig);
    return {
      columnName,
      dbType,
      lengthValue: column.lengthValue,
      precisionValue: column.precisionValue,
      scaleValue: column.scaleValue,
      nullableFlag: column.nullableFlag,
      primaryKeyFlag: column.primaryKeyFlag,
      foreignKeyFlag: column.foreignKeyFlag,
      generatorType: column.generatorType,
      generatorConfig,
      sortOrder: index
    };
  });
}

function parseGeneratorConfigText(raw: string, tableName: string, columnName: string) {
  const source = raw.trim();
  if (!source) {
    return {};
  }
  try {
    const parsed = JSON.parse(source);
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      throw new Error();
    }
    return parsed as Record<string, unknown>;
  } catch {
    throw new Error(`${tableName} / ${columnName} \u7684\u751f\u6210\u914d\u7f6e\u5fc5\u987b\u662f JSON \u5bf9\u8c61`);
  }
}

function taskByKey(taskKey: string) {
  return form.tasks.find((item) => item.taskKey === taskKey) ?? null;
}

function taskAvailableFieldPaths(task: TaskForm) {
  if (task.kafkaMessageMode === "COMPLEX") {
    return collectScalarPaths(task.payloadSchemaRoot);
  }
  return Array.from(new Set(
    task.columns
      .map((column) => column.columnName.trim())
      .filter(Boolean)
  ));
}

function taskAvailableFieldPathsByKey(taskKey: string) {
  const task = taskByKey(taskKey);
  return task ? taskAvailableFieldPaths(task) : [];
}

function resolveTaskKafkaKeyPath(task: TaskForm) {
  return task.kafkaMessageMode === "COMPLEX" ? task.keyPath : task.keyField;
}

function applyTargetConfigToTask(task: TaskForm, value: string | null | undefined) {
  const config = parseJson(value);
  task.keyMode = typeof config.keyMode === "string" ? config.keyMode as KafkaKeyMode : "NONE";
  task.keyField = typeof config.keyField === "string" ? config.keyField : "";
  task.keyPath = typeof config.keyPath === "string"
    ? config.keyPath
    : (typeof config.keyField === "string" ? config.keyField : "");
  task.fixedKey = typeof config.fixedKey === "string" ? config.fixedKey : "";
  task.partition = typeof config.partition === "number" && Number.isFinite(config.partition) ? config.partition : null;
  if (Array.isArray(config.headerDefinitions)) {
    task.headerEntries = config.headerDefinitions
      .filter((entry): entry is Record<string, unknown> => Boolean(entry) && typeof entry === "object")
      .map((entry) => ({
        name: typeof entry.name === "string" ? entry.name : "",
        mode: entry.mode === "FIELD" ? "FIELD" : "FIXED",
        value: typeof entry.value === "string" ? entry.value : "",
        path: typeof entry.path === "string" ? entry.path : ""
      }));
    return;
  }
  task.headerEntries = config.headers && typeof config.headers === "object"
    ? Object.entries(config.headers as Record<string, unknown>).map(([name, headerValue]) => ({
      name,
      mode: "FIXED" as const,
      value: String(headerValue),
      path: ""
    }))
    : [];
}

function normalizeKafkaHeaderEntriesConfig(task: TaskForm) {
  const normalizedEntries = task.headerEntries
    .map((entry) => ({
      name: entry.name.trim(),
      mode: entry.mode,
      value: entry.value.trim(),
      path: entry.path.trim()
    }))
    .filter((entry) => entry.name || entry.value || entry.path);

  if (!normalizedEntries.length) {
    return {};
  }

  const availablePaths = taskAvailableFieldPaths(task);
  const headerNames = new Set<string>();
  const fixedHeaders: Record<string, string> = {};
  const headerDefinitions: Array<Record<string, string>> = [];
  const hasFieldHeader = normalizedEntries.some((entry) => entry.mode === "FIELD");

  for (const entry of normalizedEntries) {
    if (!entry.name) {
      throw new Error(`${displayTaskLabel(task)} 的 Header 名称不能为空`);
    }
    const normalizedName = entry.name.toLowerCase();
    if (headerNames.has(normalizedName)) {
      throw new Error(`${displayTaskLabel(task)} 的 Header 名称不能重复`);
    }
    headerNames.add(normalizedName);

    if (entry.mode === "FIELD") {
      if (!entry.path) {
        throw new Error(`${displayTaskLabel(task)} 的 Header 需要选择字段路径`);
      }
      if (!availablePaths.includes(entry.path)) {
        throw new Error(`${displayTaskLabel(task)} 的 Header 字段路径必须来自当前消息字段`);
      }
      headerDefinitions.push({
        name: entry.name,
        mode: "FIELD",
        path: entry.path
      });
      continue;
    }

    if (!entry.value) {
      throw new Error(`${displayTaskLabel(task)} 的 Header 固定值不能为空`);
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
    return { headerDefinitions };
  }
  return { headers: fixedHeaders };
}

function buildTaskTargetConfigJson(task: TaskForm) {
  if (!isKafkaGroup.value) {
    return null;
  }
  if (task.writeMode !== "APPEND") {
    throw new Error("Kafka 目标端仅支持追加写入");
  }

  const availablePaths = taskAvailableFieldPaths(task);
  const keyPath = resolveTaskKafkaKeyPath(task).trim();

  if (task.keyMode === "FIELD") {
    if (!keyPath) {
      throw new Error(`${displayTaskLabel(task)} 必须选择 Key 字段`);
    }
    if (!availablePaths.includes(keyPath)) {
      throw new Error(`${displayTaskLabel(task)} 的 Key 字段必须来自当前消息结构`);
    }
  }

  if (task.keyMode === "FIXED" && !task.fixedKey.trim()) {
    throw new Error(`${displayTaskLabel(task)} 选择固定 Key 时必须填写固定值`);
  }

  const payload: Record<string, unknown> = {
    payloadFormat: "JSON",
    keyMode: task.keyMode
  };

  if (task.keyMode === "FIELD") {
    if (task.kafkaMessageMode === "COMPLEX") {
      payload.keyPath = keyPath;
    } else {
      payload.keyField = keyPath;
      payload.keyPath = keyPath;
    }
  }

  if (task.keyMode === "FIXED") {
    payload.fixedKey = task.fixedKey.trim();
  }

  const partition = sanitizeOptionalNumber(task.partition);
  if (partition !== null) {
    payload.partition = partition;
  }

  const headers = normalizeKafkaHeaderEntriesConfig(task);
  if ("headers" in headers && headers.headers && Object.keys(headers.headers).length) {
    payload.headers = headers.headers;
  }
  if ("headerDefinitions" in headers && headers.headerDefinitions?.length) {
    payload.headerDefinitions = headers.headerDefinitions;
  }

  return JSON.stringify(payload);
}

function buildTaskPayloadSchemaJson(task: TaskForm) {
  if (!task.payloadSchemaRoot) {
    throw new Error(`${displayTaskLabel(task)} 未定义 Kafka 消息结构`);
  }
  return JSON.stringify(serializePayloadSchemaNode(task.payloadSchemaRoot, true));
}

function parseMappingConfigJson(value: string | null | undefined): KafkaRelationFieldMappingForm[] {
  const config = parseJson(value);
  const rawMappings = Array.isArray(config.fieldMappings) ? config.fieldMappings : [];
  return rawMappings
    .filter((item): item is Record<string, unknown> => Boolean(item) && typeof item === "object")
    .map((item) => ({
      localId: `mapping-${nextLocalId++}`,
      from: typeof item.from === "string" ? item.from : "",
      to: typeof item.to === "string" ? item.to : "",
      required: item.required !== false
    }));
}

function createDefaultMappingEntry(sourcePaths: string[], targetPaths: string[]): KafkaRelationFieldMappingForm {
  const source = sourcePaths[0] ?? "";
  const sameNamedTarget = source ? targetPaths.find((item) => item === source || item.endsWith(`.${source}`)) : undefined;
  return {
    localId: `mapping-${nextLocalId++}`,
    from: source,
    to: sameNamedTarget ?? targetPaths[0] ?? "",
    required: true
  };
}

function buildMappingConfigJson(relation: RelationForm) {
  const sourcePaths = relationSourceFieldPaths(relation);
  const targetPaths = relationTargetFieldPaths(relation);
  const fieldMappings = relation.mappingEntries
    .map((entry) => ({
      from: entry.from.trim(),
      to: entry.to.trim(),
      required: entry.required !== false
    }))
    .filter((entry) => entry.from || entry.to);

  if (!fieldMappings.length) {
    throw new Error(`${relation.relationName || "关系"} 至少需要一条字段映射`);
  }

  for (const entry of fieldMappings) {
    if (!entry.from || !entry.to) {
      throw new Error(`${relation.relationName || "关系"} 的字段映射必须同时选择来源字段和写入路径`);
    }
    if (!sourcePaths.includes(entry.from)) {
      throw new Error(`${relation.relationName || "关系"} 的来源字段不在父任务可用字段列表中`);
    }
    if (!targetPaths.includes(entry.to)) {
      throw new Error(`${relation.relationName || "关系"} 的写入路径不在子任务消息结构中`);
    }
  }

  return JSON.stringify({ fieldMappings }, null, 2);
}

function buildGroupPayload() {
  const cronExpression = form.scheduleType === "CRON"
    ? (cronExpressionMode.value === "LEGACY" ? form.cronExpression.trim() : buildCronExpressionFromBuilder())
    : null;

  if (!form.name.trim()) {
    throw new Error("请填写任务组名称");
  }
  if (!form.connectionId) {
    throw new Error("请选择目标连接");
  }
  if (!form.tasks.length) {
    throw new Error(isKafkaGroup.value ? "请至少新增一个 Kafka 任务" : "请先选择表并导入结构");
  }
  if (form.scheduleType === "CRON" && !cronExpression) {
    throw new Error("请选择周期定时计划");
  }

  return {
    name: form.name.trim(),
    connectionId: form.connectionId,
    description: form.description.trim() || null,
    seed: form.seed,
    status: form.status,
    scheduleType: form.scheduleType,
    cronExpression: cronExpression || null,
    triggerAt: form.scheduleType === "ONCE" ? toIsoDateTime(form.triggerAt) : null,
    intervalSeconds: form.scheduleType === "INTERVAL" ? form.intervalSeconds : null,
    maxRuns: form.scheduleType === "INTERVAL" ? form.maxRuns : null,
    maxRowsTotal: form.scheduleType === "INTERVAL" ? form.maxRowsTotal : null,
    tasks: form.tasks.map((task, index) => {
      const taskName = displayTaskLabel(task);
      const rowPlan = {
        mode: task.rowPlan.mode,
        rowCount: task.rowPlan.mode === "FIXED" ? task.rowPlan.rowCount : null,
        driverTaskKey: task.rowPlan.mode === "CHILD_PER_PARENT" ? task.rowPlan.driverTaskKey || null : null,
        minChildrenPerParent: task.rowPlan.mode === "CHILD_PER_PARENT" ? task.rowPlan.minChildrenPerParent : null,
        maxChildrenPerParent: task.rowPlan.mode === "CHILD_PER_PARENT" ? task.rowPlan.maxChildrenPerParent : null
      };

      if (!task.tableName.trim()) {
        throw new Error(isKafkaGroup.value ? `${taskName} 必须填写 Topic 名称` : `${taskName} 缺少表名`);
      }
      if (!task.batchSize || task.batchSize < 1) {
        throw new Error(`${taskName} 的单批写入大小必须大于 0`);
      }
      if (task.rowPlan.mode === "FIXED" && (!task.rowPlan.rowCount || task.rowPlan.rowCount < 1)) {
        throw new Error(`${taskName} 的写入行数必须大于 0`);
      }
      if (task.rowPlan.mode === "CHILD_PER_PARENT" && !task.rowPlan.driverTaskKey) {
        throw new Error(`${taskName} 选择按父表衍生时，必须指定父任务`);
      }

      const normalizedColumns = task.kafkaMessageMode === "COMPLEX" ? [] : normalizeTaskColumnsForSubmit(task);
      if (isKafkaGroup.value && task.kafkaMessageMode === "SIMPLE" && !normalizedColumns.length) {
        throw new Error(`${taskName} 至少需要一个消息字段`);
      }

      return {
        id: task.id,
        taskKey: task.taskKey,
        name: task.name.trim() || task.tableName.trim() || `task_${index + 1}`,
        tableName: task.tableName.trim(),
        tableMode: isKafkaGroup.value ? "CREATE_IF_MISSING" : task.tableMode,
        writeMode: isKafkaGroup.value ? "APPEND" : task.writeMode,
        batchSize: task.batchSize,
        seed: task.seed,
        description: task.description.trim() || null,
        status: task.status,
        rowPlan,
        targetConfigJson: buildTaskTargetConfigJson(task),
        payloadSchemaJson: isKafkaGroup.value && task.kafkaMessageMode === "COMPLEX"
          ? buildTaskPayloadSchemaJson(task)
          : null,
        columns: normalizedColumns
      };
    }),
    relations: form.relations.map((relation, index) => {
      if (!relation.parentTaskKey || !relation.childTaskKey) {
        throw new Error(`${relation.relationName || `关系 ${index + 1}`} 必须选择父任务和子任务`);
      }
      if (relation.parentTaskKey === relation.childTaskKey) {
        throw new Error(`${relation.relationName || `关系 ${index + 1}`} 的父任务和子任务不能相同`);
      }
      const parentColumns = relation.parentColumns.filter(Boolean);
      const childColumns = relation.childColumns.filter(Boolean);
      if (!isKafkaGroup.value && (!parentColumns.length || !childColumns.length)) {
        throw new Error(`${relation.relationName || `关系 ${index + 1}`} 需要选择关联列`);
      }
      return {
        id: relation.id,
        relationName: relation.relationName.trim() || `relation_${index + 1}`,
        parentTaskKey: relation.parentTaskKey,
        childTaskKey: relation.childTaskKey,
        relationMode: isKafkaGroup.value ? "KAFKA_EVENT" : "DATABASE_COLUMNS",
        relationType: relation.relationType,
        sourceMode: isKafkaGroup.value ? "CURRENT_BATCH" : relation.sourceMode,
        selectionStrategy: relation.selectionStrategy,
        reusePolicy: relation.reusePolicy,
        parentColumns: isKafkaGroup.value ? [] : parentColumns,
        childColumns: isKafkaGroup.value ? [] : childColumns,
        nullRate: relation.nullRate,
        mixedExistingRatio: !isKafkaGroup.value && relation.sourceMode === "MIXED" ? relation.mixedExistingRatio : null,
        minChildrenPerParent: relation.minChildrenPerParent,
        maxChildrenPerParent: relation.maxChildrenPerParent,
        mappingConfigJson: isKafkaGroup.value ? buildMappingConfigJson(relation) : null,
        sortOrder: index
      };
    })
  };
}

function applyGroup(group: GroupResponse) {
  const kafkaGroup = (connections.value.find((item) => item.id === group.connectionId)?.dbType === "KAFKA")
    || group.tasks.some((task) => Boolean(task.targetConfigJson) || Boolean(task.payloadSchemaJson))
    || group.relations.some((relation) => relation.relationMode === "KAFKA_EVENT");

  form.id = group.id;
  form.name = group.name;
  form.connectionId = group.connectionId;
  form.description = group.description ?? "";
  form.seed = group.seed;
  form.status = group.status;
  form.scheduleType = group.scheduleType;
  applyCronExpressionToBuilder(group.cronExpression);
  form.triggerAt = toDateTimeLocal(group.triggerAt);
  form.intervalSeconds = group.intervalSeconds ?? 10;
  form.maxRuns = group.maxRuns ?? null;
  form.maxRowsTotal = group.maxRowsTotal ?? null;
  form.tasks = group.tasks.map((task) => {
    const nextTask: TaskForm = {
      id: task.id,
      taskKey: task.taskKey,
      name: task.name,
      tableName: task.tableName,
      tableMode: kafkaGroup ? "CREATE_IF_MISSING" : task.tableMode,
      writeMode: kafkaGroup ? "APPEND" : task.writeMode,
      batchSize: task.batchSize,
      seed: task.seed,
      description: task.description ?? "",
      status: task.status,
      rowPlan: {
        mode: task.rowPlan.mode,
        rowCount: task.rowPlan.rowCount,
        driverTaskKey: task.rowPlan.driverTaskKey ?? "",
        minChildrenPerParent: task.rowPlan.minChildrenPerParent,
        maxChildrenPerParent: task.rowPlan.maxChildrenPerParent
      },
      kafkaMessageMode: task.payloadSchemaJson ? "COMPLEX" : "SIMPLE",
      keyMode: "NONE",
      keyField: "",
      keyPath: "",
      fixedKey: "",
      partition: null,
      headerEntries: [],
      payloadSchemaRoot: task.payloadSchemaJson ? parsePayloadSchemaJson(task.payloadSchemaJson) : createDefaultPayloadSchemaRoot(),
      columns: task.columns.map((column, columnIndex) => ({
        localId: `task-column-${nextLocalId++}`,
        columnName: column.columnName,
        dbType: column.dbType,
        lengthValue: column.lengthValue,
        precisionValue: column.precisionValue,
        scaleValue: column.scaleValue,
        nullableFlag: column.nullableFlag,
        primaryKeyFlag: column.primaryKeyFlag,
        foreignKeyFlag: column.foreignKeyFlag,
        generatorType: column.generatorType,
        generatorConfig: column.generatorConfig ?? {},
        generatorConfigText: serializeConfig(column.generatorConfig ?? {}),
        enumValues: Array.isArray(column.generatorConfig?.values)
          ? (column.generatorConfig.values as string[])
          : null,
        sortOrder: column.sortOrder ?? columnIndex
      }))
    };
    if (kafkaGroup && nextTask.kafkaMessageMode === "SIMPLE" && !nextTask.columns.length) {
      nextTask.columns = [createKafkaField(0)];
    }
    applyTargetConfigToTask(nextTask, task.targetConfigJson);
    return nextTask;
  });
  form.relations = group.relations.map((relation, index) => ({
    id: relation.id,
    relationName: relation.relationName,
    parentTaskKey: relation.parentTaskKey,
    childTaskKey: relation.childTaskKey,
    relationMode: (relation.relationMode ?? (kafkaGroup ? "KAFKA_EVENT" : "DATABASE_COLUMNS")) as RelationMode,
    relationType: relation.relationType,
    sourceMode: kafkaGroup ? "CURRENT_BATCH" : relation.sourceMode,
    selectionStrategy: relation.selectionStrategy,
    reusePolicy: relation.reusePolicy,
    parentColumns: [...relation.parentColumns],
    childColumns: [...relation.childColumns],
    nullRate: relation.nullRate ?? 0,
    mixedExistingRatio: relation.mixedExistingRatio ?? 0.5,
    minChildrenPerParent: relation.minChildrenPerParent,
    maxChildrenPerParent: relation.maxChildrenPerParent,
    mappingEntries: parseMappingConfigJson(relation.mappingConfigJson),
    sortOrder: relation.sortOrder ?? index
  }));
  selectedTableNames.value = kafkaGroup ? [] : group.tasks.map((task) => task.tableName);
}

function resolveConnectionName(connectionId: number) {
  const connection = connections.value.find((item) => item.id === connectionId);
  if (!connection) {
    return `\u8fde\u63a5 #${connectionId}`;
  }
  return `${connection.name} (${labelDatabaseType(connection.dbType)})`;
}

function previewHeaders(rows: Record<string, unknown>[]) {
  return rows.length ? Object.keys(rows[0]) : [];
}

function displayPreviewValue(value: unknown) {
  if (value === null || value === undefined) {
    return "-";
  }
  if (typeof value === "object") {
    return JSON.stringify(value);
  }
  return String(value);
}

function executionTableErrors(table: GroupExecutionTable) {
  const summary = table.summary ?? {};
  const errors = summary.errors;
  if (!Array.isArray(errors)) {
    return [] as string[];
  }
  return errors
    .map((item) => String(item).trim())
    .filter(Boolean);
}

function applyExecutionResult(groupId: number, execution: GroupExecution) {
  if (optimisticExecutionGroupId.value === groupId && isTerminalExecutionStatus(execution.status)) {
    clearOptimisticExecution(groupId);
  }
  lastExecution.value = execution;
  executions.value = [execution, ...executions.value.filter((item) => item.id !== execution.id)];
  listExecutionByGroup.value = {
    ...listExecutionByGroup.value,
    [groupId]: execution
  };
  lastRuntimeRefreshAt.value = new Date().toISOString();
}

function trackOptimisticExecution(groupId: number, execution: GroupExecution) {
  optimisticExecutionGroupId.value = groupId;
  optimisticExecution.value = execution;
  applyExecutionResult(groupId, execution);
}

async function showRunFeedback(groupId: number, execution: GroupExecution, message: string, ensureEditor = false) {
  const effectiveMessage = message || (isActiveExecutionStatus(execution.status) ? text.executionSubmitted : text.executionFinished);
  if (ensureEditor || !isEditPage.value || routedGroupId.value !== groupId) {
    await router.push({ name: "relational-write-task-edit", params: { id: groupId } });
  }
  if (form.id !== groupId) {
    await selectGroup(groupId);
  }
  await loadGroups();
  if (isActiveExecutionStatus(execution.status)) {
    trackOptimisticExecution(groupId, execution);
    setFeedback("success", effectiveMessage);
    if (autoRefreshEnabled.value) {
      startAutoRefresh();
      void refreshRuntimeStatus();
    }
    return;
  }
  applyExecutionResult(groupId, execution);
  if (form.scheduleType === "INTERVAL" && autoRefreshEnabled.value) {
    startAutoRefresh();
  }
  const feedbackKind = execution.status === "FAILED" ? "error" : "success";
  const feedbackMessage = execution.status === "FAILED" && execution.errorSummary
    ? `${text.executionFailedPrefix} ${execution.errorSummary}`
    : effectiveMessage;
  setFeedback(feedbackKind, feedbackMessage);
}

function driverTaskOptions(taskKey: string) {
  const relatedParents = new Set(
    form.relations.filter((relation) => relation.childTaskKey === taskKey).map((relation) => relation.parentTaskKey)
  );
  const options = form.tasks.filter((task) => task.taskKey !== taskKey && (relatedParents.size === 0 || relatedParents.has(task.taskKey)));
  return options.length ? options : form.tasks.filter((task) => task.taskKey !== taskKey);
}

function relationParentColumnOptions(relation: RelationForm) {
  return taskColumns(relation.parentTaskKey, true);
}

function relationChildColumnOptions(relation: RelationForm) {
  return taskColumns(relation.childTaskKey, false);
}

function taskColumns(taskKey: string, preferPrimaryKey: boolean) {
  const task = form.tasks.find((item) => item.taskKey === taskKey);
  if (!task) {
    return [] as string[];
  }
  const preferred = task.columns
    .filter((column) => (preferPrimaryKey ? column.primaryKeyFlag : !column.primaryKeyFlag))
    .map((column) => column.columnName);
  if (preferred.length) {
    return preferred;
  }
  return task.columns.map((column) => column.columnName);
}

function relationSourceFieldPaths(relation: RelationForm) {
  return taskAvailableFieldPathsByKey(relation.parentTaskKey);
}

function relationTargetFieldPaths(relation: RelationForm) {
  return taskAvailableFieldPathsByKey(relation.childTaskKey);
}

function firstParentColumn(relation: RelationForm) {
  return relation.parentColumns[0] ?? "";
}

function firstChildColumn(relation: RelationForm) {
  return relation.childColumns[0] ?? "";
}

function setParentColumn(relation: RelationForm, value: string) {
  relation.parentColumns = value ? [value] : [];
}

function setChildColumn(relation: RelationForm, value: string) {
  relation.childColumns = value ? [value] : [];
}

function eventTargetValue(event: Event) {
  return (event.target as HTMLSelectElement).value;
}

function ensureRelationColumns(relation: RelationForm) {
  if (isKafkaGroup.value) {
    const sourcePaths = relationSourceFieldPaths(relation);
    const targetPaths = relationTargetFieldPaths(relation);
    relation.mappingEntries = relation.mappingEntries
      .filter((entry) => !entry.from || sourcePaths.includes(entry.from))
      .filter((entry) => !entry.to || targetPaths.includes(entry.to));
    if (!relation.mappingEntries.length && sourcePaths.length && targetPaths.length) {
      relation.mappingEntries = [createDefaultMappingEntry(sourcePaths, targetPaths)];
    }
    relation.parentColumns = [];
    relation.childColumns = [];
    relation.relationMode = "KAFKA_EVENT";
    relation.sourceMode = "CURRENT_BATCH";
    return;
  }
  const parentOptions = relationParentColumnOptions(relation);
  const childOptions = relationChildColumnOptions(relation);

  if (!parentOptions.includes(firstParentColumn(relation))) {
    setParentColumn(relation, parentOptions[0] ?? "");
  }
  if (!childOptions.includes(firstChildColumn(relation))) {
    setChildColumn(relation, childOptions[0] ?? "");
  }
}

function handleRelationParentChange(relation: RelationForm) {
  if (relation.parentTaskKey === relation.childTaskKey) {
    const fallback = form.tasks.find((task) => task.taskKey !== relation.parentTaskKey);
    relation.childTaskKey = fallback?.taskKey ?? "";
  }
  ensureRelationColumns(relation);
}

function handleRelationChildChange(relation: RelationForm) {
  if (relation.childTaskKey === relation.parentTaskKey) {
    relation.parentTaskKey = form.tasks.find((task) => task.taskKey !== relation.childTaskKey)?.taskKey ?? "";
  }
  ensureRelationColumns(relation);
}

function normalizeRelations() {
  form.relations = form.relations
    .filter((relation) => relation.parentTaskKey !== relation.childTaskKey)
    .map((relation, index) => {
      relation.relationMode = isKafkaGroup.value ? "KAFKA_EVENT" : "DATABASE_COLUMNS";
      relation.sortOrder = index;
      ensureRelationColumns(relation);
      return relation;
    });
}

function handleGeneratorTypeChange(column: TaskColumnForm) {
  const generatorConfig = buildDefaultGeneratorConfig(column.columnName, column.generatorType, column.scaleValue, column.enumValues, column.dbType);
  column.generatorConfig = generatorConfig;
  column.generatorConfigText = serializeConfig(generatorConfig);
}

function commitGeneratorConfig(column: TaskColumnForm, task: TaskForm) {
  try {
    column.generatorConfig = parseGeneratorConfigText(column.generatorConfigText, task.tableName, column.columnName);
    column.generatorConfigText = serializeConfig(column.generatorConfig);
  } catch (error) {
    setFeedback("error", readApiError(error, "\u751f\u6210\u914d\u7f6e\u683c\u5f0f\u4e0d\u6b63\u786e"));
  }
}

async function loadConnections() {
  const response = await apiClient.get<ApiResponse<ConnectionItem[]>>("/connections");
  connections.value = response.data.data;
}

async function loadGroups() {
  const response = await apiClient.get<ApiResponse<GroupResponse[]>>("/write-task-groups");
  groups.value = response.data.data;
}

async function loadTables() {
  if (!form.connectionId || isKafkaGroup.value) {
    availableTables.value = [];
    return;
  }
  const response = await apiClient.get<ApiResponse<TableItem[]>>(`/connections/${form.connectionId}/tables`);
  availableTables.value = response.data.data;
}

async function loadExecutions(groupId: number, preferredLatest: GroupExecution | null = null) {
  const response = await apiClient.get<ApiResponse<GroupExecution[]>>(`/write-task-groups/${groupId}/executions`);
  const remoteExecutions = response.data.data;
  const effectivePreferred = preferredLatest ?? (optimisticExecutionGroupId.value === groupId ? optimisticExecution.value : null);
  const matchedPreferred = effectivePreferred
    ? remoteExecutions.some((item) => matchesOptimisticExecution(item, effectivePreferred))
    : false;
  if (effectivePreferred && !matchedPreferred) {
    optimisticExecutionGroupId.value = groupId;
    optimisticExecution.value = effectivePreferred;
    executions.value = [effectivePreferred, ...remoteExecutions];
  } else {
    if (matchedPreferred) {
      clearOptimisticExecution(groupId);
    }
    executions.value = remoteExecutions;
  }
  lastExecution.value = executions.value[0] ?? null;
  if (lastExecution.value && isTerminalExecutionStatus(lastExecution.value.status)) {
    clearOptimisticExecution(groupId);
  }
  if (lastExecution.value) {
    listExecutionByGroup.value = {
      ...listExecutionByGroup.value,
      [groupId]: lastExecution.value
    };
  }
  lastRuntimeRefreshAt.value = new Date().toISOString();
}

async function refreshRuntimeStatus() {
  if (!form.id || isListPage.value) {
    return;
  }
  refreshingRuntime.value = true;
  const trackedGroupId = optimisticExecutionGroupId.value;
  const previousStatus = lastExecution.value?.status;
  try {
    await loadExecutions(form.id);
    await loadGroups();
    if (
      (trackedGroupId === form.id || isActiveExecutionStatus(previousStatus)) &&
      lastExecution.value &&
      isTerminalExecutionStatus(lastExecution.value.status)
    ) {
      const feedbackKind = lastExecution.value.status === "FAILED" ? "error" : "success";
      const feedbackMessage = lastExecution.value.status === "FAILED" && lastExecution.value.errorSummary
        ? `${text.executionFailedPrefix} ${lastExecution.value.errorSummary}`
        : text.executionFinished;
      setFeedback(feedbackKind, feedbackMessage);
    }
  } catch (error) {
    setFeedback("error", readApiError(error, "\u5237\u65b0\u6267\u884c\u60c5\u51b5\u5931\u8d25"));
  } finally {
    refreshingRuntime.value = false;
  }
}

function stopAutoRefresh() {
  if (runtimeRefreshTimer !== null) {
    window.clearInterval(runtimeRefreshTimer);
    runtimeRefreshTimer = null;
  }
}

function startAutoRefresh() {
  stopAutoRefresh();
  if (!shouldAutoRefreshRuntime()) {
    return;
  }
  runtimeRefreshTimer = window.setInterval(() => {
    if (document.hidden || refreshingRuntime.value) {
      return;
    }
    void refreshRuntimeStatus();
  }, runtimeRefreshIntervalMs.value);
}

function toggleAutoRefresh() {
  autoRefreshEnabled.value = !autoRefreshEnabled.value;
  if (autoRefreshEnabled.value) {
    startAutoRefresh();
  } else {
    stopAutoRefresh();
  }
}

function handleVisibilityChange() {
  if (document.hidden) {
    stopAutoRefresh();
    return;
  }
  if (autoRefreshEnabled.value) {
    startAutoRefresh();
    void refreshRuntimeStatus();
  }
}

async function loadPageData() {
  loading.value = true;
  try {
    await Promise.all([loadConnections(), loadGroups()]);
    if (form.connectionId) {
      await loadTables();
    }
  } catch (error) {
    setFeedback("error", readApiError(error, "\u52a0\u8f7d\u5173\u7cfb\u4efb\u52a1\u9875\u9762\u5931\u8d25"));
  } finally {
    loading.value = false;
  }
}

async function handleConnectionChange() {
  form.tasks = [];
  form.relations = [];
  previewData.value = null;
  selectedTableNames.value = [];
  lastExecution.value = null;
  executions.value = [];
  lastRuntimeRefreshAt.value = null;
  clearOptimisticExecution();
  stopAutoRefresh();
  if (form.connectionId && !isKafkaGroup.value) {
    await loadTables();
  } else {
    availableTables.value = [];
  }
}

function startCreate() {
  resetForm();
  void router.push({ name: "relational-write-task-create" });
}

function goToGroupList() {
  resetForm();
  void router.push({ name: "relational-write-tasks" });
}

async function openGroupEditor(groupId: number) {
  await router.push({ name: "relational-write-task-edit", params: { id: groupId } });
}

function openGroupExecutions(groupId: number) {
  clearFeedback();
  void router.push({ name: "relational-write-task-executions", params: { id: groupId } });
}

function openGroupExecutionDetail(groupId: number, executionId: number) {
  clearFeedback();
  void router.push({
    name: "relational-write-task-execution-detail",
    params: { id: groupId, executionId }
  });
}

function shouldAutoRefreshRuntime() {
  if (!autoRefreshEnabled.value || !form.id || isListPage.value) {
    return false;
  }
  if (optimisticExecutionGroupId.value === form.id) {
    return true;
  }
  if (hasActiveExecution.value) {
    return true;
  }
  return form.scheduleType === "INTERVAL" && (form.status === "RUNNING" || executions.value.length > 0);
}

async function syncRouteState() {
  if (isListPage.value) {
    resetForm();
    return;
  }
  if (isCreatePage.value) {
    resetForm();
    return;
  }
  if (!isEditPage.value || routedGroupId.value === null) {
    return;
  }
  if (form.id === routedGroupId.value && form.tasks.length > 0) {
    return;
  }
  await selectGroup(routedGroupId.value);
}

async function mutateGroupSchedule(groupId: number, action: "start" | "pause" | "resume" | "stop") {
  mutatingGroupId.value = groupId;
  clearFeedback();
  try {
    const response = await apiClient.post<ApiResponse<GroupResponse>>(`/write-task-groups/${groupId}/${action}`);
    await loadGroups();
    if (form.id === groupId) {
      applyGroup(response.data.data);
      normalizeRelations();
      await loadExecutions(groupId);
      if (response.data.data.scheduleType === "INTERVAL" && action === "start" && autoRefreshEnabled.value) {
        startAutoRefresh();
      }
      if (action === "stop") {
        stopAutoRefresh();
      }
    }
    setFeedback("success", response.data.message ?? "\u8c03\u5ea6\u72b6\u6001\u5df2\u66f4\u65b0");
  } catch (error) {
    setFeedback("error", readApiError(error, "\u66f4\u65b0\u8c03\u5ea6\u72b6\u6001\u5931\u8d25"));
  } finally {
    mutatingGroupId.value = null;
  }
}

function startGroup(groupId: number) {
  return mutateGroupSchedule(groupId, "start");
}

function pauseGroup(groupId: number) {
  return mutateGroupSchedule(groupId, "pause");
}

function resumeGroup(groupId: number) {
  return mutateGroupSchedule(groupId, "resume");
}

function stopGroup(groupId: number) {
  return mutateGroupSchedule(groupId, "stop");
}

async function importSchema() {
  if (!form.connectionId) {
    setFeedback("error", "\u8bf7\u5148\u9009\u62e9\u76ee\u6807\u8fde\u63a5");
    return false;
  }
  if (!selectedTableNames.value.length) {
    setFeedback("error", "\u8bf7\u5148\u9009\u62e9\u9700\u8981\u5bfc\u5165\u7684\u8868");
    return false;
  }

  importingSchema.value = true;
  try {
    const params = new URLSearchParams();
    selectedTableNames.value.forEach((tableName) => params.append("tableNames", tableName));
    const response = await apiClient.get<ApiResponse<DatabaseModel>>(
      `/connections/${form.connectionId}/schema-model?${params.toString()}`
    );
    const model = response.data.data;
    const availableTableNames = model.tables.map((table) => table.tableName);
    const normalizedRelations = model.relations.map((relation) => ({
      ...relation,
      parentTable: resolveModelTableName(relation.parentTable, availableTableNames),
      childTable: resolveModelTableName(relation.childTable, availableTableNames)
    }));
    const driverMap = new Map(normalizedRelations.map((relation) => [relation.childTable, relation.parentTable]));
    const tasks = model.tables.map((table) =>
      buildTaskFromSchema(table, driverMap.has(table.tableName), normalizeTaskKey(driverMap.get(table.tableName) ?? ""))
    );
    const taskKeyByTable = new Map(tasks.map((task) => [task.tableName, task.taskKey]));
    const relations: RelationForm[] = normalizedRelations
      .filter((relation) => taskKeyByTable.has(relation.parentTable) && taskKeyByTable.has(relation.childTable))
      .map((relation, index) => ({
        id: null,
        relationName: relation.constraintName,
        parentTaskKey: taskKeyByTable.get(relation.parentTable) ?? "",
        childTaskKey: taskKeyByTable.get(relation.childTable) ?? "",
        relationMode: "DATABASE_COLUMNS" as RelationMode,
        relationType: "ONE_TO_MANY",
        sourceMode: "CURRENT_BATCH",
        selectionStrategy: "PARENT_DRIVEN",
        reusePolicy: "ALLOW_REPEAT",
        parentColumns: [...relation.parentColumns],
        childColumns: [...relation.childColumns],
        nullRate: 0,
        mixedExistingRatio: 0.5,
        minChildrenPerParent: 1,
        maxChildrenPerParent: 3,
        mappingEntries: [],
        sortOrder: index
      }));

    form.tasks = tasks.map((task) => {
      const childRelations = relations.filter((relation) => relation.childTaskKey === task.taskKey);
      for (const relation of childRelations) {
        task.columns = task.columns.map((column) =>
          relation.childColumns.includes(column.columnName) ? { ...column, foreignKeyFlag: true } : column
        );
        if (task.rowPlan.mode === "CHILD_PER_PARENT" && !task.rowPlan.driverTaskKey) {
          task.rowPlan.driverTaskKey = relation.parentTaskKey;
        }
      }
      return task;
    });
    form.relations = relations;
    normalizeRelations();

    if (!form.name.trim()) {
      form.name = `\u5173\u7cfb\u4efb\u52a1\u7ec4 ${new Date().toLocaleDateString("zh-CN")}`;
    }

    setFeedback("success", `\u5df2\u5bfc\u5165 ${form.tasks.length} \u5f20\u8868\uff0c\u81ea\u52a8\u8bc6\u522b ${form.relations.length} \u6761\u5173\u7cfb`);
    return true;
  } catch (error) {
    setFeedback("error", readApiError(error, "\u5bfc\u5165\u8868\u7ed3\u6784\u5931\u8d25"));
    return false;
  } finally {
    importingSchema.value = false;
  }
}

async function ensureTasksPrepared() {
  if (form.tasks.length > 0) {
    return;
  }
  if (isKafkaGroup.value) {
    throw new Error("请至少新增一个 Kafka 任务");
  }
  if (selectedTableNames.value.length > 0) {
    const imported = await importSchema();
    if (imported && form.tasks.length > 0) {
      return;
    }
  }
  throw new Error("\u8bf7\u5148\u9009\u62e9\u8868\u5e76\u5bfc\u5165\u7ed3\u6784");
}

function addTask() {
  if (!isKafkaGroup.value) {
    return;
  }
  form.tasks.push(createKafkaTask(form.tasks.length));
  normalizeRelations();
}

function removeTask(taskKey: string) {
  form.tasks = form.tasks.filter((task) => task.taskKey !== taskKey);
  form.relations = form.relations.filter((relation) => relation.parentTaskKey !== taskKey && relation.childTaskKey !== taskKey);
  form.tasks.forEach((task) => {
    if (task.rowPlan.driverTaskKey === taskKey) {
      task.rowPlan.driverTaskKey = form.tasks.find((item) => item.taskKey !== task.taskKey)?.taskKey ?? "";
    }
  });
  normalizeRelations();
}

function addTaskField(task: TaskForm) {
  const column = createKafkaField(task.columns.length);
  task.columns.push(column);
  startTaskFieldNameEdit(column, task.columns.length - 1);
}

function removeTaskField(task: TaskForm, index: number) {
  const [removedColumn] = task.columns.splice(index, 1);
  if (removedColumn?.localId && editingTaskFieldLocalId.value === removedColumn.localId) {
    cancelTaskFieldNameEdit();
  }
  task.columns.forEach((column, columnIndex) => {
    column.sortOrder = columnIndex;
  });
}

function handleTaskGeneratorTypeChange(task: TaskForm, column: TaskColumnForm) {
  const generatorConfig = buildDefaultGeneratorConfig(column.columnName || "field", column.generatorType, column.scaleValue, column.enumValues, column.dbType);
  column.generatorConfig = generatorConfig;
  column.generatorConfigText = serializeConfig(generatorConfig);
  commitGeneratorConfig(column, task);
}

function switchTaskKafkaMessageMode(task: TaskForm, mode: KafkaMessageMode) {
  task.kafkaMessageMode = mode;
  task.keyField = "";
  task.keyPath = "";
  if (mode === "COMPLEX") {
    task.payloadSchemaRoot = task.payloadSchemaRoot ?? createDefaultPayloadSchemaRoot();
    task.columns = [];
  } else if (!task.columns.length) {
    task.columns = [createKafkaField(0)];
  }
}

function applyImportedKafkaSchemaToTask(task: TaskForm, result: KafkaSchemaImportResult) {
  task.kafkaMessageMode = "COMPLEX";
  task.payloadSchemaRoot = parsePayloadSchemaJson(result.payloadSchemaJson);
  task.keyField = "";
  task.keyPath = "";
  setFeedback("success", result.warnings.length ? `消息结构已导入，包含 ${result.warnings.length} 条提示` : "消息结构已导入");
}

function addRelation() {
  if (form.tasks.length < 2) {
    setFeedback("error", "\u81f3\u5c11\u9700\u8981\u4e24\u5f20\u8868\u624d\u80fd\u65b0\u589e\u5173\u7cfb");
    return;
  }
  const parent = form.tasks[0];
  const child = form.tasks[1];
  const relation: RelationForm = {
    id: null,
    relationName: `relation_${form.relations.length + 1}`,
    parentTaskKey: parent.taskKey,
    childTaskKey: child.taskKey,
    relationMode: isKafkaGroup.value ? "KAFKA_EVENT" : "DATABASE_COLUMNS",
    relationType: "ONE_TO_MANY",
    sourceMode: "CURRENT_BATCH",
    selectionStrategy: isKafkaGroup.value ? "PARENT_DRIVEN" : "RANDOM_UNIFORM",
    reusePolicy: "ALLOW_REPEAT",
    parentColumns: taskColumns(parent.taskKey, true).slice(0, 1),
    childColumns: taskColumns(child.taskKey, false).slice(0, 1),
    nullRate: 0,
    mixedExistingRatio: 0.5,
    minChildrenPerParent: 1,
    maxChildrenPerParent: 3,
    mappingEntries: isKafkaGroup.value
      ? [createDefaultMappingEntry(taskAvailableFieldPaths(parent), taskAvailableFieldPaths(child))]
      : [],
    sortOrder: form.relations.length
  };
  form.relations.push(relation);
  normalizeRelations();
}

function removeRelation(index: number) {
  form.relations.splice(index, 1);
  normalizeRelations();
}

async function saveGroup() {
  saving.value = true;
  clearFeedback();
  try {
    await ensureTasksPrepared();
    normalizeRelations();
    const payload = buildGroupPayload();
    const response = form.id
      ? await apiClient.put<ApiResponse<GroupResponse>>(`/write-task-groups/${form.id}`, payload)
      : await apiClient.post<ApiResponse<GroupResponse>>("/write-task-groups", payload);
    const savedGroup = response.data.data;
    applyGroup(savedGroup);
    normalizeRelations();
    await loadGroups();
    if (!isEditPage.value || routedGroupId.value !== savedGroup.id) {
      await router.replace({ name: "relational-write-task-edit", params: { id: savedGroup.id } });
    }
    await loadExecutions(savedGroup.id);
    if (savedGroup.scheduleType === "INTERVAL" && autoRefreshEnabled.value) {
      startAutoRefresh();
    }
    setFeedback("success", response.data.message ?? "\u5173\u7cfb\u4efb\u52a1\u7ec4\u5df2\u4fdd\u5b58");
  } catch (error) {
    setFeedback("error", readApiError(error, "\u4fdd\u5b58\u5173\u7cfb\u4efb\u52a1\u7ec4\u5931\u8d25"));
  } finally {
    saving.value = false;
  }
}

async function previewCurrent() {
  previewing.value = true;
  clearFeedback();
  try {
    await ensureTasksPrepared();
    normalizeRelations();
    const response = await apiClient.post<ApiResponse<PreviewResponse>>(
      "/write-task-groups/preview",
      {
        group: buildGroupPayload(),
        previewCount: 5
      },
      { timeout: REQUEST_TIMEOUT.longRunning }
    );
    previewData.value = response.data.data;
    setFeedback("success", "\u9884\u89c8\u6570\u636e\u5df2\u751f\u6210");
  } catch (error) {
    setFeedback("error", readApiError(error, "\u9884\u89c8\u6570\u636e\u5931\u8d25"));
  } finally {
    previewing.value = false;
  }
}

async function previewExisting(groupId: number) {
  previewing.value = true;
  clearFeedback();
  try {
    if (form.id !== groupId) {
      await selectGroup(groupId);
    }
    if (!isEditPage.value || routedGroupId.value !== groupId) {
      await router.push({ name: "relational-write-task-edit", params: { id: groupId } });
    }
    const response = await apiClient.get<ApiResponse<PreviewResponse>>(
      `/write-task-groups/${groupId}/preview?previewCount=5`,
      { timeout: REQUEST_TIMEOUT.longRunning }
    );
    previewData.value = response.data.data;
    setFeedback("success", "\u9884\u89c8\u6570\u636e\u5df2\u751f\u6210");
  } catch (error) {
    setFeedback("error", readApiError(error, "\u8bfb\u53d6\u9884\u89c8\u6570\u636e\u5931\u8d25"));
  } finally {
    previewing.value = false;
  }
}

async function runCurrent() {
  running.value = true;
  clearFeedback();
  let groupId: number | null = null;
  let startedAt: string | null = null;
  try {
    await ensureTasksPrepared();
    if (!form.id) {
      await saveGroup();
    }
    if (!form.id) {
      throw new Error("\u5f53\u524d\u4efb\u52a1\u7ec4\u8fd8\u6ca1\u6709\u4fdd\u5b58\u6210\u529f");
    }
    groupId = form.id;
    startedAt = new Date().toISOString();
    previewData.value = null;
    const response = await apiClient.post<ApiResponse<GroupExecution>>(
      `/write-task-groups/${groupId}/run`,
      undefined,
      { timeout: REQUEST_TIMEOUT.runSubmit }
    );
    await showRunFeedback(groupId, response.data.data, response.data.message ?? "");
  } catch (error) {
    if (groupId && startedAt && isApiTimeoutError(error)) {
      await showRunFeedback(
        groupId,
        createOptimisticExecution(groupId, startedAt, form.tasks.length),
        text.executionSubmitted
      );
    } else {
      setFeedback("error", readApiError(error, "\u6267\u884c\u5173\u7cfb\u4efb\u52a1\u7ec4\u5931\u8d25"));
    }
  } finally {
    running.value = false;
  }
}

async function runExisting(groupId: number) {
  running.value = true;
  clearFeedback();
  const group = groups.value.find((item) => item.id === groupId);
  const startedAt = new Date().toISOString();
  try {
    previewData.value = null;
    const response = await apiClient.post<ApiResponse<GroupExecution>>(
      `/write-task-groups/${groupId}/run`,
      undefined,
      { timeout: REQUEST_TIMEOUT.runSubmit }
    );
    await showRunFeedback(groupId, response.data.data, response.data.message ?? "", true);
  } catch (error) {
    if (isApiTimeoutError(error)) {
      await showRunFeedback(
        groupId,
        createOptimisticExecution(groupId, startedAt, group?.tasks.length ?? 0),
        text.executionSubmitted,
        true
      );
    } else {
      setFeedback("error", readApiError(error, "\u6267\u884c\u5173\u7cfb\u4efb\u52a1\u7ec4\u5931\u8d25"));
    }
  } finally {
    running.value = false;
  }
}

async function selectGroup(groupId: number) {
  clearFeedback();
  try {
    const response = await apiClient.get<ApiResponse<GroupResponse>>(`/write-task-groups/${groupId}`);
    previewData.value = null;
    applyGroup(response.data.data);
    normalizeRelations();
    await loadTables();
    await loadExecutions(groupId);
    if (response.data.data.scheduleType === "INTERVAL" && autoRefreshEnabled.value) {
      startAutoRefresh();
    } else {
      stopAutoRefresh();
    }
  } catch (error) {
    setFeedback("error", readApiError(error, "\u8bfb\u53d6\u5173\u7cfb\u4efb\u52a1\u7ec4\u5931\u8d25"));
  }
}

watch(
  () => [route.name, route.params.id],
  async () => {
    await syncRouteState();
  }
);

watch(
  () => groups.value.length,
  () => {
    groupPage.value = clampPage(groupPage.value, groups.value.length, groupPageSize.value);
  }
);

watch(
  () => executions.value.length,
  () => {
    executionPage.value = clampPage(executionPage.value, executions.value.length, executionPageSize.value);
  }
);

watch(groupPageSize, () => {
  groupPage.value = 1;
});

watch(executionPageSize, () => {
  executionPage.value = 1;
});

defineExpose({ form });

onMounted(async () => {
  document.addEventListener("visibilitychange", handleVisibilityChange);
  await loadPageData();
  await syncRouteState();
});

watch(
  () => [form.id, form.scheduleType, form.status, isListPage.value, autoRefreshEnabled.value, executions.value.length, lastExecution.value?.status, optimisticExecutionGroupId.value],
  () => {
    if (shouldAutoRefreshRuntime()) {
      startAutoRefresh();
      return;
    }
    stopAutoRefresh();
  }
);

onBeforeUnmount(() => {
  document.removeEventListener("visibilitychange", handleVisibilityChange);
  stopAutoRefresh();
});
</script>

<style scoped>
.relational-layout--editor {
  grid-template-columns: minmax(0, 1fr);
}

.action-dock {
  position: relative;
  z-index: 1;
  display: grid;
  grid-template-columns: minmax(0, 1fr) auto;
  align-items: center;
  gap: 18px;
  padding: 18px 20px;
  border: 1px solid rgba(47, 125, 87, 0.18);
  border-radius: 20px;
  background: rgba(255, 255, 255, 0.94);
  box-shadow: 0 10px 28px rgba(36, 56, 42, 0.08);
  backdrop-filter: blur(16px);
}

.action-dock__summary {
  display: grid;
  gap: 4px;
  min-width: 0;
}

.action-dock__summary span {
  color: var(--muted);
  font-size: 13px;
}

.action-dock__summary strong {
  font-size: 18px;
}

.action-dock__meta {
  display: flex;
  flex-wrap: wrap;
  gap: 8px 12px;
  color: var(--muted);
  font-size: 13px;
}

.action-dock__actions {
  display: flex;
  gap: 10px;
  flex-wrap: wrap;
  justify-content: flex-end;
}

.task-editor-card {
  display: grid;
  gap: 18px;
  align-content: start;
}

.task-editor-card > .field,
.task-editor-card > .panel,
.task-editor-card > .field-grid {
  margin: 0;
}

.task-editor-card .field {
  gap: 16px;
}

.task-editor-card .field label {
  gap: 12px;
}

.task-editor-card .field span {
  line-height: 1.45;
}

.task-editor-row {
  grid-template-columns: repeat(3, minmax(0, 1fr));
  align-items: start;
  gap: 18px;
}

.task-editor-row__item,
.task-editor-row__spacer {
  min-width: 0;
}

.task-editor-row__spacer {
  visibility: hidden;
  pointer-events: none;
}

.runtime-card {
  display: grid;
  gap: 18px;
}

.runtime-card__refresh {
  display: flex;
  align-items: center;
  gap: 10px;
  flex-wrap: wrap;
}

.metric-grid {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 14px;
}

.metric-card {
  display: grid;
  gap: 8px;
  padding: 16px;
  border: 1px solid rgba(48, 69, 54, 0.1);
  border-radius: 16px;
  background: rgba(247, 248, 244, 0.82);
}

.metric-card span {
  color: var(--muted);
  font-size: 13px;
}

.metric-card strong {
  font-size: 22px;
}

.execution-result-table,
.field-grid {
  display: grid;
  gap: 10px;
}

.runtime-card__empty {
  padding: 12px 14px;
  border-radius: 14px;
  background: rgba(243, 247, 240, 0.9);
  border: 1px dashed rgba(47, 125, 87, 0.18);
}

.execution-result-table__head,
.execution-result-table__row,
.field-grid__head,
.field-grid__row {
  display: grid;
  gap: 12px;
  align-items: start;
}

.execution-result-table__head,
.execution-result-table__row {
  grid-template-columns: minmax(260px, 1.8fr) 80px 90px 90px 90px minmax(260px, 1.4fr);
}

.execution-result-table__row > span {
  min-width: 0;
}

.execution-result-table__validation {
  display: grid;
  gap: 8px;
  min-width: 0;
}

.execution-result-table__errors {
  display: grid;
  gap: 6px;
  padding: 10px 12px;
  border-radius: 12px;
  background: rgba(184, 74, 48, 0.08);
  color: #8f3b28;
  line-height: 1.5;
  overflow-wrap: anywhere;
}

.execution-result-table__errors strong {
  color: #7a2f1f;
}

.execution-result-table__table-name {
  min-width: 0;
  line-height: 1.45;
  overflow-wrap: anywhere;
  word-break: break-word;
}

.execution-result-table__head,
.field-grid__head {
  padding: 0 0 10px;
  border-bottom: 1px solid var(--line);
  color: var(--muted);
  font-size: 13px;
}

.execution-result-table__row,
.field-grid__row {
  padding: 12px 0;
  border-bottom: 1px dashed rgba(48, 69, 54, 0.12);
}

.task-row-plan {
  grid-template-columns: minmax(280px, 1.35fr) repeat(2, minmax(180px, 0.85fr));
  gap: 16px;
}

.task-row-plan label {
  height: 100%;
}

.task-row-plan label > span {
  display: flex;
  align-items: flex-end;
  min-height: 20px;
}

.schedule-mode-panel {
  display: grid;
  gap: 16px;
}

.schedule-mode-grid {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 12px;
}

.schedule-mode-card {
  display: grid;
  gap: 8px;
  padding: 16px;
  text-align: left;
  border: 1px solid var(--line);
  border-radius: 18px;
  background: rgba(255, 255, 255, 0.74);
  color: var(--ink);
}

.schedule-mode-card strong {
  font-size: 16px;
}

.schedule-mode-card span {
  color: var(--muted);
  line-height: 1.5;
}

.schedule-mode-card--active {
  border-color: rgba(47, 125, 87, 0.3);
  background: rgba(47, 125, 87, 0.1);
  box-shadow: inset 0 0 0 1px rgba(47, 125, 87, 0.08);
}

.schedule-current {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  padding: 14px 16px;
  border: 1px solid var(--line);
  border-radius: 16px;
  background: rgba(247, 248, 244, 0.72);
}

.schedule-current span {
  color: var(--muted);
}

.schedule-builder {
  display: grid;
  gap: 14px;
}

.cron-builder-summary {
  padding: 14px 16px;
  border: 1px solid var(--line);
  border-radius: 16px;
  background: rgba(247, 248, 244, 0.72);
  color: var(--muted);
  line-height: 1.6;
}

.cron-builder-summary strong {
  color: var(--ink);
}

.section-lead {
  margin: 6px 0 0;
  color: var(--muted);
  line-height: 1.6;
}

.table-selector {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
  gap: 10px;
}

.selector-chip {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 10px 12px;
  border: 1px solid var(--line);
  border-radius: 14px;
  background: rgba(255, 255, 255, 0.78);
}

.selector-chip input {
  margin: 0;
}

.empty-state--embedded {
  margin-top: 16px;
}

.relation-grid {
  display: grid;
  gap: 14px;
}

.kafka-task-panel {
  display: grid;
  gap: 16px;
}

.column-summary {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  margin-top: 0;
}

.badge {
  display: inline-flex;
  align-items: center;
  padding: 2px 8px;
  border-radius: 999px;
  background: rgba(36, 56, 42, 0.08);
  font-size: 12px;
}

.badge--accent {
  background: var(--accent-soft);
  color: var(--accent-strong);
}

.badge--muted {
  background: rgba(103, 117, 102, 0.1);
  color: var(--muted);
}

.relation-card {
  display: grid;
  gap: 16px;
  padding: 18px;
  border: 1px solid var(--line);
  border-radius: 16px;
  background: rgba(255, 255, 255, 0.78);
}

.preview-table {
  overflow: auto;
}

.preview-table table {
  width: 100%;
  border-collapse: collapse;
}

.preview-table th,
.preview-table td {
  padding: 10px 12px;
  border-bottom: 1px solid var(--line);
  text-align: left;
  white-space: nowrap;
}

.field-grid__head,
.field-grid__row {
  grid-template-columns: minmax(140px, 1.15fr) minmax(150px, 0.95fr) minmax(170px, 1fr) minmax(180px, 1fr) minmax(280px, 1.45fr);
}

.field-grid--kafka-simple {
  gap: 12px;
  min-width: 0;
}

.field-grid--kafka-simple .field-grid__head,
.field-grid--kafka-simple .field-grid__row {
  grid-template-columns:
    minmax(0, 1.25fr)
    minmax(0, 0.9fr)
    minmax(0, 1fr)
    minmax(0, 0.95fr)
    minmax(0, 1.3fr)
    84px;
  gap: 12px;
  width: 100%;
}

.field-grid--kafka-simple .field-grid__head {
  padding: 0 8px 10px;
}

.field-grid--kafka-simple .field-grid__head > span:last-child {
  justify-content: flex-end;
}

.field-grid__row--editable {
  align-items: start;
}

.field-grid--kafka-simple .field-grid__row {
  position: relative;
  padding: 12px 8px;
  border: 1px solid rgba(48, 69, 54, 0.1);
  border-radius: 16px;
  background: rgba(255, 255, 255, 0.86);
  min-width: 0;
}

.field-grid__head > span,
.field-grid__cell {
  min-width: 0;
}

.field-grid__head > span {
  display: flex;
  align-items: flex-end;
  min-height: 20px;
}

.field-grid__cell {
  display: grid;
  align-content: start;
  gap: 10px;
}

.field-grid--kafka-simple .field-grid__cell {
  gap: 10px;
}

.field-grid__name {
  padding-top: 2px;
}

.field-grid__name strong {
  font-size: 15px;
  line-height: 1.5;
}

.field-grid__type,
.field-grid__flags,
.field-grid__generator {
  padding-top: 2px;
}

.field-grid__flags {
  display: flex;
  flex-wrap: wrap;
  align-content: flex-start;
}

.field-grid--editable .field-grid__cell input:not([type="checkbox"]),
.field-grid--editable .field-grid__cell select {
  width: 100%;
}

.field-grid--kafka-simple .field-grid__cell input:not([type="checkbox"]),
.field-grid--kafka-simple .field-grid__cell select {
  min-height: 44px;
}

.compact-control,
.generator-rule {
  min-width: 0;
}

.compact-control select {
  width: 100%;
  min-height: 42px;
  border: 1px solid var(--line);
  border-radius: 12px;
  padding: 10px 12px;
  background: rgba(255, 255, 255, 0.92);
  color: var(--ink);
  font: inherit;
}

.generator-rule {
  display: grid;
  gap: 10px;
  align-content: start;
}

.generator-rule__summary {
  display: flex;
  align-items: center;
  min-height: 42px;
  color: var(--muted);
  line-height: 1.5;
}

.config-details {
  border: 1px solid rgba(48, 69, 54, 0.1);
  border-radius: 14px;
  background: rgba(255, 255, 255, 0.82);
  overflow: hidden;
}

.config-details[open] {
  background: #fbfcf8;
  box-shadow: inset 0 0 0 1px rgba(47, 125, 87, 0.05);
}

.config-details summary {
  cursor: pointer;
  padding: 10px 12px;
  font-weight: 600;
  color: var(--muted);
}

.config-details textarea {
  border: none;
  border-top: 1px solid rgba(48, 69, 54, 0.08);
  border-radius: 0;
  min-height: 96px;
  max-height: 180px;
  padding: 12px;
  background: #fbfcf8;
  font-family: Consolas, "SFMono-Regular", monospace;
  font-size: 12px;
  line-height: 1.55;
  resize: vertical;
}

.field-grid--kafka-simple .field-grid__cell--name,
.field-grid--kafka-simple .field-grid__cell--rule {
  position: relative;
}

.field-grid--kafka-simple .field-grid__cell--flags {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 10px;
  align-items: center;
}

.field-grid--kafka-simple .field-grid__cell--actions {
  display: flex;
  align-items: center;
  justify-content: flex-end;
}

.field-name-shell {
  position: relative;
  min-width: 0;
}

.field-name-display {
  display: grid;
  grid-template-columns: minmax(0, 1fr) auto;
  align-items: center;
  gap: 10px;
  min-height: 44px;
  padding: 0 12px;
  border: 1px solid rgba(48, 69, 54, 0.1);
  border-radius: 12px;
  background: rgba(244, 247, 241, 0.74);
  min-width: 0;
}

.field-name-display__value {
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  font-weight: 600;
  color: var(--ink);
}

.field-grid--kafka-simple .field-name-display__action,
.field-grid--kafka-simple .field-name-editor__action,
.field-grid--kafka-simple .field-grid__remove,
.field-grid--kafka-simple .config-details summary {
  min-height: 36px;
  padding: 0 12px;
  border-radius: 10px;
  font-size: 13px;
  line-height: 1;
  white-space: nowrap;
}

.field-grid--kafka-simple .field-name-display__action,
.field-grid--kafka-simple .field-name-editor__action,
.field-grid--kafka-simple .field-grid__remove {
  color: var(--ink);
}

.field-name-editor {
  position: absolute;
  top: calc(100% + 8px);
  left: 0;
  z-index: 20;
  display: grid;
  gap: 10px;
  width: min(320px, calc(100vw - 96px));
  padding: 12px;
  border: 1px solid rgba(48, 69, 54, 0.14);
  border-radius: 14px;
  background: #ffffff;
  box-shadow: 0 12px 28px rgba(36, 56, 42, 0.12);
}

.field-name-editor__actions {
  display: flex;
  justify-content: flex-end;
  gap: 8px;
}

.field-grid--kafka-simple .field-name-editor input {
  min-height: 40px;
  padding: 0 12px;
}

.field-grid--kafka-simple .checkbox-chip {
  justify-content: center;
  min-height: 44px;
  padding: 0 12px;
  border-radius: 12px;
  background: rgba(255, 255, 255, 0.94);
}

.field-grid--kafka-simple .checkbox-chip span {
  white-space: nowrap;
}

.field-grid--kafka-simple .field-grid__cell--rule {
  grid-template-columns: minmax(0, 1fr) auto;
  align-items: center;
  gap: 10px;
  min-width: 0;
}

.field-grid--kafka-simple .generator-rule__summary {
  min-width: 0;
  min-height: 44px;
  padding: 0 12px;
  border: 1px solid rgba(48, 69, 54, 0.1);
  border-radius: 12px;
  background: rgba(244, 247, 241, 0.74);
  color: var(--ink);
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.field-grid--kafka-simple .config-details {
  position: relative;
  border: none;
  background: transparent;
  overflow: visible;
}

.field-grid--kafka-simple .config-details[open] {
  background: transparent;
  box-shadow: none;
}

.field-grid--kafka-simple .config-details summary {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  border: 1px solid var(--line);
  background: rgba(255, 255, 255, 0.92);
}

.field-grid--kafka-simple .config-details textarea {
  position: absolute;
  top: calc(100% + 8px);
  right: 0;
  z-index: 20;
  width: min(420px, calc(100vw - 96px));
  min-height: 180px;
  max-height: 280px;
  padding: 12px;
  border: 1px solid rgba(48, 69, 54, 0.14);
  border-radius: 14px;
  border-top: 1px solid rgba(48, 69, 54, 0.14);
  background: #fbfcf8;
  box-shadow: 0 12px 28px rgba(36, 56, 42, 0.12);
}

.task-list__runtime {
  display: flex;
  flex-wrap: wrap;
  gap: 8px 12px;
  padding-top: 12px;
  border-top: 1px solid rgba(48, 69, 54, 0.08);
  color: var(--muted);
  font-size: 13px;
}

.task-list__runtime strong {
  color: var(--ink);
}

@media (max-width: 1080px) {
  .metric-grid,
  .schedule-mode-grid {
    grid-template-columns: 1fr;
  }

  .task-editor-row,
  .task-row-plan {
    grid-template-columns: 1fr;
  }

  .task-editor-row__spacer {
    display: none;
  }

  .execution-result-table__head,
  .execution-result-table__row,
  .field-grid__head,
  .field-grid__row {
    grid-template-columns: 1fr;
  }

  .field-grid--kafka-simple .field-grid__head {
    display: none;
  }

  .field-grid--kafka-simple .field-grid__row {
    grid-template-columns: repeat(2, minmax(0, 1fr));
    padding: 14px;
  }

  .field-grid--kafka-simple .field-grid__cell--name,
  .field-grid--kafka-simple .field-grid__cell--rule {
    grid-column: 1 / -1;
  }

  .field-grid--kafka-simple .field-grid__cell--actions {
    justify-content: flex-start;
  }
}

@media (max-width: 720px) {
  .action-dock {
    grid-template-columns: 1fr;
    padding: 16px;
  }

  .action-dock__actions {
    justify-content: flex-start;
  }

  .schedule-current {
    align-items: flex-start;
    flex-direction: column;
  }

  .field-grid--kafka-simple .field-grid__row {
    grid-template-columns: 1fr;
  }

  .field-grid--kafka-simple .field-grid__cell--name,
  .field-grid--kafka-simple .field-grid__cell--rule {
    grid-column: auto;
  }

  .field-grid--kafka-simple .field-grid__cell--flags {
    grid-template-columns: 1fr;
  }

  .field-name-editor,
  .field-grid--kafka-simple .config-details textarea {
    position: static;
    width: 100%;
  }
}
</style>
