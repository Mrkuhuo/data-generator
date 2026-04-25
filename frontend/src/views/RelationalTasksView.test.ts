import { AxiosError } from "axios";
import { enableAutoUnmount, flushPromises, mount, type VueWrapper } from "@vue/test-utils";
import { createMemoryHistory, createRouter } from "vue-router";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { REQUEST_TIMEOUT } from "../api/client";
import RelationalTasksView from "./RelationalTasksView.vue";

const { getMock, postMock, putMock } = vi.hoisted(() => ({
  getMock: vi.fn(),
  postMock: vi.fn(),
  putMock: vi.fn()
}));

vi.mock("../api/client", async () => {
  const actual = await vi.importActual<typeof import("../api/client")>("../api/client");
  return {
    ...actual,
    apiClient: {
      get: getMock,
      post: postMock,
      put: putMock
    }
  };
});

function ok<T>(data: T, message: string | null = null) {
  return Promise.resolve({
    data: {
      success: true,
      data,
      message
    }
  });
}

function sampleGroup(overrides: Partial<Record<string, unknown>> = {}) {
  return {
    id: 12,
    name: "order-flow",
    connectionId: 9,
    description: "demo",
    seed: 20260422,
    status: "READY",
    scheduleType: "MANUAL",
    cronExpression: null,
    triggerAt: null,
    intervalSeconds: null,
    maxRuns: null,
    maxRowsTotal: null,
    lastTriggeredAt: null,
    schedulerState: "MANUAL",
    nextFireAt: null,
    previousFireAt: null,
    tasks: [
      {
        id: 1,
        taskKey: "customer",
        name: "demo.customer",
        tableName: "demo.customer",
        tableMode: "USE_EXISTING",
        writeMode: "APPEND",
        batchSize: 500,
        seed: null,
        description: null,
        status: "READY",
        rowPlan: {
          mode: "FIXED",
          rowCount: 100,
          driverTaskKey: null,
          minChildrenPerParent: null,
          maxChildrenPerParent: null
        },
        columns: [
          {
            columnName: "id",
            dbType: "INT",
            lengthValue: null,
            precisionValue: 10,
            scaleValue: undefined,
            nullableFlag: false,
            primaryKeyFlag: true,
            foreignKeyFlag: false,
            generatorType: "SEQUENCE",
            generatorConfig: { start: 1, step: 1 },
            sortOrder: 0
          }
        ]
      }
    ],
    relations: [],
    ...overrides
  };
}

function sampleKafkaGroup(overrides: Partial<Record<string, unknown>> = {}) {
  return sampleGroup({
    id: 77,
    name: "kafka-flow",
    connectionId: 11,
    description: "kafka demo",
    tasks: [
      {
        id: 101,
        taskKey: "orders",
        name: "Orders Topic",
        tableName: "demo.orders",
        tableMode: "CREATE_IF_MISSING",
        writeMode: "APPEND",
        batchSize: 100,
        seed: null,
        description: null,
        status: "READY",
        rowPlan: {
          mode: "FIXED",
          rowCount: 2,
          driverTaskKey: null,
          minChildrenPerParent: null,
          maxChildrenPerParent: null
        },
        targetConfigJson: JSON.stringify({
          payloadFormat: "JSON",
          keyMode: "FIELD",
          keyField: "orderId",
          keyPath: "orderId"
        }),
        payloadSchemaJson: null,
        columns: [
          {
            columnName: "orderId",
            dbType: "BIGINT",
            lengthValue: null,
            precisionValue: null,
            scaleValue: null,
            nullableFlag: false,
            primaryKeyFlag: true,
            foreignKeyFlag: false,
            generatorType: "SEQUENCE",
            generatorConfig: { start: 1, step: 1 },
            sortOrder: 0
          },
          {
            columnName: "userId",
            dbType: "BIGINT",
            lengthValue: null,
            precisionValue: null,
            scaleValue: null,
            nullableFlag: false,
            primaryKeyFlag: false,
            foreignKeyFlag: false,
            generatorType: "SEQUENCE",
            generatorConfig: { start: 101, step: 1 },
            sortOrder: 1
          }
        ]
      },
      {
        id: 102,
        taskKey: "items",
        name: "Order Items Topic",
        tableName: "demo.items",
        tableMode: "CREATE_IF_MISSING",
        writeMode: "APPEND",
        batchSize: 100,
        seed: null,
        description: null,
        status: "READY",
        rowPlan: {
          mode: "CHILD_PER_PARENT",
          rowCount: null,
          driverTaskKey: "orders",
          minChildrenPerParent: 2,
          maxChildrenPerParent: 2
        },
        targetConfigJson: JSON.stringify({
          payloadFormat: "JSON",
          keyMode: "FIELD",
          keyPath: "orderId"
        }),
        payloadSchemaJson: JSON.stringify({
          type: "OBJECT",
          children: [
            {
              name: "orderId",
              type: "SCALAR",
              valueType: "LONG",
              generatorType: "SEQUENCE",
              generatorConfig: { start: 1, step: 1 },
              nullable: false
            },
            {
              name: "buyer",
              type: "OBJECT",
              children: [
                {
                  name: "userId",
                  type: "SCALAR",
                  valueType: "LONG",
                  generatorType: "SEQUENCE",
                  generatorConfig: { start: 1, step: 1 },
                  nullable: false
                }
              ]
            }
          ]
        }),
        columns: []
      }
    ],
    relations: [
      {
        id: 201,
        relationName: "order_to_item",
        parentTaskKey: "orders",
        childTaskKey: "items",
        relationMode: "KAFKA_EVENT",
        relationType: "ONE_TO_MANY",
        sourceMode: "CURRENT_BATCH",
        selectionStrategy: "PARENT_DRIVEN",
        reusePolicy: "ALLOW_REPEAT",
        parentColumns: [],
        childColumns: [],
        nullRate: 0,
        mixedExistingRatio: null,
        minChildrenPerParent: 2,
        maxChildrenPerParent: 2,
        mappingConfigJson: JSON.stringify({
          fieldMappings: [
            { from: "orderId", to: "orderId", required: true },
            { from: "userId", to: "buyer.userId", required: true }
          ]
        }),
        sortOrder: 0
      }
    ],
    ...overrides
  });
}

enableAutoUnmount(afterEach);

afterEach(() => {
  vi.useRealTimers();
});

describe("RelationalTasksView", () => {
  beforeEach(() => {
    getMock.mockReset();
    postMock.mockReset();
    putMock.mockReset();

    getMock.mockImplementation((url: string) => {
      if (url === "/connections") {
        return ok([
          { id: 9, name: "mysql-demo", dbType: "MYSQL" },
          { id: 11, name: "kafka-demo", dbType: "KAFKA" }
        ]);
      }
      if (url === "/write-task-groups") {
        return ok([
          sampleGroup({ id: 1, name: "interval-ready", scheduleType: "INTERVAL", intervalSeconds: 10, status: "READY", schedulerState: "STOPPED" }),
          sampleGroup({ id: 2, name: "cron-ready", scheduleType: "CRON", cronExpression: "0 0/5 * * * ?", schedulerState: "NORMAL" }),
          sampleGroup({ id: 3, name: "cron-paused", scheduleType: "CRON", cronExpression: "0 0/5 * * * ?", status: "PAUSED", schedulerState: "PAUSED" }),
          sampleGroup({ id: 4, name: "interval-running", scheduleType: "INTERVAL", intervalSeconds: 5, status: "RUNNING", schedulerState: "NORMAL" })
        ]);
      }
      if (url === "/write-task-groups/12") {
        return ok(sampleGroup({
          id: 12,
          name: "saved-group",
          scheduleType: "CRON",
          cronExpression: "0 0/5 * * * ?",
          schedulerState: "NORMAL",
          nextFireAt: "2026-04-23T02:00:00Z"
        }));
      }
      if (url === "/write-task-groups/77") {
        return ok(sampleKafkaGroup());
      }
      if (url === "/write-task-groups/77/executions") {
        return ok([]);
      }
      if (url === "/write-task-groups/12/executions") {
        return ok([
          {
            id: 501,
            triggerType: "MANUAL",
            status: "SUCCESS",
            startedAt: "2026-04-23T02:00:00Z",
            finishedAt: "2026-04-23T02:00:05Z",
            plannedTableCount: 1,
            completedTableCount: 1,
            successTableCount: 1,
            failureTableCount: 0,
            insertedRowCount: 120,
            summary: {},
            tables: [
              {
                id: 1,
                tableName: "demo.customer",
                status: "SUCCESS",
                beforeWriteRowCount: 1000,
                afterWriteRowCount: 1120,
                insertedCount: 120,
                nullViolationCount: 0,
                blankStringCount: 0,
                fkMissCount: 0,
                pkDuplicateCount: 0
              }
            ]
          }
        ]);
      }
      if (url === "/connections/9/tables") {
        return ok([
          { schemaName: "demo", tableName: "customer" },
          { schemaName: "demo", tableName: "orders" }
        ]);
      }
      if (url.startsWith("/connections/9/schema-model")) {
        return ok({
          tables: [
            {
              tableName: "demo.customer",
              columns: [
                { columnName: "id", dbType: "BIGINT", length: null, precision: null, scale: null, nullable: false, primaryKey: true, autoIncrement: false }
              ],
              foreignKeys: []
            },
            {
              tableName: "demo.orders",
              columns: [
                { columnName: "id", dbType: "BIGINT", length: null, precision: null, scale: null, nullable: false, primaryKey: true, autoIncrement: false },
                { columnName: "customer_id", dbType: "BIGINT", length: null, precision: null, scale: null, nullable: false, primaryKey: false, autoIncrement: false },
                {
                  columnName: "status",
                  dbType: "ENUM",
                  length: null,
                  precision: null,
                  scale: null,
                  nullable: false,
                  primaryKey: false,
                  autoIncrement: false,
                  enumValues: ["pending", "processing", "completed"]
                }
              ],
              foreignKeys: []
            }
          ],
          relations: [
            {
              constraintName: "fk_orders_customer",
              parentTable: "customer",
              parentColumns: ["id"],
              childTable: "orders",
              childColumns: ["customer_id"]
            }
          ]
        });
      }
      if (url === "/write-task-groups/9/executions") {
        return ok([
          {
            id: 901,
            triggerType: "CONTINUOUS",
            status: "SUCCESS",
            startedAt: "2026-04-23T02:10:00Z",
            finishedAt: "2026-04-23T02:10:05Z",
            plannedTableCount: 1,
            completedTableCount: 1,
            successTableCount: 1,
            failureTableCount: 0,
            insertedRowCount: 50,
            summary: {},
            tables: [
              {
                id: 91,
                tableName: "demo.customer",
                status: "SUCCESS",
                beforeWriteRowCount: 1120,
                afterWriteRowCount: 1170,
                insertedCount: 50,
                nullViolationCount: 0,
                blankStringCount: 0,
                fkMissCount: 0,
                pkDuplicateCount: 0
              }
            ]
          }
        ]);
      }
      return Promise.reject(new Error(`Unhandled GET ${url}`));
    });

    postMock.mockImplementation((url: string, payload?: unknown) => {
      if (url === "/write-task-groups/preview") {
        return ok({
          seed: 20260422,
          tables: [
            {
              taskId: 1,
              taskKey: "customer",
              taskName: "customer",
              tableName: "demo.customer",
              generatedRowCount: 2,
              previewRowCount: 2,
              foreignKeyMissCount: 0,
              nullViolationCount: 0,
              rows: [{ id: 1 }, { id: 2 }]
            }
          ]
        });
      }
      if (url === "/write-task-groups") {
        return ok(
          (payload as { connectionId?: number }).connectionId === 11
            ? sampleKafkaGroup({
              id: 77,
              name: (payload as { name?: string }).name ?? "kafka-flow",
              scheduleType: (payload as { scheduleType?: string }).scheduleType ?? "MANUAL",
              cronExpression: (payload as { cronExpression?: string | null }).cronExpression ?? null,
              schedulerState: "NORMAL"
            })
            : sampleGroup({
              id: 12,
              name: "saved-group",
              scheduleType: (payload as { scheduleType?: string }).scheduleType ?? "MANUAL",
              cronExpression: (payload as { cronExpression?: string | null }).cronExpression ?? null,
              schedulerState: "NORMAL"
            }),
          "saved"
        );
      }
      if (url === "/write-task-groups/12/run") {
        return ok({
          id: 601,
          triggerType: "MANUAL",
          status: "SUCCESS",
          startedAt: "2026-04-23T02:20:00Z",
          finishedAt: "2026-04-23T02:20:04Z",
          plannedTableCount: 1,
          completedTableCount: 1,
          successTableCount: 1,
          failureTableCount: 0,
          insertedRowCount: 88,
          summary: {},
          tables: [
            {
              id: 61,
              tableName: "demo.customer",
              status: "SUCCESS",
              beforeWriteRowCount: 1200,
              afterWriteRowCount: 1288,
              insertedCount: 88,
              nullViolationCount: 0,
              blankStringCount: 0,
              fkMissCount: 0,
              pkDuplicateCount: 0
            }
          ]
        }, "ran");
      }
      if (/\/write-task-groups\/\d+\/(start|pause|resume|stop)$/.test(url)) {
        return ok(sampleGroup({ id: Number(url.split("/")[2]) }), "updated");
      }
      return Promise.reject(new Error(`Unhandled POST ${url}`));
    });
  });

  async function mountAt(path: string) {
    const router = createRouter({
      history: createMemoryHistory(),
      routes: [
        { path: "/relational-write-tasks", name: "relational-write-tasks", component: RelationalTasksView },
        { path: "/relational-write-tasks/new", name: "relational-write-task-create", component: RelationalTasksView },
        { path: "/relational-write-tasks/:id/edit", name: "relational-write-task-edit", component: RelationalTasksView }
      ]
    });

    await router.push(path);
    await router.isReady();

    const wrapper = mount(RelationalTasksView, {
      global: {
        plugins: [router]
      }
    });

    await flushPromises();
    return { wrapper, router };
  }

  async function selectTables(wrapper: VueWrapper) {
    const selects = wrapper.findAll("select");
    await selects[0].setValue("9");
    await flushPromises();

    const checkboxes = wrapper.findAll('input[type="checkbox"]');
    await checkboxes[0].setValue(true);
    await checkboxes[1].setValue(true);
    await flushPromises();
  }

  async function importSchema(wrapper: VueWrapper) {
    await selectTables(wrapper);
    await wrapper.get('[data-testid="import-schema"]').trigger("click");
    await flushPromises();
  }

  it("imports schema and renders generated tasks on create route", async () => {
    const { wrapper } = await mountAt("/relational-write-tasks/new");

    await importSchema(wrapper);

    expect(wrapper.text()).toContain("demo.customer");
    expect(wrapper.text()).toContain("demo.orders");
    expect(wrapper.text()).toContain("fk_orders_customer");
  }, 15000);

  it("auto imports selected tables before preview when tasks are still empty", async () => {
    const { wrapper } = await mountAt("/relational-write-tasks/new");

    await selectTables(wrapper);
    await wrapper.get('[data-testid="preview-current"]').trigger("click");
    await flushPromises();

    expect(getMock).toHaveBeenCalledWith("/connections/9/schema-model?tableNames=demo.customer&tableNames=demo.orders");
    expect(postMock).toHaveBeenCalledWith(
      "/write-task-groups/preview",
      expect.objectContaining({
        group: expect.objectContaining({
          scheduleType: "MANUAL"
        }),
        previewCount: 5
      }),
      expect.objectContaining({ timeout: REQUEST_TIMEOUT.longRunning })
    );
    expect(wrapper.findAll("tbody tr")).toHaveLength(2);
  }, 15000);

  it("saves configured cron schedule fields and switches to edit route", async () => {
    const { wrapper, router } = await mountAt("/relational-write-tasks/new");

    await importSchema(wrapper);
    await wrapper.get('[data-testid="schedule-type-CRON"]').trigger("click");
    await wrapper.get('[data-testid="cron-builder-mode"]').setValue("EVERY_MINUTES");
    await wrapper.get('[data-testid="cron-minute-step"]').setValue("15");
    await wrapper.get('[data-testid="group-form"]').trigger("submit");
    await flushPromises();

    expect(postMock).toHaveBeenCalledWith("/write-task-groups", expect.objectContaining({
      scheduleType: "CRON",
      cronExpression: "0 0/15 * * * ?"
    }));
    expect(wrapper.text()).toContain("每 15 分钟执行一次");
    expect(router.currentRoute.value.name).toBe("relational-write-task-edit");
    expect(router.currentRoute.value.params.id).toBe("12");
  }, 15000);

  it("parses existing cron tasks into configurable schedule controls", async () => {
    const { wrapper } = await mountAt("/relational-write-tasks/12/edit");

    expect((wrapper.get('[data-testid="cron-builder-mode"]').element as HTMLSelectElement).value).toBe("EVERY_MINUTES");
    expect(wrapper.text()).toContain("每 5 分钟执行一次");
  }, 15000);

  it("renders scheduling actions on list route and calls the matching endpoints", async () => {
    const { wrapper } = await mountAt("/relational-write-tasks");

    await wrapper.get('[data-testid="start-group-1"]').trigger("click");
    await flushPromises();
    await wrapper.get('[data-testid="pause-group-2"]').trigger("click");
    await flushPromises();
    await wrapper.get('[data-testid="resume-group-3"]').trigger("click");
    await flushPromises();
    await wrapper.get('[data-testid="stop-group-4"]').trigger("click");
    await flushPromises();

    expect(postMock).toHaveBeenCalledWith("/write-task-groups/1/start");
    expect(postMock).toHaveBeenCalledWith("/write-task-groups/2/pause");
    expect(postMock).toHaveBeenCalledWith("/write-task-groups/3/resume");
    expect(postMock).toHaveBeenCalledWith("/write-task-groups/4/stop");
  }, 15000);

  it("renders top action dock and execution summary after manual run", async () => {
    const { wrapper } = await mountAt("/relational-write-tasks/12/edit");

    expect(wrapper.get('[data-testid="editor-action-dock"]').text()).toContain("保存任务组");
    expect(wrapper.get('[data-testid="execution-summary"]').text()).toContain("本次写入结果");
    expect(wrapper.text()).toContain("demo.customer");
    expect(wrapper.text()).toContain("INT(10)");
    expect(wrapper.text()).not.toContain("undefined");

    await wrapper.get('[data-testid="run-current"]').trigger("click");
    await flushPromises();

    expect(postMock).toHaveBeenCalledWith(
      "/write-task-groups/12/run",
      undefined,
      expect.objectContaining({ timeout: REQUEST_TIMEOUT.runSubmit })
    );
    expect(wrapper.get('[data-testid="execution-summary"]').text()).toContain("88");
    expect(wrapper.get('[data-testid="execution-summary"]').text()).toContain("1200");
    expect(wrapper.get('[data-testid="execution-summary"]').text()).toContain("1288");
    expect(wrapper.get(".execution-result-table__table-name").text()).toBe("demo.customer");
  }, 15000);

  it("shows submitted feedback and refreshes execution result after manual run request times out", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-04-24T03:00:00Z"));
    Object.defineProperty(document, "hidden", { value: false, configurable: true });

    let executionPollCount = 0;
    getMock.mockImplementation((url: string) => {
      if (url === "/connections") {
        return ok([
          { id: 9, name: "mysql-demo", dbType: "MYSQL" }
        ]);
      }
      if (url === "/write-task-groups") {
        return ok([
          sampleGroup({ id: 12, name: "saved-group", scheduleType: "MANUAL", schedulerState: "MANUAL" })
        ]);
      }
      if (url === "/write-task-groups/12") {
        return ok(sampleGroup({
          id: 12,
          name: "saved-group",
          scheduleType: "MANUAL",
          schedulerState: "MANUAL"
        }));
      }
      if (url === "/write-task-groups/12/executions") {
        executionPollCount += 1;
        if (executionPollCount < 3) {
          return ok([]);
        }
        return ok([
          {
            id: 777,
            writeTaskGroupId: 12,
            triggerType: "MANUAL",
            status: "SUCCESS",
            startedAt: "2026-04-24T03:00:01Z",
            finishedAt: "2026-04-24T03:00:06Z",
            plannedTableCount: 1,
            completedTableCount: 1,
            successTableCount: 1,
            failureTableCount: 0,
            insertedRowCount: 66,
            errorSummary: null,
            summary: {},
            tables: [
              {
                id: 71,
                tableName: "demo.customer",
                status: "SUCCESS",
                beforeWriteRowCount: 1500,
                afterWriteRowCount: 1566,
                insertedCount: 66,
                nullViolationCount: 0,
                blankStringCount: 0,
                fkMissCount: 0,
                pkDuplicateCount: 0
              }
            ]
          }
        ]);
      }
      if (url === "/connections/9/tables") {
        return ok([]);
      }
      return Promise.reject(new Error(`Unhandled GET ${url}`));
    });

    postMock.mockImplementation((url: string) => {
      if (url === "/write-task-groups/12/run") {
        return Promise.reject(new AxiosError("timeout of 4000ms exceeded", "ECONNABORTED"));
      }
      return Promise.reject(new Error(`Unhandled POST ${url}`));
    });

    const { wrapper } = await mountAt("/relational-write-tasks/12/edit");

    await wrapper.get('[data-testid="run-current"]').trigger("click");
    await flushPromises();

    expect(wrapper.text()).toContain("任务已提交");
    expect(wrapper.get('[data-testid="execution-summary"]').text()).toContain("待执行");

    await vi.advanceTimersByTimeAsync(3200);
    await flushPromises();
    await vi.advanceTimersByTimeAsync(3200);
    await flushPromises();

    expect(wrapper.get('[data-testid="execution-summary"]').text()).toContain("66");
    expect(wrapper.get('[data-testid="execution-summary"]').text()).toContain("1500");
    expect(wrapper.text()).toContain("执行已完成");
  }, 15000);

  it("shows auto refresh controls for interval groups", async () => {
    Object.defineProperty(document, "hidden", { value: false, configurable: true });
    getMock.mockImplementation((url: string) => {
      if (url === "/connections") {
        return ok([{ id: 9, name: "mysql-demo", dbType: "MYSQL" }]);
      }
      if (url === "/write-task-groups") {
        return ok([sampleGroup({ id: 9, name: "interval-running", scheduleType: "INTERVAL", intervalSeconds: 5, status: "RUNNING", schedulerState: "NORMAL" })]);
      }
      if (url === "/write-task-groups/9") {
        return ok(sampleGroup({ id: 9, name: "interval-running", scheduleType: "INTERVAL", intervalSeconds: 5, status: "RUNNING", schedulerState: "NORMAL" }));
      }
      if (url === "/write-task-groups/9/executions") {
        return ok([
          {
            id: 901,
            triggerType: "CONTINUOUS",
            status: "SUCCESS",
            startedAt: "2026-04-23T02:10:00Z",
            finishedAt: "2026-04-23T02:10:05Z",
            plannedTableCount: 1,
            completedTableCount: 1,
            successTableCount: 1,
            failureTableCount: 0,
            insertedRowCount: 50,
            summary: {},
            tables: [
              {
                id: 91,
                tableName: "demo.customer",
                status: "SUCCESS",
                beforeWriteRowCount: 1120,
                afterWriteRowCount: 1170,
                insertedCount: 50,
                nullViolationCount: 0,
                blankStringCount: 0,
                fkMissCount: 0,
                pkDuplicateCount: 0
              }
            ]
          }
        ]);
      }
      if (url === "/connections/9/tables") {
        return ok([]);
      }
      return Promise.reject(new Error(`Unhandled GET ${url}`));
    });

    const { wrapper } = await mountAt("/relational-write-tasks/9/edit");

    expect(wrapper.get('[data-testid="execution-summary"]').text()).toContain("自动刷新 5 秒");
    expect(wrapper.get('[data-testid="execution-summary"]').text()).toContain("50");
  }, 15000);

  it("imports enum columns with enum generator values", async () => {
    const { wrapper } = await mountAt("/relational-write-tasks/new");

    await importSchema(wrapper);

    const vm = wrapper.vm as unknown as {
      form: {
        tasks: Array<{
          tableName: string;
          columns: Array<{
            columnName: string;
            generatorType: string;
            generatorConfig: Record<string, unknown>;
          }>;
        }>;
      };
    };

    const orderTask = vm.form.tasks.find((task) => task.tableName === "demo.orders");
    const statusColumn = orderTask?.columns.find((column) => column.columnName === "status");

    expect(statusColumn?.generatorType).toBe("ENUM");
    expect(statusColumn?.generatorConfig).toEqual({ values: ["pending", "processing", "completed"] });
  }, 15000);

  it("saves kafka group payload with topic config, payload schema and event mappings", async () => {
    const { wrapper } = await mountAt("/relational-write-tasks/new");

    const selects = wrapper.findAll("select");
    await selects[0].setValue("11");
    await flushPromises();

    await wrapper.get('[data-testid="add-kafka-task"]').trigger("click");
    await wrapper.get('[data-testid="add-kafka-task"]').trigger("click");
    await flushPromises();

    const vm = wrapper.vm as unknown as {
      form: {
        name: string;
        connectionId: number | null;
        tasks: Array<Record<string, unknown>>;
        relations: Array<Record<string, unknown>>;
      };
    };

    vm.form.name = "kafka-flow";
    vm.form.connectionId = 11;
    vm.form.tasks[0].name = "Orders Topic";
    vm.form.tasks[0].tableName = "orders.topic";
    vm.form.tasks[0].keyMode = "FIELD";
    vm.form.tasks[0].keyField = "orderId";
    vm.form.tasks[0].columns = [
      {
        columnName: "orderId",
        dbType: "BIGINT",
        lengthValue: null,
        precisionValue: null,
        scaleValue: null,
        nullableFlag: false,
        primaryKeyFlag: true,
        foreignKeyFlag: false,
        generatorType: "SEQUENCE",
        generatorConfig: { start: 1, step: 1 },
        generatorConfigText: JSON.stringify({ start: 1, step: 1 }),
        enumValues: null,
        sortOrder: 0
      },
      {
        columnName: "userId",
        dbType: "BIGINT",
        lengthValue: null,
        precisionValue: null,
        scaleValue: null,
        nullableFlag: false,
        primaryKeyFlag: false,
        foreignKeyFlag: false,
        generatorType: "SEQUENCE",
        generatorConfig: { start: 101, step: 1 },
        generatorConfigText: JSON.stringify({ start: 101, step: 1 }),
        enumValues: null,
        sortOrder: 1
      }
    ];
    vm.form.tasks[1].name = "Order Items Topic";
    vm.form.tasks[1].tableName = "items.topic";
    vm.form.tasks[1].kafkaMessageMode = "COMPLEX";
    vm.form.tasks[1].keyMode = "FIELD";
    vm.form.tasks[1].keyPath = "orderId";
    vm.form.tasks[1].rowPlan = {
      mode: "CHILD_PER_PARENT",
      rowCount: null,
      driverTaskKey: String(vm.form.tasks[0].taskKey),
      minChildrenPerParent: 2,
      maxChildrenPerParent: 2
    };
    vm.form.tasks[1].payloadSchemaRoot = {
      id: "schema-root",
      name: "",
      type: "OBJECT",
      nullable: false,
      valueType: "STRING",
      generatorType: "STRING",
      generatorConfigJson: "{}",
      minItems: null,
      maxItems: null,
      children: [
        {
          id: "schema-order-id",
          name: "orderId",
          type: "SCALAR",
          nullable: false,
          valueType: "LONG",
          generatorType: "SEQUENCE",
          generatorConfigJson: JSON.stringify({ start: 1, step: 1 }),
          children: [],
          itemSchema: null,
          minItems: null,
          maxItems: null
        },
        {
          id: "schema-buyer",
          name: "buyer",
          type: "OBJECT",
          nullable: false,
          valueType: "STRING",
          generatorType: "STRING",
          generatorConfigJson: "{}",
          minItems: null,
          maxItems: null,
          children: [
            {
              id: "schema-user-id",
              name: "userId",
              type: "SCALAR",
              nullable: false,
              valueType: "LONG",
              generatorType: "SEQUENCE",
              generatorConfigJson: JSON.stringify({ start: 101, step: 1 }),
              children: [],
              itemSchema: null,
              minItems: null,
              maxItems: null
            }
          ],
          itemSchema: null
        }
      ],
      itemSchema: null
    };
    vm.form.tasks[1].columns = [];
    vm.form.relations = [
      {
        id: null,
        relationName: "order_to_item",
        parentTaskKey: String(vm.form.tasks[0].taskKey),
        childTaskKey: String(vm.form.tasks[1].taskKey),
        relationMode: "KAFKA_EVENT",
        relationType: "ONE_TO_MANY",
        sourceMode: "CURRENT_BATCH",
        selectionStrategy: "PARENT_DRIVEN",
        reusePolicy: "ALLOW_REPEAT",
        parentColumns: [],
        childColumns: [],
        nullRate: 0,
        mixedExistingRatio: null,
        minChildrenPerParent: 2,
        maxChildrenPerParent: 2,
        mappingEntries: [
          { localId: "m1", from: "orderId", to: "orderId", required: true },
          { localId: "m2", from: "userId", to: "buyer.userId", required: true }
        ],
        sortOrder: 0
      }
    ];

    await wrapper.get('[data-testid="group-form"]').trigger("submit");
    await flushPromises();

    expect(postMock).toHaveBeenCalledWith("/write-task-groups", expect.objectContaining({
      connectionId: 11,
      tasks: expect.arrayContaining([
        expect.objectContaining({
          tableName: "orders.topic",
          targetConfigJson: expect.stringContaining("\"keyField\":\"orderId\""),
          payloadSchemaJson: null
        }),
        expect.objectContaining({
          tableName: "items.topic",
          targetConfigJson: expect.stringContaining("\"keyPath\":\"orderId\""),
          payloadSchemaJson: expect.stringContaining("\"buyer\"")
        })
      ]),
      relations: [
        expect.objectContaining({
          relationMode: "KAFKA_EVENT",
          mappingConfigJson: expect.stringContaining("\"buyer.userId\"")
        })
      ]
    }));
  }, 15000);

  it("loads kafka edit page and restores payload schema with relation mappings", async () => {
    const { wrapper } = await mountAt("/relational-write-tasks/77/edit");

    expect(wrapper.text()).toContain("Kafka 任务列表");
    expect(wrapper.text()).toContain("Orders Topic");
    expect(wrapper.text()).toContain("Order Items Topic");
    expect(wrapper.text()).toContain("buyer.userId");
    expect(wrapper.text()).toContain("orderId");
  }, 15000);
});
