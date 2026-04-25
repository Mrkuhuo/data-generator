import { enableAutoUnmount, flushPromises, mount } from "@vue/test-utils";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import JobsView from "./JobsView.vue";

const { getMock, postMock, putMock, deleteMock, routeState, routerPushMock, routerReplaceMock } = vi.hoisted(() => ({
  getMock: vi.fn(),
  postMock: vi.fn(),
  putMock: vi.fn(),
  deleteMock: vi.fn(),
  routeState: { current: null as null | { name: string; params: Record<string, unknown>; query: Record<string, unknown> } },
  routerPushMock: vi.fn(),
  routerReplaceMock: vi.fn()
}));

vi.mock("../api/client", async () => {
  const actual = await vi.importActual<typeof import("../api/client")>("../api/client");
  return {
    ...actual,
    apiClient: {
      get: getMock,
      post: postMock,
      put: putMock,
      delete: deleteMock
    }
  };
});

vi.mock("vue-router", async () => {
  const actual = await vi.importActual<typeof import("vue-router")>("vue-router");
  const { reactive } = await vi.importActual<typeof import("vue")>("vue");
  const route = reactive({
    name: "write-tasks",
    params: {} as Record<string, unknown>,
    query: {} as Record<string, unknown>
  });

  routeState.current = route;

  async function applyLocation(to: unknown) {
    if (!to || typeof to !== "object") {
      return;
    }
    const location = to as { name?: string; params?: Record<string, unknown>; query?: Record<string, unknown> };
    if (location.name) {
      route.name = location.name;
    }
    route.params = location.params ? { ...location.params } : {};
    route.query = location.query ? { ...location.query } : {};
  }

  routerPushMock.mockImplementation(async (to: unknown) => {
    await applyLocation(to);
  });
  routerReplaceMock.mockImplementation(async (to: unknown) => {
    await applyLocation(to);
  });

  return {
    ...actual,
    useRoute: () => route,
    useRouter: () => ({
      push: routerPushMock,
      replace: routerReplaceMock
    })
  };
});

function setRoute(name: string, params: Record<string, unknown> = {}, query: Record<string, unknown> = {}) {
  if (!routeState.current) {
    throw new Error("Route state not initialized");
  }
  routeState.current.name = name;
  routeState.current.params = { ...params };
  routeState.current.query = { ...query };
}

function ok<T>(data: T, message: string | null = null) {
  return Promise.resolve({
    data: {
      success: true,
      data,
      message
    }
  });
}

enableAutoUnmount(afterEach);

function buildTask(overrides: Record<string, unknown> = {}) {
  return {
    id: 7,
    createdAt: "2026-04-14T00:00:00Z",
    updatedAt: "2026-04-14T00:00:00Z",
    name: "nightly task",
    connectionId: 2,
    tableName: "synthetic_orders",
    tableMode: "USE_EXISTING",
    writeMode: "APPEND",
    rowCount: 50,
    batchSize: 100,
    seed: 1,
    status: "READY",
    scheduleType: "MANUAL",
    cronExpression: null,
    triggerAt: null,
    intervalSeconds: null,
    maxRuns: null,
    maxRowsTotal: null,
    description: "existing task",
    targetConfigJson: null,
    payloadSchemaJson: null,
    lastTriggeredAt: null,
    schedulerState: "MANUAL",
    nextFireAt: null,
    previousFireAt: null,
    columns: [
      {
        columnName: "id",
        dbType: "BIGINT",
        lengthValue: null,
        precisionValue: null,
        scaleValue: null,
        nullableFlag: false,
        primaryKeyFlag: true,
        generatorType: "SEQUENCE",
        generatorConfig: { start: 1, step: 1 },
        sortOrder: 0
      }
    ],
    ...overrides
  };
}

function configureWorkspace(
  initialTasks = [buildTask()],
  overrides?: {
    connections?: Array<{ id: number; name: string; dbType: string; configJson?: string | null }>;
    tables?: Array<{ schemaName: string | null; tableName: string }>;
    tableColumnsByName?: Record<string, Array<Record<string, unknown>>>;
  }
) {
  const connections = overrides?.connections ?? [{ id: 2, name: "target-db", dbType: "MYSQL" }];
  const tables = overrides?.tables ?? [{ schemaName: "demo", tableName: "synthetic_orders" }];
  const tableColumnsByName = overrides?.tableColumnsByName ?? {
    synthetic_orders: [
      {
        columnName: "order_no",
        dbType: "VARCHAR",
        length: 40,
        precision: null,
        scale: null,
        nullable: false,
        primaryKey: false,
        autoIncrement: false
      },
      {
        columnName: "amount",
        dbType: "DECIMAL",
        length: null,
        precision: 10,
        scale: 2,
        nullable: false,
        primaryKey: false,
        autoIncrement: false
      }
    ]
  };
  const tasks = [...initialTasks];

  getMock.mockImplementation((url: string, config?: { params?: Record<string, unknown> }) => {
    if (url === "/write-tasks") {
      return ok(tasks);
    }
    if (url === "/connections") {
      return ok(connections);
    }
    if (url === "/connections/2/tables") {
      return ok(tables);
    }
    if (url === "/connections/2/table-columns") {
      const tableName = String(config?.params?.tableName ?? "");
      if (tableName in tableColumnsByName) {
        return ok(tableColumnsByName[tableName]);
      }
    }
    return Promise.reject(new Error(`Unhandled GET ${url}`));
  });

  return { connections, tables, tasks };
}

function findButtonByText(wrapper: ReturnType<typeof mount>, text: string) {
  return wrapper.findAll("button").find((button) => button.text().includes(text));
}

describe("JobsView", () => {
  beforeEach(() => {
    getMock.mockReset();
    postMock.mockReset();
    putMock.mockReset();
    deleteMock.mockReset();
    routerPushMock.mockClear();
    routerReplaceMock.mockClear();
    setRoute("write-tasks");
    window.scrollTo = vi.fn();
    Element.prototype.scrollIntoView = vi.fn();
  });

  it("renders the loaded workspace", async () => {
    configureWorkspace();

    const wrapper = mount(JobsView);
    setRoute("write-task-edit", { id: 7 });
    await flushPromises();

    expect(getMock).toHaveBeenCalledTimes(2);
    expect(wrapper.text()).toContain("nightly task");
    expect(wrapper.text()).toContain("synthetic_orders");
    expect(wrapper.text()).toContain("50");
    expect(wrapper.text()).toContain("手动执行");
  });

  it("paginates long task lists", async () => {
    const manyTasks = Array.from({ length: 9 }, (_, index) =>
      buildTask({
        id: index + 1,
        name: `task-${index + 1}`,
        tableName: `table_${index + 1}`
      })
    );
    configureWorkspace(manyTasks);

    const wrapper = mount(JobsView);
    await flushPromises();

    expect(wrapper.text()).toContain("task-1");
    expect(wrapper.text()).not.toContain("task-9");

    const pagination = wrapper.find(".task-list-panel > .list-pagination");
    await pagination.findAll("button")[1].trigger("click");
    await flushPromises();

    expect(wrapper.text()).toContain("task-9");
    expect(wrapper.text()).not.toContain("task-1");
  });

  it("shows the write plan section before the field section on the editor page", async () => {
    configureWorkspace([]);
    setRoute("write-task-create");

    const wrapper = mount(JobsView);
    await flushPromises();

    const sectionTitles = wrapper.findAll(".section-title h3").map((node) => node.text());
    expect(sectionTitles).toEqual(["目标表信息", "写入计划", "字段与规则"]);
  });

  it("creates a write task with normalized generator config", async () => {
    const workspace = configureWorkspace();
    setRoute("write-task-create");
    postMock.mockImplementation((_url: string, payload: Record<string, unknown>) => {
      const createdTask = buildTask({
        ...payload,
        id: 9,
        createdAt: "2026-04-14T01:00:00Z",
        updatedAt: "2026-04-14T01:00:00Z",
        schedulerState: "MANUAL",
        columns: [
          {
            columnName: "order_no",
            dbType: "VARCHAR",
            lengthValue: 40,
            precisionValue: null,
            scaleValue: null,
            nullableFlag: false,
            primaryKeyFlag: false,
            generatorType: "STRING",
            generatorConfig: {
              mode: "random",
              length: 12,
              charset: "abcdefghijklmnopqrstuvwxyz0123456789",
              prefix: "",
              suffix: ""
            },
            sortOrder: 0
          },
          {
            columnName: "amount",
            dbType: "DECIMAL",
            lengthValue: null,
            precisionValue: 10,
            scaleValue: 2,
            nullableFlag: false,
            primaryKeyFlag: false,
            generatorType: "RANDOM_DECIMAL",
            generatorConfig: {
              min: 0,
              max: 1000,
              scale: 2
            },
            sortOrder: 1
          }
        ]
      });
      workspace.tasks.push(createdTask);
      return ok(createdTask, "task created");
    });

    const wrapper = mount(JobsView);
    await flushPromises();

    await wrapper.find("input[name='taskName']").setValue("new task");
    await wrapper.find("select[name='connectionId']").setValue("2");
    await flushPromises();
    await wrapper.find("select[name='tableName']").setValue("synthetic_orders");
    await flushPromises();
    await wrapper.find("input[name='rowCount']").setValue("25");
    await wrapper.find("input[name='batchSize']").setValue("50");
    await wrapper.find("form").trigger("submit.prevent");
    await flushPromises();

    expect(postMock).toHaveBeenCalledWith("/write-tasks", {
      name: "new task",
      connectionId: 2,
      tableName: "synthetic_orders",
      tableMode: "USE_EXISTING",
      writeMode: "APPEND",
      rowCount: 25,
      batchSize: 50,
      seed: null,
      status: "READY",
      scheduleType: "MANUAL",
      cronExpression: null,
      triggerAt: null,
      intervalSeconds: null,
      maxRuns: null,
      maxRowsTotal: null,
      description: null,
      targetConfigJson: null,
      payloadSchemaJson: null,
      columns: [
        {
          columnName: "order_no",
          dbType: "VARCHAR",
          lengthValue: 40,
          precisionValue: null,
          scaleValue: null,
          nullableFlag: false,
          primaryKeyFlag: false,
          generatorType: "STRING",
          generatorConfig: {
            mode: "random",
            length: 12,
            charset: "abcdefghijklmnopqrstuvwxyz0123456789",
            prefix: "",
            suffix: ""
          },
          sortOrder: 0
        },
        {
          columnName: "amount",
          dbType: "DECIMAL",
          lengthValue: null,
          precisionValue: 10,
          scaleValue: 2,
          nullableFlag: false,
          primaryKeyFlag: false,
          generatorType: "RANDOM_DECIMAL",
          generatorConfig: {
            min: 0,
            max: 1000,
            scale: 2
          },
          sortOrder: 1
        }
      ]
    });
    expect(wrapper.text()).toContain("task created");
  });

  it("imports table columns from the selected target connection", async () => {
    configureWorkspace();
    setRoute("write-task-create");

    const wrapper = mount(JobsView);
    await flushPromises();

    await wrapper.find("select[name='connectionId']").setValue("2");
    await flushPromises();
    await wrapper.find("select[name='tableName']").setValue("synthetic_orders");
    await flushPromises();

    expect(getMock).toHaveBeenCalledWith("/connections/2/table-columns", {
      params: { tableName: "synthetic_orders" }
    });

    const textInputs = wrapper
      .findAll("input[type='text']")
      .map((input) => (input.element as HTMLInputElement).value);
    expect(textInputs).toContain("order_no");
    expect(textInputs).toContain("amount");
    expect(wrapper.findAll(".column-card").length).toBe(2);
    expect(wrapper.findAll(".checkbox-chip").length).toBe(4);
    expect(wrapper.find(".status-banner--success").exists()).toBe(false);
  });

  it("prefills the create form from the selected connection table route", async () => {
    configureWorkspace([]);
    setRoute("write-task-create", {}, { connectionId: "2", tableName: "synthetic_orders" });

    const wrapper = mount(JobsView);
    await flushPromises();

    expect((wrapper.find("select[name='connectionId']").element as HTMLSelectElement).value).toBe("2");
    expect((wrapper.find("select[name='tableName']").element as HTMLSelectElement).value).toBe("synthetic_orders");
    expect(getMock).toHaveBeenCalledWith("/connections/2/table-columns", {
      params: { tableName: "synthetic_orders" }
    });
    const textInputs = wrapper
      .findAll("input[type='text']")
      .map((input) => (input.element as HTMLInputElement).value);
    expect(textInputs).toContain("order_no");
    expect(textInputs).toContain("amount");
  });

  it("uses selectable column types and fills defaults for manual tables", async () => {
    const workspace = configureWorkspace([], {
      connections: [{ id: 2, name: "target-db", dbType: "MYSQL" }],
      tables: []
    });
    setRoute("write-task-create");

    postMock.mockImplementation((_url: string, payload: Record<string, unknown>) => {
      const createdTask = buildTask({
        ...payload,
        id: 15,
        tableName: "manual_orders",
        tableMode: "CREATE_IF_MISSING",
        columns: payload.columns
      });
      workspace.tasks.push(createdTask);
      return ok(createdTask, "manual task created");
    });

    const wrapper = mount(JobsView);
    await flushPromises();

    await wrapper.find("input[name='taskName']").setValue("manual task");
    await wrapper.find("select[name='connectionId']").setValue("2");
    await flushPromises();

    const manualModeButton = findButtonByText(wrapper, "新建表");
    expect(manualModeButton).toBeTruthy();
    await manualModeButton!.trigger("click");
    await flushPromises();

    await wrapper.find("input[name='tableName']").setValue("manual_orders");

    let typeSelects = wrapper.findAll("select.column-type-select");
    expect(typeSelects).toHaveLength(1);
    expect((typeSelects[0].element as HTMLSelectElement).value).toBe("BIGINT");

    const addColumnButton = findButtonByText(wrapper, "新增字段");
    expect(addColumnButton).toBeTruthy();
    await addColumnButton!.trigger("click");
    await flushPromises();

    typeSelects = wrapper.findAll("select.column-type-select");
    expect(typeSelects).toHaveLength(2);
    expect((typeSelects[1].element as HTMLSelectElement).value).toBe("VARCHAR");

    let columnCards = wrapper.findAll("article.panel--subtle");
    expect((columnCards[1].find(".column-length-input").element as HTMLInputElement).value).toBe("255");
    expect(columnCards[1].find(".column-precision-input").exists()).toBe(false);
    expect(columnCards[1].find(".column-scale-input").exists()).toBe(false);

    await typeSelects[1].setValue("DECIMAL");
    await flushPromises();

    columnCards = wrapper.findAll("article.panel--subtle");
    expect(columnCards[1].find(".column-length-input").exists()).toBe(false);
    expect((columnCards[1].find(".column-precision-input").element as HTMLInputElement).value).toBe("18");
    expect((columnCards[1].find(".column-scale-input").element as HTMLInputElement).value).toBe("2");

    await wrapper.find("form").trigger("submit.prevent");
    await flushPromises();

    expect(postMock).toHaveBeenCalledWith("/write-tasks", expect.objectContaining({
      tableName: "manual_orders",
      tableMode: "CREATE_IF_MISSING",
      columns: expect.arrayContaining([
        expect.objectContaining({
          columnName: "id",
          dbType: "BIGINT",
          generatorType: "SEQUENCE"
        }),
        expect.objectContaining({
          columnName: "name",
          dbType: "DECIMAL",
          precisionValue: 18,
          scaleValue: 2,
          generatorType: "RANDOM_DECIMAL"
        })
      ])
    }));
    expect(wrapper.text()).toContain("manual task created");
  });

  it("uses charset presets and supports custom charset input for random strings", async () => {
    const workspace = configureWorkspace([], {
      connections: [{ id: 2, name: "target-db", dbType: "MYSQL" }],
      tables: []
    });
    setRoute("write-task-create");

    postMock.mockImplementation((_url: string, payload: Record<string, unknown>) => {
      const createdTask = buildTask({
        ...payload,
        id: 16,
        tableName: "manual_charset_task",
        tableMode: "CREATE_IF_MISSING",
        columns: payload.columns
      });
      workspace.tasks.push(createdTask);
      return ok(createdTask, "charset task created");
    });

    const wrapper = mount(JobsView);
    await flushPromises();

    await wrapper.find("input[name='taskName']").setValue("charset task");
    await wrapper.find("select[name='connectionId']").setValue("2");
    await flushPromises();

    const manualModeButton = findButtonByText(wrapper, "新建表");
    expect(manualModeButton).toBeTruthy();
    await manualModeButton!.trigger("click");
    await flushPromises();

    await wrapper.find("input[name='tableName']").setValue("manual_charset_task");

    const addColumnButton = findButtonByText(wrapper, "新增字段");
    expect(addColumnButton).toBeTruthy();
    await addColumnButton!.trigger("click");
    await flushPromises();

    const columnCards = wrapper.findAll("article.panel--subtle");
    const stringColumnCard = columnCards[1];
    const charsetPreset = stringColumnCard.find("select[name='charsetPreset']");
    expect(charsetPreset.exists()).toBe(true);
    expect((charsetPreset.element as HTMLSelectElement).value).toBe("LOWERCASE_DIGITS");

    await charsetPreset.setValue("CUSTOM");
    await flushPromises();

    const customCharsetInput = stringColumnCard.find("input[name='charsetCustom']");
    expect(customCharsetInput.exists()).toBe(true);
    await customCharsetInput.setValue("abc123_-");
    await wrapper.find("form").trigger("submit.prevent");
    await flushPromises();

    expect(postMock).toHaveBeenCalledWith("/write-tasks", expect.objectContaining({
      tableName: "manual_charset_task",
      columns: expect.arrayContaining([
        expect.objectContaining({
          columnName: "name",
          generatorType: "STRING",
          generatorConfig: expect.objectContaining({
            mode: "random",
            length: 12,
            charset: "abc123_-"
          })
        })
      ])
    }));
    expect(wrapper.text()).toContain("charset task created");
  });

  it("creates a kafka write task with targetConfigJson", async () => {
    const workspace = configureWorkspace([], {
      connections: [{ id: 2, name: "kafka-target", dbType: "KAFKA", configJson: "{\"bootstrapServers\":\"127.0.0.1:9092\"}" }],
      tables: []
    });
    setRoute("write-task-create");

    postMock.mockImplementation((_url: string, payload: Record<string, unknown>) => {
      const createdTask = buildTask({
        ...payload,
        id: 21,
        connectionId: 2,
        tableName: "synthetic.user.activity",
        tableMode: "CREATE_IF_MISSING",
        writeMode: "APPEND",
        targetConfigJson: payload.targetConfigJson,
        columns: payload.columns
      });
      workspace.tasks.push(createdTask);
      return ok(createdTask, "kafka task created");
    });

    const wrapper = mount(JobsView);
    await flushPromises();

    await wrapper.find("input[name='taskName']").setValue("kafka task");
    await wrapper.find("select[name='connectionId']").setValue("2");
    await flushPromises();

    expect(wrapper.find("select[name='tableName']").exists()).toBe(false);
    expect(wrapper.find("input[name='tableName']").exists()).toBe(true);

    await wrapper.find("input[name='tableName']").setValue("synthetic.user.activity");

    const firstColumnCard = wrapper.find("article.panel--subtle");
    const firstColumnTextInputs = firstColumnCard.findAll("input[type='text']");
    await firstColumnTextInputs[0].setValue("event_id");
    await flushPromises();

    await wrapper.find("select[name='keyMode']").setValue("FIELD");
    await flushPromises();
    await wrapper.find("select[name='keyField']").setValue("event_id");
    await wrapper.find("input[name='partition']").setValue("1");
    await wrapper.find("[data-action='add-kafka-header']").trigger("click");
    await flushPromises();

    const headerRow = wrapper.find("[data-header-index='0']");
    await headerRow.find("[data-header-field='name']").setValue("source");
    await headerRow.find("[data-header-field='value']").setValue("mdg");
    await wrapper.find("form").trigger("submit.prevent");
    await flushPromises();

    expect(postMock).toHaveBeenCalledWith("/write-tasks", expect.objectContaining({
      name: "kafka task",
      connectionId: 2,
      tableName: "synthetic.user.activity",
      tableMode: "CREATE_IF_MISSING",
      writeMode: "APPEND",
      targetConfigJson: JSON.stringify({
        payloadFormat: "JSON",
        keyMode: "FIELD",
        keyField: "event_id",
        keyPath: "event_id",
        partition: 1,
        headers: {
          source: "mdg"
        }
      }),
      payloadSchemaJson: null
    }));
    expect(wrapper.text()).toContain("kafka task created");
  });

  it("creates a kafka complex payload task with payloadSchemaJson", async () => {
    const workspace = configureWorkspace([], {
      connections: [{ id: 2, name: "kafka-target", dbType: "KAFKA", configJson: "{\"bootstrapServers\":\"127.0.0.1:9092\"}" }],
      tables: []
    });
    setRoute("write-task-create");

    postMock.mockImplementation((_url: string, payload: Record<string, unknown>) => {
      const createdTask = buildTask({
        ...payload,
        id: 31,
        connectionId: 2,
        tableName: "synthetic.complex.activity",
        tableMode: "CREATE_IF_MISSING",
        writeMode: "APPEND",
        columns: [],
        payloadSchemaJson: payload.payloadSchemaJson
      });
      workspace.tasks.push(createdTask);
      return ok(createdTask, "complex kafka task created");
    });

    const wrapper = mount(JobsView);
    await flushPromises();

    await wrapper.find("input[name='taskName']").setValue("complex kafka task");
    await wrapper.find("select[name='connectionId']").setValue("2");
    await flushPromises();
    await wrapper.find("input[name='tableName']").setValue("synthetic.complex.activity");

    const complexModeButton = findButtonByText(wrapper, "复杂 JSON 模式");
    expect(complexModeButton).toBeTruthy();
    await complexModeButton!.trigger("click");
    await flushPromises();

    const treeNodesBefore = wrapper.findAll("[data-tree-node-id]");
    expect(treeNodesBefore.length).toBeGreaterThanOrEqual(2);

    await wrapper.find("[data-node-action='add-object']").trigger("click");
    await flushPromises();

    await wrapper.find("[data-node-field='name']").setValue("order");
    await wrapper.find("[data-node-action='add-scalar']").trigger("click");
    await flushPromises();

    await wrapper.find("[data-node-field='name']").setValue("id");
    await wrapper.find("[data-node-field='valueType']").setValue("LONG");
    await wrapper.find("[data-node-field='generatorType']").setValue("SEQUENCE");

    await wrapper.find("select[name='keyMode']").setValue("FIELD");
    await flushPromises();
    await wrapper.find("select[name='keyPath']").setValue("order.id");
    await wrapper.find("form").trigger("submit.prevent");
    await flushPromises();

    expect(postMock).toHaveBeenCalledTimes(1);
    const payload = postMock.mock.calls[0][1] as Record<string, unknown>;
    expect(payload.columns).toEqual([]);
    expect(payload.targetConfigJson).toBe(JSON.stringify({
      payloadFormat: "JSON",
      keyMode: "FIELD",
      keyPath: "order.id"
    }));
    expect(typeof payload.payloadSchemaJson).toBe("string");

    const schema = JSON.parse(String(payload.payloadSchemaJson)) as Record<string, unknown>;
    expect(schema.type).toBe("OBJECT");
  expect(Array.isArray(schema.children)).toBe(true);
  expect(wrapper.text()).toContain("complex kafka task created");
});

  it("imports kafka schema from example json", async () => {
    configureWorkspace([], {
      connections: [{ id: 2, name: "kafka-target", dbType: "KAFKA", configJson: "{\"bootstrapServers\":\"127.0.0.1:9092\"}" }],
      tables: []
    });
    setRoute("write-task-create");

    postMock.mockImplementation((url: string) => {
      if (url === "/write-tasks/kafka/schema/example") {
        return ok({
          schemaSource: "EXAMPLE_JSON",
          payloadSchemaJson: JSON.stringify({
            type: "OBJECT",
            children: [
              {
                name: "order",
                type: "OBJECT",
                children: [
                  {
                    name: "id",
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
          scalarPaths: ["order.id"],
          warnings: []
        });
      }
      return Promise.reject(new Error(`Unhandled POST ${url}`));
    });

    const wrapper = mount(JobsView);
    await flushPromises();

    await wrapper.find("select[name='connectionId']").setValue("2");
    await flushPromises();

    const complexModeButton = wrapper.findAll(".kafka-panel .mode-switch__item")[1];
    expect(complexModeButton.exists()).toBe(true);
    await complexModeButton.trigger("click");
    await flushPromises();

    await wrapper.find("textarea[name='kafkaSchemaImportContent']").setValue("{\"order\":{\"id\":1001}}");
    const importButtons = wrapper.findAll(".kafka-import-panel .panel__actions .button");
    expect(importButtons).toHaveLength(2);
    await importButtons[1].trigger("click");
    await flushPromises();

    expect(postMock).toHaveBeenCalledWith("/write-tasks/kafka/schema/example", {
      content: "{\"order\":{\"id\":1001}}"
    });

    const keyPathSelect = wrapper.find("select[name='keyPath']");
    expect(keyPathSelect.exists()).toBe(true);
    const options = keyPathSelect.findAll("option").map((option) => (option.element as HTMLOptionElement).value);
    expect(options).toContain("order.id");
  });

it("focuses the blank kafka payload field when complex schema validation fails", async () => {
  configureWorkspace([], {
    connections: [{ id: 2, name: "kafka-target", dbType: "KAFKA", configJson: "{\"bootstrapServers\":\"127.0.0.1:9092\"}" }],
    tables: []
  });
  setRoute("write-task-create");

  const wrapper = mount(JobsView, { attachTo: document.body });
  await flushPromises();

  await wrapper.find("input[name='taskName']").setValue("complex validation task");
  await wrapper.find("select[name='connectionId']").setValue("2");
  await flushPromises();
  await wrapper.find("input[name='tableName']").setValue("synthetic.validation.topic");

  const complexModeButton = wrapper.findAll(".kafka-panel .mode-switch__item")[1];
  expect(complexModeButton.exists()).toBe(true);
  await complexModeButton.trigger("click");
  await flushPromises();

  const defaultFieldNode = wrapper.findAll("[data-tree-node-id]")[1];
  expect(defaultFieldNode.exists()).toBe(true);
  await defaultFieldNode.trigger("click");
  await flushPromises();

  const nameInput = wrapper.find("[data-node-field='name']");
  expect(nameInput.exists()).toBe(true);
  await nameInput.setValue("");
  await wrapper.find("form").trigger("submit.prevent");
  await flushPromises();

  const highlightedNameInput = wrapper.find("[data-node-field='name']");
  expect(postMock).not.toHaveBeenCalled();
  expect(highlightedNameInput.exists()).toBe(true);
  expect(highlightedNameInput.classes()).toContain("field-control--error");
  expect(document.activeElement).toBe(highlightedNameInput.element);
  wrapper.unmount();
});

it("focuses keyPath when kafka complex mode references a missing field", async () => {
  configureWorkspace([], {
    connections: [{ id: 2, name: "kafka-target", dbType: "KAFKA", configJson: "{\"bootstrapServers\":\"127.0.0.1:9092\"}" }],
    tables: []
  });
  setRoute("write-task-create");

  const wrapper = mount(JobsView, { attachTo: document.body });
  await flushPromises();

  await wrapper.find("input[name='taskName']").setValue("complex key path task");
  await wrapper.find("select[name='connectionId']").setValue("2");
  await flushPromises();
  await wrapper.find("input[name='tableName']").setValue("synthetic.validation.topic");

  const complexModeButton = wrapper.findAll(".kafka-panel .mode-switch__item")[1];
  expect(complexModeButton.exists()).toBe(true);
  await complexModeButton.trigger("click");
  await flushPromises();

  await wrapper.find("[data-node-action='add-object']").trigger("click");
  await flushPromises();
  await wrapper.find("[data-node-field='name']").setValue("order");
  await wrapper.find("[data-node-action='add-scalar']").trigger("click");
  await flushPromises();
  await wrapper.find("[data-node-field='name']").setValue("id");

  await wrapper.find("select[name='keyMode']").setValue("FIELD");
  await flushPromises();
  const keyPathInput = wrapper.find("select[name='keyPath']");
  await keyPathInput.setValue("order.id");
  await wrapper.find("[data-node-field='name']").setValue("code");
  await flushPromises();
  await wrapper.find("form").trigger("submit.prevent");
  await flushPromises();

  expect(postMock).not.toHaveBeenCalled();
  expect(keyPathInput.classes()).toContain("field-control--error");
  expect(document.activeElement).toBe(keyPathInput.element);
  wrapper.unmount();
});

  it("uses schema-qualified SQL Server table names and infers UNIQUEIDENTIFIER columns", async () => {
    const workspace = configureWorkspace([], {
      connections: [{ id: 2, name: "sqlserver-db", dbType: "SQLSERVER" }],
      tables: [{ schemaName: "sales", tableName: "orders" }],
      tableColumnsByName: {
        "sales.orders": [
          {
            columnName: "row_guid",
            dbType: "UNIQUEIDENTIFIER",
            length: null,
            precision: null,
            scale: null,
            nullable: false,
            primaryKey: true,
            autoIncrement: false
          },
          {
            columnName: "amount",
            dbType: "DECIMAL",
            length: null,
            precision: 10,
            scale: 2,
            nullable: false,
            primaryKey: false,
            autoIncrement: false
          }
        ]
      }
    });
    setRoute("write-task-create");

    postMock.mockImplementation((_url: string, payload: Record<string, unknown>) => {
      const createdTask = buildTask({
        ...payload,
        id: 12,
        connectionId: 2,
        tableName: "sales.orders",
        columns: payload.columns
      });
      workspace.tasks.push(createdTask);
      return ok(createdTask, "sqlserver task created");
    });

    const wrapper = mount(JobsView);
    await flushPromises();

    await wrapper.find("input[name='taskName']").setValue("sqlserver task");
    await wrapper.find("select[name='connectionId']").setValue("2");
    await flushPromises();

    const tableSelect = wrapper.find("select[name='tableName']");
    expect(tableSelect.text()).toContain("sales.orders");

    await tableSelect.setValue("sales.orders");
    await flushPromises();
    await wrapper.find("form").trigger("submit.prevent");
    await flushPromises();

    expect(getMock).toHaveBeenCalledWith("/connections/2/table-columns", {
      params: { tableName: "sales.orders" }
    });
    expect(postMock).toHaveBeenCalledWith("/write-tasks", expect.objectContaining({
      tableName: "sales.orders",
      columns: expect.arrayContaining([
        expect.objectContaining({
          columnName: "row_guid",
          generatorType: "UUID"
        })
      ])
    }));
  });

  it("infers Oracle NUMBER columns when importing an existing table", async () => {
    const workspace = configureWorkspace([], {
      connections: [{ id: 2, name: "oracle-db", dbType: "ORACLE" }],
      tables: [{ schemaName: "APPUSER", tableName: "INVOICES" }],
      tableColumnsByName: {
        "APPUSER.INVOICES": [
          {
            columnName: "id",
            dbType: "NUMBER",
            length: null,
            precision: 19,
            scale: 0,
            nullable: false,
            primaryKey: true,
            autoIncrement: false
          },
          {
            columnName: "amount",
            dbType: "NUMBER",
            length: null,
            precision: 10,
            scale: 2,
            nullable: false,
            primaryKey: false,
            autoIncrement: false
          }
        ]
      }
    });
    setRoute("write-task-create");

    postMock.mockImplementation((_url: string, payload: Record<string, unknown>) => {
      const createdTask = buildTask({
        ...payload,
        id: 13,
        connectionId: 2,
        tableName: "APPUSER.INVOICES",
        columns: payload.columns
      });
      workspace.tasks.push(createdTask);
      return ok(createdTask, "oracle task created");
    });

    const wrapper = mount(JobsView);
    await flushPromises();

    await wrapper.find("input[name='taskName']").setValue("oracle task");
    await wrapper.find("select[name='connectionId']").setValue("2");
    await flushPromises();
    await wrapper.find("select[name='tableName']").setValue("APPUSER.INVOICES");
    await flushPromises();
    await wrapper.find("form").trigger("submit.prevent");
    await flushPromises();

    expect(postMock).toHaveBeenCalledWith("/write-tasks", expect.objectContaining({
      tableName: "APPUSER.INVOICES",
      columns: expect.arrayContaining([
        expect.objectContaining({
          columnName: "id",
          generatorType: "SEQUENCE"
        }),
        expect.objectContaining({
          columnName: "amount",
          generatorType: "RANDOM_DECIMAL"
        })
      ])
    }));
  });

  it("preserves PostgreSQL JSONB columns when importing an existing table", async () => {
    const workspace = configureWorkspace([], {
      connections: [{ id: 2, name: "pg-db", dbType: "POSTGRESQL" }],
      tables: [{ schemaName: "public", tableName: "synthetic_user_activity" }],
      tableColumnsByName: {
        "public.synthetic_user_activity": [
          {
            columnName: "userId",
            dbType: "INT8",
            length: null,
            precision: 19,
            scale: 0,
            nullable: false,
            primaryKey: false,
            autoIncrement: false
          },
          {
            columnName: "profile",
            dbType: "JSONB",
            length: null,
            precision: null,
            scale: null,
            nullable: true,
            primaryKey: false,
            autoIncrement: false
          }
        ]
      }
    });
    setRoute("write-task-create");

    postMock.mockImplementation((_url: string, payload: Record<string, unknown>) => {
      const createdTask = buildTask({
        ...payload,
        id: 14,
        connectionId: 2,
        tableName: "public.synthetic_user_activity",
        columns: payload.columns
      });
      workspace.tasks.push(createdTask);
      return ok(createdTask, "postgresql task created");
    });

    const wrapper = mount(JobsView);
    await flushPromises();

    await wrapper.find("input[name='taskName']").setValue("postgresql task");
    await wrapper.find("select[name='connectionId']").setValue("2");
    await flushPromises();
    await wrapper.find("select[name='tableName']").setValue("public.synthetic_user_activity");
    await flushPromises();
    await wrapper.find("form").trigger("submit.prevent");
    await flushPromises();

    expect(postMock).toHaveBeenCalledWith("/write-tasks", expect.objectContaining({
      tableName: "public.synthetic_user_activity",
      columns: expect.arrayContaining([
        expect.objectContaining({
          columnName: "profile",
          dbType: "JSONB",
          generatorType: "STRING"
        })
      ])
    }));
  });

  it("updates an existing write task and shows the save confirmation", async () => {
    const workspace = configureWorkspace();
    setRoute("write-task-edit", { id: 7 });
    putMock.mockImplementation((_url: string, payload: Record<string, unknown>) => {
      workspace.tasks[0] = buildTask({
        ...workspace.tasks[0],
        ...payload,
        id: 7,
        updatedAt: "2026-04-14T02:00:00Z",
        schedulerState: "MANUAL"
      });
      return ok(workspace.tasks[0], "task updated");
    });

    const wrapper = mount(JobsView);
    await flushPromises();
    await wrapper.vm.$nextTick();

    await wrapper.find("input[name='taskName']").setValue("updated task");
    await wrapper.find("input[name='rowCount']").setValue("60");
    await wrapper.find("input[name='batchSize']").setValue("120");
    await wrapper.find("form").trigger("submit.prevent");
    await flushPromises();

    expect(putMock).toHaveBeenCalledWith("/write-tasks/7", expect.objectContaining({
      name: "updated task",
      rowCount: 60,
      batchSize: 120,
      scheduleType: "MANUAL"
    }));
    expect(wrapper.text()).toContain("task updated");
    expect((wrapper.find("input[name='taskName']").element as HTMLInputElement).value).toBe("updated task");
  });

  it("builds cron expressions from selectable schedule controls", async () => {
    const workspace = configureWorkspace([]);
    setRoute("write-task-create");
    postMock.mockImplementation((_url: string, payload: Record<string, unknown>) => {
      const createdTask = buildTask({
        ...payload,
        id: 18,
        scheduleType: "CRON",
        cronExpression: payload.cronExpression,
        columns: payload.columns
      });
      workspace.tasks.push(createdTask);
      return ok(createdTask, "cron task created");
    });

    const wrapper = mount(JobsView);
    await flushPromises();

    await wrapper.find("input[name='taskName']").setValue("cron task");
    await wrapper.find("select[name='connectionId']").setValue("2");
    await flushPromises();
    await wrapper.find("select[name='tableName']").setValue("synthetic_orders");
    await flushPromises();

    await wrapper.find("select[name='scheduleType']").setValue("CRON");
    await flushPromises();
    await wrapper.find("select[name='cronBuilderMode']").setValue("WEEKLY");
    await flushPromises();
    await wrapper.find("select[name='cronWeekDay']").setValue("FRI");
    await wrapper.find("select[name='cronAtHour']").setValue("9");
    await wrapper.find("select[name='cronAtMinute']").setValue("30");
    await wrapper.find("form").trigger("submit.prevent");
    await flushPromises();

    expect(postMock).toHaveBeenCalledWith("/write-tasks", expect.objectContaining({
      name: "cron task",
      scheduleType: "CRON",
      cronExpression: "0 30 9 ? * FRI"
    }));
    expect(wrapper.text()).toContain("cron task created");
    expect(wrapper.text()).toContain("周五 09:30 执行");
  });

  it("starts a continuous write task and refreshes the runtime state", async () => {
    const workspace = configureWorkspace([
      buildTask({
        id: 11,
        name: "stream task",
        scheduleType: "INTERVAL",
        status: "READY",
        intervalSeconds: 5,
        maxRuns: 3,
        maxRowsTotal: 150,
        schedulerState: "STOPPED"
      })
    ]);
    postMock.mockImplementation((url: string) => {
      if (url === "/write-tasks/11/start") {
        workspace.tasks[0] = buildTask({
          ...workspace.tasks[0],
          id: 11,
          name: "stream task",
          scheduleType: "INTERVAL",
          status: "RUNNING",
          intervalSeconds: 5,
          maxRuns: 3,
          maxRowsTotal: 150,
          schedulerState: "RUNNING",
          nextFireAt: "2026-04-14T02:05:00Z"
        });
        return ok(workspace.tasks[0], "continuous started");
      }
      return Promise.reject(new Error(`Unhandled POST ${url}`));
    });

    const wrapper = mount(JobsView);
    await flushPromises();

    const startButton = findButtonByText(wrapper, "启动持续写入");
    expect(startButton).toBeDefined();

    await startButton!.trigger("click");
    await flushPromises();

    expect(postMock).toHaveBeenCalledWith("/write-tasks/11/start");
    expect(wrapper.text()).toContain("continuous started");
    expect(wrapper.text()).toContain("运行中");
    expect(wrapper.text()).toContain("每 5 秒写入一批");
  });

  it("shows an error feedback when the execution result is failed", async () => {
    configureWorkspace();
    postMock.mockImplementation((url: string) => {
      if (url === "/write-tasks/7/run") {
        return ok({
          id: 51,
          writeTaskId: 7,
          status: "FAILED",
          successCount: 0,
          errorCount: 1,
          errorSummary: "You have an error in your SQL syntax",
          deliveryDetailsJson: JSON.stringify({
            tableName: "synthetic_orders",
            writeMode: "APPEND",
            tableMode: "CREATE_IF_MISSING",
            beforeWriteRowCount: 0,
            afterWriteRowCount: 0,
            rowDelta: 0,
            writtenRowCount: 0,
            nonNullValidation: {
              passed: true,
              nullValueCount: 0,
              blankStringCount: 0
            }
          })
        });
      }
      return Promise.reject(new Error(`Unhandled POST ${url}`));
    });

    const wrapper = mount(JobsView);
    await flushPromises();

    const runButton = findButtonByText(wrapper, "执行");
    expect(runButton).toBeDefined();

    await runButton!.trigger("click");
    await flushPromises();

    expect(wrapper.text()).toContain("You have an error in your SQL syntax");
    expect(wrapper.text()).not.toContain("执行完成：本次写入 0 条");
  });

  it("does not force-scroll when the execution summary is already visible", async () => {
    configureWorkspace();
    postMock.mockImplementation((url: string) => {
      if (url === "/write-tasks/7/run") {
        return ok({
          id: 52,
          writeTaskId: 7,
          status: "SUCCESS",
          successCount: 100,
          errorCount: 0,
          errorSummary: null,
          deliveryDetailsJson: JSON.stringify({
            tableName: "synthetic_orders",
            writeMode: "APPEND",
            tableMode: "CREATE_IF_MISSING",
            beforeWriteRowCount: 0,
            afterWriteRowCount: 100,
            rowDelta: 100,
            writtenRowCount: 100,
            nonNullValidation: {
              passed: true,
              nullValueCount: 0,
              blankStringCount: 0
            }
          })
        });
      }
      return Promise.reject(new Error(`Unhandled POST ${url}`));
    });

    const scrollSpy = vi.spyOn(window, "scrollTo").mockImplementation(() => undefined);
    const rectSpy = vi.spyOn(HTMLElement.prototype, "getBoundingClientRect").mockImplementation(function (this: HTMLElement) {
      if (this.matches("section.panel.panel--embedded")) {
        return {
          x: 0,
          y: 160,
          width: 640,
          height: 180,
          top: 160,
          bottom: 340,
          left: 0,
          right: 640,
          toJSON: () => ({})
        } as DOMRect;
      }
      return {
        x: 0,
        y: 0,
        width: 0,
        height: 0,
        top: 0,
        bottom: 0,
        left: 0,
        right: 0,
        toJSON: () => ({})
      } as DOMRect;
    });

    const wrapper = mount(JobsView);
    await flushPromises();

    const runButton = findButtonByText(wrapper, "执行");
    expect(runButton).toBeDefined();

    await runButton!.trigger("click");
    await flushPromises();

    expect(wrapper.text()).toContain("执行完成：本次写入 100 条，写入后共 100 条");
    expect(scrollSpy).not.toHaveBeenCalled();

    rectSpy.mockRestore();
    scrollSpy.mockRestore();
  });
});
