import { enableAutoUnmount, flushPromises, mount } from "@vue/test-utils";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import ConnectorsView from "./ConnectorsView.vue";

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
    name: "connections",
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

function buildConnection(overrides: Record<string, unknown> = {}) {
  return {
    id: 5,
    name: "MySQL 目标库",
    dbType: "MYSQL",
    host: "127.0.0.1",
    port: 3306,
    databaseName: "synthetic_demo_target",
    schemaName: null,
    username: "root",
    jdbcParams: "useUnicode=true&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai",
    configJson: null,
    status: "READY",
    description: "用于模拟数据写入",
    hasPassword: true,
    lastTestStatus: "READY",
    lastTestMessage: "连接成功",
    lastTestDetailsJson: "{\"productName\":\"MySQL\"}",
    ...overrides
  };
}

function buildKafkaConnection(overrides: Record<string, unknown> = {}) {
  return buildConnection({
    id: 6,
    name: "Kafka 目标端",
    dbType: "KAFKA",
    host: "127.0.0.1",
    port: 9092,
    databaseName: "kafka",
    schemaName: null,
    username: "",
    jdbcParams: null,
    configJson: JSON.stringify({
      bootstrapServers: "127.0.0.1:9092",
      securityProtocol: "PLAINTEXT",
      acks: "all"
    }),
    description: "写入实时 Topic",
    ...overrides
  });
}

function configureConnections(initialConnections = [buildConnection()]) {
  const connections = [...initialConnections];

  getMock.mockImplementation((url: string) => {
    if (url === "/connections") {
      return ok(connections);
    }
    if (url === "/connections/5/tables") {
      return ok([
        { schemaName: "demo", tableName: "orders" },
        { schemaName: "demo", tableName: "customers" }
      ]);
    }
    return Promise.reject(new Error(`Unhandled GET ${url}`));
  });

  return { connections };
}

function findButtonByText(wrapper: ReturnType<typeof mount>, text: string) {
  return wrapper.findAll("button").find((button) => button.text().includes(text));
}

describe("ConnectorsView", () => {
  beforeEach(() => {
    getMock.mockReset();
    postMock.mockReset();
    putMock.mockReset();
    deleteMock.mockReset();
    routerPushMock.mockClear();
    routerReplaceMock.mockClear();
    setRoute("connections");
    Element.prototype.scrollIntoView = vi.fn();
  });

  it("tests the current mysql draft configuration without saving", async () => {
    configureConnections();
    setRoute("connection-create");
    postMock.mockImplementation((url: string) => {
      if (url === "/connections/test") {
        return ok({
          connectionId: null,
          success: true,
          status: "READY",
          message: "连接成功",
          detailsJson: "{\"productName\":\"MySQL\"}"
        }, "当前配置连接测试已完成");
      }
      return Promise.reject(new Error(`Unhandled POST ${url}`));
    });

    const wrapper = mount(ConnectorsView);
    await flushPromises();

    await wrapper.find("input[name='connectionName']").setValue("临时 MySQL");
    await wrapper.find("input[name='host']").setValue("10.0.0.8");
    await wrapper.find("input[name='databaseName']").setValue("demo_target");
    await wrapper.find("input[name='username']").setValue("root");
    await wrapper.find("input[name='password']").setValue("123456");
    await findButtonByText(wrapper, "测试当前配置")?.trigger("click");
    await flushPromises();

    setRoute("connection-edit", { id: 5 });
    await flushPromises();

    expect(postMock).toHaveBeenCalledWith("/connections/test", {
      connectionId: null,
      connection: {
        name: "临时 MySQL",
        dbType: "MYSQL",
        host: "10.0.0.8",
        port: 3306,
        databaseName: "demo_target",
        schemaName: null,
        username: "root",
        password: "123456",
        jdbcParams: "useUnicode=true&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai",
        configJson: null,
        status: "READY",
        description: null
      }
    });
    expect(wrapper.text()).toContain("连接成功");
  });

  it("switches database defaults for sql server, oracle and kafka", async () => {
    configureConnections([]);
    setRoute("connection-create");

    const wrapper = mount(ConnectorsView);
    await flushPromises();

    await wrapper.find("select[name='dbType']").setValue("SQLSERVER");
    await flushPromises();

    expect((wrapper.find("input[name='port']").element as HTMLInputElement).value).toBe("1433");
    expect((wrapper.find("input[name='jdbcParams']").element as HTMLInputElement).value).toBe("encrypt=true;trustServerCertificate=true");

    await wrapper.find("select[name='dbType']").setValue("ORACLE");
    await flushPromises();

    expect((wrapper.find("input[name='port']").element as HTMLInputElement).value).toBe("1521");
    expect((wrapper.find("input[name='jdbcParams']").element as HTMLInputElement).value).toBe("");

    await wrapper.find("select[name='dbType']").setValue("KAFKA");
    await flushPromises();

    expect((wrapper.find("input[name='bootstrapServers']").element as HTMLInputElement).value).toBe("");
    expect(wrapper.find("input[name='host']").exists()).toBe(false);
    expect(wrapper.find("input[name='jdbcParams']").exists()).toBe(false);
  });

  it("reuses the saved connection password when testing an edited draft", async () => {
    configureConnections();
    setRoute("connection-edit", { id: 5 });
    postMock.mockImplementation((url: string) => {
      if (url === "/connections/test") {
        return ok({
          connectionId: 5,
          success: true,
          status: "READY",
          message: "连接成功",
          detailsJson: "{\"productName\":\"MySQL\"}"
        }, "当前配置连接测试已完成");
      }
      return Promise.reject(new Error(`Unhandled POST ${url}`));
    });

    const wrapper = mount(ConnectorsView);
    setRoute("connection-edit", { id: 5 });
    await wrapper.vm.$nextTick();
    await flushPromises();

    await findButtonByText(wrapper, "编辑")?.trigger("click");
    await flushPromises();
    await wrapper.find("input[name='connectionName']").setValue("MySQL 目标库-编辑");
    await findButtonByText(wrapper, "测试当前配置")?.trigger("click");
    await flushPromises();

    expect(postMock).toHaveBeenCalledWith("/connections/test", {
      connectionId: 5,
      connection: expect.objectContaining({
        name: "MySQL 目标库-编辑",
        password: null,
        configJson: null
      })
    });
    expect(wrapper.text()).toContain("连接成功");
  }, 15000);

  it("saves a kafka connection with configJson payload", async () => {
    const workspace = configureConnections([]);
    setRoute("connection-create");
    postMock.mockImplementation((url: string, payload: Record<string, unknown>) => {
      if (url === "/connections") {
        const created = buildKafkaConnection({
          id: 9,
          name: payload.name,
          configJson: payload.configJson
        });
        workspace.connections.push(created);
        return ok(created, "Kafka 连接已创建");
      }
      return Promise.reject(new Error(`Unhandled POST ${url}`));
    });

    const wrapper = mount(ConnectorsView);
    await flushPromises();

    await wrapper.find("select[name='dbType']").setValue("KAFKA");
    await flushPromises();
    await wrapper.find("input[name='connectionName']").setValue("Kafka 测试连接");
    await wrapper.find("input[name='bootstrapServers']").setValue("127.0.0.1:9092,127.0.0.1:9093");
    await wrapper.find("select[name='securityProtocol']").setValue("PLAINTEXT");
    await wrapper.find("select[name='acks']").setValue("all");
    await wrapper.find("textarea[name='propertiesJson']").setValue("{\"linger.ms\":\"5\"}");
    await wrapper.find("form").trigger("submit.prevent");
    await flushPromises();

    expect(postMock).toHaveBeenCalledWith("/connections", {
      name: "Kafka 测试连接",
      dbType: "KAFKA",
      host: "127.0.0.1",
      port: 9092,
      databaseName: "kafka",
      schemaName: null,
      username: "",
      password: null,
      jdbcParams: null,
      configJson: JSON.stringify({
        bootstrapServers: "127.0.0.1:9092,127.0.0.1:9093",
        securityProtocol: "PLAINTEXT",
        acks: "all",
        properties: {
          "linger.ms": "5"
        }
      }),
      status: "READY",
      description: null
    });
    expect(wrapper.text()).toContain("Kafka 连接已创建");
    expect(findButtonByText(wrapper, "查看表")).toBeUndefined();
  });

  it("shows an error banner when saved connection test reports failure", async () => {
    configureConnections();
    postMock.mockImplementation((url: string) => {
      if (url === "/connections/5/test") {
        return ok({
          connectionId: 5,
          success: false,
          status: "FAILED",
          message: "认证失败",
          detailsJson: null
        }, "数据源连接测试执行完成");
      }
      return Promise.reject(new Error(`Unhandled POST ${url}`));
    });

    const wrapper = mount(ConnectorsView);
    await flushPromises();

    const testButton = wrapper.findAll("button").find((button) => button.text() === "测试");
    if (!testButton) {
      throw new Error("Expected saved connection test button");
    }

    await testButton.trigger("click");
    await flushPromises();

    expect(wrapper.text()).toContain("认证失败");
  });

  it("shows table names inline and starts a new task from a selected table", async () => {
    configureConnections();

    const wrapper = mount(ConnectorsView);
    await flushPromises();

    await findButtonByText(wrapper, "查看表")?.trigger("click");
    await flushPromises();

    expect(getMock).toHaveBeenCalledWith("/connections/5/tables");
    expect(wrapper.text()).toContain("orders");
    expect(wrapper.text()).toContain("customers");

    await findButtonByText(wrapper, "用于新建任务")?.trigger("click");
    await flushPromises();

    expect(routerPushMock).toHaveBeenCalledWith({
      name: "write-task-create",
      query: {
        connectionId: "5",
        tableName: "demo.orders"
      }
    });
  });

  it("paginates long connection lists and table lists", async () => {
    const manyConnections = Array.from({ length: 9 }, (_, index) =>
      buildConnection({
        id: index + 1,
        name: `conn-${index + 1}`,
        host: `10.0.0.${index + 1}`
      })
    );
    const manyTables = Array.from({ length: 11 }, (_, index) => ({
      schemaName: "demo",
      tableName: `table_${index + 1}`
    }));

    getMock.mockImplementation((url: string) => {
      if (url === "/connections") {
        return ok(manyConnections);
      }
      if (url === "/connections/1/tables") {
        return ok(manyTables);
      }
      return Promise.reject(new Error(`Unhandled GET ${url}`));
    });

    const wrapper = mount(ConnectorsView);
    await flushPromises();

    expect(wrapper.text()).toContain("conn-1");
    expect(wrapper.text()).not.toContain("conn-9");

    await wrapper.findAll(".task-list__item")[0].findAll("button")[2].trigger("click");
    await flushPromises();

    expect(wrapper.findAll(".connection-table-panel .data-list__row strong").map((node) => node.text())).toEqual([
      "table_1",
      "table_2",
      "table_3",
      "table_4",
      "table_5",
      "table_6",
      "table_7",
      "table_8",
      "table_9",
      "table_10"
    ]);

    const tablePagination = wrapper.find(".connection-table-panel .list-pagination");
    await tablePagination.findAll("button")[1].trigger("click");
    await flushPromises();

    expect(wrapper.findAll(".connection-table-panel .data-list__row strong").map((node) => node.text())).toEqual([
      "table_11"
    ]);

    const connectionPagination = wrapper.find(".connector-list-panel > .list-pagination");
    await connectionPagination.findAll("button")[1].trigger("click");
    await flushPromises();

    expect(wrapper.findAll(".connector-list-panel > .task-list .task-list__item h3").map((node) => node.text())).toEqual([
      "conn-9"
    ]);
  });
});
