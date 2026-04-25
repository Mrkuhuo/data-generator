import { enableAutoUnmount, flushPromises, mount } from "@vue/test-utils";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import ExecutionsView from "./ExecutionsView.vue";

const { getMock, routeState, routerPushMock, routerReplaceMock } = vi.hoisted(() => ({
  getMock: vi.fn(),
  routeState: { current: null as null | { name: string; params: Record<string, unknown>; query: Record<string, unknown> } },
  routerPushMock: vi.fn(),
  routerReplaceMock: vi.fn()
}));

vi.mock("../api/client", async () => {
  const actual = await vi.importActual<typeof import("../api/client")>("../api/client");
  return {
    ...actual,
    apiClient: {
      get: getMock
    }
  };
});

vi.mock("vue-router", async () => {
  const actual = await vi.importActual<typeof import("vue-router")>("vue-router");
  const { reactive } = await vi.importActual<typeof import("vue")>("vue");
  const route = reactive({
    name: "executions",
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

function ok<T>(data: T) {
  return Promise.resolve({
    data: {
      success: true,
      data,
      message: null
    }
  });
}

enableAutoUnmount(afterEach);

function configureJdbcLedger() {
  const executions = [
    {
      id: 7,
      writeTaskId: 3,
      triggerType: "API",
      status: "SUCCESS",
      startedAt: "2026-04-12T14:00:00Z",
      finishedAt: "2026-04-12T14:01:00Z",
      generatedCount: 100,
      successCount: 100,
      errorCount: 0,
      errorSummary: null,
      deliveryDetailsJson: JSON.stringify({
        tableName: "synthetic_orders",
        writeMode: "APPEND",
        writtenRowCount: 100,
        beforeWriteRowCount: 500,
        afterWriteRowCount: 600,
        rowDelta: 100,
        nonNullValidation: {
          passed: true,
          nullValueCount: 0,
          blankStringCount: 0,
          issues: []
        }
      })
    }
  ];

  const tasks = [{ id: 3, name: "订单模拟写入" }];
  const logs = [
    {
      id: 91,
      logLevel: "INFO",
      message: "开始写入目标表",
      detailJson: JSON.stringify({
        tableName: "synthetic_orders",
        successCount: 100
      })
    }
  ];

  getMock.mockImplementation((url: string) => {
    if (url === "/write-tasks/executions") {
      return ok(executions);
    }
    if (url === "/write-tasks") {
      return ok(tasks);
    }
    if (url === "/write-tasks/executions/7/logs") {
      return ok(logs);
    }
    return Promise.reject(new Error(`Unhandled GET ${url}`));
  });
}

function configureKafkaLedger() {
  const executions = [
    {
      id: 11,
      writeTaskId: 8,
      triggerType: "API",
      status: "SUCCESS",
      startedAt: "2026-04-12T14:00:00Z",
      finishedAt: "2026-04-12T14:00:10Z",
      generatedCount: 50,
      successCount: 50,
      errorCount: 0,
      errorSummary: null,
      deliveryDetailsJson: JSON.stringify({
        targetType: "KAFKA",
        deliveryType: "KAFKA",
        topic: "synthetic.user.activity",
        payloadFormat: "JSON",
        keyMode: "FIELD",
        keyField: "event_id",
        partition: 1,
        headers: {
          source: "mdg",
          env: "test"
        },
        writtenRowCount: 50
      })
    }
  ];

  const tasks = [{ id: 8, name: "Kafka 行为写入" }];
  const logs = [
    {
      id: 101,
      logLevel: "INFO",
      message: "开始投递 Kafka 消息",
      detailJson: JSON.stringify({
        topic: "synthetic.user.activity",
        successCount: 50
      })
    }
  ];

  getMock.mockImplementation((url: string) => {
    if (url === "/write-tasks/executions") {
      return ok(executions);
    }
    if (url === "/write-tasks") {
      return ok(tasks);
    }
    if (url === "/write-tasks/executions/11/logs") {
      return ok(logs);
    }
    return Promise.reject(new Error(`Unhandled GET ${url}`));
  });
}

describe("ExecutionsView", () => {
  beforeEach(() => {
    getMock.mockReset();
    routerPushMock.mockClear();
    routerReplaceMock.mockClear();
    setRoute("executions");
    Object.defineProperty(window, "scrollTo", {
      value: vi.fn(),
      writable: true,
      configurable: true
    });
    Object.defineProperty(window, "scrollY", {
      value: 0,
      writable: true,
      configurable: true
    });
  });

  it("renders jdbc execution metrics", async () => {
    configureJdbcLedger();

    const wrapper = mount(ExecutionsView);
    await flushPromises();

    expect(getMock).toHaveBeenCalledTimes(2);
    expect(wrapper.text()).toContain("订单模拟写入");
    expect(wrapper.text()).toContain("本次写入：100 条 / 写入前：500 条 / 写入后：600 条 / 净变化：100 条");
    expect(wrapper.text()).toContain("非空校验：通过 / 空值：0 / 空字符串：0");
  });

  it("renders kafka delivery metrics", async () => {
    configureKafkaLedger();

    const wrapper = mount(ExecutionsView);
    await flushPromises();

    expect(wrapper.text()).toContain("Kafka 行为写入");
    expect(wrapper.text()).toContain("Topic：synthetic.user.activity");
    expect(wrapper.text()).toContain("Payload：JSON");
    expect(wrapper.text()).toContain("Key 模式：FIELD");
    expect(wrapper.text()).toContain("Headers：2");
  });

  it("loads execution logs and shows delivery details in the detail panel", async () => {
    configureJdbcLedger();

    const wrapper = mount(ExecutionsView);
    await flushPromises();
    Object.defineProperty(window, "scrollY", {
      value: 640,
      writable: true,
      configurable: true
    });

    const inspectLogsButton = wrapper.findAll("button").find((button) => button.text().includes("查看详情与日志"));
    if (!inspectLogsButton) {
      throw new Error("Expected inspect logs button");
    }

    await inspectLogsButton.trigger("click");
    await flushPromises();
    setRoute("execution-detail", { id: 7 });
    await flushPromises();
    await wrapper.vm.$nextTick();

    expect(wrapper.text()).toContain("已加载执行记录 #7 的 1 条日志");
    expect(wrapper.text()).toContain("开始写入目标表");
    expect(wrapper.text()).toContain("\"tableName\": \"synthetic_orders\"");
    expect(wrapper.text()).toContain("500 / 600");
    expect(wrapper.text()).toContain("展开列表");
    expect(wrapper.text()).not.toContain("执行列表");

    const expandButton = wrapper.findAll("button").find((button) => button.text().includes("展开列表"));
    if (!expandButton) {
      throw new Error("Expected expand list button");
    }

    await expandButton.trigger("click");
    await flushPromises();

    expect(wrapper.text()).toContain("执行列表");
    expect(window.scrollTo).toHaveBeenCalledWith({
      top: 640,
      behavior: "smooth"
    });
  });

  it("paginates long execution lists", async () => {
    const executions = Array.from({ length: 9 }, (_, index) => ({
      id: index + 1,
      writeTaskId: index + 1,
      triggerType: "API",
      status: "SUCCESS",
      startedAt: "2026-04-12T14:00:00Z",
      finishedAt: "2026-04-12T14:01:00Z",
      generatedCount: 10,
      successCount: 10,
      errorCount: 0,
      errorSummary: null,
      deliveryDetailsJson: JSON.stringify({
        writtenRowCount: 10
      })
    }));
    const tasks = executions.map((execution) => ({
      id: execution.writeTaskId,
      name: `exec-task-${execution.id}`
    }));

    getMock.mockImplementation((url: string) => {
      if (url === "/write-tasks/executions") {
        return ok(executions);
      }
      if (url === "/write-tasks") {
        return ok(tasks);
      }
      if (url.startsWith("/write-tasks/executions/")) {
        return ok([]);
      }
      return Promise.reject(new Error(`Unhandled GET ${url}`));
    });

    const wrapper = mount(ExecutionsView);
    await flushPromises();

    expect(wrapper.text()).toContain("exec-task-1");
    expect(wrapper.text()).not.toContain("exec-task-9");

    const pagination = wrapper.find(".executions-list .list-pagination");
    await pagination.findAll("button")[1].trigger("click");
    await flushPromises();

    expect(wrapper.text()).toContain("exec-task-9");
    expect(wrapper.text()).not.toContain("exec-task-1");
  });

  it("paginates long validation issue and log lists in the detail panel", async () => {
    const execution = {
      id: 21,
      writeTaskId: 5,
      triggerType: "API",
      status: "PARTIAL_SUCCESS",
      startedAt: "2026-04-12T14:00:00Z",
      finishedAt: "2026-04-12T14:01:00Z",
      generatedCount: 12,
      successCount: 10,
      errorCount: 2,
      errorSummary: "validation failed",
      deliveryDetailsJson: JSON.stringify({
        writtenRowCount: 10,
        nonNullValidation: {
          passed: false,
          nullValueCount: 2,
          blankStringCount: 1,
          issues: Array.from({ length: 11 }, (_, index) => ({
            columnName: `col_${index + 1}`,
            issueType: "NULL",
            affectedRowCount: 1,
            message: `issue-${index + 1}`
          }))
        }
      })
    };
    const logs = Array.from({ length: 11 }, (_, index) => ({
      id: index + 1,
      logLevel: "INFO",
      message: `log-${index + 1}`,
      detailJson: JSON.stringify({ index: index + 1 })
    }));

    getMock.mockImplementation((url: string) => {
      if (url === "/write-tasks/executions") {
        return ok([execution]);
      }
      if (url === "/write-tasks") {
        return ok([{ id: 5, name: "detail-task" }]);
      }
      if (url === "/write-tasks/executions/21/logs") {
        return ok(logs);
      }
      return Promise.reject(new Error(`Unhandled GET ${url}`));
    });

    const wrapper = mount(ExecutionsView);
    await flushPromises();

    await wrapper.find(".executions-list .task-list__item button").trigger("click");
    await flushPromises();
    setRoute("execution-detail", { id: 21 });
    await flushPromises();

    const detailListsBefore = wrapper.findAll(".record-detail__section .log-list");
    expect(detailListsBefore[0].text()).toContain("issue-1");
    expect(detailListsBefore[0].text()).not.toContain("issue-11");
    expect(detailListsBefore[1].text()).toContain("log-1");
    expect(detailListsBefore[1].text()).not.toContain("log-11");

    const paginations = wrapper.findAll(".record-detail__section .list-pagination");
    await paginations[0].findAll("button")[1].trigger("click");
    await paginations[1].findAll("button")[1].trigger("click");
    await flushPromises();

    const detailListsAfter = wrapper.findAll(".record-detail__section .log-list");
    expect(detailListsAfter[0].text()).toContain("issue-11");
    expect(detailListsAfter[1].text()).toContain("log-11");
  });

  it("shows an error banner when initial loading fails", async () => {
    getMock.mockImplementation((url: string) => {
      if (url === "/write-tasks/executions") {
        return Promise.reject(new Error("Execution API unavailable"));
      }
      if (url === "/write-tasks") {
        return ok([]);
      }
      return Promise.reject(new Error(`Unhandled GET ${url}`));
    });

    const wrapper = mount(ExecutionsView);
    await flushPromises();

    expect(wrapper.text()).toContain("Execution API unavailable");
    expect(wrapper.text()).toContain("暂无执行记录");
  });
});
