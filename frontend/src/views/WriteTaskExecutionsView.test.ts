import { enableAutoUnmount, flushPromises, mount } from "@vue/test-utils";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import WriteTaskExecutionsView from "./WriteTaskExecutionsView.vue";

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
    name: "write-task-executions",
    params: { taskId: 7 } as Record<string, unknown>,
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

describe("WriteTaskExecutionsView", () => {
  beforeEach(() => {
    getMock.mockReset();
    routerPushMock.mockClear();
    routerReplaceMock.mockClear();
    setRoute("write-task-executions", { taskId: 7 });
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

    getMock.mockImplementation((url: string) => {
      if (url === "/write-tasks/7/executions") {
        return ok([
          {
            id: 31,
            writeTaskId: 7,
            triggerType: "API",
            status: "SUCCESS",
            startedAt: "2026-04-25T05:00:00Z",
            finishedAt: "2026-04-25T05:00:10Z",
            generatedCount: 20,
            successCount: 20,
            errorCount: 0,
            errorSummary: null,
            deliveryDetailsJson: JSON.stringify({
              writtenRowCount: 20,
              beforeWriteRowCount: 100,
              afterWriteRowCount: 120,
              rowDelta: 20,
              nonNullValidation: {
                passed: true,
                nullValueCount: 0,
                blankStringCount: 0,
                issues: []
              }
            })
          }
        ]);
      }
      if (url === "/write-tasks/7") {
        return ok({
          id: 7,
          name: "订单写入任务",
          tableName: "synthetic_orders"
        });
      }
      if (url === "/write-tasks/executions/31/logs") {
        return ok([
          {
            id: 91,
            logLevel: "INFO",
            message: "开始写入目标表",
            detailJson: JSON.stringify({
              tableName: "synthetic_orders",
              successCount: 20
            })
          }
        ]);
      }
      return Promise.reject(new Error(`Unhandled GET ${url}`));
    });
  });

  it("renders task scoped executions", async () => {
    const wrapper = mount(WriteTaskExecutionsView);
    await flushPromises();

    expect(getMock).toHaveBeenCalledWith("/write-tasks/7/executions");
    expect(getMock).toHaveBeenCalledWith("/write-tasks/7");
    expect(wrapper.find(".executions-workspace--single").exists()).toBe(true);
    expect(wrapper.text()).toContain("订单写入任务");
    expect(wrapper.text()).toContain("实例 #31");
    expect(wrapper.text()).toContain("本次写入：20 条 / 写入前：100 条 / 写入后：120 条 / 净变化：20 条");
  });

  it("opens execution detail and logs, then can go back to the task", async () => {
    const wrapper = mount(WriteTaskExecutionsView);
    await flushPromises();

    const detailButton = wrapper.findAll("button").find((button) => button.text().includes("查看实例详情"));
    if (!detailButton) {
      throw new Error("Expected detail button");
    }

    await detailButton.trigger("click");
    await flushPromises();

    expect(routerPushMock).toHaveBeenCalledWith({
      name: "write-task-execution-detail",
      params: {
        taskId: 7,
        executionId: 31
      }
    });
    expect(wrapper.text()).toContain("开始写入目标表");

    const backButton = wrapper.findAll("button").find((button) => button.text().includes("返回任务"));
    if (!backButton) {
      throw new Error("Expected back button");
    }

    await backButton.trigger("click");
    await flushPromises();

    expect(routerPushMock).toHaveBeenLastCalledWith({
      name: "write-task-edit",
      params: { id: 7 }
    });
  });
});
