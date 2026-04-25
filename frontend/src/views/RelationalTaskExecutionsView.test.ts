import { enableAutoUnmount, flushPromises, mount } from "@vue/test-utils";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import RelationalTaskExecutionsView from "./RelationalTaskExecutionsView.vue";

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
    name: "relational-write-task-executions",
    params: { id: 12 } as Record<string, unknown>,
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

describe("RelationalTaskExecutionsView", () => {
  beforeEach(() => {
    getMock.mockReset();
    routerPushMock.mockClear();
    routerReplaceMock.mockClear();
    setRoute("relational-write-task-executions", { id: 12 });
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
      if (url === "/write-task-groups/12") {
        return ok({
          id: 12,
          name: "订单链路",
          tasks: [{ id: 1 }, { id: 2 }],
          relations: [{ id: 11 }]
        });
      }
      if (url === "/write-task-groups/12/executions") {
        return ok([
          {
            id: 51,
            writeTaskGroupId: 12,
            triggerType: "MANUAL",
            status: "SUCCESS",
            startedAt: "2026-04-25T05:10:00Z",
            finishedAt: "2026-04-25T05:10:20Z",
            plannedTableCount: 2,
            completedTableCount: 2,
            successTableCount: 2,
            failureTableCount: 0,
            insertedRowCount: 120,
            errorSummary: null,
            summary: {
              insertedRowCount: 120,
              deliveryType: "MYSQL"
            },
            tables: [
              {
                id: 101,
                writeTaskId: 1,
                tableName: "orders",
                status: "SUCCESS",
                beforeWriteRowCount: 10,
                afterWriteRowCount: 70,
                insertedCount: 60,
                nullViolationCount: 0,
                blankStringCount: 0,
                fkMissCount: 0,
                pkDuplicateCount: 0,
                errorSummary: null,
                summary: {
                  writtenRowCount: 60
                }
              },
              {
                id: 102,
                writeTaskId: 2,
                tableName: "order_items",
                status: "SUCCESS",
                beforeWriteRowCount: 30,
                afterWriteRowCount: 90,
                insertedCount: 60,
                nullViolationCount: 0,
                blankStringCount: 0,
                fkMissCount: 0,
                pkDuplicateCount: 0,
                errorSummary: null,
                summary: {
                  writtenRowCount: 60
                }
              }
            ]
          }
        ]);
      }
      return Promise.reject(new Error(`Unhandled GET ${url}`));
    });
  });

  it("renders relational execution ledger", async () => {
    const wrapper = mount(RelationalTaskExecutionsView);
    await flushPromises();

    expect(getMock).toHaveBeenCalledWith("/write-task-groups/12");
    expect(getMock).toHaveBeenCalledWith("/write-task-groups/12/executions");
    expect(wrapper.find(".executions-workspace--single").exists()).toBe(true);
    expect(wrapper.text()).toContain("订单链路");
    expect(wrapper.text()).toContain("实例 #51");
    expect(wrapper.text()).toContain("写入 120 条");
    expect(wrapper.text()).toContain("成功表 2 / 2");
  });

  it("opens execution detail and can go back to the relational task", async () => {
    const wrapper = mount(RelationalTaskExecutionsView);
    await flushPromises();

    const detailButton = wrapper.findAll("button").find((button) => button.text().includes("查看实例详情"));
    if (!detailButton) {
      throw new Error("Expected detail button");
    }

    await detailButton.trigger("click");
    await flushPromises();

    expect(routerPushMock).toHaveBeenCalledWith({
      name: "relational-write-task-execution-detail",
      params: {
        id: 12,
        executionId: 51
      }
    });
    expect(wrapper.text()).toContain("orders");
    expect(wrapper.text()).toContain("order_items");

    const backButton = wrapper.findAll("button").find((button) => button.text().includes("返回任务"));
    if (!backButton) {
      throw new Error("Expected back button");
    }

    await backButton.trigger("click");
    await flushPromises();

    expect(routerPushMock).toHaveBeenLastCalledWith({
      name: "relational-write-task-edit",
      params: { id: 12 }
    });
  });
});
