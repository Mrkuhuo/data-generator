import { flushPromises, mount } from "@vue/test-utils";
import { beforeEach, describe, expect, it, vi } from "vitest";
import ExecutionsView from "./ExecutionsView.vue";

const { getMock } = vi.hoisted(() => ({
  getMock: vi.fn()
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

function ok<T>(data: T) {
  return Promise.resolve({
    data: {
      success: true,
      data,
      message: null
    }
  });
}

function configureLedger() {
  const executions = [
    {
      id: 7,
      jobDefinitionId: 3,
      triggerType: "MANUAL",
      status: "SUCCESS",
      startedAt: "2026-04-12T14:00:00Z",
      finishedAt: "2026-04-12T14:01:00Z",
      generatedCount: 100,
      successCount: 100,
      errorCount: 0,
      errorSummary: null,
      deliveryDetailsJson: null
    }
  ];
  const jobs = [{ id: 3, name: "Kafka activity stream" }];
  const logs = [
    {
      id: 91,
      logLevel: "INFO",
      message: "连接器投递完成",
      detailJson: JSON.stringify({
        connectorId: 5,
        deliveryStatus: "SUCCESS",
        deliveredCount: 100,
        errorCount: 0,
        details: JSON.stringify({
          topic: "synthetic.user.activity"
        })
      })
    }
  ];

  getMock.mockImplementation((url: string) => {
    if (url === "/executions") {
      return ok(executions);
    }
    if (url === "/jobs") {
      return ok(jobs);
    }
    if (url === "/executions/7/logs") {
      return ok(logs);
    }
    return Promise.reject(new Error(`Unhandled GET ${url}`));
  });
}

describe("ExecutionsView", () => {
  beforeEach(() => {
    getMock.mockReset();
  });

  it("renders the loaded execution ledger", async () => {
    configureLedger();

    const wrapper = mount(ExecutionsView);
    await flushPromises();

    expect(getMock).toHaveBeenCalledTimes(2);
    expect(wrapper.text()).toContain("执行记录 #7");
    expect(wrapper.text()).toContain("Kafka activity stream / 触发方式 手动");
    expect(wrapper.text()).toContain("暂时没有执行摘要。");
  });

  it("loads execution logs and derives the delivery snapshot", async () => {
    configureLedger();

    const wrapper = mount(ExecutionsView);
    await flushPromises();

    const inspectLogsButton = wrapper.findAll("button").find((button) => button.text().includes("查看日志"));
    if (!inspectLogsButton) {
      throw new Error("Expected Inspect Logs button");
    }

    await inspectLogsButton.trigger("click");
    await flushPromises();

    expect(wrapper.text()).toContain("已加载执行记录 #7 的 1 条日志");
    expect(wrapper.text()).toContain("连接器投递完成");
    expect(wrapper.text()).toContain('"deliveryStatus": "SUCCESS"');
    expect(wrapper.text()).toContain('"topic": "synthetic.user.activity"');
  });

  it("shows an error banner when initial loading fails", async () => {
    getMock.mockImplementation((url: string) => {
      if (url === "/executions") {
        return Promise.reject(new Error("Execution API unavailable"));
      }
      if (url === "/jobs") {
        return ok([]);
      }
      return Promise.reject(new Error(`Unhandled GET ${url}`));
    });

    const wrapper = mount(ExecutionsView);
    await flushPromises();

    expect(wrapper.text()).toContain("Execution API unavailable");
    expect(wrapper.text()).toContain("还没有执行记录");
  });
});
