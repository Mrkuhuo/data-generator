import { flushPromises, mount } from "@vue/test-utils";
import { beforeEach, describe, expect, it, vi } from "vitest";
import JobsView from "./JobsView.vue";

const { getMock, postMock, putMock, deleteMock } = vi.hoisted(() => ({
  getMock: vi.fn(),
  postMock: vi.fn(),
  putMock: vi.fn(),
  deleteMock: vi.fn()
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

function ok<T>(data: T) {
  return Promise.resolve({
    data: {
      success: true,
      data,
      message: null
    }
  });
}

function configureWorkspace() {
  const datasets = [{ id: 1, name: "Synthetic Profiles" }];
  const connectors = [{ id: 2, name: "Warehouse Sink", connectorType: "MYSQL" }];
  const jobs = [
    {
      id: 7,
      name: "Nightly profile export",
      datasetDefinitionId: 1,
      targetConnectorId: 2,
      writeStrategy: "UPSERT",
      scheduleType: "CRON",
      cronExpression: "0 */5 * * * ?",
      status: "READY",
      runtimeConfigJson: JSON.stringify({ count: 50 }),
      schedulerState: "NORMAL",
      nextFireAt: "2026-04-12T15:00:00Z",
      previousFireAt: null
    }
  ];

  getMock.mockImplementation((url: string) => {
    if (url === "/jobs") {
      return ok(jobs);
    }
    if (url === "/datasets") {
      return ok(datasets);
    }
    if (url === "/connectors") {
      return ok(connectors);
    }
    return Promise.reject(new Error(`Unhandled GET ${url}`));
  });
}

describe("JobsView", () => {
  beforeEach(() => {
    getMock.mockReset();
    postMock.mockReset();
    putMock.mockReset();
    deleteMock.mockReset();
  });

  it("renders the loaded job workspace", async () => {
    configureWorkspace();

    const wrapper = mount(JobsView);
    await flushPromises();

    expect(getMock).toHaveBeenCalledTimes(3);
    expect(wrapper.text()).toContain("Nightly profile export");
    expect(wrapper.text()).toContain("Synthetic Profiles");
    expect(wrapper.text()).toContain("Warehouse Sink（MySQL）");
    expect(wrapper.text()).toContain("调度状态：正常");
  });

  it("creates a job with normalized runtime config", async () => {
    configureWorkspace();
    postMock.mockResolvedValue({
      data: {
        success: true,
        data: { id: 9 },
        message: null
      }
    });

    const wrapper = mount(JobsView);
    await flushPromises();

    const textInputs = wrapper.findAll("input[type='text']");
    const selects = wrapper.findAll("select");
    const runtimeConfig = wrapper.find("textarea");

    await textInputs[0]!.setValue("Realtime profile sync");
    await selects[0]!.setValue("1");
    await selects[1]!.setValue("2");
    await selects[4]!.setValue("CRON");
    await textInputs[1]!.setValue("0 */10 * * * ?");
    await runtimeConfig.setValue('{"count":25,"target":{"table":"synthetic_profiles"}}');
    await wrapper.find("form").trigger("submit.prevent");
    await flushPromises();

    expect(postMock).toHaveBeenCalledWith("/jobs", {
      name: "Realtime profile sync",
      datasetDefinitionId: 1,
      targetConnectorId: 2,
      writeStrategy: "APPEND",
      scheduleType: "CRON",
      cronExpression: "0 */10 * * * ?",
      status: "READY",
      runtimeConfigJson: JSON.stringify(
        {
          count: 25,
          target: {
            table: "synthetic_profiles"
          }
        },
        null,
        2
      )
    });
    expect(wrapper.text()).toContain("任务已创建");
  });

  it("shows a validation message when runtime config is invalid JSON", async () => {
    configureWorkspace();

    const wrapper = mount(JobsView);
    await flushPromises();

    await wrapper.find("textarea").setValue("{not-valid");
    await wrapper.find("form").trigger("submit.prevent");
    await flushPromises();

    expect(postMock).not.toHaveBeenCalled();
    expect(wrapper.text()).toContain("运行时配置 必须是合法的 JSON");
  });
});
