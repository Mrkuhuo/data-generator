import { enableAutoUnmount, flushPromises, mount } from "@vue/test-utils";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import DatasetsView from "./DatasetsView.vue";

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

function ok<T>(data: T, message: string | null = null) {
  return Promise.resolve({
    data: {
      success: true,
      data,
      message
    }
  });
}

function buildDataset(id: number) {
  return {
    id,
    name: `Dataset ${id}`,
    category: "demo",
    version: "v1",
    status: "READY" as const,
    description: `Description ${id}`,
    schemaJson: JSON.stringify({ id, field: `value_${id}` }, null, 2),
    sampleConfigJson: JSON.stringify({ count: 5, seed: 20260412 }, null, 2)
  };
}

enableAutoUnmount(afterEach);

describe("DatasetsView", () => {
  beforeEach(() => {
    getMock.mockReset();
    postMock.mockReset();
    putMock.mockReset();
    deleteMock.mockReset();

    getMock.mockImplementation((url: string) => {
      if (url === "/datasets") {
        return ok(Array.from({ length: 12 }, (_, index) => buildDataset(index + 1)));
      }
      return Promise.reject(new Error(`Unhandled GET ${url}`));
    });
  });

  it("paginates dataset cards and hides pagination when one page is enough", async () => {
    const wrapper = mount(DatasetsView);
    await flushPromises();

    expect(wrapper.findAll("section.panel-grid > article.panel")).toHaveLength(8);
    expect(wrapper.text()).toContain("Dataset 1");
    expect(wrapper.text()).toContain("Dataset 8");
    expect(wrapper.text()).not.toContain("Dataset 9");
    expect(wrapper.find(".list-pagination").exists()).toBe(true);

    const paginationButtons = wrapper.findAll(".list-pagination button");
    await paginationButtons[paginationButtons.length - 1]?.trigger("click");
    await flushPromises();

    expect(wrapper.findAll("section.panel-grid > article.panel")).toHaveLength(4);
    expect(wrapper.text()).toContain("Dataset 9");
    expect(wrapper.text()).toContain("Dataset 12");

    await wrapper.find(".list-pagination__size select").setValue("20");
    await flushPromises();

    expect(wrapper.findAll("section.panel-grid > article.panel")).toHaveLength(12);
    expect(wrapper.find(".list-pagination").exists()).toBe(false);
  });
});
