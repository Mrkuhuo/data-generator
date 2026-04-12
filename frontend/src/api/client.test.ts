import type { AxiosError } from "axios";
import { describe, expect, it } from "vitest";
import { readApiError } from "./client";

describe("readApiError", () => {
  it("prefers backend response messages from axios errors", () => {
    const error = {
      isAxiosError: true,
      message: "请求失败",
      response: {
        data: {
          message: "未找到数据集定义"
        }
      }
    } as AxiosError<{ message: string }>;

    expect(readApiError(error)).toBe("未找到数据集定义");
  });

  it("falls back to axios error messages when no backend message is present", () => {
    const error = {
      isAxiosError: true,
      message: "Network Error"
    } as AxiosError;

    expect(readApiError(error)).toBe("Network Error");
  });

  it("supports plain Error instances", () => {
    expect(readApiError(new Error("请求参数校验失败"))).toBe("请求参数校验失败");
  });

  it("returns the provided fallback for unknown values", () => {
    expect(readApiError({ detail: "unexpected" }, "加载失败")).toBe("加载失败");
  });
});
