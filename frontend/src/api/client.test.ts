import type { AxiosError } from "axios";
import { afterEach, describe, expect, it } from "vitest";
import {
  apiClient,
  AUTH_STORAGE_KEY,
  clearApiCredentials,
  getApiCredentials,
  readApiError,
  REQUEST_TIMEOUT,
  setApiCredentials
} from "./client";

afterEach(() => {
  clearApiCredentials();
});

describe("apiClient", () => {
  it("uses the shared default timeout", () => {
    expect(apiClient.defaults.timeout).toBe(REQUEST_TIMEOUT.default);
  });

  it("stores basic auth credentials for request interceptors", () => {
    setApiCredentials("admin", "123456");

    expect(getApiCredentials()).toEqual({
      username: "admin",
      token: "YWRtaW46MTIzNDU2"
    });
    expect(localStorage.getItem(AUTH_STORAGE_KEY)).toContain("admin");
  });

  it("clears invalid auth storage values", () => {
    localStorage.setItem(AUTH_STORAGE_KEY, "{invalid");

    expect(getApiCredentials()).toBeNull();
    expect(localStorage.getItem(AUTH_STORAGE_KEY)).toBeNull();
  });
});

describe("readApiError", () => {
  it("prefers backend response messages from axios errors", () => {
    const error = {
      isAxiosError: true,
      message: "请求失败",
      response: {
        data: {
          message: "未找到数据源连接"
        }
      }
    } as AxiosError<{ message: string }>;

    expect(readApiError(error)).toBe("未找到数据源连接");
  });

  it("returns a friendly network error message", () => {
    const error = {
      isAxiosError: true,
      message: "Network Error"
    } as AxiosError;

    expect(readApiError(error)).toBe("无法连接后端服务，请确认接口服务已经启动。");
  });

  it("returns a friendly unauthorized message", () => {
    const error = {
      isAxiosError: true,
      message: "Request failed with status code 401",
      response: {
        status: 401,
        data: {}
      }
    } as AxiosError;

    expect(readApiError(error)).toBe("登录已失效或账号密码错误，请在顶部重新登录。");
  });

  it("returns a friendly timeout message", () => {
    const error = {
      isAxiosError: true,
      code: "ECONNABORTED",
      message: "timeout of 8000ms exceeded"
    } as AxiosError;

    expect(readApiError(error)).toBe("请求耗时较长，后台可能仍在处理。请稍后刷新执行结果。");
  });

  it("supports plain Error instances", () => {
    expect(readApiError(new Error("请求参数校验失败"))).toBe("请求参数校验失败");
  });

  it("returns the provided fallback for unknown values", () => {
    expect(readApiError({ detail: "unexpected" }, "加载失败")).toBe("加载失败");
  });
});
