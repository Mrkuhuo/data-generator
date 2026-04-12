import type { AxiosError } from "axios";
import { describe, expect, it } from "vitest";
import { readApiError } from "./client";

describe("readApiError", () => {
  it("prefers backend response messages from axios errors", () => {
    const error = {
      isAxiosError: true,
      message: "Request failed",
      response: {
        data: {
          message: "Dataset definition not found"
        }
      }
    } as AxiosError<{ message: string }>;

    expect(readApiError(error)).toBe("Dataset definition not found");
  });

  it("falls back to axios error messages when no backend message is present", () => {
    const error = {
      isAxiosError: true,
      message: "Network Error"
    } as AxiosError;

    expect(readApiError(error)).toBe("Network Error");
  });

  it("supports plain Error instances", () => {
    expect(readApiError(new Error("Validation failed"))).toBe("Validation failed");
  });

  it("returns the provided fallback for unknown values", () => {
    expect(readApiError({ detail: "unexpected" }, "Unable to load")).toBe("Unable to load");
  });
});
