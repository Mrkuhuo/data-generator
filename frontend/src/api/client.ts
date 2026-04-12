import axios from "axios";

export interface ApiResponse<T> {
  success: boolean;
  data: T;
  message: string | null;
}

export const apiClient = axios.create({
  baseURL: import.meta.env.VITE_API_BASE_URL ?? "http://localhost:8888/api",
  timeout: 8000
});

export function readApiError(error: unknown, fallback = "请求失败") {
  if (axios.isAxiosError(error)) {
    const message = error.response?.data?.message;
    if (typeof message === "string" && message.trim()) {
      return message;
    }
    if (error.message) {
      return error.message;
    }
  }

  if (error instanceof Error && error.message) {
    return error.message;
  }

  return fallback;
}
