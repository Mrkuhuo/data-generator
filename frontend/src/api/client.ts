import axios from "axios";

export const AUTH_STORAGE_KEY = "mdg.basicAuth";

export const REQUEST_TIMEOUT = {
  default: 30000,
  longRunning: 120000,
  runSubmit: 4000
} as const;

export interface ApiResponse<T> {
  success: boolean;
  data: T;
  message: string | null;
}

export const apiClient = axios.create({
  baseURL: import.meta.env.VITE_API_BASE_URL ?? "/api",
  timeout: REQUEST_TIMEOUT.default
});

export interface ApiAuthState {
  username: string;
  token: string;
}

export function setApiCredentials(username: string, password: string) {
  const normalizedUsername = username.trim();
  if (!normalizedUsername || !password) {
    throw new Error("请输入登录用户名和密码");
  }
  const token = encodeBasicToken(normalizedUsername, password);
  localStorage.setItem(AUTH_STORAGE_KEY, JSON.stringify({ username: normalizedUsername, token }));
}

export function getApiCredentials(): ApiAuthState | null {
  if (typeof localStorage === "undefined") {
    return null;
  }
  const rawValue = localStorage.getItem(AUTH_STORAGE_KEY);
  if (!rawValue) {
    return null;
  }
  try {
    const parsed = JSON.parse(rawValue) as Partial<ApiAuthState>;
    if (parsed.username && parsed.token) {
      return { username: parsed.username, token: parsed.token };
    }
  } catch {
    localStorage.removeItem(AUTH_STORAGE_KEY);
  }
  return null;
}

export function clearApiCredentials() {
  localStorage.removeItem(AUTH_STORAGE_KEY);
}

apiClient.interceptors.request.use((config) => {
  const credentials = getApiCredentials();
  if (credentials) {
    config.headers.Authorization = `Basic ${credentials.token}`;
  }
  return config;
});

function encodeBasicToken(username: string, password: string) {
  const raw = `${username}:${password}`;
  const bytes = new TextEncoder().encode(raw);
  let binary = "";
  bytes.forEach((byte) => {
    binary += String.fromCharCode(byte);
  });
  return btoa(binary);
}

export function isApiTimeoutError(error: unknown) {
  return axios.isAxiosError(error)
    && (error.code === "ECONNABORTED" || error.message.toLowerCase().includes("timeout"));
}

export function readApiError(error: unknown, fallback = "\u8bf7\u6c42\u5931\u8d25") {
  if (axios.isAxiosError(error)) {
    const message = error.response?.data?.message;
    if (typeof message === "string" && message.trim()) {
      return message;
    }
    if (error.response?.status === 401) {
      return "登录已失效或账号密码错误，请在顶部重新登录。";
    }
    if (!error.response && error.message === "Network Error") {
      return "\u65e0\u6cd5\u8fde\u63a5\u540e\u7aef\u670d\u52a1\uff0c\u8bf7\u786e\u8ba4\u63a5\u53e3\u670d\u52a1\u5df2\u7ecf\u542f\u52a8\u3002";
    }
    if (isApiTimeoutError(error)) {
      return "\u8bf7\u6c42\u8017\u65f6\u8f83\u957f\uff0c\u540e\u53f0\u53ef\u80fd\u4ecd\u5728\u5904\u7406\u3002\u8bf7\u7a0d\u540e\u5237\u65b0\u6267\u884c\u7ed3\u679c\u3002";
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
