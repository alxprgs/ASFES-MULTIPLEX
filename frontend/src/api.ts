export type User = {
  user_id: string;
  username: string;
  is_root: boolean;
  permissions: string[];
  email: string | null;
  tg_id: string | null;
  vk_id: string | null;
  two_factor_enabled: boolean;
  created_at: string;
  updated_at: string;
};

export type RuntimeSettings = {
  registration_enabled: boolean;
  mcp_enabled: boolean;
  redis_runtime_enabled: boolean;
  redis_mode: string;
};

export type Bootstrap = {
  app_name: string;
  app_version: string;
  api_prefix: string;
  mcp_path: string;
  public_base_url: string;
  access_cookie_name: string;
  refresh_cookie_name: string;
  csrf_cookie_name: string;
  user: User | null;
  runtime: RuntimeSettings | null;
};

export type Health = {
  status: string;
  mongodb: string;
  redis: string;
  mcp_enabled: boolean;
};

export type SystemUpdateResult = {
  command: string[];
  returncode: number;
  stdout: string;
  stderr: string;
  truncated: boolean;
  duration_ms: number;
};

export type Permission = {
  key: string;
  description: string;
};

export type PluginInfo = {
  key: string;
  name: string;
  version: string;
  description: string;
  enabled: boolean;
  os_support: string[];
  tool_keys: string[];
  available: boolean;
  availability_reason: string | null;
  required_backends: string[];
  providers: string[];
};

export type ToolInfo = {
  key: string;
  plugin_key: string;
  name: string;
  description: string;
  read_only: boolean;
  permissions: string[];
  tags: string[];
  global_enabled: boolean;
  available: boolean;
  availability_reason: string | null;
  os_support: string[];
  required_backends: string[];
  providers: string[];
};

export type AuditEvent = {
  event_id: string;
  event_type: string;
  actor_user_id: string | null;
  actor_username: string | null;
  target: Record<string, unknown>;
  result: string;
  ip: string | null;
  user_agent: string | null;
  metadata: Record<string, unknown>;
  created_at: string;
};

export type LoginResult =
  | { two_factor_required?: false; user: User }
  | { two_factor_required: true; challenge_token: string; expires_in: number; user_id: string; username: string };

export type TwoFactorSetup = {
  secret: string;
  otpauth_uri: string;
  qr_svg: string;
};

export class ApiError extends Error {
  status: number;

  constructor(status: number, message: string) {
    super(message);
    this.status = status;
  }
}

let csrfCookieName = "multiplex_csrf";

export function setCsrfCookieName(name: string) {
  csrfCookieName = name;
}

function getCookie(name: string): string | null {
  const prefix = `${encodeURIComponent(name)}=`;
  const item = document.cookie.split("; ").find((value) => value.startsWith(prefix));
  return item ? decodeURIComponent(item.slice(prefix.length)) : null;
}

async function apiFetch<T>(path: string, init: RequestInit = {}): Promise<T> {
  const method = (init.method || "GET").toUpperCase();
  const headers = new Headers(init.headers);
  if (init.body && !headers.has("Content-Type")) {
    headers.set("Content-Type", "application/json");
  }
  if (!["GET", "HEAD", "OPTIONS"].includes(method)) {
    const csrf = getCookie(csrfCookieName);
    if (csrf) {
      headers.set("X-CSRF-Token", csrf);
    }
  }

  const response = await fetch(`/api${path}`, {
    ...init,
    method,
    headers,
    credentials: "include"
  });

  if (!response.ok) {
    let detail = response.statusText;
    try {
      const payload = await response.json();
      detail = typeof payload.detail === "string" ? payload.detail : detail;
    } catch {
      // Тело ошибки может быть пустым, тогда оставляем HTTP status text.
    }
    throw new ApiError(response.status, detail);
  }

  if (response.status === 204) {
    return undefined as T;
  }
  return (await response.json()) as T;
}

export const api = {
  bootstrap: () => apiFetch<Bootstrap>("/bootstrap"),
  health: () => apiFetch<Health>("/health"),
  login: (username: string, password: string) =>
    apiFetch<LoginResult>("/auth/login", {
      method: "POST",
      body: JSON.stringify({ username, password })
    }),
  login2fa: (challengeToken: string, code: string) =>
    apiFetch<{ user: User }>("/auth/login/2fa", {
      method: "POST",
      body: JSON.stringify({ challenge_token: challengeToken, code })
    }),
  refresh: () =>
    apiFetch<{ user: User }>("/auth/refresh", {
      method: "POST"
    }),
  logout: () =>
    apiFetch<void>("/auth/logout", {
      method: "POST"
    }),
  profile: (payload: { email: string | null; tg_id: string | null; vk_id: string | null }) =>
    apiFetch<User>("/account/profile", {
      method: "PUT",
      body: JSON.stringify(payload)
    }),
  twoFactorStatus: () => apiFetch<{ enabled: boolean; pending: boolean }>("/auth/2fa/status"),
  twoFactorSetup: (currentPassword: string) =>
    apiFetch<TwoFactorSetup>("/auth/2fa/setup", {
      method: "POST",
      body: JSON.stringify({ current_password: currentPassword })
    }),
  twoFactorEnable: (code: string) =>
    apiFetch<{ user: User; recovery_codes: string[] }>("/auth/2fa/enable", {
      method: "POST",
      body: JSON.stringify({ code })
    }),
  twoFactorDisable: (code: string) =>
    apiFetch<User>("/auth/2fa/disable", {
      method: "POST",
      body: JSON.stringify({ code })
    }),
  users: () => apiFetch<User[]>("/users"),
  permissions: () => apiFetch<Permission[]>("/permissions"),
  mutatePermissions: (userId: string, permissions: string[], mode: "grant" | "revoke") =>
    apiFetch<User>(`/users/${encodeURIComponent(userId)}/permissions`, {
      method: "PUT",
      body: JSON.stringify({ permissions, mode })
    }),
  runtime: () => apiFetch<RuntimeSettings>("/settings/mcp"),
  setRegistration: (enabled: boolean) =>
    apiFetch<RuntimeSettings>("/settings/registration", {
      method: "PUT",
      body: JSON.stringify({ enabled })
    }),
  setMcp: (enabled: boolean) =>
    apiFetch<RuntimeSettings>("/settings/mcp", {
      method: "PUT",
      body: JSON.stringify({ enabled })
    }),
  setRedis: (enabled: boolean) =>
    apiFetch<RuntimeSettings>("/settings/redis", {
      method: "PUT",
      body: JSON.stringify({ enabled })
    }),
  runSystemUpdate: () =>
    apiFetch<SystemUpdateResult>("/system/update", {
      method: "POST"
    }),
  plugins: () => apiFetch<PluginInfo[]>("/mcp/plugins"),
  togglePlugin: (pluginKey: string, enabled: boolean) =>
    apiFetch<PluginInfo>(`/mcp/plugins/${encodeURIComponent(pluginKey)}`, {
      method: "PUT",
      body: JSON.stringify({ enabled })
    }),
  reloadPlugins: () =>
    apiFetch<{ reloaded: string[] }>("/mcp/plugins/reload", {
      method: "POST",
      body: JSON.stringify({ plugin_keys: null })
    }),
  tools: () => apiFetch<ToolInfo[]>("/mcp/tools"),
  toggleTool: (toolKey: string, enabled: boolean) =>
    apiFetch<ToolInfo>(`/mcp/tools/${encodeURIComponent(toolKey)}`, {
      method: "PUT",
      body: JSON.stringify({ enabled })
    }),
  audit: () => apiFetch<{ items: AuditEvent[] }>("/audit/logs")
};
