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
  mongodb?: string;
  redis?: string;
  mcp_enabled?: boolean;
};

export type HealthDetails = {
  status: string;
  mongodb: string;
  redis: string;
  mcp_enabled: boolean;
};

export type MCPConnectedServiceUser = {
  user_id: string;
  username: string | null;
};

export type MCPConnectedService = {
  client_id: string;
  client_name: string;
  confidential: boolean;
  allowed_scopes: string[];
  active_session_count: number;
  user_count: number;
  users: MCPConnectedServiceUser[];
  last_token_issued_at: string | null;
  last_tool_call_at: string | null;
};

export type SystemUpdateResult = {
  command: string[];
  returncode: number;
  stdout: string;
  stderr: string;
  truncated: boolean;
  duration_ms: number;
};

export type SystemUpdateStage = {
  key: string;
  title: string;
  status: string;
  needed: boolean;
  forced: boolean;
  detail: string | null;
  returncode: number | null;
};

export type SystemUpdateSession = {
  session_id: string;
  kind: string;
  status: string;
  stages: SystemUpdateStage[];
  result: SystemUpdateResult | null;
  error: string | null;
  requires_restart: boolean;
  created_at: string;
  updated_at: string;
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

export type Passkey = {
  passkey_id: string;
  name: string;
  created_at: string;
  last_used_at: string | null;
  transports: string[];
  authenticator_attachment: string | null;
  credential_device_type: string | null;
  credential_backed_up: boolean;
};

export type PasskeyOptions = {
  challenge_id: string;
  options: Record<string, unknown>;
};

export type ApiKey = {
  key_id: string;
  name: string;
  token_prefix: string;
  created_at: string;
  expires_at: string | null;
  last_used_at: string | null;
  is_active: boolean;
};

export type ApiKeyCreateResult = ApiKey & { token: string };

export type ProxyProtocol = "http" | "https" | "socks5";

export type ProxyCheckDetail = {
  ok: boolean;
  latency_ms: number | null;
  external_ip: string | null;
};

export type ProxyCheckResult = {
  checked_at: string;
  ok: boolean;
  avg_latency_ms: number | null;
  details: Record<string, ProxyCheckDetail>;
};

export type Proxy = {
  proxy_id: string;
  user_id: string;
  protocol: ProxyProtocol;
  host: string;
  port: number;
  username: string | null;
  label: string | null;
  last_check: ProxyCheckResult | null;
  created_at: string;
};

export type ProxyBulkImportResult = {
  imported: number;
  skipped: number;
  errors: string[];
};

export type ProxyTgExport = {
  deep_link: string;
  web_url: string;
};

export class ApiError extends Error {
  status: number;

  constructor(status: number, message: string) {
    super(message);
    this.status = status;
  }
}

let csrfCookieName = "multiplex_csrf";
let sessionRefreshPromise: Promise<void> | null = null;

type ApiFetchInit = RequestInit & {
  skipAuthRefresh?: boolean;
};

export function setCsrfCookieName(name: string) {
  csrfCookieName = name;
}

function getCookie(name: string): string | null {
  const prefix = `${encodeURIComponent(name)}=`;
  const item = document.cookie.split("; ").find((value) => value.startsWith(prefix));
  return item ? decodeURIComponent(item.slice(prefix.length)) : null;
}

function shouldRefreshSession(path: string, response: Response, skipAuthRefresh: boolean, retrying: boolean): boolean {
  if (skipAuthRefresh || retrying || response.status !== 401) {
    return false;
  }
  if (path === "/auth/refresh" || path === "/auth/register" || path.startsWith("/auth/login") || path.startsWith("/auth/passkeys/authentication")) {
    return false;
  }
  return true;
}

async function refreshSessionOnce(): Promise<void> {
  sessionRefreshPromise ??= apiFetch<{ user: User }>("/auth/refresh", {
    method: "POST",
    skipAuthRefresh: true
  }).then(() => undefined);

  try {
    await sessionRefreshPromise;
  } finally {
    sessionRefreshPromise = null;
  }
}

async function apiFetch<T>(path: string, init: ApiFetchInit = {}, retrying = false): Promise<T> {
  const { skipAuthRefresh = false, ...requestInit } = init;
  const method = (requestInit.method || "GET").toUpperCase();
  const headers = new Headers(requestInit.headers);
  if (requestInit.body && !headers.has("Content-Type")) {
    headers.set("Content-Type", "application/json");
  }
  if (!["GET", "HEAD", "OPTIONS"].includes(method)) {
    const csrf = getCookie(csrfCookieName);
    if (csrf) {
      headers.set("X-CSRF-Token", csrf);
    }
  }

  const response = await fetch(`/api${path}`, {
    ...requestInit,
    method,
    headers,
    credentials: "include"
  });

  if (shouldRefreshSession(path, response, skipAuthRefresh, retrying)) {
    await refreshSessionOnce();
    return apiFetch<T>(path, init, true);
  }

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
  healthDetails: () => apiFetch<HealthDetails>("/health/details"),
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
  passkeyAuthenticationOptions: (username: string | null) =>
    apiFetch<PasskeyOptions>("/auth/passkeys/authentication/options", {
      method: "POST",
      body: JSON.stringify({ username })
    }),
  passkeyAuthenticationVerify: (challengeId: string, credential: Record<string, unknown>) =>
    apiFetch<{ user: User }>("/auth/passkeys/authentication/verify", {
      method: "POST",
      body: JSON.stringify({ challenge_id: challengeId, credential })
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
  twoFactorDisable: (code: string, currentPassword: string) =>
    apiFetch<User>("/auth/2fa/disable", {
      method: "POST",
      body: JSON.stringify({ code, current_password: currentPassword })
    }),
  passkeys: () => apiFetch<Passkey[]>("/auth/passkeys"),
  passkeyRegistrationOptions: (currentPassword: string, name: string | null) =>
    apiFetch<PasskeyOptions>("/auth/passkeys/registration/options", {
      method: "POST",
      body: JSON.stringify({ current_password: currentPassword, name })
    }),
  passkeyRegistrationVerify: (challengeId: string, name: string | null, credential: Record<string, unknown>) =>
    apiFetch<Passkey>("/auth/passkeys/registration/verify", {
      method: "POST",
      body: JSON.stringify({ challenge_id: challengeId, name, credential })
    }),
  renamePasskey: (passkeyId: string, name: string) =>
    apiFetch<Passkey>(`/auth/passkeys/${encodeURIComponent(passkeyId)}`, {
      method: "PUT",
      body: JSON.stringify({ name })
    }),
  deletePasskey: (passkeyId: string) =>
    apiFetch<void>(`/auth/passkeys/${encodeURIComponent(passkeyId)}`, {
      method: "DELETE"
    }),
  apiKeys: () => apiFetch<ApiKey[]>("/auth/api-keys"),
  createApiKey: (name: string, expiresInDays: number | null) =>
    apiFetch<ApiKeyCreateResult>("/auth/api-keys", {
      method: "POST",
      body: JSON.stringify({ name, expires_in_days: expiresInDays })
    }),
  revokeApiKey: (keyId: string) =>
    apiFetch<void>(`/auth/api-keys/${encodeURIComponent(keyId)}`, {
      method: "DELETE"
    }),
  updateApiKey: (keyId: string, name?: string, expiresInDays?: number | null) =>
    apiFetch<ApiKey>(`/auth/api-keys/${encodeURIComponent(keyId)}`, {
      method: "PATCH",
      body: JSON.stringify({ name, expires_in_days: expiresInDays })
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
  checkSystemUpdate: () =>
    apiFetch<{ session_id: string }>("/system/update/check", {
      method: "POST"
    }),
  runSystemUpdateSession: (stages: string[], forceStages: string[]) =>
    apiFetch<{ session_id: string }>("/system/update/run", {
      method: "POST",
      body: JSON.stringify({ stages, force_stages: forceStages })
    }),
  systemUpdateSession: (sessionId: string) => apiFetch<SystemUpdateSession>(`/system/update/sessions/${encodeURIComponent(sessionId)}`),
  systemUpdateEventsUrl: (sessionId: string) => `/api/system/update/sessions/${encodeURIComponent(sessionId)}/events`,
  runSystemRestart: () =>
    apiFetch<SystemUpdateResult>("/system/restart", {
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
  connectedServices: () => apiFetch<MCPConnectedService[]>("/mcp/connected-services"),
  audit: () => apiFetch<{ items: AuditEvent[] }>("/audit/logs"),
  
  proxies: () => apiFetch<Proxy[]>("/proxy/proxies"),
  createProxy: (data: { protocol: string; host: string; port: number; username?: string; password?: string; label?: string }) =>
    apiFetch<Proxy>("/proxy/proxies", { method: "POST", body: JSON.stringify(data) }),
  createProxyFromUrl: (url: string, protocol: string, label?: string) =>
    apiFetch<Proxy>("/proxy/proxies/from-url", { method: "POST", body: JSON.stringify({ url, protocol, label }) }),
  deleteProxy: (proxyId: string) =>
    apiFetch<{ status: string }>(`/proxy/proxies/${encodeURIComponent(proxyId)}`, { method: "DELETE" }),
  importProxifier: (xmlContent: string) =>
    apiFetch<ProxyBulkImportResult>("/proxy/proxies/import/proxifier", { method: "POST", body: JSON.stringify({ xml_content: xmlContent }) }),
  checkProxy: (proxyId: string) =>
    apiFetch<ProxyCheckResult>(`/proxy/proxies/${encodeURIComponent(proxyId)}/check`, { method: "POST" }),
  checkAllProxies: () =>
    apiFetch<{ status: string }>("/proxy/proxies/check-all", { method: "POST" }),
  exportProxyUrl: (proxyId: string) =>
    apiFetch<{ url: string }>(`/proxy/proxies/${encodeURIComponent(proxyId)}/export/url`),
  exportProxyTg: (proxyId: string, secret?: string) => {
    const q = secret ? `?secret=${encodeURIComponent(secret)}` : "";
    return apiFetch<ProxyTgExport>(`/proxy/proxies/${encodeURIComponent(proxyId)}/export/tg${q}`);
  },
  exportProxyLines: (proxyId: string) =>
    apiFetch<{ lines: string }>(`/proxy/proxies/${encodeURIComponent(proxyId)}/export/lines`),
  exportProxifier: (proxyIds: string[]) =>
    apiFetch<{ xml_content: string }>("/proxy/proxies/export/proxifier", { method: "POST", body: JSON.stringify({ proxy_ids: proxyIds }) }),

  // ------------------------------------------------------------------
  // PyPI Mirror
  // ------------------------------------------------------------------
  pypiStats: () =>
    apiFetch<PyPIStats>("/pypi/stats"),
  pypiPackages: (params?: { search?: string; page?: number; per_page?: number }) => {
    const q = new URLSearchParams();
    if (params?.search) q.set("search", params.search);
    if (params?.page) q.set("page", String(params.page));
    if (params?.per_page) q.set("per_page", String(params.per_page));
    const qs = q.toString() ? `?${q.toString()}` : "";
    return apiFetch<PyPIPackageListResponse>(`/pypi/packages${qs}`);
  },
  pypiPackage: (name: string) =>
    apiFetch<PyPIPackage>(`/pypi/packages/${encodeURIComponent(name)}`),
  pypiInstall: (name: string, version?: string, with_dependencies?: boolean) =>
    apiFetch<PyPIJobStatus>("/pypi/packages/install", {
      method: "POST",
      body: JSON.stringify({ name, version: version || null, with_dependencies: !!with_dependencies })
    }),
  pypiBulkInstall: (packages: string[], with_dependencies?: boolean) =>
    apiFetch<PyPIJobStatus>("/pypi/packages/bulk-install", {
      method: "POST",
      body: JSON.stringify({ packages, with_dependencies: !!with_dependencies })
    }),
  pypiDeletePackage: (name: string) =>
    apiFetch<{ ok: boolean }>(`/pypi/packages/${encodeURIComponent(name)}`, { method: "DELETE" }),
  pypiDeleteVersion: (name: string, version: string) =>
    apiFetch<{ ok: boolean }>(`/pypi/packages/${encodeURIComponent(name)}/versions/${encodeURIComponent(version)}`, { method: "DELETE" }),
  pypiGetBlocklist: () =>
    apiFetch<PyPIBlocklist>("/pypi/blocklist"),
  pypiBlock: (name: string, version?: string) =>
    apiFetch<{ ok: boolean }>("/pypi/blocklist", {
      method: "POST",
      body: JSON.stringify({ name, version: version || null })
    }),
  pypiUnblockPackage: (name: string) =>
    apiFetch<{ ok: boolean }>(`/pypi/blocklist/${encodeURIComponent(name)}`, { method: "DELETE" }),
  pypiUnblockVersion: (name: string, version: string) =>
    apiFetch<{ ok: boolean }>(`/pypi/blocklist/${encodeURIComponent(name)}/versions/${encodeURIComponent(version)}`, { method: "DELETE" }),
  pypiSyncAllPackages: () =>
    apiFetch<PyPIJobStatus>("/pypi/packages/sync-all", { method: "POST" }),
  pypiVerify: (name?: string) => {
    if (name) {
      return apiFetch<PyPIJobStatus>(`/pypi/packages/${encodeURIComponent(name)}/verify`, { method: "POST" });
    }
    return apiFetch<PyPIJobStatus>("/pypi/verify", { method: "POST" });
  },
  pypiJobStatus: (jobId: string) =>
    apiFetch<PyPIJobStatus>(`/pypi/jobs/${encodeURIComponent(jobId)}`),
  pypiCancelJob: (jobId: string) =>
    apiFetch<{ ok: boolean; remaining_packages: string[] }>(`/pypi/jobs/${encodeURIComponent(jobId)}`, { method: "DELETE" }),
  pypiBulkDownload: () =>
    apiFetch<PyPIJobStatus>("/pypi/bulk-download", { method: "POST" })
};

// ---------------------------------------------------------------------------
// PyPI Mirror types
// ---------------------------------------------------------------------------

export type PyPIStats = {
  packages_count: number;
  versions_count: number;
  files_count: number;
  total_size_bytes: number;
  total_size_human: string;
  blocked_packages: number;
  blocked_versions: number;
  active_jobs: number;
};

export type PyPIPackageVersion = {
  version: string;
  files_count: number;
  size_bytes: number;
  size_human: string;
  is_blocked: boolean;
};

export type PyPIPackage = {
  name: string;
  versions: PyPIPackageVersion[];
  total_versions: number;
  total_size_bytes: number;
  total_size_human: string;
  is_blocked: boolean;
  blocked_versions: string[];
};

export type PyPIPackageListItem = {
  name: string;
  versions_count: number;
  total_size_human: string;
  latest_version: string | null;
  is_blocked: boolean;
  has_blocked_versions: boolean;
};

export type PyPIPackageListResponse = {
  items: PyPIPackageListItem[];
  total: number;
  page: number;
  per_page: number;
};

export type PyPIJobStatus = {
  job_id: string;
  kind: string;
  status: string;
  name: string | null;
  total: number;
  done: number;
  failed: number;
  progress_pct: number;
  eta_seconds: number | null;
  message: string | null;
  started_at: string;
  finished_at: string | null;
  remaining_packages: string[];
};

export type PyPIBlocklist = {
  blocked_packages: string[];
  blocked_versions: Record<string, string[]>;
};
