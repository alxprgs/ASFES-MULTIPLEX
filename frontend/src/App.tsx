import {
  Activity,
  Database,
  KeyRound,
  Loader2,
  LogOut,
  Plug,
  Power,
  QrCode,
  RefreshCw,
  Save,
  ScrollText,
  Shield,
  SlidersHorizontal,
  UserCircle,
  Users,
  Wrench,
  X
} from "lucide-react";
import { FormEvent, ReactNode, useCallback, useEffect, useMemo, useRef, useState } from "react";
import { ApiError, AuditEvent, Bootstrap, Health, Permission, PluginInfo, RuntimeSettings, SystemUpdateResult, ToolInfo, TwoFactorSetup, User, api, setCsrfCookieName } from "./api";

type View = "overview" | "users" | "plugins" | "tools" | "audit" | "profile";
type ToastTone = "success" | "error" | "info" | "warning";

type Toast = {
  id: string;
  tone: ToastTone;
  title: string;
  message?: string;
};

type Confirmation = {
  title: string;
  message: string;
  confirmLabel: string;
  tone?: "default" | "danger";
  onConfirm: () => void;
};

type UpdateLogState = {
  open: boolean;
  running: boolean;
  result: SystemUpdateResult | null;
  error: string | null;
};

const navItems: Array<{ view: View; label: string; icon: ReactNode }> = [
  { view: "overview", label: "Обзор", icon: <Activity size={18} /> },
  { view: "users", label: "Пользователи", icon: <Users size={18} /> },
  { view: "plugins", label: "Плагины", icon: <Plug size={18} /> },
  { view: "tools", label: "Инструменты", icon: <Wrench size={18} /> },
  { view: "audit", label: "Аудит", icon: <ScrollText size={18} /> },
  { view: "profile", label: "Профиль", icon: <UserCircle size={18} /> }
];

const runtimeLabels: Record<"registration_enabled" | "mcp_enabled" | "redis_runtime_enabled", string> = {
  registration_enabled: "Регистрация",
  mcp_enabled: "MCP",
  redis_runtime_enabled: "Redis во время работы"
};

function formatDate(value: string): string {
  return new Intl.DateTimeFormat("ru-RU", {
    dateStyle: "short",
    timeStyle: "short"
  }).format(new Date(value));
}

function formatResult(value: string): string {
  if (value === "success") {
    return "успех";
  }
  if (value === "failure" || value === "error") {
    return "ошибка";
  }
  return value || "неизвестно";
}

function enabledText(value: unknown, enabled = "включён", disabled = "отключён"): string {
  if (typeof value !== "boolean") {
    return "обновлён";
  }
  return value ? enabled : disabled;
}

function textValue(value: unknown): string | null {
  return typeof value === "string" && value.trim() ? value : null;
}

function ErrorBanner({ message }: { message: string | null }) {
  if (!message) {
    return null;
  }
  return <div className="notice notice-error">{message}</div>;
}

function Badge({ tone, children }: { tone: "ok" | "warn" | "muted" | "danger"; children: ReactNode }) {
  return <span className={`badge badge-${tone}`}>{children}</span>;
}

function ToastItem({ toast, onDismiss }: { toast: Toast; onDismiss: (id: string) => void }) {
  useEffect(() => {
    const timer = window.setTimeout(() => onDismiss(toast.id), 5000);
    return () => window.clearTimeout(timer);
  }, [onDismiss, toast.id]);

  return (
    <div className={`toast toast-${toast.tone}`} role={toast.tone === "error" ? "alert" : "status"}>
      <div>
        <strong>{toast.title}</strong>
        {toast.message ? <small>{toast.message}</small> : null}
      </div>
      <button type="button" className="toast-close" onClick={() => onDismiss(toast.id)} aria-label="Закрыть уведомление">
        <X size={16} />
      </button>
    </div>
  );
}

function ToastViewport({ toasts, onDismiss }: { toasts: Toast[]; onDismiss: (id: string) => void }) {
  if (!toasts.length) {
    return null;
  }
  return (
    <div className="toast-viewport" aria-live="polite">
      {toasts.map((toast) => (
        <ToastItem key={toast.id} toast={toast} onDismiss={onDismiss} />
      ))}
    </div>
  );
}

function ConfirmDialog({ confirmation, onCancel }: { confirmation: Confirmation | null; onCancel: () => void }) {
  if (!confirmation) {
    return null;
  }
  return (
    <div className="confirm-backdrop" role="presentation" onMouseDown={onCancel}>
      <section className="confirm-dialog" role="dialog" aria-modal="true" aria-labelledby="confirm-title" onMouseDown={(event) => event.stopPropagation()}>
        <h2 id="confirm-title">{confirmation.title}</h2>
        <p>{confirmation.message}</p>
        <div className="confirm-actions">
          <button type="button" className="secondary-button" onClick={onCancel}>
            Отмена
          </button>
          <button
            type="button"
            className={confirmation.tone === "danger" ? "danger-button" : "primary-button"}
            onClick={() => {
              onCancel();
              confirmation.onConfirm();
            }}
          >
            {confirmation.confirmLabel}
          </button>
        </div>
      </section>
    </div>
  );
}

function UpdateLogDialog({ state, onClose }: { state: UpdateLogState; onClose: () => void }) {
  if (!state.open) {
    return null;
  }
  const command = state.result?.command.join(" ") || "scripts/update.sh";
  const stdout = state.result?.stdout.trim() || (state.running ? "Ожидаю вывод update.sh..." : "");
  const stderr = state.result?.stderr.trim() || "";
  const canClose = !state.running;
  return (
    <div className="confirm-backdrop" role="presentation">
      <section className="update-log-dialog" role="dialog" aria-modal="true" aria-labelledby="update-log-title">
        <div className="update-log-head">
          <div>
            <h2 id="update-log-title">Логи обновления</h2>
            <p>{state.running ? "Обновление выполняется. Полный вывод появится здесь после завершения команды." : `Код выхода: ${state.result?.returncode ?? "-"}`}</p>
          </div>
          {state.running ? <Loader2 className="spin" size={22} /> : null}
        </div>
        <div className="update-log-meta">
          <span>Команда</span>
          <code>{command}</code>
        </div>
        <pre className="update-log-output">{[
          stdout ? `$ stdout\n${stdout}` : "",
          stderr ? `$ stderr\n${stderr}` : "",
          state.error ? `$ error\n${state.error}` : ""
        ].filter(Boolean).join("\n\n") || "Вывод пуст."}</pre>
        <div className="confirm-actions">
          <button type="button" className="secondary-button" onClick={onClose} disabled={!canClose}>
            Закрыть
          </button>
        </div>
      </section>
    </div>
  );
}

function Toggle({
  checked,
  disabled,
  busy,
  onChange,
  label
}: {
  checked: boolean;
  disabled?: boolean;
  busy?: boolean;
  onChange: (checked: boolean) => void;
  label: string;
}) {
  const isDisabled = disabled || busy;
  return (
    <label className={`switch ${busy ? "switch-busy" : ""}`} title={label}>
      <input
        type="checkbox"
        checked={checked}
        disabled={isDisabled}
        onChange={(event) => onChange(event.target.checked)}
        aria-label={label}
      />
      <span className="switch-track">
        {busy ? <Loader2 className="switch-spinner" size={13} /> : null}
      </span>
    </label>
  );
}

function LoginView({ onLogin }: { onLogin: (user: User) => void }) {
  const [username, setUsername] = useState("root");
  const [password, setPassword] = useState("");
  const [code, setCode] = useState("");
  const [challenge, setChallenge] = useState<{ token: string; username: string } | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function submit(event: FormEvent) {
    event.preventDefault();
    setBusy(true);
    setError(null);
    try {
      if (challenge) {
        const result = await api.login2fa(challenge.token, code);
        onLogin(result.user);
        return;
      }
      const result = await api.login(username, password);
      if (result.two_factor_required) {
        setChallenge({ token: result.challenge_token, username: result.username });
        setCode("");
        return;
      }
      onLogin(result.user);
    } catch (exc) {
      setError(exc instanceof Error ? exc.message : "Не удалось войти");
    } finally {
      setBusy(false);
    }
  }

  return (
    <main className="login-screen">
      <section className="login-panel">
        <div className="brand-row">
          <Shield size={28} />
          <div>
            <h1>ASFES Multiplex</h1>
            <p>Домашняя панель управления</p>
          </div>
        </div>
        <ErrorBanner message={error} />
        <form onSubmit={submit} className="form-grid">
          {challenge ? (
            <>
              <div className="security-note">
                <KeyRound size={18} />
                <span>Введите код из приложения-аутентификатора для {challenge.username}</span>
              </div>
              <label>
                Код 2FA
                <input value={code} onChange={(event) => setCode(event.target.value)} inputMode="numeric" autoComplete="one-time-code" autoFocus />
              </label>
            </>
          ) : (
            <>
              <label>
                Логин
                <input value={username} onChange={(event) => setUsername(event.target.value)} autoComplete="username" />
              </label>
              <label>
                Пароль
                <input
                  value={password}
                  onChange={(event) => setPassword(event.target.value)}
                  type="password"
                  autoComplete="current-password"
                />
              </label>
            </>
          )}
          <button className="primary-button" type="submit" disabled={busy || !username || !password || (Boolean(challenge) && !code)}>
            {busy ? "Вход..." : challenge ? "Подтвердить" : "Войти"}
          </button>
          {challenge ? (
            <button className="secondary-button" type="button" onClick={() => {
              setChallenge(null);
              setCode("");
            }}>
              Назад
            </button>
          ) : null}
        </form>
      </section>
    </main>
  );
}

function OverviewView({
  health,
  runtime,
  pendingKeys,
  onToggleRuntime,
  onConfirmUpdate,
  onRunRestart,
  onRefresh
}: {
  health: Health | null;
  runtime: RuntimeSettings | null;
  pendingKeys: ReadonlySet<string>;
  onToggleRuntime: (key: "registration_enabled" | "mcp_enabled" | "redis_runtime_enabled", value: boolean) => void;
  onConfirmUpdate: () => void;
  onRunRestart: () => void;
  onRefresh: () => void;
}) {
  return (
    <section className="page-grid">
      <div className="panel span-2">
        <div className="panel-head">
          <div>
            <h2>Состояние сервиса</h2>
            <p>MongoDB, Redis и MCP во время работы</p>
          </div>
          <button className="icon-button" onClick={onRefresh} title="Обновить">
            <RefreshCw size={18} />
          </button>
        </div>
        <div className="metrics-grid">
          <div className="metric">
            <span>API</span>
            <strong>{health?.status || "неизвестно"}</strong>
            <Badge tone={health?.status === "ok" ? "ok" : "warn"}>{health?.status === "ok" ? "OK" : "ПРОВЕРИТЬ"}</Badge>
          </div>
          <div className="metric">
            <span>MongoDB</span>
            <strong>{health?.mongodb || "неизвестно"}</strong>
            <Badge tone={health?.mongodb === "ok" ? "ok" : "danger"}>{health?.mongodb === "ok" ? "OK" : health?.mongodb || "неизвестно"}</Badge>
          </div>
          <div className="metric">
            <span>Redis</span>
            <strong>{health?.redis || runtime?.redis_mode || "неизвестно"}</strong>
            <Badge tone={health?.redis === "enabled" ? "ok" : "muted"}>{health?.redis === "enabled" ? "включён" : "отключён"}</Badge>
          </div>
          <div className="metric">
            <span>MCP</span>
            <strong>{runtime?.mcp_enabled ? "включён" : "отключён"}</strong>
            <Badge tone={runtime?.mcp_enabled ? "ok" : "warn"}>{runtime?.mcp_enabled ? "ВКЛ" : "ВЫКЛ"}</Badge>
          </div>
        </div>
      </div>
      <div className="panel">
        <h2>Настройки</h2>
        <div className="setting-list">
          <div className="setting-row">
            <div>
              <strong>Регистрация</strong>
              <span>Самостоятельное создание аккаунтов</span>
            </div>
            <Toggle
              checked={Boolean(runtime?.registration_enabled)}
              busy={pendingKeys.has("runtime:registration_enabled")}
              onChange={(value) => onToggleRuntime("registration_enabled", value)}
              label="Регистрация"
            />
          </div>
          <div className="setting-row">
            <div>
              <strong>MCP</strong>
              <span>Доступ клиентов к MCP-инструментам</span>
            </div>
            <Toggle
              checked={Boolean(runtime?.mcp_enabled)}
              busy={pendingKeys.has("runtime:mcp_enabled")}
              onChange={(value) => onToggleRuntime("mcp_enabled", value)}
              label="MCP"
            />
          </div>
          <div className="setting-row">
            <div>
              <strong>Redis во время работы</strong>
              <span>Ограничение частоты через Redis</span>
            </div>
            <Toggle
              checked={Boolean(runtime?.redis_runtime_enabled)}
              busy={pendingKeys.has("runtime:redis_runtime_enabled")}
              onChange={(value) => onToggleRuntime("redis_runtime_enabled", value)}
              label="Redis во время работы"
            />
          </div>
        </div>
        <button className="secondary-button update-button" onClick={onConfirmUpdate} disabled={pendingKeys.has("system:update")}>
          <RefreshCw size={16} className={pendingKeys.has("system:update") ? "spin" : ""} />
          Обновить приложение
        </button>
        <button className="secondary-button update-button" onClick={onRunRestart} disabled={pendingKeys.has("system:restart")}>
          <Power size={16} className={pendingKeys.has("system:restart") ? "spin" : ""} />
          Перезапустить приложение
        </button>
      </div>
    </section>
  );
}

function UsersView({
  users,
  permissions,
  onPermissionChange
}: {
  users: User[];
  permissions: Permission[];
  onPermissionChange: (user: User, permission: string, enabled: boolean) => void;
}) {
  const [selectedId, setSelectedId] = useState<string>("");
  const selectedUser = users.find((user) => user.user_id === selectedId) || users[0];

  useEffect(() => {
    if (!selectedId && users[0]) {
      setSelectedId(users[0].user_id);
    }
  }, [selectedId, users]);

  return (
    <section className="page-grid">
      <div className="panel">
        <h2>Пользователи</h2>
        <div className="list">
          {users.map((item) => (
            <button
              key={item.user_id}
              className={`list-row ${selectedUser?.user_id === item.user_id ? "selected" : ""}`}
              onClick={() => setSelectedId(item.user_id)}
            >
              <span>
                <strong>{item.username}</strong>
                <small>{item.email || "email не задан"}</small>
              </span>
              {item.is_root ? <Badge tone="ok">root</Badge> : <Badge tone="muted">{item.permissions.length}</Badge>}
            </button>
          ))}
        </div>
      </div>
      <div className="panel span-2">
        <div className="panel-head">
          <div>
            <h2>{selectedUser?.username || "Пользователь"}</h2>
            <p>{selectedUser ? `Создан: ${formatDate(selectedUser.created_at)}` : "Нет пользователей"}</p>
          </div>
        </div>
        {selectedUser ? (
          <div className="permission-grid">
            {permissions.map((permission) => {
              const checked = selectedUser.is_root || selectedUser.permissions.includes(permission.key);
              return (
                <label key={permission.key} className="permission-row">
                  <input
                    type="checkbox"
                    checked={checked}
                    disabled={selectedUser.is_root}
                    onChange={(event) => onPermissionChange(selectedUser, permission.key, event.target.checked)}
                  />
                  <span>
                    <strong>{permission.key}</strong>
                    <small>{permission.description}</small>
                  </span>
                </label>
              );
            })}
          </div>
        ) : null}
      </div>
    </section>
  );
}

function PluginsView({
  plugins,
  pendingKeys,
  onToggle,
  onReload
}: {
  plugins: PluginInfo[];
  pendingKeys: ReadonlySet<string>;
  onToggle: (plugin: PluginInfo, enabled: boolean) => void;
  onReload: () => void;
}) {
  return (
    <section className="panel">
      <div className="panel-head">
        <div>
          <h2>Плагины</h2>
          <p>{plugins.length} модулей MCP</p>
        </div>
        <button className="secondary-button" onClick={onReload} disabled={pendingKeys.has("plugins:reload")}>
          <RefreshCw size={16} className={pendingKeys.has("plugins:reload") ? "spin" : ""} />
          Перезагрузить
        </button>
      </div>
      <div className="table">
        {plugins.map((plugin) => (
          <div className="table-row" key={plugin.key}>
            <div>
              <strong>{plugin.name}</strong>
              <small>{plugin.description}</small>
            </div>
            <Badge tone={plugin.available ? "ok" : "warn"}>{plugin.available ? "доступен" : "ограничен"}</Badge>
            <span>{plugin.tool_keys.length} инструментов</span>
            <Toggle
              checked={plugin.enabled}
              busy={pendingKeys.has(`plugin:${plugin.key}`)}
              onChange={(value) => onToggle(plugin, value)}
              label={`Плагин ${plugin.name}`}
            />
          </div>
        ))}
      </div>
    </section>
  );
}

function ToolsView({
  tools,
  pendingKeys,
  onToggle
}: {
  tools: ToolInfo[];
  pendingKeys: ReadonlySet<string>;
  onToggle: (tool: ToolInfo, enabled: boolean) => void;
}) {
  const [query, setQuery] = useState("");
  const [mode, setMode] = useState<"all" | "read" | "write">("all");
  const filtered = tools.filter((tool) => {
    const text = `${tool.key} ${tool.name} ${tool.plugin_key}`.toLowerCase();
    const matchesQuery = text.includes(query.toLowerCase());
    const matchesMode = mode === "all" || (mode === "read" ? tool.read_only : !tool.read_only);
    return matchesQuery && matchesMode;
  });

  return (
    <section className="panel">
      <div className="panel-head">
        <div>
          <h2>MCP-инструменты</h2>
          <p>{filtered.length} из {tools.length}</p>
        </div>
        <div className="toolbar">
          <input className="search" value={query} onChange={(event) => setQuery(event.target.value)} placeholder="Поиск" />
          <div className="segmented">
            {(["all", "read", "write"] as const).map((item) => (
              <button key={item} className={mode === item ? "active" : ""} onClick={() => setMode(item)}>
                {item === "all" ? "Все" : item === "read" ? "Чтение" : "Запись"}
              </button>
            ))}
          </div>
        </div>
      </div>
      <div className="table">
        {filtered.map((tool) => (
          <div className="table-row table-row-tools" key={tool.key}>
            <div>
              <strong>{tool.name}</strong>
              <small>{tool.key}</small>
            </div>
            <Badge tone={tool.read_only ? "ok" : "warn"}>{tool.read_only ? "чтение" : "запись"}</Badge>
            <Badge tone={tool.available ? "ok" : "danger"}>{tool.available ? tool.plugin_key : "недоступен"}</Badge>
            <Toggle
              checked={tool.global_enabled}
              busy={pendingKeys.has(`tool:${tool.key}`)}
              onChange={(value) => onToggle(tool, value)}
              label={`Инструмент ${tool.name}`}
            />
          </div>
        ))}
      </div>
    </section>
  );
}

function formatAuditEvent(event: AuditEvent, plugins: PluginInfo[], tools: ToolInfo[]): { title: string; detail: string } {
  const targetPluginKey = textValue(event.target.plugin_key);
  const targetToolKey = textValue(event.target.tool_key);
  const plugin = targetPluginKey ? plugins.find((item) => item.key === targetPluginKey) : null;
  const tool = targetToolKey ? tools.find((item) => item.key === targetToolKey) : null;
  const pluginName = textValue(event.metadata.plugin_name) || plugin?.name || targetPluginKey || "плагин";
  const toolName = textValue(event.metadata.tool_name) || tool?.name || targetToolKey || "инструмент";

  switch (event.event_type) {
    case "mcp.plugin.update":
      return {
        title: `Плагин «${pluginName}» ${enabledText(event.metadata.enabled)}`,
        detail: event.metadata.changed === false ? "Состояние уже было таким" : "Состояние плагина обновлено"
      };
    case "mcp.tool.global.update":
      return {
        title: `Инструмент «${toolName}» ${enabledText(event.metadata.enabled)}`,
        detail: `Глобальное состояние инструмента обновлено${textValue(event.metadata.plugin_key) ? ` · ${event.metadata.plugin_key}` : ""}`
      };
    case "mcp.plugins.reload":
      return { title: "Плагины MCP перезагружены", detail: "Реестр плагинов перечитан сервером" };
    case "mcp.tool.call":
      return { title: `Инструмент «${toolName}» вызван`, detail: targetToolKey || "MCP-вызов" };
    case "settings.registration.update":
      return { title: `Регистрация ${enabledText(event.metadata.enabled, "включена", "отключена")}`, detail: "Настройка самостоятельной регистрации обновлена" };
    case "settings.mcp.update":
      return { title: `MCP ${enabledText(event.metadata.enabled)}`, detail: "Глобальная настройка MCP обновлена" };
    case "settings.redis.update":
      return { title: `Redis во время работы ${enabledText(event.metadata.enabled)}`, detail: "Настройка Redis во время работы обновлена" };
    case "system.update":
      return { title: "Обновление приложения запущено", detail: `Скрипт update.sh завершился: ${formatResult(event.result)}` };
    case "system.restart":
      return { title: "Перезапуск приложения запланирован", detail: `Скрипт restart.sh завершился: ${formatResult(event.result)}` };
    case "users.permission.mutate":
      return { title: "Права пользователя обновлены", detail: textValue(event.target.user_id) || event.event_type };
    case "account.profile.update":
      return { title: "Профиль обновлён", detail: textValue(event.target.user_id) || event.event_type };
    case "account.2fa.setup":
      return { title: "Настройка 2FA начата", detail: textValue(event.target.user_id) || event.event_type };
    case "account.2fa.enable":
      return { title: "2FA включена", detail: textValue(event.target.user_id) || event.event_type };
    case "account.2fa.disable":
      return { title: "2FA отключена", detail: textValue(event.target.user_id) || event.event_type };
    case "auth.login":
      return { title: "Вход выполнен", detail: textValue(event.target.user_id) || event.event_type };
    case "auth.logout":
      return { title: "Выход выполнен", detail: textValue(event.target.user_id) || event.event_type };
    case "auth.login.failed":
      return { title: "Неудачная попытка входа", detail: textValue(event.target.username) || event.event_type };
    case "auth.login.2fa_required":
      return { title: "Запрошен код 2FA", detail: textValue(event.target.user_id) || event.event_type };
    case "auth.login.2fa_failed":
      return { title: "Ошибка проверки 2FA", detail: textValue(event.target.user_id) || event.event_type };
    case "oauth.client.create":
      return { title: "OAuth-клиент создан", detail: textValue(event.target.client_id) || event.event_type };
    case "oauth.client.dynamic_register":
      return { title: "OAuth-клиент зарегистрирован динамически", detail: textValue(event.target.client_id) || event.event_type };
    case "oauth.authorize":
      return { title: "OAuth-авторизация создана", detail: textValue(event.target.client_id) || event.event_type };
    case "oauth.token.issue":
      return { title: "OAuth-токен выпущен", detail: textValue(event.target.client_id) || event.event_type };
    default:
      return { title: "Событие аудита", detail: `Код события: ${event.event_type}` };
  }
}

function AuditView({ events, plugins, tools }: { events: AuditEvent[]; plugins: PluginInfo[]; tools: ToolInfo[] }) {
  return (
    <section className="panel">
      <h2>Аудит</h2>
      <div className="timeline">
        {events.map((event) => {
          const formatted = formatAuditEvent(event, plugins, tools);
          return (
            <div className="timeline-row" key={event.event_id}>
              <span className="timeline-dot" />
              <div className="timeline-content">
                <strong className="timeline-title">{formatted.title}</strong>
                <small className="timeline-meta">{formatDate(event.created_at)} · {event.actor_username || "система"} · {formatResult(event.result)}</small>
                <small className="timeline-detail">{formatted.detail}</small>
              </div>
            </div>
          );
        })}
      </div>
    </section>
  );
}

function ProfileView({
  user,
  onSave,
  onUserUpdate
}: {
  user: User;
  onSave: (payload: { email: string | null; tg_id: string | null; vk_id: string | null }) => void;
  onUserUpdate: (user: User) => void;
}) {
  const [email, setEmail] = useState(user.email || "");
  const [tgId, setTgId] = useState(user.tg_id || "");
  const [vkId, setVkId] = useState(user.vk_id || "");
  const [currentPassword, setCurrentPassword] = useState("");
  const [twoFactorCode, setTwoFactorCode] = useState("");
  const [setup, setSetup] = useState<TwoFactorSetup | null>(null);
  const [recoveryCodes, setRecoveryCodes] = useState<string[]>([]);
  const [twoFactorMessage, setTwoFactorMessage] = useState<string | null>(null);
  const [twoFactorError, setTwoFactorError] = useState<string | null>(null);
  const [twoFactorBusy, setTwoFactorBusy] = useState(false);

  useEffect(() => {
    setEmail(user.email || "");
    setTgId(user.tg_id || "");
    setVkId(user.vk_id || "");
  }, [user]);

  async function runTwoFactor(action: () => Promise<void>) {
    setTwoFactorBusy(true);
    setTwoFactorError(null);
    setTwoFactorMessage(null);
    try {
      await action();
    } catch (exc) {
      setTwoFactorError(exc instanceof Error ? exc.message : "Не удалось обновить 2FA");
    } finally {
      setTwoFactorBusy(false);
    }
  }

  return (
    <section className="profile-grid">
      <div className="panel narrow">
        <h2>Профиль</h2>
        <div className="form-grid">
          <label>
            Email
            <input value={email} onChange={(event) => setEmail(event.target.value)} />
          </label>
          <label>
            Telegram ID
            <input value={tgId} onChange={(event) => setTgId(event.target.value)} />
          </label>
          <label>
            VK ID
            <input value={vkId} onChange={(event) => setVkId(event.target.value)} />
          </label>
          <button className="primary-button" onClick={() => onSave({ email: email || null, tg_id: tgId || null, vk_id: vkId || null })}>
            <Save size={16} />
            Сохранить
          </button>
        </div>
      </div>
      <div className="panel narrow">
        <div className="panel-head">
          <div>
            <h2>Двухэтапная аутентификация</h2>
            <p>{user.two_factor_enabled ? "Включена для входа и MCP OAuth" : "Защитите вход и подключение MCP-клиентов"}</p>
          </div>
          <Badge tone={user.two_factor_enabled ? "ok" : "warn"}>{user.two_factor_enabled ? "ВКЛ" : "ВЫКЛ"}</Badge>
        </div>
        <ErrorBanner message={twoFactorError} />
        {twoFactorMessage ? <div className="notice notice-ok">{twoFactorMessage}</div> : null}
        {recoveryCodes.length ? (
          <div className="recovery-grid">
            {recoveryCodes.map((item) => <code key={item}>{item}</code>)}
          </div>
        ) : null}
        {user.two_factor_enabled ? (
          <div className="form-grid">
            <div className="security-note">
              <KeyRound size={18} />
              <span>MCP-подключение через OAuth будет дополнительно спрашивать код аутентификатора.</span>
            </div>
            <label>
              Код 2FA или резервный код
              <input value={twoFactorCode} onChange={(event) => setTwoFactorCode(event.target.value)} inputMode="numeric" autoComplete="one-time-code" />
            </label>
            <button
              className="secondary-button danger-button"
              disabled={twoFactorBusy || !twoFactorCode}
              onClick={() => runTwoFactor(async () => {
                const updated = await api.twoFactorDisable(twoFactorCode);
                onUserUpdate(updated);
                setTwoFactorCode("");
                setSetup(null);
                setRecoveryCodes([]);
                setTwoFactorMessage("2FA отключена");
              })}
            >
              Отключить 2FA
            </button>
          </div>
        ) : (
          <div className="form-grid">
            <label>
              Текущий пароль
              <input value={currentPassword} onChange={(event) => setCurrentPassword(event.target.value)} type="password" autoComplete="current-password" />
            </label>
            <button
              className="secondary-button"
              disabled={twoFactorBusy || !currentPassword}
              onClick={() => runTwoFactor(async () => {
                const nextSetup = await api.twoFactorSetup(currentPassword);
                setSetup(nextSetup);
                setTwoFactorCode("");
                setRecoveryCodes([]);
                setTwoFactorMessage("Отсканируйте QR-код и подтвердите одноразовый код");
              })}
            >
              <QrCode size={16} />
              Создать QR-код
            </button>
            {setup ? (
              <div className="two-factor-setup">
                <img alt="QR-код для 2FA" src={`data:image/svg+xml;utf8,${encodeURIComponent(setup.qr_svg)}`} />
                <div>
                  <small>Ключ для ручного ввода</small>
                  <code>{setup.secret}</code>
                </div>
                <label>
                  Код из приложения
                  <input value={twoFactorCode} onChange={(event) => setTwoFactorCode(event.target.value)} inputMode="numeric" autoComplete="one-time-code" />
                </label>
                <button
                  className="primary-button"
                  disabled={twoFactorBusy || !twoFactorCode}
                  onClick={() => runTwoFactor(async () => {
                    const result = await api.twoFactorEnable(twoFactorCode);
                    onUserUpdate(result.user);
                    setRecoveryCodes(result.recovery_codes);
                    setTwoFactorCode("");
                    setCurrentPassword("");
                    setSetup(null);
                    setTwoFactorMessage("2FA включена. Сохраните резервные коды.");
                  })}
                >
                  Включить 2FA
                </button>
              </div>
            ) : null}
          </div>
        )}
      </div>
    </section>
  );
}

export function App() {
  const [bootstrap, setBootstrap] = useState<Bootstrap | null>(null);
  const [user, setUser] = useState<User | null>(null);
  const [health, setHealth] = useState<Health | null>(null);
  const [runtime, setRuntime] = useState<RuntimeSettings | null>(null);
  const [users, setUsers] = useState<User[]>([]);
  const [permissions, setPermissions] = useState<Permission[]>([]);
  const [plugins, setPlugins] = useState<PluginInfo[]>([]);
  const [tools, setTools] = useState<ToolInfo[]>([]);
  const [events, setEvents] = useState<AuditEvent[]>([]);
  const [view, setView] = useState<View>("overview");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [toasts, setToasts] = useState<Toast[]>([]);
  const [confirmation, setConfirmation] = useState<Confirmation | null>(null);
  const [updateLog, setUpdateLog] = useState<UpdateLogState>({ open: false, running: false, result: null, error: null });
  const [pendingActions, setPendingActions] = useState<Set<string>>(new Set());
  const pendingActionsRef = useRef<Set<string>>(new Set());

  const title = useMemo(() => navItems.find((item) => item.view === view)?.label || "Обзор", [view]);

  const dismissToast = useCallback((id: string) => {
    setToasts((items) => items.filter((item) => item.id !== id));
  }, []);

  const pushToast = useCallback((tone: ToastTone, title: string, message?: string) => {
    const id = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
    setToasts((items) => [...items, { id, tone, title, message }].slice(-5));
  }, []);

  function setActionPending(key: string, pending: boolean) {
    const next = new Set(pendingActionsRef.current);
    if (pending) {
      next.add(key);
    } else {
      next.delete(key);
    }
    pendingActionsRef.current = next;
    setPendingActions(next);
  }

  async function loadBootstrap() {
    setLoading(true);
    setError(null);
    try {
      const data = await api.bootstrap();
      setCsrfCookieName(data.csrf_cookie_name);
      setBootstrap(data);
      setUser(data.user);
      setRuntime(data.runtime);
      if (data.user) {
        await loadAll();
      }
    } catch (exc) {
      const message = exc instanceof Error ? exc.message : "Не удалось загрузить bootstrap";
      setError(message);
      pushToast("error", "Ошибка загрузки", message);
    } finally {
      setLoading(false);
    }
  }

  async function loadAll() {
    const [nextHealth, nextRuntime, nextUsers, nextPermissions, nextPlugins, nextTools, nextAudit] = await Promise.all([
      api.health(),
      api.runtime(),
      api.users(),
      api.permissions(),
      api.plugins(),
      api.tools(),
      api.audit()
    ]);
    setHealth(nextHealth);
    setRuntime(nextRuntime);
    setUsers(nextUsers);
    setPermissions(nextPermissions);
    setPlugins(nextPlugins);
    setTools(nextTools);
    setEvents(nextAudit.items);
  }

  useEffect(() => {
    void loadBootstrap();
  }, []);

  async function runAction(action: () => Promise<void>, options: { pendingKey?: string; errorTitle?: string } = {}) {
    const { pendingKey, errorTitle = "Операция не выполнена" } = options;
    if (pendingKey && pendingActionsRef.current.has(pendingKey)) {
      return;
    }
    setError(null);
    if (pendingKey) {
      setActionPending(pendingKey, true);
    }
    try {
      await action();
    } catch (exc) {
      const message = exc instanceof ApiError || exc instanceof Error ? exc.message : "Операция не выполнена";
      setError(message);
      pushToast("error", errorTitle, message);
    } finally {
      if (pendingKey) {
        setActionPending(pendingKey, false);
      }
    }
  }

  function requestUpdate() {
    setConfirmation({
      title: "Обновить приложение?",
      message: "Будет выполнен git fetch, git pull, установка зависимостей, сборка фронтенда и перезапуск сервиса.",
      confirmLabel: "Обновить",
      onConfirm: () => void runUpdateWithLog()
    });
  }

  async function runUpdateWithLog() {
    if (pendingActionsRef.current.has("system:update")) {
      return;
    }
    setError(null);
    setActionPending("system:update", true);
    setUpdateLog({ open: true, running: true, result: null, error: null });
    try {
      const result = await api.runSystemUpdate();
      setUpdateLog({ open: true, running: false, result, error: null });
      const detail = result.stdout.trim().split("\n").slice(-2).join(" · ") || result.stderr.trim().split("\n").slice(-2).join(" · ");
      if (result.returncode === 0) {
        pushToast("success", "Обновление завершено", detail || `Код выхода: ${result.returncode}`);
        void loadAll();
      } else {
        const message = detail || `Код выхода: ${result.returncode}`;
        setError(message);
        pushToast("error", "Обновление завершилось с ошибкой", message);
      }
    } catch (exc) {
      const message = exc instanceof ApiError || exc instanceof Error ? exc.message : "Не удалось обновить приложение";
      setError(message);
      setUpdateLog({ open: true, running: false, result: null, error: message });
      pushToast("error", "Не удалось обновить приложение", message);
    } finally {
      setActionPending("system:update", false);
    }
  }

  function requestRestart() {
    setConfirmation({
      title: "Перезапустить приложение?",
      message: "Сервис ASFES Multiplex будет перезапущен через systemctl. Интерфейс может быть недоступен несколько секунд.",
      confirmLabel: "Перезапустить",
      tone: "danger",
      onConfirm: () =>
        void runAction(async () => {
          const result = await api.runSystemRestart();
          const detail = result.stdout.trim().split("\n").slice(-2).join(" · ");
          pushToast("success", "Перезапуск запланирован", detail || `Код выхода: ${result.returncode}`);
        }, { pendingKey: "system:restart", errorTitle: "Не удалось перезапустить приложение" })
    });
  }

  if (loading && !bootstrap) {
    return <div className="loading">Загрузка ASFES Multiplex...</div>;
  }

  if (!user) {
    return (
      <>
        <ToastViewport toasts={toasts} onDismiss={dismissToast} />
        <ConfirmDialog confirmation={confirmation} onCancel={() => setConfirmation(null)} />
        <UpdateLogDialog state={updateLog} onClose={() => setUpdateLog((current) => ({ ...current, open: false }))} />
        <LoginView onLogin={(nextUser) => {
          setUser(nextUser);
          void loadAll();
        }} />
      </>
    );
  }

  return (
    <div className="app-shell">
      <ToastViewport toasts={toasts} onDismiss={dismissToast} />
      <ConfirmDialog confirmation={confirmation} onCancel={() => setConfirmation(null)} />
      <UpdateLogDialog state={updateLog} onClose={() => setUpdateLog((current) => ({ ...current, open: false }))} />
      <aside className="sidebar">
        <div className="brand">
          <Shield size={24} />
          <div>
            <strong>{bootstrap?.app_name || "ASFES Multiplex"}</strong>
            <small>{bootstrap?.app_version || ""}</small>
          </div>
        </div>
        <nav>
          {navItems.map((item) => (
            <button key={item.view} className={view === item.view ? "active" : ""} onClick={() => setView(item.view)}>
              {item.icon}
              {item.label}
            </button>
          ))}
        </nav>
        <button
          className="logout"
          onClick={() => runAction(async () => {
            await api.logout();
            setUser(null);
            pushToast("success", "Выход выполнен");
          })}
        >
          <LogOut size={18} />
          Выйти
        </button>
      </aside>
      <main className="workspace">
        <header className="topbar">
          <div>
            <h1>{title}</h1>
            <p>{user.username} · {user.is_root ? "root" : `${user.permissions.length} прав`}</p>
          </div>
          <div className="status-strip">
            <Database size={18} />
            <span>{health?.mongodb || "mongo"}</span>
            <SlidersHorizontal size={18} />
            <span>{runtime?.mcp_enabled ? "MCP включён" : "MCP отключён"}</span>
          </div>
        </header>
        <ErrorBanner message={error} />
        {view === "overview" ? (
          <OverviewView
            health={health}
            runtime={runtime}
            pendingKeys={pendingActions}
            onRefresh={() => runAction(loadAll, { pendingKey: "app:refresh", errorTitle: "Не удалось обновить данные" })}
            onToggleRuntime={(key, value) =>
              runAction(async () => {
                const nextRuntime =
                  key === "registration_enabled" ? await api.setRegistration(value) : key === "mcp_enabled" ? await api.setMcp(value) : await api.setRedis(value);
                setRuntime(nextRuntime);
                await loadAll();
                pushToast("success", `Настройка «${runtimeLabels[key]}» ${value ? "включена" : "отключена"}`);
              }, { pendingKey: `runtime:${key}`, errorTitle: "Не удалось переключить настройку" })
            }
            onConfirmUpdate={requestUpdate}
            onRunRestart={requestRestart}
          />
        ) : null}
        {view === "users" ? (
          <UsersView
            users={users}
            permissions={permissions}
            onPermissionChange={(targetUser, permission, enabled) =>
              runAction(async () => {
                const updated = await api.mutatePermissions(targetUser.user_id, [permission], enabled ? "grant" : "revoke");
                setUsers((items) => items.map((item) => (item.user_id === updated.user_id ? updated : item)));
                await loadAll();
                pushToast("success", `Право «${permission}» ${enabled ? "выдано" : "отозвано"}`);
              }, { pendingKey: `permission:${targetUser.user_id}:${permission}`, errorTitle: "Не удалось обновить права" })
            }
          />
        ) : null}
        {view === "plugins" ? (
          <PluginsView
            plugins={plugins}
            pendingKeys={pendingActions}
            onReload={() => runAction(async () => {
              const result = await api.reloadPlugins();
              await loadAll();
              pushToast("success", "Плагины перезагружены", `Обновлено: ${result.reloaded.length}`);
            }, { pendingKey: "plugins:reload", errorTitle: "Не удалось перезагрузить плагины" })}
            onToggle={(plugin, enabled) =>
              runAction(async () => {
                const updated = await api.togglePlugin(plugin.key, enabled);
                setPlugins((items) => items.map((item) => (item.key === updated.key ? updated : item)));
                await loadAll();
                pushToast("success", `Плагин «${updated.name}» ${enabled ? "включён" : "отключён"}`);
              }, { pendingKey: `plugin:${plugin.key}`, errorTitle: "Не удалось переключить плагин" })
            }
          />
        ) : null}
        {view === "tools" ? (
          <ToolsView
            tools={tools}
            pendingKeys={pendingActions}
            onToggle={(tool, enabled) =>
              runAction(async () => {
                const updated = await api.toggleTool(tool.key, enabled);
                setTools((items) => items.map((item) => (item.key === updated.key ? updated : item)));
                await loadAll();
                pushToast("success", `Инструмент «${updated.name}» ${enabled ? "включён" : "отключён"}`);
              }, { pendingKey: `tool:${tool.key}`, errorTitle: "Не удалось переключить инструмент" })
            }
          />
        ) : null}
        {view === "audit" ? <AuditView events={events} plugins={plugins} tools={tools} /> : null}
        {view === "profile" ? (
          <ProfileView
            user={user}
            onUserUpdate={setUser}
            onSave={(payload) =>
              runAction(async () => {
                const updated = await api.profile(payload);
                setUser(updated);
                await loadAll();
                pushToast("success", "Профиль сохранён");
              }, { pendingKey: "profile:save", errorTitle: "Не удалось сохранить профиль" })
            }
          />
        ) : null}
      </main>
    </div>
  );
}
