import {
  Activity,
  Database,
  LogOut,
  Plug,
  RefreshCw,
  Save,
  ScrollText,
  Shield,
  SlidersHorizontal,
  UserCircle,
  Users,
  Wrench
} from "lucide-react";
import { FormEvent, ReactNode, useEffect, useMemo, useState } from "react";
import { ApiError, AuditEvent, Bootstrap, Health, Permission, PluginInfo, RuntimeSettings, ToolInfo, User, api, setCsrfCookieName } from "./api";

type View = "overview" | "users" | "plugins" | "tools" | "audit" | "profile";

const navItems: Array<{ view: View; label: string; icon: ReactNode }> = [
  { view: "overview", label: "Обзор", icon: <Activity size={18} /> },
  { view: "users", label: "Пользователи", icon: <Users size={18} /> },
  { view: "plugins", label: "Плагины", icon: <Plug size={18} /> },
  { view: "tools", label: "Tools", icon: <Wrench size={18} /> },
  { view: "audit", label: "Аудит", icon: <ScrollText size={18} /> },
  { view: "profile", label: "Профиль", icon: <UserCircle size={18} /> }
];

function formatDate(value: string): string {
  return new Intl.DateTimeFormat("ru-RU", {
    dateStyle: "short",
    timeStyle: "short"
  }).format(new Date(value));
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

function Toggle({
  checked,
  disabled,
  onChange,
  label
}: {
  checked: boolean;
  disabled?: boolean;
  onChange: (checked: boolean) => void;
  label: string;
}) {
  return (
    <label className="switch" title={label}>
      <input type="checkbox" checked={checked} disabled={disabled} onChange={(event) => onChange(event.target.checked)} />
      <span />
    </label>
  );
}

function LoginView({ onLogin }: { onLogin: (user: User) => void }) {
  const [username, setUsername] = useState("root");
  const [password, setPassword] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function submit(event: FormEvent) {
    event.preventDefault();
    setBusy(true);
    setError(null);
    try {
      const result = await api.login(username, password);
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
            <p>Домашний control plane</p>
          </div>
        </div>
        <ErrorBanner message={error} />
        <form onSubmit={submit} className="form-grid">
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
          <button className="primary-button" type="submit" disabled={busy || !username || !password}>
            {busy ? "Вход..." : "Войти"}
          </button>
        </form>
      </section>
    </main>
  );
}

function OverviewView({
  health,
  runtime,
  onToggleRuntime,
  onRefresh
}: {
  health: Health | null;
  runtime: RuntimeSettings | null;
  onToggleRuntime: (key: "registration_enabled" | "mcp_enabled" | "redis_runtime_enabled", value: boolean) => void;
  onRefresh: () => void;
}) {
  return (
    <section className="page-grid">
      <div className="panel span-2">
        <div className="panel-head">
          <div>
            <h2>Состояние сервиса</h2>
            <p>MongoDB, Redis и MCP runtime</p>
          </div>
          <button className="icon-button" onClick={onRefresh} title="Обновить">
            <RefreshCw size={18} />
          </button>
        </div>
        <div className="metrics-grid">
          <div className="metric">
            <span>API</span>
            <strong>{health?.status || "unknown"}</strong>
            <Badge tone={health?.status === "ok" ? "ok" : "warn"}>{health?.status === "ok" ? "OK" : "DEGRADED"}</Badge>
          </div>
          <div className="metric">
            <span>MongoDB</span>
            <strong>{health?.mongodb || "unknown"}</strong>
            <Badge tone={health?.mongodb === "ok" ? "ok" : "danger"}>{health?.mongodb || "unknown"}</Badge>
          </div>
          <div className="metric">
            <span>Redis</span>
            <strong>{health?.redis || runtime?.redis_mode || "unknown"}</strong>
            <Badge tone={health?.redis === "enabled" ? "ok" : "muted"}>{health?.redis || "disabled"}</Badge>
          </div>
          <div className="metric">
            <span>MCP</span>
            <strong>{runtime?.mcp_enabled ? "enabled" : "disabled"}</strong>
            <Badge tone={runtime?.mcp_enabled ? "ok" : "warn"}>{runtime?.mcp_enabled ? "ON" : "OFF"}</Badge>
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
            <Toggle checked={Boolean(runtime?.registration_enabled)} onChange={(value) => onToggleRuntime("registration_enabled", value)} label="Регистрация" />
          </div>
          <div className="setting-row">
            <div>
              <strong>MCP</strong>
              <span>Доступ клиентов к MCP tools</span>
            </div>
            <Toggle checked={Boolean(runtime?.mcp_enabled)} onChange={(value) => onToggleRuntime("mcp_enabled", value)} label="MCP" />
          </div>
          <div className="setting-row">
            <div>
              <strong>Redis runtime</strong>
              <span>Rate limit через Redis</span>
            </div>
            <Toggle checked={Boolean(runtime?.redis_runtime_enabled)} onChange={(value) => onToggleRuntime("redis_runtime_enabled", value)} label="Redis runtime" />
          </div>
        </div>
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
          {users.map((user) => (
            <button
              key={user.user_id}
              className={`list-row ${selectedUser?.user_id === user.user_id ? "selected" : ""}`}
              onClick={() => setSelectedId(user.user_id)}
            >
              <span>
                <strong>{user.username}</strong>
                <small>{user.email || "email не задан"}</small>
              </span>
              {user.is_root ? <Badge tone="ok">root</Badge> : <Badge tone="muted">{user.permissions.length}</Badge>}
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
  onToggle,
  onReload
}: {
  plugins: PluginInfo[];
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
        <button className="secondary-button" onClick={onReload}>
          <RefreshCw size={16} />
          Reload
        </button>
      </div>
      <div className="table">
        {plugins.map((plugin) => (
          <div className="table-row" key={plugin.key}>
            <div>
              <strong>{plugin.name}</strong>
              <small>{plugin.description}</small>
            </div>
            <Badge tone={plugin.available ? "ok" : "warn"}>{plugin.available ? "available" : "limited"}</Badge>
            <span>{plugin.tool_keys.length} tools</span>
            <Toggle checked={plugin.enabled} onChange={(value) => onToggle(plugin, value)} label={`Плагин ${plugin.name}`} />
          </div>
        ))}
      </div>
    </section>
  );
}

function ToolsView({ tools, onToggle }: { tools: ToolInfo[]; onToggle: (tool: ToolInfo, enabled: boolean) => void }) {
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
          <h2>MCP tools</h2>
          <p>{filtered.length} из {tools.length}</p>
        </div>
        <div className="toolbar">
          <input className="search" value={query} onChange={(event) => setQuery(event.target.value)} placeholder="Поиск" />
          <div className="segmented">
            {(["all", "read", "write"] as const).map((item) => (
              <button key={item} className={mode === item ? "active" : ""} onClick={() => setMode(item)}>
                {item === "all" ? "Все" : item === "read" ? "Read" : "Write"}
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
            <Badge tone={tool.read_only ? "ok" : "warn"}>{tool.read_only ? "read" : "write"}</Badge>
            <Badge tone={tool.available ? "ok" : "danger"}>{tool.available ? tool.plugin_key : "unavailable"}</Badge>
            <Toggle checked={tool.global_enabled} onChange={(value) => onToggle(tool, value)} label={`Tool ${tool.name}`} />
          </div>
        ))}
      </div>
    </section>
  );
}

function AuditView({ events }: { events: AuditEvent[] }) {
  return (
    <section className="panel">
      <h2>Аудит</h2>
      <div className="timeline">
        {events.map((event) => (
          <div className="timeline-row" key={event.event_id}>
            <span />
            <div>
              <strong>{event.event_type}</strong>
              <small>{formatDate(event.created_at)} · {event.actor_username || "system"} · {event.result}</small>
            </div>
          </div>
        ))}
      </div>
    </section>
  );
}

function ProfileView({ user, onSave }: { user: User; onSave: (payload: { email: string | null; tg_id: string | null; vk_id: string | null }) => void }) {
  const [email, setEmail] = useState(user.email || "");
  const [tgId, setTgId] = useState(user.tg_id || "");
  const [vkId, setVkId] = useState(user.vk_id || "");

  useEffect(() => {
    setEmail(user.email || "");
    setTgId(user.tg_id || "");
    setVkId(user.vk_id || "");
  }, [user]);

  return (
    <section className="panel narrow">
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

  const title = useMemo(() => navItems.find((item) => item.view === view)?.label || "Обзор", [view]);

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
      setError(exc instanceof Error ? exc.message : "Не удалось загрузить bootstrap");
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

  async function runAction(action: () => Promise<void>) {
    setError(null);
    try {
      await action();
    } catch (exc) {
      const message = exc instanceof ApiError || exc instanceof Error ? exc.message : "Операция не выполнена";
      setError(message);
    }
  }

  if (loading && !bootstrap) {
    return <div className="loading">Загрузка ASFES Multiplex...</div>;
  }

  if (!user) {
    return <LoginView onLogin={(nextUser) => {
      setUser(nextUser);
      void loadAll();
    }} />;
  }

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="brand">
          <Shield size={24} />
          <div>
            <strong>{bootstrap?.app_name || "ASFES Multiplex"}</strong>
            <small>{bootstrap?.app_version || "0.1.0"}</small>
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
            <p>{user.username} · {user.is_root ? "root" : `${user.permissions.length} permissions`}</p>
          </div>
          <div className="status-strip">
            <Database size={18} />
            <span>{health?.mongodb || "mongo"}</span>
            <SlidersHorizontal size={18} />
            <span>{runtime?.mcp_enabled ? "MCP on" : "MCP off"}</span>
          </div>
        </header>
        <ErrorBanner message={error} />
        {view === "overview" ? (
          <OverviewView
            health={health}
            runtime={runtime}
            onRefresh={() => runAction(loadAll)}
            onToggleRuntime={(key, value) =>
              runAction(async () => {
                const nextRuntime =
                  key === "registration_enabled" ? await api.setRegistration(value) : key === "mcp_enabled" ? await api.setMcp(value) : await api.setRedis(value);
                setRuntime(nextRuntime);
                await loadAll();
              })
            }
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
              })
            }
          />
        ) : null}
        {view === "plugins" ? (
          <PluginsView
            plugins={plugins}
            onReload={() => runAction(async () => {
              await api.reloadPlugins();
              await loadAll();
            })}
            onToggle={(plugin, enabled) =>
              runAction(async () => {
                const updated = await api.togglePlugin(plugin.key, enabled);
                setPlugins((items) => items.map((item) => (item.key === updated.key ? updated : item)));
                await loadAll();
              })
            }
          />
        ) : null}
        {view === "tools" ? (
          <ToolsView
            tools={tools}
            onToggle={(tool, enabled) =>
              runAction(async () => {
                const updated = await api.toggleTool(tool.key, enabled);
                setTools((items) => items.map((item) => (item.key === updated.key ? updated : item)));
              })
            }
          />
        ) : null}
        {view === "audit" ? <AuditView events={events} /> : null}
        {view === "profile" ? (
          <ProfileView
            user={user}
            onSave={(payload) =>
              runAction(async () => {
                const updated = await api.profile(payload);
                setUser(updated);
              })
            }
          />
        ) : null}
      </main>
    </div>
  );
}
