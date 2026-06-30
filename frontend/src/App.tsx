import {
  Activity,
  Database,
  Fingerprint,
  KeyRound,
  Loader2,
  LogOut,
  MoreHorizontal,
  Plug,
  Power,
  QrCode,
  RefreshCw,
  Save,
  ScrollText,
  Shield,
  SlidersHorizontal,
  Trash2,
  UserCircle,
  Users,
  Wrench,
  X,
  Copy,
  Check,
  Globe,
  Plus,
  Download,
  Upload,
  Play
} from "lucide-react";
import { FormEvent, ReactNode, useCallback, useEffect, useMemo, useRef, useState } from "react";
import { ApiError, AuditEvent, Bootstrap, Health, MCPConnectedService, Passkey, Permission, PluginInfo, RuntimeSettings, SystemUpdateResult, SystemUpdateSession, SystemUpdateStage, ToolInfo, TwoFactorSetup, User, ApiKey, ApiKeyCreateResult, api, setCsrfCookieName, Proxy, ProxyProtocol, ProxyTgExport } from "./api";
import { PyPIView } from "./PyPIView";

type View = "overview" | "users" | "plugins" | "tools" | "services" | "audit" | "profile" | "proxy" | "pypi";
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

type UpdateStageKey = "code" | "python" | "frontend" | "restart";

type UpdateFlowState = {
  open: boolean;
  running: boolean;
  checking: boolean;
  session: SystemUpdateSession | null;
  logs: string[];
  result: SystemUpdateResult | null;
  error: string | null;
  forceStages: Record<UpdateStageKey, boolean>;
  optionsOpen: boolean;
  escortingRestart: boolean;
  restartMessage: string | null;
};

const navItems: Array<{ view: View; label: string; icon: ReactNode }> = [
  { view: "overview", label: "Обзор", icon: <Activity size={18} /> },
  { view: "users", label: "Пользователи", icon: <Users size={18} /> },
  { view: "plugins", label: "Плагины", icon: <Plug size={18} /> },
  { view: "tools", label: "Инструменты", icon: <Wrench size={18} /> },
  { view: "services", label: "Подключения", icon: <KeyRound size={18} /> },
  { view: "audit", label: "Аудит", icon: <ScrollText size={18} /> },
  { view: "profile", label: "Профиль", icon: <UserCircle size={18} /> },
  { view: "proxy", label: "Proxy Tools", icon: <Globe size={18} /> },
  { view: "pypi", label: "PyPI", icon: <Database size={18} /> }
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

function base64UrlToBuffer(value: string): ArrayBuffer {
  const normalized = value.replace(/-/g, "+").replace(/_/g, "/");
  const padded = normalized.padEnd(normalized.length + ((4 - (normalized.length % 4)) % 4), "=");
  const binary = window.atob(padded);
  const bytes = new Uint8Array(binary.length);
  for (let index = 0; index < binary.length; index += 1) {
    bytes[index] = binary.charCodeAt(index);
  }
  return bytes.buffer;
}

function bufferToBase64Url(buffer: ArrayBuffer): string {
  const bytes = new Uint8Array(buffer);
  let binary = "";
  for (const byte of bytes) {
    binary += String.fromCharCode(byte);
  }
  return window.btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
}

function credentialCreationOptionsFromJson(options: Record<string, unknown>): PublicKeyCredentialCreationOptions {
  type CredentialDescriptorJson = Omit<PublicKeyCredentialDescriptor, "id"> & { id: string };
  const value = options as unknown as Omit<PublicKeyCredentialCreationOptions, "challenge" | "user" | "excludeCredentials"> & {
    challenge: string;
    user: Omit<PublicKeyCredentialUserEntity, "id"> & { id: string };
    excludeCredentials?: CredentialDescriptorJson[];
  };
  return {
    ...value,
    challenge: base64UrlToBuffer(value.challenge),
    user: {
      ...value.user,
      id: base64UrlToBuffer(value.user.id)
    },
    excludeCredentials: value.excludeCredentials?.map((item) => ({
      ...item,
      id: base64UrlToBuffer(item.id)
    }))
  };
}

function credentialRequestOptionsFromJson(options: Record<string, unknown>): PublicKeyCredentialRequestOptions {
  type CredentialDescriptorJson = Omit<PublicKeyCredentialDescriptor, "id"> & { id: string };
  const value = options as unknown as Omit<PublicKeyCredentialRequestOptions, "challenge" | "allowCredentials"> & {
    challenge: string;
    allowCredentials?: CredentialDescriptorJson[];
  };
  return {
    ...value,
    challenge: base64UrlToBuffer(value.challenge),
    allowCredentials: value.allowCredentials?.map((item) => ({
      ...item,
      id: base64UrlToBuffer(item.id)
    }))
  };
}

function publicKeyCredentialToJson(credential: Credential | null): Record<string, unknown> {
  if (!credential || credential.type !== "public-key") {
    throw new Error("Passkey was not selected");
  }
  const publicKeyCredential = credential as PublicKeyCredential & { authenticatorAttachment?: string | null };
  const response = publicKeyCredential.response;
  const base = {
    id: publicKeyCredential.id,
    rawId: bufferToBase64Url(publicKeyCredential.rawId),
    type: publicKeyCredential.type,
    authenticatorAttachment: publicKeyCredential.authenticatorAttachment ?? undefined,
    clientExtensionResults: publicKeyCredential.getClientExtensionResults()
  };
  if ("attestationObject" in response) {
    const registration = response as AuthenticatorAttestationResponse & { getTransports?: () => string[] };
    return {
      ...base,
      response: {
        attestationObject: bufferToBase64Url(registration.attestationObject),
        clientDataJSON: bufferToBase64Url(registration.clientDataJSON),
        transports: registration.getTransports?.() || []
      }
    };
  }
  const authentication = response as AuthenticatorAssertionResponse;
  return {
    ...base,
    response: {
      authenticatorData: bufferToBase64Url(authentication.authenticatorData),
      clientDataJSON: bufferToBase64Url(authentication.clientDataJSON),
      signature: bufferToBase64Url(authentication.signature),
      userHandle: authentication.userHandle ? bufferToBase64Url(authentication.userHandle) : undefined
    }
  };
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

function UpdateLogDialog({ state, onClose }: { state: UpdateFlowState; onClose: () => void }) {
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

const updateStageKeys: UpdateStageKey[] = ["code", "python", "frontend", "restart"];

function stageTone(stage: SystemUpdateStage): "ok" | "warn" | "danger" | "muted" {
  if (stage.status === "success") {
    return stage.needed || stage.forced ? "ok" : "muted";
  }
  if (stage.status === "error") {
    return "danger";
  }
  if (stage.status === "running") {
    return "warn";
  }
  return "muted";
}

function forceLabel(stage: UpdateStageKey): string {
  if (stage === "code") {
    return "Принудительно обновить код";
  }
  if (stage === "python") {
    return "Принудительно обновить Python-зависимости";
  }
  if (stage === "frontend") {
    return "Принудительно собрать frontend";
  }
  return "Принудительно перезапустить сервис";
}

function UpdateControlDialog({
  state,
  onClose,
  onCheck,
  onRun,
  onForceChange,
  onToggleOptions
}: {
  state: UpdateFlowState;
  onClose: () => void;
  onCheck: () => void;
  onRun: () => void;
  onForceChange: (stage: UpdateStageKey, enabled: boolean) => void;
  onToggleOptions: () => void;
}) {
  if (!state.open) {
    return null;
  }
  const stages = state.session?.stages || [];
  const selectedStages = stages.filter((stage) => stage.needed || state.forceStages[stage.key as UpdateStageKey]);
  const canRun = !state.running && !state.checking && (selectedStages.length > 0 || Object.values(state.forceStages).some(Boolean));
  const canClose = !state.running && !state.checking && !state.escortingRestart;
  const consoleOutput = state.logs.join("\n") || (state.running || state.checking ? "Ожидаю вывод команды..." : "Вывод пуст.");
  return (
    <div className="confirm-backdrop" role="presentation">
      <section className="update-log-dialog" role="dialog" aria-modal="true" aria-labelledby="update-log-title">
        <div className="update-log-head update-control-head">
          <button type="button" className="icon-button" onClick={onToggleOptions} title="Дополнительные параметры">
            <MoreHorizontal size={18} />
          </button>
          <div>
            <h2 id="update-log-title">Обновления</h2>
            <p>{state.escortingRestart ? state.restartMessage || "Сервис загружается..." : state.running || state.checking ? "Команды выполняются, вывод появляется в реальном времени." : `Статус: ${state.session?.status || "ожидает проверки"}`}</p>
          </div>
          <button type="button" className="icon-button" onClick={onClose} disabled={!canClose} title="Закрыть">
            <X size={18} />
          </button>
        </div>
        {state.optionsOpen ? (
          <div className="update-options">
            {updateStageKeys.map((stage) => (
              <label key={stage}>
                <input
                  type="checkbox"
                  checked={state.forceStages[stage]}
                  onChange={(event) => onForceChange(stage, event.target.checked)}
                  disabled={state.running || state.checking}
                />
                <span>{forceLabel(stage)}</span>
              </label>
            ))}
          </div>
        ) : null}
        <div className="update-stage-grid">
          {stages.length ? stages.map((stage) => (
            <div className="update-stage" key={stage.key}>
              <div>
                <strong>{stage.title}</strong>
                <span>{stage.detail || (stage.status === "pending" ? "Ожидает проверки" : stage.status)}</span>
              </div>
              <Badge tone={stageTone(stage)}>{stage.forced ? "ФОРС" : stage.needed ? "НУЖНО" : stage.status === "running" ? "ИДЁТ" : stage.status === "error" ? "ОШИБКА" : "OK"}</Badge>
            </div>
          )) : (
            <div className="update-empty">Нажмите проверку, чтобы увидеть доступные обновления.</div>
          )}
        </div>
        <pre className="update-log-output">{[
          consoleOutput,
          state.error ? `$ error\n${state.error}` : "",
          state.result ? `$ result\nКод выхода: ${state.result.returncode}` : ""
        ].filter(Boolean).join("\n\n")}</pre>
        <div className="confirm-actions">
          <button type="button" className="secondary-button" onClick={onCheck} disabled={state.running || state.checking}>
            {state.checking ? <Loader2 className="spin" size={16} /> : <RefreshCw size={16} />}
            Проверить обновления
          </button>
          <button type="button" className="primary-button" onClick={onRun} disabled={!canRun}>
            {state.running || state.escortingRestart ? <Loader2 className="spin" size={16} /> : <Power size={16} />}
            Запустить
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
  const [passkeyBusy, setPasskeyBusy] = useState(false);
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

  async function signInWithPasskey() {
    if (!navigator.credentials) {
      setError("Passkey не поддерживается этим браузером");
      return;
    }
    setPasskeyBusy(true);
    setError(null);
    try {
      const options = await api.passkeyAuthenticationOptions(username || null);
      const credential = await navigator.credentials.get({
        publicKey: credentialRequestOptionsFromJson(options.options)
      });
      const result = await api.passkeyAuthenticationVerify(options.challenge_id, publicKeyCredentialToJson(credential));
      onLogin(result.user);
    } catch (exc) {
      setError(exc instanceof Error ? exc.message : "Не удалось войти с passkey");
    } finally {
      setPasskeyBusy(false);
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
          {!challenge ? (
            <button className="secondary-button" type="button" disabled={passkeyBusy} onClick={signInWithPasskey}>
              {passkeyBusy ? <Loader2 size={16} className="spin" /> : <Fingerprint size={16} />}
              Войти с passkey
            </button>
          ) : null}
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

function ConnectedServicesView({ services }: { services: MCPConnectedService[] }) {
  return (
    <section className="panel">
      <div className="panel-head">
        <div>
          <h2>MCP-подключения</h2>
          <p>{services.length ? `${services.length} активных OAuth-клиентов` : "Активных MCP-сессий нет"}</p>
        </div>
      </div>
      <div className="table">
        {services.map((service) => (
          <div className="table-row table-row-services" key={service.client_id}>
            <div>
              <strong>{service.client_name}</strong>
              <small>{service.client_id}</small>
              <small>{service.allowed_scopes.join(", ") || "без scope"}</small>
            </div>
            <Badge tone={service.confidential ? "ok" : "warn"}>{service.confidential ? "confidential" : "public"}</Badge>
            <span>{service.active_session_count} сессий</span>
            <span>{service.user_count} пользователей</span>
            <span>{service.last_token_issued_at ? formatDate(service.last_token_issued_at) : "токенов нет"}</span>
            <span>{service.last_tool_call_at ? formatDate(service.last_tool_call_at) : "вызовов нет"}</span>
          </div>
        ))}
      </div>
    </section>
  );
}

function formatAuditEvent(event: AuditEvent, plugins: PluginInfo[], tools: ToolInfo[]): { title: string; detail: string } {
  const target = (event.payload?.target as Record<string, any>) || {};
  const metadata = (event.payload?.metadata as Record<string, any>) || {};

  const targetPluginKey = textValue(target.plugin_key);
  const targetToolKey = textValue(target.tool_key);
  const plugin = targetPluginKey ? plugins.find((item) => item.key === targetPluginKey) : null;
  const tool = targetToolKey ? tools.find((item) => item.key === targetToolKey) : null;
  const pluginName = textValue(metadata.plugin_name) || plugin?.name || targetPluginKey || "Плагин";
  const toolName = textValue(metadata.tool_name) || tool?.name || targetToolKey || "Неизвестный";

  switch (event.event_type) {
    case "mcp.plugin.update":
      return {
        title: `Плагин «${pluginName}» ${enabledText(metadata.enabled)}`,
        detail: metadata.changed === false ? "Состояние не изменилось" : "Состояние успешно изменено"
      };
    case "mcp.tool.global.update":
      return {
        title: `Инструмент «${toolName}» ${enabledText(metadata.enabled)}`,
        detail: `Глобальное состояние инструмента успешно изменено${textValue(metadata.plugin_key) ? ` в ${metadata.plugin_key}` : ""}`
      };
    case "admin.settings.registration":
      return { title: "Настройка регистрации", detail: `Регистрация ${enabledText(target.enabled)}` };
    case "admin.settings.mcp":
      return { title: "Настройка MCP", detail: `MCP-сервер ${enabledText(target.enabled)}` };
    case "mcp.plugins.reload":
      return { title: "Плагины MCP перезагружены", detail: "Реестр плагинов перечитан сервером" };
    case "mcp.tool.call":
      return {
        title: `Вызов MCP-инструмента «${toolName}»`,
        detail: `Использование: ${metadata.read_only ? "чтение" : "запись"}, Аргументы: ${JSON.stringify(metadata.arguments || {})}`
      };
    case "system.update":
      return { title: "Обновление приложения запущено", detail: `Скрипт update.sh завершился: ${formatResult(event.result)}` };
    case "system.restart":
      return { title: "Перезапуск приложения запланирован", detail: `Скрипт restart.sh завершился: ${formatResult(event.result)}` };
    case "users.permission.mutate":
      return { title: "Права пользователя обновлены", detail: textValue(target.user_id) || event.event_type };
    case "account.api_key.create":
      return { title: "Создан API-ключ", detail: `API-ключ «${textValue(metadata.name) || "без имени"}» создан` };
    case "account.2fa.setup":
      return { title: "Настройка 2FA начата", detail: textValue(target.user_id) || event.event_type };
    case "account.2fa.enable":
      return { title: "2FA включена", detail: textValue(target.user_id) || event.event_type };
    case "account.2fa.disable":
      return { title: "2FA отключена", detail: textValue(target.user_id) || event.event_type };
    case "auth.login":
      return { title: "Вход выполнен", detail: textValue(target.user_id) || event.event_type };
    case "auth.logout":
      return { title: "Выход выполнен", detail: textValue(target.user_id) || event.event_type };
    case "auth.login.failed":
      return { title: "Неудачная попытка входа", detail: textValue(target.username) || event.event_type };
    case "auth.login.2fa_required":
      return { title: "Запрошен код 2FA", detail: textValue(target.user_id) || event.event_type };
    case "auth.login.2fa_failed":
      return { title: "Ошибка проверки 2FA", detail: textValue(target.user_id) || event.event_type };
    case "oauth.client.create":
      return { title: "OAuth-клиент создан", detail: textValue(target.client_id) || event.event_type };
    case "oauth.client.dynamic_register":
      return { title: "OAuth-клиент зарегистрирован динамически", detail: textValue(target.client_id) || event.event_type };
    case "oauth.authorize":
      return { title: "OAuth-авторизация создана", detail: textValue(target.client_id) || event.event_type };
    case "oauth.token.issue":
      return { title: "OAuth-токен выпущен", detail: textValue(target.client_id) || event.event_type };
    default:
      return { title: event.event_type, detail: JSON.stringify(event.payload || {}) };
  }
}

function AuditView({ events, plugins, tools }: { events: AuditEvent[]; plugins: PluginInfo[]; tools: ToolInfo[] }) {
  const handleExport = () => {
    window.location.href = "/api/admin/audit/logs/export";
  };

  return (
    <section className="panel">
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "1rem" }}>
        <h2>Аудит</h2>
        <button className="button" onClick={handleExport}>
          <ScrollText size={16} style={{marginRight: "6px", verticalAlign: "middle"}} /> Экспорт
        </button>
      </div>
      <div className="timeline">
        {events.map((event) => {
          const formatted = formatAuditEvent(event, plugins, tools);
          return (
            <div className="timeline-row" key={event.event_id}>
              <span className="timeline-dot" />
              <div className="timeline-content">
                <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                  <strong className="timeline-title">{formatted.title}</strong>
                  <span style={{ fontSize: "11px", color: "var(--text-muted)", fontFamily: "monospace", background: "var(--bg-card)", padding: "2px 6px", borderRadius: "4px" }}>
                    ID: {event.correlation_id?.substring(0,8)}
                  </span>
                </div>
                <small className="timeline-meta">{formatDate(event.timestamp)} · {event.actor?.username || "система"} · {formatResult(event.result)}</small>
                <small className="timeline-detail">{formatted.detail}</small>
              </div>
            </div>
          );
        })}
      </div>
    </section>
  );
}

function ApiKeysPanel() {
  const [keys, setKeys] = useState<ApiKey[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState<string | null>(null);

  // Form state
  const [newKeyName, setNewKeyName] = useState("");
  const [expiryOption, setExpiryOption] = useState<number | null>(30);
  const [createdToken, setCreatedToken] = useState<string | null>(null);
  const [copied, setCopied] = useState(false);

  // Edit state (key_id -> name)
  const [keyNames, setKeyNames] = useState<Record<string, string>>({});

  const refreshKeys = useCallback(async () => {
    try {
      const items = await api.apiKeys();
      setKeys(items);
      setKeyNames(Object.fromEntries(items.map((item) => [item.key_id, item.name])));
    } catch (exc) {
      setError(exc instanceof Error ? exc.message : "Не удалось загрузить API-ключи");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    refreshKeys();
  }, [refreshKeys]);

  async function handleCreate(event: FormEvent) {
    event.preventDefault();
    if (!newKeyName.trim()) return;
    setBusy(true);
    setError(null);
    setMessage(null);
    setCreatedToken(null);
    setCopied(false);
    try {
      const result = await api.createApiKey(newKeyName.trim(), expiryOption);
      setCreatedToken(result.token);
      setNewKeyName("");
      setMessage("API-ключ успешно создан");
      await refreshKeys();
    } catch (exc) {
      setError(exc instanceof Error ? exc.message : "Не удалось создать API-ключ");
    } finally {
      setBusy(false);
    }
  }

  async function handleRevoke(keyId: string) {
    if (!confirm("Вы уверены, что хотите отозвать этот API-ключ? Доступ по нему будет мгновенно заблокирован.")) {
      return;
    }
    setBusy(true);
    setError(null);
    setMessage(null);
    try {
      await api.revokeApiKey(keyId);
      setMessage("API-ключ отозван");
      await refreshKeys();
    } catch (exc) {
      setError(exc instanceof Error ? exc.message : "Не удалось отозвать API-ключ");
    } finally {
      setBusy(false);
    }
  }

  async function handleRename(keyId: string, name: string) {
    setBusy(true);
    setError(null);
    setMessage(null);
    try {
      await api.updateApiKey(keyId, name.trim());
      setMessage("Название API-ключа обновлено");
      await refreshKeys();
    } catch (exc) {
      setError(exc instanceof Error ? exc.message : "Не удалось обновить API-ключ");
    } finally {
      setBusy(false);
    }
  }

  function copyToClipboard() {
    if (!createdToken) return;
    navigator.clipboard.writeText(createdToken);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  }

  return (
    <div className="panel narrow">
      <div className="panel-head">
        <div>
          <h2>API-ключи доступа</h2>
          <p>Персональные стационарные токены для внешних MCP-клиентов и скриптов</p>
        </div>
        <Badge tone={keys.length ? "ok" : "muted"}>{keys.length} / 20</Badge>
      </div>

      <ErrorBanner message={error} />
      {message ? <div className="notice notice-ok">{message}</div> : null}

      {createdToken && (
        <div className="notice notice-warn" style={{ display: "flex", flexDirection: "column", gap: "8px" }}>
          <strong>Сохраните этот API-ключ!</strong>
          <p>Он показывается только один раз и не может быть восстановлен.</p>
          <div style={{ display: "flex", alignItems: "center", gap: "8px", marginTop: "4px" }}>
            <code style={{ flex: 1, wordBreak: "break-all", padding: "6px", background: "rgba(0,0,0,0.2)", borderRadius: "4px", fontFamily: "monospace" }}>
              {createdToken}
            </code>
            <button className="icon-button" onClick={copyToClipboard} title="Копировать в буфер">
              {copied ? <Check size={16} style={{ color: "var(--color-ok)" }} /> : <Copy size={16} />}
            </button>
          </div>
        </div>
      )}

      <form onSubmit={handleCreate} className="form-grid">
        <label>
          Название ключа
          <input
            value={newKeyName}
            onChange={(event) => setNewKeyName(event.target.value)}
            placeholder="Например, Antigravity MCP"
            required
            disabled={busy}
          />
        </label>
        <label>
          Срок действия
          <select
            value={expiryOption ?? ""}
            onChange={(event) => setExpiryOption(event.target.value ? Number(event.target.value) : null)}
            disabled={busy}
            style={{ width: "100%", padding: "8px", borderRadius: "4px", background: "var(--color-bg-input)", border: "1px solid var(--color-border-input)", color: "var(--color-text)" }}
          >
            <option value="30">30 дней</option>
            <option value="90">90 дней</option>
            <option value="365">365 дней</option>
            <option value="">Без ограничения</option>
          </select>
        </label>
        <button className="secondary-button" type="submit" disabled={busy || !newKeyName.trim() || keys.length >= 20}>
          {busy ? <Loader2 size={16} className="spin" /> : <KeyRound size={16} />}
          Создать API-ключ
        </button>
      </form>

      {loading ? (
        <div style={{ display: "flex", justifyContent: "center", padding: "16px" }}>
          <Loader2 size={24} className="spin" />
        </div>
      ) : keys.length > 0 ? (
        <div className="passkey-list" style={{ marginTop: "16px" }}>
          {keys.map((item) => {
            const isNameUnchanged = (keyNames[item.key_id] ?? item.name).trim() === item.name;
            return (
              <div className="passkey-row" key={item.key_id}>
                <div style={{ flex: 1 }}>
                  <input
                    value={keyNames[item.key_id] ?? item.name}
                    onChange={(event) => setKeyNames((current) => ({ ...current, [item.key_id]: event.target.value }))}
                    style={{ width: "100%", padding: "4px 0", border: "none", background: "transparent", color: "var(--color-text)", fontWeight: "bold" }}
                    aria-label="Название API-ключа"
                  />
                  <small style={{ display: "block", color: "var(--color-text-muted)", marginTop: "4px" }}>
                    Префикс: <code>{item.token_prefix}...</code> · Создан: {formatDate(item.created_at)}
                  </small>
                  <small style={{ display: "block", color: "var(--color-text-muted)", marginTop: "2px" }}>
                    Истекает: {item.expires_at ? formatDate(item.expires_at) : "никогда"}
                    {item.last_used_at ? ` · Использован: ${formatDate(item.last_used_at)}` : " · Не использовался"}
                  </small>
                </div>
                <div className="passkey-actions">
                  <button
                    className="icon-button"
                    title="Сохранить название"
                    disabled={busy || isNameUnchanged}
                    onClick={() => handleRename(item.key_id, keyNames[item.key_id] ?? item.name)}
                  >
                    <Save size={16} />
                  </button>
                  <button
                    className="icon-button danger-icon"
                    title="Отозвать API-ключ"
                    disabled={busy}
                    onClick={() => handleRevoke(item.key_id)}
                  >
                    <Trash2 size={16} />
                  </button>
                </div>
              </div>
            );
          })}
        </div>
      ) : (
        <p style={{ textAlign: "center", color: "var(--color-text-muted)", marginTop: "16px" }}>
          У вас ещё нет созданных API-ключей.
        </p>
      )}
    </div>
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
  const [passkeys, setPasskeys] = useState<Passkey[]>([]);
  const [passkeyNames, setPasskeyNames] = useState<Record<string, string>>({});
  const [newPasskeyName, setNewPasskeyName] = useState("");
  const [passkeyPassword, setPasskeyPassword] = useState("");
  const [passkeyMessage, setPasskeyMessage] = useState<string | null>(null);
  const [passkeyError, setPasskeyError] = useState<string | null>(null);
  const [passkeyBusy, setPasskeyBusy] = useState(false);

  useEffect(() => {
    setEmail(user.email || "");
    setTgId(user.tg_id || "");
    setVkId(user.vk_id || "");
  }, [user]);

  useEffect(() => {
    let cancelled = false;
    api.passkeys()
      .then((items) => {
        if (cancelled) {
          return;
        }
        setPasskeys(items);
        setPasskeyNames(Object.fromEntries(items.map((item) => [item.passkey_id, item.name])));
      })
      .catch((exc) => {
        if (!cancelled) {
          setPasskeyError(exc instanceof Error ? exc.message : "Не удалось загрузить passkey");
        }
      });
    return () => {
      cancelled = true;
    };
  }, [user.user_id]);

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

  async function runPasskey(action: () => Promise<void>) {
    setPasskeyBusy(true);
    setPasskeyError(null);
    setPasskeyMessage(null);
    try {
      await action();
    } catch (exc) {
      setPasskeyError(exc instanceof Error ? exc.message : "Не удалось обновить passkey");
    } finally {
      setPasskeyBusy(false);
    }
  }

  async function refreshPasskeys() {
    const items = await api.passkeys();
    setPasskeys(items);
    setPasskeyNames(Object.fromEntries(items.map((item) => [item.passkey_id, item.name])));
  }

  async function createPasskey() {
    if (!navigator.credentials) {
      throw new Error("Passkey не поддерживается этим браузером");
    }
    const options = await api.passkeyRegistrationOptions(passkeyPassword, newPasskeyName || null);
    const credential = await navigator.credentials.create({
      publicKey: credentialCreationOptionsFromJson(options.options)
    });
    await api.passkeyRegistrationVerify(options.challenge_id, newPasskeyName || null, publicKeyCredentialToJson(credential));
    setNewPasskeyName("");
    setPasskeyPassword("");
    setPasskeyMessage("Passkey добавлен");
    await refreshPasskeys();
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
            <label>
              Текущий пароль
              <input value={currentPassword} onChange={(event) => setCurrentPassword(event.target.value)} type="password" autoComplete="current-password" />
            </label>
            <button
              className="secondary-button danger-button"
              disabled={twoFactorBusy || !twoFactorCode || !currentPassword}
              onClick={() => runTwoFactor(async () => {
                const updated = await api.twoFactorDisable(twoFactorCode, currentPassword);
                onUserUpdate(updated);
                setTwoFactorCode("");
                setCurrentPassword("");
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
      <div className="panel narrow">
        <div className="panel-head">
          <div>
            <h2>Passkey</h2>
            <p>{passkeys.length ? `${passkeys.length} ключей для входа` : "Windows Hello, Google Password Manager и FIDO2-ключи"}</p>
          </div>
          <Badge tone={passkeys.length ? "ok" : "muted"}>{passkeys.length}</Badge>
        </div>
        <ErrorBanner message={passkeyError} />
        {passkeyMessage ? <div className="notice notice-ok">{passkeyMessage}</div> : null}
        <div className="form-grid">
          <label>
            Название
            <input value={newPasskeyName} onChange={(event) => setNewPasskeyName(event.target.value)} placeholder="Например, Windows Hello" autoComplete="off" />
          </label>
          <label>
            Текущий пароль
            <input value={passkeyPassword} onChange={(event) => setPasskeyPassword(event.target.value)} type="password" autoComplete="current-password" />
          </label>
          <button className="secondary-button" disabled={passkeyBusy || !passkeyPassword} onClick={() => runPasskey(createPasskey)}>
            {passkeyBusy ? <Loader2 size={16} className="spin" /> : <Fingerprint size={16} />}
            Добавить passkey
          </button>
        </div>
        <div className="passkey-list">
          {passkeys.map((item) => (
            <div className="passkey-row" key={item.passkey_id}>
              <div>
                <input
                  value={passkeyNames[item.passkey_id] ?? item.name}
                  onChange={(event) => setPasskeyNames((current) => ({ ...current, [item.passkey_id]: event.target.value }))}
                  aria-label="Название passkey"
                />
                <small>
                  Создан: {formatDate(item.created_at)}
                  {item.last_used_at ? ` · Последний вход: ${formatDate(item.last_used_at)}` : ""}
                </small>
                <small>{[item.authenticator_attachment, item.credential_device_type, item.credential_backed_up ? "синхронизируется" : null, ...item.transports].filter(Boolean).join(" · ") || "без дополнительных данных"}</small>
              </div>
              <div className="passkey-actions">
                <button
                  className="icon-button"
                  title="Сохранить название"
                  disabled={passkeyBusy || (passkeyNames[item.passkey_id] ?? item.name).trim() === item.name}
                  onClick={() => runPasskey(async () => {
                    await api.renamePasskey(item.passkey_id, passkeyNames[item.passkey_id] ?? item.name);
                    setPasskeyMessage("Название passkey обновлено");
                    await refreshPasskeys();
                  })}
                >
                  <Save size={16} />
                </button>
                <button
                  className="icon-button danger-icon"
                  title="Удалить passkey"
                  disabled={passkeyBusy}
                  onClick={() => runPasskey(async () => {
                    await api.deletePasskey(item.passkey_id);
                    setPasskeyMessage("Passkey удалён");
                    await refreshPasskeys();
                  })}
                >
                  <Trash2 size={16} />
                </button>
              </div>
            </div>
          ))}
        </div>
      </div>
      <ApiKeysPanel />
    </section>
  );
}

export function ProxyToolsView({
  pendingKeys,
  pushToast,
  runAction
}: {
  pendingKeys: ReadonlySet<string>;
  pushToast: (tone: ToastTone, title: string, message?: string) => void;
  runAction: (action: () => Promise<void>, options?: { pendingKey?: string; errorTitle?: string }) => Promise<void>;
}) {
  const [proxies, setProxies] = useState<Proxy[]>([]);
  const [loading, setLoading] = useState(true);
  const [addTab, setAddTab] = useState<"manual" | "url">("manual");

  // Manual Form States
  const [protocol, setProtocol] = useState<ProxyProtocol>("socks5");
  const [host, setHost] = useState("");
  const [port, setPort] = useState(1080);
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [label, setLabel] = useState("");

  // URL Form States
  const [urlInput, setUrlInput] = useState("");
  const [urlProtocol, setUrlProtocol] = useState<ProxyProtocol>("socks5");
  const [urlLabel, setUrlLabel] = useState("");

  // Bulk operations
  const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set());
  const [exportDropdownOpen, setExportDropdownOpen] = useState(false);
  const [importOpen, setImportOpen] = useState(false);
  const [importXml, setImportXml] = useState("");

  // TG Proxy export dialog
  const [tgExportProxy, setTgExportProxy] = useState<Proxy | null>(null);
  const [tgSecret, setTgSecret] = useState("");
  const [tgLinks, setTgLinks] = useState<ProxyTgExport | null>(null);

  const loadProxies = useCallback(async () => {
    setLoading(true);
    try {
      const data = await api.proxies();
      setProxies(data);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void loadProxies();
  }, [loadProxies]);

  const isPollingRef = useRef(false);
  const startPolling = useCallback(() => {
    if (isPollingRef.current) return;
    isPollingRef.current = true;
    let attempts = 0;
    const interval = window.setInterval(async () => {
      attempts += 1;
      try {
        const data = await api.proxies();
        setProxies(data);
      } catch {
        // Ignore errors during background checks polling
      }
      if (attempts >= 8) {
        window.clearInterval(interval);
        isPollingRef.current = false;
      }
    }, 3000);
  }, []);

  async function handleCheck(proxyId: string) {
    await runAction(
      async () => {
        pushToast("info", "Проверка прокси запущена");
        const result = await api.checkProxy(proxyId);
        setProxies((current) =>
          current.map((p) => (p.proxy_id === proxyId ? { ...p, last_check: result } : p))
        );
        if (result.ok) {
          pushToast("success", "Прокси работает", `Средний отклик: ${result.avg_latency_ms} мс`);
        } else {
          pushToast("error", "Прокси не отвечает", "Все тестовые запросы завершились ошибкой");
        }
      },
      { pendingKey: `proxy:check:${proxyId}`, errorTitle: "Ошибка проверки прокси" }
    );
  }

  async function handleCheckAll() {
    await runAction(
      async () => {
        await api.checkAllProxies();
        pushToast("info", "Запущена массовая проверка в фоновом режиме");
        startPolling();
      },
      { pendingKey: "proxy:check-all", errorTitle: "Не удалось запустить проверку" }
    );
  }

  async function handleDelete(proxyId: string) {
    await runAction(
      async () => {
        await api.deleteProxy(proxyId);
        setProxies((current) => current.filter((p) => p.proxy_id !== proxyId));
        setSelectedIds((current) => {
          const next = new Set(current);
          next.delete(proxyId);
          return next;
        });
        pushToast("success", "Прокси успешно удален");
      },
      { pendingKey: `proxy:delete:${proxyId}`, errorTitle: "Не удалось удалить прокси" }
    );
  }

  async function handleAddManual(e: FormEvent) {
    e.preventDefault();
    if (!host || !port) return;
    await runAction(
      async () => {
        const created = await api.createProxy({
          protocol,
          host: host.trim(),
          port,
          username: username.trim() || undefined,
          password: password || undefined,
          label: label.trim() || undefined
        });
        setProxies((current) => [created, ...current]);
        setHost("");
        setUsername("");
        setPassword("");
        setLabel("");
        pushToast("success", "Прокси добавлен вручную");
      },
      { pendingKey: "proxy:create", errorTitle: "Не удалось добавить прокси" }
    );
  }

  async function handleAddUrl(e: FormEvent) {
    e.preventDefault();
    if (!urlInput) return;
    await runAction(
      async () => {
        const created = await api.createProxyFromUrl(
          urlInput.trim(),
          urlProtocol,
          urlLabel.trim() || undefined
        );
        setProxies((current) => [created, ...current]);
        setUrlInput("");
        setUrlLabel("");
        pushToast("success", "Прокси успешно импортирован из ссылки");
      },
      { pendingKey: "proxy:create-url", errorTitle: "Не удалось импортировать прокси" }
    );
  }

  async function handleImportSubmit(e: FormEvent) {
    e.preventDefault();
    if (!importXml.trim()) return;
    await runAction(
      async () => {
        const result = await api.importProxifier(importXml);
        await loadProxies();
        setImportXml("");
        setImportOpen(false);
        pushToast(
          "success",
          "Импорт завершен",
          `Импортировано: ${result.imported}, Пропущено (дубликаты): ${result.skipped}`
        );
      },
      { pendingKey: "proxy:import", errorTitle: "Не удалось импортировать профиль" }
    );
  }

  function handleFileChange(e: React.ChangeEvent<HTMLInputElement>) {
    const file = e.target.files?.[0];
    if (!file) return;
    const reader = new FileReader();
    reader.onload = (event) => {
      const text = event.target?.result as string;
      setImportXml(text);
    };
    reader.readAsText(file);
  }

  function handleSelectRow(proxyId: string, checked: boolean) {
    setSelectedIds((current) => {
      const next = new Set(current);
      if (checked) {
        next.add(proxyId);
      } else {
        next.delete(proxyId);
      }
      return next;
    });
  }

  function handleSelectAll(checked: boolean) {
    if (checked) {
      setSelectedIds(new Set(proxies.map((p) => p.proxy_id)));
    } else {
      setSelectedIds(new Set());
    }
  }

  async function handleExportUrls() {
    if (selectedIds.size === 0) return;
    try {
      const lines: string[] = [];
      for (const id of Array.from(selectedIds)) {
        const res = await api.exportProxyUrl(id);
        lines.push(res.url);
      }
      await navigator.clipboard.writeText(lines.join("\n"));
      pushToast("success", "Ссылки экспортированы в буфер обмена");
    } catch (err) {
      pushToast("error", "Ошибка при копировании", err instanceof Error ? err.message : String(err));
    }
    setExportDropdownOpen(false);
  }

  async function handleExportLines() {
    if (selectedIds.size === 0) return;
    try {
      const blocks: string[] = [];
      for (const id of Array.from(selectedIds)) {
        const res = await api.exportProxyLines(id);
        blocks.push(res.lines);
      }
      await navigator.clipboard.writeText(blocks.join("\n\n"));
      pushToast("success", "Данные прокси экспортированы в буфер обмена");
    } catch (err) {
      pushToast("error", "Ошибка при копировании", err instanceof Error ? err.message : String(err));
    }
    setExportDropdownOpen(false);
  }

  async function handleExportProxifier() {
    if (selectedIds.size === 0) return;
    try {
      const res = await api.exportProxifier(Array.from(selectedIds));
      const blob = new Blob([res.xml_content], { type: "application/xml" });
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = "proxies.ppx";
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      window.URL.revokeObjectURL(url);
      pushToast("success", "Профиль Proxifier (.ppx) успешно сохранен");
    } catch (err) {
      pushToast("error", "Ошибка при сохранении", err instanceof Error ? err.message : String(err));
    }
    setExportDropdownOpen(false);
  }

  function handleOpenTgExport(proxy: Proxy) {
    setTgExportProxy(proxy);
    setTgSecret("");
    setTgLinks(null);
  }

  async function handleTgExportSubmit(e: FormEvent) {
    e.preventDefault();
    if (!tgExportProxy) return;
    try {
      const res = await api.exportProxyTg(tgExportProxy.proxy_id, tgSecret || undefined);
      setTgLinks(res);
    } catch (err) {
      pushToast("error", "Ошибка генерации TG Proxy", err instanceof Error ? err.message : String(err));
    }
  }

  if (loading && proxies.length === 0) {
    return <div className="loading">Загрузка прокси-инструментов...</div>;
  }

  return (
    <section className="profile-grid span-2" style={{ display: "grid", gap: "20px", gridTemplateColumns: "1fr 1fr", width: "100%" }}>
      {/* ADD PROXY PANEL */}
      <div className="panel" style={{ padding: "20px" }}>
        <div className="panel-head" style={{ display: "flex", justifyContent: "space-between", marginBottom: "16px" }}>
          <div>
            <h2>Добавить прокси</h2>
            <p>Настройки подключения</p>
          </div>
          <div className="segmented" style={{ display: "flex", gap: "4px" }}>
            <button className={addTab === "manual" ? "active" : ""} onClick={() => setAddTab("manual")}>Вручную</button>
            <button className={addTab === "url" ? "active" : ""} onClick={() => setAddTab("url")}>По ссылке</button>
          </div>
        </div>

        {addTab === "manual" ? (
          <form className="form-grid" onSubmit={handleAddManual} style={{ display: "grid", gap: "10px" }}>
            <div style={{ display: "grid", gridTemplateColumns: "1fr 2fr 1fr", gap: "10px" }}>
              <label>
                Протокол
                <select value={protocol} onChange={(e) => setProtocol(e.target.value as ProxyProtocol)} style={{ width: "100%", height: "38px", borderRadius: "6px", border: "1px solid #dfe6ea", padding: "0 8px" }}>
                  <option value="socks5">SOCKS5</option>
                  <option value="http">HTTP</option>
                  <option value="https">HTTPS</option>
                </select>
              </label>
              <label>
                Адрес сервера (IP/Хост)
                <input value={host} onChange={(e) => setHost(e.target.value)} placeholder="например 192.168.1.1" required />
              </label>
              <label>
                Порт
                <input type="number" value={port} onChange={(e) => setPort(parseInt(e.target.value) || 1080)} min={1} max={65535} required />
              </label>
            </div>
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "10px" }}>
              <label>
                Имя пользователя
                <input value={username} onChange={(e) => setUsername(e.target.value)} placeholder="необязательно" />
              </label>
              <label>
                Пароль
                <input type="password" value={password} onChange={(e) => setPassword(e.target.value)} placeholder="необязательно" />
              </label>
            </div>
            <label>
              Метка (Название)
              <input value={label} onChange={(e) => setLabel(e.target.value)} placeholder="например, Рабочий прокси" />
            </label>
            <button type="submit" className="primary-button" style={{ marginTop: "10px" }}>
              <Plus size={16} /> Добавить
            </button>
          </form>
        ) : (
          <form className="form-grid" onSubmit={handleAddUrl} style={{ display: "grid", gap: "10px" }}>
            <div style={{ display: "grid", gridTemplateColumns: "1fr 3fr", gap: "10px" }}>
              <label>
                Протокол
                <select value={urlProtocol} onChange={(e) => setUrlProtocol(e.target.value as ProxyProtocol)} style={{ width: "100%", height: "38px", borderRadius: "6px", border: "1px solid #dfe6ea", padding: "0 8px" }}>
                  <option value="socks5">SOCKS5</option>
                  <option value="http">HTTP</option>
                  <option value="https">HTTPS</option>
                </select>
              </label>
              <label>
                Строка подключения
                <input value={urlInput} onChange={(e) => setUrlInput(e.target.value)} placeholder="user:password@host:port" required />
              </label>
            </div>
            <label>
              Метка (Название)
              <input value={urlLabel} onChange={(e) => setUrlLabel(e.target.value)} placeholder="например, Мой импорт" />
            </label>
            <button type="submit" className="primary-button" style={{ marginTop: "10px" }}>
              <Plus size={16} /> Импортировать
            </button>
          </form>
        )}
      </div>

      {/* QUICK ACTIONS & STATS PANEL */}
      <div className="panel" style={{ padding: "20px", display: "flex", flexDirection: "column", justifyContent: "space-between" }}>
        <div>
          <h2>Управление прокси</h2>
          <p>Быстрый импорт / проверка работоспособности</p>
        </div>
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "10px", margin: "20px 0" }}>
          <div className="metric">
            <span>Всего прокси</span>
            <strong>{proxies.length} <small style={{ display: "inline", color: "#888" }}>/ 500</small></strong>
          </div>
          <div className="metric">
            <span>Активных (OK)</span>
            <strong>{proxies.filter(p => p.last_check?.ok).length}</strong>
          </div>
        </div>
        <div style={{ display: "flex", gap: "10px" }}>
          <button className="secondary-button" style={{ flex: 1 }} onClick={() => setImportOpen(true)}>
            <Upload size={16} /> Импорт Proxifier (.ppx)
          </button>
          <button className="secondary-button" style={{ flex: 1 }} onClick={handleCheckAll} disabled={pendingKeys.has("proxy:check-all")}>
            <RefreshCw size={16} className={pendingKeys.has("proxy:check-all") ? "spin" : ""} /> Проверить все
          </button>
        </div>
      </div>

      {/* LIST PANEL */}
      <div className="panel span-2" style={{ padding: "20px", gridColumn: "span 2" }}>
        <div className="panel-head" style={{ display: "flex", justifyContent: "space-between", marginBottom: "16px" }}>
          <div>
            <h2>Список ваших прокси-серверов</h2>
            <p>Выбрано: {selectedIds.size} из {proxies.length}</p>
          </div>
          <div style={{ position: "relative" }}>
            <button className="secondary-button" disabled={selectedIds.size === 0} onClick={() => setExportDropdownOpen(!exportDropdownOpen)}>
              <Download size={16} /> Экспортировать ({selectedIds.size}) ▾
            </button>
            {exportDropdownOpen && (
              <div style={{ position: "absolute", right: 0, top: "42px", background: "white", border: "1px solid #dfe6ea", borderRadius: "6px", boxShadow: "0 4px 12px rgba(0,0,0,0.1)", zIndex: 10, display: "flex", flexDirection: "column", minWidth: "200px" }}>
                <button className="logout" style={{ color: "#333", background: "none", padding: "10px 14px", border: "none", width: "100%", justifyContent: "flex-start", margin: 0, borderRadius: 0 }} onClick={handleExportUrls}>Копировать URL-ссылки</button>
                <button className="logout" style={{ color: "#333", background: "none", padding: "10px 14px", border: "none", width: "100%", justifyContent: "flex-start", margin: 0, borderRadius: 0 }} onClick={handleExportLines}>Скопировать построчно</button>
                <button className="logout" style={{ color: "#333", background: "none", padding: "10px 14px", border: "none", width: "100%", justifyContent: "flex-start", margin: 0, borderRadius: 0 }} onClick={handleExportProxifier}>Скачать Proxifier profile</button>
              </div>
            )}
          </div>
        </div>

        <div style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "14px" }}>
            <thead>
              <tr style={{ borderBottom: "2px solid #dfe6ea", textAlign: "left", color: "#697782" }}>
                <th style={{ padding: "10px 8px", width: "40px" }}>
                  <input type="checkbox" onChange={(e) => handleSelectAll(e.target.checked)} checked={selectedIds.size === proxies.length && proxies.length > 0} />
                </th>
                <th style={{ padding: "10px 8px" }}>Название</th>
                <th style={{ padding: "10px 8px", width: "100px" }}>Протокол</th>
                <th style={{ padding: "10px 8px" }}>Адрес сервера</th>
                <th style={{ padding: "10px 8px" }}>Логин</th>
                <th style={{ padding: "10px 8px", width: "120px" }}>Тест доступности</th>
                <th style={{ padding: "10px 8px", width: "120px" }}>Задержка (avg)</th>
                <th style={{ padding: "10px 8px", width: "150px" }}>Действия</th>
              </tr>
            </thead>
            <tbody>
              {proxies.map(p => (
                <tr key={p.proxy_id} style={{ borderBottom: "1px solid #eef2f3" }}>
                  <td style={{ padding: "10px 8px" }}>
                    <input type="checkbox" checked={selectedIds.has(p.proxy_id)} onChange={(e) => handleSelectRow(p.proxy_id, e.target.checked)} />
                  </td>
                  <td style={{ padding: "10px 8px", fontWeight: "bold" }}>{p.label || "Без названия"}</td>
                  <td style={{ padding: "10px 8px" }}>
                    <Badge tone={p.protocol === "socks5" ? "ok" : "warn"}>{p.protocol.toUpperCase()}</Badge>
                  </td>
                  <td style={{ padding: "10px 8px" }}><code>{p.host}:{p.port}</code></td>
                  <td style={{ padding: "10px 8px" }}>{p.username || <span style={{ color: "#aaa" }}>нет</span>}</td>
                  <td style={{ padding: "10px 8px" }}>
                    {p.last_check ? (
                      <span style={{ color: p.last_check.ok ? "#166534" : "#be123c", fontWeight: "bold", display: "inline-flex", alignItems: "center", gap: "6px" }} title={
                        p.last_check.details ? 
                          Object.entries(p.last_check.details).map(([target, d]) => `${target}: ${d.ok ? `${d.latency_ms}ms` : "FAIL"}`).join(" | ") : ""
                      }>
                        <span style={{ display: "inline-block", width: "8px", height: "8px", borderRadius: "50%", background: p.last_check.ok ? "#166534" : "#be123c" }}></span>
                        {p.last_check.ok ? "Доступен" : "Недоступен"}
                      </span>
                    ) : (
                      <span style={{ color: "#697782" }}>Не проверялся</span>
                    )}
                  </td>
                  <td style={{ padding: "10px 8px" }}>
                    {p.last_check?.avg_latency_ms ? (
                      <strong>{p.last_check.avg_latency_ms} мс</strong>
                    ) : (
                      <span style={{ color: "#697782" }}>—</span>
                    )}
                  </td>
                  <td style={{ padding: "10px 8px", display: "flex", gap: "6px", alignItems: "center" }}>
                    <button className="secondary-button" style={{ height: "30px", padding: "0 10px" }} title="Проверить скорость" onClick={() => handleCheck(p.proxy_id)} disabled={pendingKeys.has(`proxy:check:${p.proxy_id}`)}>
                      {pendingKeys.has(`proxy:check:${p.proxy_id}`) ? <Loader2 size={12} className="spin" /> : <Play size={12} />}
                    </button>
                    {p.protocol === "socks5" && (
                      <button className="secondary-button" style={{ height: "30px", padding: "0 10px" }} title="TG Proxy Link" onClick={() => handleOpenTgExport(p)}>
                        TG
                      </button>
                    )}
                    <button className="secondary-button icon-button danger-icon" style={{ height: "30px", width: "30px" }} title="Удалить" onClick={() => handleDelete(p.proxy_id)} disabled={pendingKeys.has(`proxy:delete:${p.proxy_id}`)}>
                      <Trash2 size={12} />
                    </button>
                  </td>
                </tr>
              ))}
              {proxies.length === 0 && (
                <tr>
                  <td colSpan={8} style={{ textAlign: "center", padding: "30px", color: "#697782" }}>Прокси-серверов пока нет. Добавьте вручную или импортируйте XML-профиль.</td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>

      {/* IMPORT PROXIFIER DIALOG */}
      {importOpen && (
        <div className="confirm-backdrop" role="presentation" onMouseDown={() => setImportOpen(false)}>
          <section className="confirm-dialog" role="dialog" aria-modal="true" onMouseDown={(e) => e.stopPropagation()} style={{ maxWidth: "500px", width: "100%", padding: "20px" }}>
            <h2>Импорт из Proxifier Profile</h2>
            <p style={{ color: "#697782", marginBottom: "14px" }}>Выберите XML-файл профиля Proxifier (.ppx) для парсинга и импорта списка прокси.</p>
            <form onSubmit={handleImportSubmit} style={{ display: "grid", gap: "12px" }}>
              <label style={{ display: "flex", flexDirection: "column", gap: "6px", padding: "20px", border: "2px dashed #dfe6ea", borderRadius: "8px", textAlign: "center", cursor: "pointer", background: "#f8fafb" }}>
                <span style={{ color: "#0b5c76", fontWeight: "bold" }}>Выбрать .ppx / .xml файл</span>
                <input type="file" accept=".ppx,.xml" onChange={handleFileChange} style={{ display: "none" }} />
              </label>
              <label>
                Содержимое XML файла
                <textarea value={importXml} onChange={(e) => setImportXml(e.target.value)} rows={6} placeholder="<?xml version=..." style={{ width: "100%", border: "1px solid #dfe6ea", borderRadius: "6px", padding: "8px", fontFamily: "monospace", fontSize: "12px" }} required />
              </label>
              <div className="confirm-actions" style={{ display: "flex", justifyContent: "flex-end", gap: "10px", marginTop: "10px" }}>
                <button type="button" className="secondary-button" onClick={() => setImportOpen(false)}>Отмена</button>
                <button type="submit" className="primary-button" disabled={!importXml.trim()}>Импортировать</button>
              </div>
            </form>
          </section>
        </div>
      )}

      {/* TG EXPORT DIALOG */}
      {tgExportProxy && (
        <div className="confirm-backdrop" role="presentation" onMouseDown={() => setTgExportProxy(null)}>
          <section className="confirm-dialog" role="dialog" aria-modal="true" onMouseDown={(e) => e.stopPropagation()} style={{ maxWidth: "450px", width: "100%", padding: "20px" }}>
            <h2>Экспорт в TG Proxy</h2>
            <p style={{ color: "#697782", marginBottom: "12px" }}>Экспорт прокси SOCKS5 <code>{tgExportProxy.host}:{tgExportProxy.port}</code> для мессенджера Telegram.</p>
            
            <form onSubmit={handleTgExportSubmit} style={{ display: "grid", gap: "10px" }}>
              <label>
                MTProto Secret (необязательно)
                <input value={tgSecret} onChange={(e) => setTgSecret(e.target.value)} placeholder="например d41d8cd98f00b204e9800998ecf8427e" />
              </label>
              <button type="submit" className="primary-button">Сгенерировать ссылки</button>
            </form>

            {tgLinks && (
              <div style={{ marginTop: "16px", display: "grid", gap: "12px" }}>
                <div>
                  <small style={{ color: "#697782" }}>Deep Link (в приложении)</small>
                  <div style={{ display: "flex", gap: "8px", alignItems: "center" }}>
                    <code style={{ flex: 1, wordBreak: "break-all", background: "#f8fafb", border: "1px solid #dfe6ea", padding: "6px", borderRadius: "4px" }}>
                      {tgLinks.deep_link}
                    </code>
                    <button className="icon-button" onClick={() => {
                      void navigator.clipboard.writeText(tgLinks.deep_link);
                      pushToast("success", "Deep Link скопирован");
                    }}>
                      <Copy size={14} />
                    </button>
                  </div>
                </div>
                <div>
                  <small style={{ color: "#697782" }}>Web URL (в браузере)</small>
                  <div style={{ display: "flex", gap: "8px", alignItems: "center" }}>
                    <code style={{ flex: 1, wordBreak: "break-all", background: "#f8fafb", border: "1px solid #dfe6ea", padding: "6px", borderRadius: "4px" }}>
                      {tgLinks.web_url}
                    </code>
                    <button className="icon-button" onClick={() => {
                      void navigator.clipboard.writeText(tgLinks.web_url);
                      pushToast("success", "Web URL скопирован");
                    }}>
                      <Copy size={14} />
                    </button>
                  </div>
                </div>
              </div>
            )}

            <div className="confirm-actions" style={{ display: "flex", justifyContent: "flex-end", marginTop: "16px" }}>
              <button type="button" className="secondary-button" onClick={() => setTgExportProxy(null)}>Закрыть</button>
            </div>
          </section>
        </div>
      )}
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
  const [connectedServices, setConnectedServices] = useState<MCPConnectedService[]>([]);
  const [events, setEvents] = useState<AuditEvent[]>([]);
  const [view, setView] = useState<View>("overview");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [toasts, setToasts] = useState<Toast[]>([]);
  const [confirmation, setConfirmation] = useState<Confirmation | null>(null);
  const [updateLog, setUpdateLog] = useState<UpdateFlowState>({
    open: false,
    running: false,
    checking: false,
    session: null,
    logs: [],
    result: null,
    error: null,
    forceStages: { code: false, python: false, frontend: false, restart: false },
    optionsOpen: false,
    escortingRestart: false,
    restartMessage: null
  });
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
      setRuntime(data.runtime);
      let activeUser = data.user;
      if (!activeUser) {
        try {
          const refreshed = await api.refresh();
          activeUser = refreshed.user;
        } catch {
        }
      }
      setUser(activeUser);
      if (activeUser) {
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
    const [nextHealth, nextRuntime, nextUsers, nextPermissions, nextPlugins, nextTools, nextConnectedServices, nextAudit] = await Promise.all([
      api.healthDetails().catch(() => api.health()),
      api.runtime(),
      api.users(),
      api.permissions(),
      api.plugins(),
      api.tools(),
      api.connectedServices().catch(() => []),
      api.audit()
    ]);
    setHealth(nextHealth);
    setRuntime(nextRuntime);
    setUsers(nextUsers);
    setPermissions(nextPermissions);
    setPlugins(nextPlugins);
    setTools(nextTools);
    setConnectedServices(nextConnectedServices);
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
    setUpdateLog((current) => ({ ...current, open: true, running: true, result: null, error: null }));
    try {
      const result = await api.runSystemUpdate();
      setUpdateLog((current) => ({ ...current, open: true, running: false, result, error: null }));
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
      setUpdateLog((current) => ({ ...current, open: true, running: false, result: null, error: message }));
      pushToast("error", "Не удалось обновить приложение", message);
    } finally {
      setActionPending("system:update", false);
    }
  }

  function openUpdateDialog() {
    setUpdateLog((current) => ({ ...current, open: true, error: null }));
  }

  function attachUpdateEvents(sessionId: string, mode: "check" | "run") {
    const source = new EventSource(api.systemUpdateEventsUrl(sessionId));
    const appendLog = (line: string) => setUpdateLog((current) => ({ ...current, logs: [...current.logs, line].slice(-800) }));
    source.addEventListener("log", (event) => {
      const payload = JSON.parse((event as MessageEvent).data) as { stream: string; line: string };
      appendLog(`[${payload.stream}] ${payload.line}`);
    });
    source.addEventListener("stage", (event) => {
      const payload = JSON.parse((event as MessageEvent).data) as { stage: SystemUpdateStage };
      setUpdateLog((current) => current.session ? ({
        ...current,
        session: {
          ...current.session,
          stages: current.session.stages.map((stage) => stage.key === payload.stage.key ? payload.stage : stage)
        }
      }) : current);
    });
    source.addEventListener("status", (event) => {
      const payload = JSON.parse((event as MessageEvent).data) as { status: string; session: SystemUpdateSession };
      setUpdateLog((current) => ({
        ...current,
        session: payload.session,
        checking: mode === "check" && payload.status === "running",
        running: mode === "run" && payload.status === "running"
      }));
      if (payload.status === "success" || payload.status === "error") {
        source.close();
        setActionPending("system:update", false);
      }
    });
    source.addEventListener("result", (event) => {
      const payload = JSON.parse((event as MessageEvent).data) as { result: SystemUpdateResult; requires_restart: boolean };
      setUpdateLog((current) => ({ ...current, result: payload.result, running: false, checking: false }));
      if (payload.requires_restart) {
        void escortRestart();
      } else {
        pushToast("success", "Обновление завершено", `Код выхода: ${payload.result.returncode}`);
        void loadAll();
      }
    });
    source.addEventListener("error", (event) => {
      const data = (event as MessageEvent).data;
      if (!data) {
        return;
      }
      const payload = JSON.parse(data) as { message: string; result?: SystemUpdateResult };
      setUpdateLog((current) => ({ ...current, error: payload.message, result: payload.result || current.result, running: false, checking: false }));
      pushToast("error", mode === "check" ? "Проверка обновлений не выполнена" : "Обновление завершилось с ошибкой", payload.message);
      source.close();
      setActionPending("system:update", false);
    });
  }

  async function runUpdateCheck() {
    if (pendingActionsRef.current.has("system:update")) {
      return;
    }
    setError(null);
    setActionPending("system:update", true);
    setUpdateLog((current) => ({ ...current, open: true, checking: true, running: false, logs: [], result: null, error: null, session: null }));
    try {
      const { session_id: sessionId } = await api.checkSystemUpdate();
      const session = await api.systemUpdateSession(sessionId);
      setUpdateLog((current) => ({ ...current, session }));
      attachUpdateEvents(sessionId, "check");
    } catch (exc) {
      const message = exc instanceof ApiError || exc instanceof Error ? exc.message : "Не удалось проверить обновления";
      setError(message);
      setUpdateLog((current) => ({ ...current, checking: false, running: false, error: message }));
      pushToast("error", "Не удалось проверить обновления", message);
      setActionPending("system:update", false);
    }
  }

  async function runManagedUpdate() {
    if (pendingActionsRef.current.has("system:update")) {
      return;
    }
    const sessionStages = updateLog.session?.stages || [];
    const stages = updateStageKeys.filter((stage) => updateLog.forceStages[stage] || sessionStages.some((item) => item.key === stage && item.needed));
    setError(null);
    setActionPending("system:update", true);
    setUpdateLog((current) => ({ ...current, open: true, running: true, checking: false, logs: [], result: null, error: null }));
    try {
      const { session_id: sessionId } = await api.runSystemUpdateSession(stages, updateStageKeys.filter((stage) => updateLog.forceStages[stage]));
      const session = await api.systemUpdateSession(sessionId);
      setUpdateLog((current) => ({ ...current, session }));
      attachUpdateEvents(sessionId, "run");
    } catch (exc) {
      const message = exc instanceof ApiError || exc instanceof Error ? exc.message : "Не удалось обновить приложение";
      setError(message);
      setUpdateLog((current) => ({ ...current, running: false, checking: false, error: message }));
      pushToast("error", "Не удалось обновить приложение", message);
      setActionPending("system:update", false);
    }
  }

  async function escortRestart() {
    setUpdateLog((current) => ({ ...current, escortingRestart: true, restartMessage: "Сервис перезапускается..." }));
    for (let attempt = 0; attempt < 40; attempt += 1) {
      await new Promise((resolve) => window.setTimeout(resolve, Math.min(1000 + attempt * 250, 4000)));
      try {
        await api.health();
        setUpdateLog((current) => ({ ...current, restartMessage: "Сервис доступен, проверяю сессию..." }));
        const nextBootstrap = await api.bootstrap();
        setCsrfCookieName(nextBootstrap.csrf_cookie_name);
        if (nextBootstrap.user) {
          setUser(nextBootstrap.user);
          setView("overview");
          window.location.assign("/");
        } else {
          setUser(null);
        }
        return;
      } catch {
        setUpdateLog((current) => ({ ...current, restartMessage: "Ожидаю healthcheck сервиса..." }));
      }
    }
    setUpdateLog((current) => ({ ...current, escortingRestart: false, restartMessage: "Сервис не ответил вовремя" }));
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
          setUpdateLog((current) => ({ ...current, open: true, result, logs: detail ? [detail] : current.logs }));
          void escortRestart();
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
        <UpdateControlDialog
          state={updateLog}
          onClose={() => setUpdateLog((current) => ({ ...current, open: false }))}
          onCheck={() => void runUpdateCheck()}
          onRun={() => void runManagedUpdate()}
          onForceChange={(stage, enabled) => setUpdateLog((current) => ({ ...current, forceStages: { ...current.forceStages, [stage]: enabled } }))}
          onToggleOptions={() => setUpdateLog((current) => ({ ...current, optionsOpen: !current.optionsOpen }))}
        />
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
      <UpdateControlDialog
        state={updateLog}
        onClose={() => setUpdateLog((current) => ({ ...current, open: false }))}
        onCheck={() => void runUpdateCheck()}
        onRun={() => void runManagedUpdate()}
        onForceChange={(stage, enabled) => setUpdateLog((current) => ({ ...current, forceStages: { ...current.forceStages, [stage]: enabled } }))}
        onToggleOptions={() => setUpdateLog((current) => ({ ...current, optionsOpen: !current.optionsOpen }))}
      />
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
            onConfirmUpdate={openUpdateDialog}
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
        {view === "services" ? <ConnectedServicesView services={connectedServices} /> : null}
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
        {view === "proxy" ? (
          <ProxyToolsView
            pendingKeys={pendingActions}
            pushToast={pushToast}
            runAction={runAction}
          />
        ) : null}
        {view === "pypi" ? (
          <PyPIView
            pendingKeys={pendingActions}
            pushToast={pushToast}
            runAction={runAction}
          />
        ) : null}
      </main>
    </div>
  );
}
