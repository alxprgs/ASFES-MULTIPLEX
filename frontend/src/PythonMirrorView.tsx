import { useState, useEffect, useCallback, useRef, FormEvent } from "react";
import {
  Download,
  RefreshCw,
  X,
  CheckCircle,
  Loader2,
  Trash2,
  ChevronDown,
  ChevronRight,
  Search,
  Cpu,
  HardDrive,
  Package,
  Layers,
  Zap
} from "lucide-react";
import {
  api,
  PythonMirrorStats,
  PythonMirrorListItem,
  PythonMirrorVersion,
  PythonMirrorFile,
  PythonMirrorJobStatus,
  PythonMirrorSuggestion
} from "./api";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

type ToastTone = "success" | "error" | "info" | "warning";

interface PythonMirrorViewProps {
  pendingKeys: ReadonlySet<string>;
  pushToast: (tone: ToastTone, title: string, message?: string) => void;
  runAction: (
    action: () => Promise<void>,
    options?: { pendingKey?: string; errorTitle?: string }
  ) => Promise<void>;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function osEmoji(os: string): string {
  const l = os.toLowerCase();
  if (l === "windows" || l === "win") return "🪟";
  if (l === "linux") return "🐧";
  if (l === "macos" || l === "darwin" || l === "mac") return "🍎";
  if (l === "source") return "📦";
  return "🖥️";
}

function statusBadgeClass(status: string): string {
  if (status === "running" || status === "pending") return "badge badge-warn";
  if (status === "done") return "badge badge-ok";
  if (status === "error") return "badge badge-danger";
  if (status === "cancelled") return "badge badge-muted";
  return "badge badge-muted";
}

function fmtEta(secs: number | null): string {
  if (secs === null) return "";
  if (secs < 60) return `~${secs}s`;
  const m = Math.floor(secs / 60);
  const s = secs % 60;
  return `~${m}m ${s}s`;
}

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

export function PythonMirrorView({ pendingKeys, pushToast, runAction }: PythonMirrorViewProps) {
  // Stats
  const [stats, setStats] = useState<PythonMirrorStats | null>(null);
  const [loadingStats, setLoadingStats] = useState(false);

  // Installed versions table
  const [versions, setVersions] = useState<PythonMirrorListItem[]>([]);
  const [loadingVersions, setLoadingVersions] = useState(false);
  const [expandedVersion, setExpandedVersion] = useState<string | null>(null);
  const [versionDetail, setVersionDetail] = useState<Record<string, PythonMirrorVersion>>({});
  const [loadingDetail, setLoadingDetail] = useState<Record<string, boolean>>({});

  // Download modal
  const [downloadModalOpen, setDownloadModalOpen] = useState(false);
  const [downloadVersions, setDownloadVersions] = useState<string[]>([]);
  const [downloadVersionInput, setDownloadVersionInput] = useState("");
  const [remoteVersions, setRemoteVersions] = useState<string[]>([]);
  const [loadingRemote, setLoadingRemote] = useState(false);

  // Smart Picker modal
  const [pickerOpen, setPickerOpen] = useState(false);
  const [pickerQuery, setPickerQuery] = useState("");
  const [pickerOs, setPickerOs] = useState("");
  const [pickerArch, setPickerArch] = useState("");
  const [pickerFileType, setPickerFileType] = useState("");
  const [pickerSuggestions, setPickerSuggestions] = useState<PythonMirrorSuggestion[]>([]);
  const [pickerBest, setPickerBest] = useState<PythonMirrorSuggestion | null>(null);
  const [pickerLoading, setPickerLoading] = useState(false);
  const pickerDebounce = useRef<ReturnType<typeof setTimeout> | null>(null);

  // Jobs
  const [activeJobs, setActiveJobs] = useState<PythonMirrorJobStatus[]>([]);
  const activeJobsRef = useRef<PythonMirrorJobStatus[]>([]);
  const wsRef = useRef<WebSocket | null>(null);
  const wsFailures = useRef<number>(0);
  const usingFallback = useRef<boolean>(false);
  const jobPollInterval = useRef<number | null>(null);

  // ---------------------------------------------------------------------------
  // Fetch helpers
  // ---------------------------------------------------------------------------

  const fetchStats = useCallback(async () => {
    setLoadingStats(true);
    try {
      const data = await api.pythonMirrorStats();
      setStats(data);
    } catch (err) {
      console.error("Failed to fetch Python Mirror stats:", err);
    } finally {
      setLoadingStats(false);
    }
  }, []);

  const fetchVersions = useCallback(async () => {
    setLoadingVersions(true);
    try {
      const data = await api.pythonMirrorVersions();
      setVersions(data.items);
    } catch (err) {
      console.error("Failed to fetch Python Mirror versions:", err);
    } finally {
      setLoadingVersions(false);
    }
  }, []);

  // Initial load
  useEffect(() => {
    void fetchStats();
    void fetchVersions();
  }, [fetchStats, fetchVersions]);

  // ---------------------------------------------------------------------------
  // Sync ref for WebSocket closure
  // ---------------------------------------------------------------------------

  useEffect(() => {
    activeJobsRef.current = activeJobs;
  }, [activeJobs]);

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      wsRef.current?.close();
      wsRef.current = null;
      if (jobPollInterval.current !== null) {
        window.clearInterval(jobPollInterval.current);
        jobPollInterval.current = null;
      }
    };
  }, []);

  // ---------------------------------------------------------------------------
  // Job update handler
  // ---------------------------------------------------------------------------

  const handleJobUpdates = useCallback(
    (updatedJobs: PythonMirrorJobStatus[]) => {
      const next = [...activeJobsRef.current];
      let changed = false;

      for (const st of updatedJobs) {
        const idx = next.findIndex((j) => j.job_id === st.job_id);
        if (st.status === "running" || st.status === "pending") {
          if (idx !== -1) {
            next[idx] = st;
          } else {
            next.push(st);
          }
          changed = true;
        } else {
          // finished
          if (idx !== -1) {
            next.splice(idx, 1);
            changed = true;
          }
          void fetchStats();
          void fetchVersions();
          if (st.status === "done") {
            pushToast("success", "Задача завершена", st.message || `Python ${st.version ?? ""} завершён`);
          } else if (st.status === "error") {
            pushToast("error", "Ошибка задачи", st.message || `Ошибка для версии ${st.version ?? ""}`);
          } else if (st.status === "cancelled") {
            pushToast("info", "Задача отменена", `Задача ${st.job_id} была отменена`);
          }
        }
      }

      if (changed) {
        setActiveJobs(next);
      }
    },
    [fetchStats, fetchVersions, pushToast]
  );

  // ---------------------------------------------------------------------------
  // Fallback REST polling
  // ---------------------------------------------------------------------------

  const pollJobsFallback = useCallback(async (ids: string[]) => {
    const result: PythonMirrorJobStatus[] = [];
    for (const id of ids) {
      try {
        const st = await api.pythonMirrorJobStatus(id);
        result.push(st);
      } catch {
        /* gone — ignore */
      }
    }
    return result;
  }, []);

  // ---------------------------------------------------------------------------
  // WebSocket job tracking
  // ---------------------------------------------------------------------------

  useEffect(() => {
    const ids = activeJobs.map((j) => j.job_id);

    if (ids.length === 0) {
      wsRef.current?.close();
      wsRef.current = null;
      if (jobPollInterval.current !== null) {
        window.clearInterval(jobPollInterval.current);
        jobPollInterval.current = null;
      }
      return;
    }

    if (usingFallback.current) {
      if (!jobPollInterval.current) {
        jobPollInterval.current = window.setInterval(() => {
          void pollJobsFallback(activeJobsRef.current.map((j) => j.job_id)).then(
            handleJobUpdates
          );
        }, 1500);
      }
      return;
    }

    if (!wsRef.current || wsRef.current.readyState === WebSocket.CLOSED) {
      const proto = window.location.protocol === "https:" ? "wss:" : "ws:";
      const ws = new WebSocket(`${proto}//${window.location.host}/api/python-mirror/jobs/ws`);
      wsRef.current = ws;

      ws.onopen = () => {
        wsFailures.current = 0;
        ws.send(
          JSON.stringify({ action: "subscribe", job_ids: activeJobsRef.current.map((j) => j.job_id) })
        );
      };

      ws.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data as string);
          if (data.type === "jobs_update" && Array.isArray(data.jobs)) {
            handleJobUpdates(data.jobs as PythonMirrorJobStatus[]);
          }
        } catch (e) {
          console.error("WS parse error", e);
        }
      };

      ws.onclose = () => {
        wsRef.current = null;
        if (activeJobsRef.current.length > 0) {
          wsFailures.current += 1;
          if (wsFailures.current >= 5) {
            console.warn("WebSocket failed 5 times, falling back to REST polling");
            usingFallback.current = true;
            setActiveJobs([...activeJobsRef.current]);
          } else {
            setTimeout(() => setActiveJobs([...activeJobsRef.current]), 1000);
          }
        }
      };
    } else if (wsRef.current.readyState === WebSocket.OPEN) {
      wsRef.current.send(JSON.stringify({ action: "subscribe", job_ids: ids }));
    }
  }, [activeJobs, pollJobsFallback, handleJobUpdates]);

  // ---------------------------------------------------------------------------
  // Add / remove active jobs
  // ---------------------------------------------------------------------------

  const addActiveJob = (job: PythonMirrorJobStatus) => {
    setActiveJobs((cur) => {
      if (cur.some((j) => j.job_id === job.job_id)) return cur;
      return [...cur, job];
    });
    pushToast("info", "Задача запущена", job.message || `Python ${job.version ?? ""} – ${job.kind}`);
  };

  // ---------------------------------------------------------------------------
  // Actions
  // ---------------------------------------------------------------------------

  const handleInstallVersion = async (v: string, closeAfter = true) => {
    await runAction(
      async () => {
        const job = await api.pythonMirrorInstall(v);
        addActiveJob(job);
        if (closeAfter) {
          setDownloadModalOpen(false);
          setDownloadVersions([]);
          setDownloadVersionInput("");
        }
      },
      { pendingKey: `pymir:install:${v}`, errorTitle: "Не удалось запустить установку" }
    );
  };

  const handleDeleteVersion = async (v: string) => {
    if (!window.confirm(`Удалить Python ${v} из зеркала?`)) return;
    await runAction(
      async () => {
        await api.pythonMirrorDelete(v);
        pushToast("success", "Версия удалена", `Python ${v} удален из зеркала`);
        void fetchStats();
        void fetchVersions();
        if (expandedVersion === v) setExpandedVersion(null);
      },
      { pendingKey: `pymir:delete:${v}`, errorTitle: "Не удалось удалить версию" }
    );
  };

  const handleVerifyVersion = async (v: string) => {
    await runAction(
      async () => {
        const job = await api.pythonMirrorVerify(v);
        addActiveJob(job);
      },
      { pendingKey: `pymir:verify:${v}`, errorTitle: "Не удалось запустить проверку" }
    );
  };

  const handleVerifyAll = async () => {
    await runAction(
      async () => {
        const job = await api.pythonMirrorVerify();
        addActiveJob(job);
      },
      { pendingKey: "pymir:verify-all", errorTitle: "Не удалось запустить глобальную проверку" }
    );
  };

  const handleCancelJob = async (id: string) => {
    await runAction(
      async () => {
        await api.pythonMirrorCancelJob(id);
        setActiveJobs((cur) => cur.filter((j) => j.job_id !== id));
        void fetchStats();
        pushToast("info", "Задача отменена");
      },
      { pendingKey: `pymir:cancel:${id}`, errorTitle: "Не удалось отменить задачу" }
    );
  };

  // ---------------------------------------------------------------------------
  // Expand version row → load file list
  // ---------------------------------------------------------------------------

  const handleToggleExpand = async (v: string) => {
    if (expandedVersion === v) {
      setExpandedVersion(null);
      return;
    }
    setExpandedVersion(v);
    if (versionDetail[v]) return; // already cached

    setLoadingDetail((d) => ({ ...d, [v]: true }));
    try {
      const detail = await api.pythonMirrorVersion(v);
      setVersionDetail((d) => ({ ...d, [v]: detail }));
    } catch (err) {
      pushToast("error", "Не удалось загрузить файлы версии", err instanceof Error ? err.message : String(err));
    } finally {
      setLoadingDetail((d) => ({ ...d, [v]: false }));
    }
  };

  // ---------------------------------------------------------------------------
  // Download modal
  // ---------------------------------------------------------------------------

  const handleOpenDownloadModal = async () => {
    setDownloadModalOpen(true);
    setDownloadVersions([]);
    setDownloadVersionInput("");
    setLoadingRemote(true);
    try {
      const res = await api.pythonMirrorRemoteVersions();
      setRemoteVersions(res.versions);
    } catch {
      pushToast("error", "Не удалось загрузить удаленные версии");
    } finally {
      setLoadingRemote(false);
    }
  };

  const handleDownloadSubmit = async (e: FormEvent) => {
    e.preventDefault();
    const versionsToInstall = new Set([...downloadVersions]);
    const inputVal = downloadVersionInput.trim();
    if (inputVal) {
      versionsToInstall.add(inputVal);
    }
    
    if (versionsToInstall.size === 0) return;
    
    for (const v of versionsToInstall) {
      await handleInstallVersion(v, false);
    }
    setDownloadModalOpen(false);
    setDownloadVersions([]);
    setDownloadVersionInput("");
  };

  // ---------------------------------------------------------------------------
  // Smart Picker debounce
  // ---------------------------------------------------------------------------

  useEffect(() => {
    if (!pickerOpen) return;
    if (pickerDebounce.current) clearTimeout(pickerDebounce.current);
    pickerDebounce.current = setTimeout(async () => {
      setPickerLoading(true);
      try {
        const res = await api.pythonMirrorSuggest({
          version_query: pickerQuery || undefined,
          os_type: pickerOs || undefined,
          arch: pickerArch || undefined,
          file_type: pickerFileType || undefined
        });
        setPickerSuggestions(res.suggestions);
        setPickerBest(res.best_match);
      } catch (err) {
        pushToast("error", "Ошибка подбора", err instanceof Error ? err.message : String(err));
      } finally {
        setPickerLoading(false);
      }
    }, 400);
    return () => {
      if (pickerDebounce.current) clearTimeout(pickerDebounce.current);
    };
  }, [pickerOpen, pickerQuery, pickerOs, pickerArch, pickerFileType, pushToast]);

  const handlePickerInstall = async (s: PythonMirrorSuggestion) => {
    await handleInstallVersion(s.version);
    setPickerOpen(false);
  };

  // ---------------------------------------------------------------------------
  // File download
  // ---------------------------------------------------------------------------

  const handleFileDownload = (version: string, filename: string) => {
    window.open(`/api/python-mirror/files/${encodeURIComponent(version)}/${encodeURIComponent(filename)}`, "_blank");
  };

  // ---------------------------------------------------------------------------
  // Render helpers
  // ---------------------------------------------------------------------------

  const renderStatCards = () => (
    <div
      style={{
        display: "grid",
        gridTemplateColumns: "repeat(auto-fit, minmax(160px, 1fr))",
        gap: "14px"
      }}
    >
      {[
        {
          icon: <Layers size={18} color="#0b5c76" />,
          label: "Сохранённые версии",
          value: stats?.versions_count ?? "—",
          accent: false
        },
        {
          icon: <Package size={18} color="#0b5c76" />,
          label: "Кэшированные файлы",
          value: stats?.files_count ?? "—",
          accent: false
        },
        {
          icon: <HardDrive size={18} color="#0b5c76" />,
          label: "Использовано места",
          value: stats?.total_size_human ?? "—",
          accent: false
        },
        {
          icon: <HardDrive size={18} color="#697782" />,
          label: "Свободно на диске",
          value: stats?.disk_free_human ?? "—",
          accent: false
        },
        {
          icon: <Zap size={18} color={stats?.active_jobs ? "#f59e0b" : "#0b5c76"} />,
          label: "Активные задачи",
          value: stats?.active_jobs ?? 0,
          accent: (stats?.active_jobs ?? 0) > 0
        }
      ].map((card) => (
        <div
          key={card.label}
          className="metric"
          style={{ borderLeft: card.accent ? "4px solid #f59e0b" : undefined }}
        >
          <div style={{ display: "flex", alignItems: "center", gap: "8px" }}>
            {card.icon}
            <span style={{ fontSize: "12px", textTransform: "uppercase", fontWeight: 700 }}>
              {card.label}
            </span>
          </div>
          <strong style={{ color: card.accent ? "#b45309" : undefined }}>{card.value}</strong>
        </div>
      ))}
    </div>
  );

  const renderFilesTable = (files: PythonMirrorFile[], version: string) => (
    <div style={{ marginTop: "10px", overflowX: "auto" }}>
      <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "13px" }}>
        <thead>
          <tr style={{ borderBottom: "2px solid #dfe6ea", color: "#697782" }}>
            <th style={{ padding: "8px 6px", textAlign: "left" }}>Имя</th>
            <th style={{ padding: "8px 6px", textAlign: "left" }}>ОС</th>
            <th style={{ padding: "8px 6px", textAlign: "left" }}>Архитектура</th>
            <th style={{ padding: "8px 6px", textAlign: "left" }}>Тип</th>
            <th style={{ padding: "8px 6px", textAlign: "left" }}>Размер</th>
            <th style={{ padding: "8px 6px", textAlign: "left" }}>Скачано</th>
            <th style={{ padding: "8px 6px", textAlign: "right" }}>Действия</th>
          </tr>
        </thead>
        <tbody>
          {files.map((f) => (
            <tr key={f.name} style={{ borderBottom: "1px solid #eef2f3" }}>
              <td style={{ padding: "8px 6px", fontFamily: "ui-monospace, monospace", fontSize: "12px" }}>
                {f.name}
              </td>
              <td style={{ padding: "8px 6px" }}>
                <span title={f.os_type}>{osEmoji(f.os_type)} {f.os_type}</span>
              </td>
              <td style={{ padding: "8px 6px" }}>{f.arch}</td>
              <td style={{ padding: "8px 6px" }}>
                <span className="badge badge-muted">{f.file_type}</span>
              </td>
              <td style={{ padding: "8px 6px" }}>{f.size_human}</td>
              <td style={{ padding: "8px 6px", color: "#697782", fontSize: "11px" }}>
                {f.downloaded_at ? new Date(f.downloaded_at).toLocaleString() : "—"}
              </td>
              <td style={{ padding: "8px 6px", textAlign: "right" }}>
                <button
                  className="icon-button"
                  title="Скачать файл"
                  onClick={() => handleFileDownload(version, f.name)}
                >
                  <Download size={13} />
                </button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );

  // ---------------------------------------------------------------------------
  // JSX
  // ---------------------------------------------------------------------------

  return (
    <section style={{ width: "100%", display: "flex", flexDirection: "column", gap: "20px" }}>

      {/* ── Header + Stats ─────────────────────────────────────────────────── */}
      <div className="panel" style={{ padding: "20px" }}>
        <div
          style={{
            display: "flex",
            justifyContent: "space-between",
            alignItems: "center",
            flexWrap: "wrap",
            gap: "14px",
            marginBottom: "18px"
          }}
        >
          <div>
            <h2>Зеркало дистрибутивов Python</h2>
            <p>Скачивание и предоставление установщиков CPython для изолированных сред</p>
          </div>
          <div style={{ display: "flex", gap: "10px", flexWrap: "wrap" }}>
            <button
              className="secondary-button"
              onClick={() => { void fetchStats(); void fetchVersions(); }}
              disabled={loadingStats}
            >
              <RefreshCw size={15} className={loadingStats ? "spin" : ""} />
              Обновить
            </button>
            <button
              className="secondary-button"
              onClick={handleVerifyAll}
              disabled={pendingKeys.has("pymir:verify-all")}
            >
              <CheckCircle size={15} />
              Проверить всё
            </button>
            <button
              className="secondary-button"
              onClick={handleOpenDownloadModal}
            >
              <Download size={15} />
              Скачать версию
            </button>
            <button
              className="primary-button"
              onClick={() => setPickerOpen(true)}
            >
              <Search size={15} />
              Умный подбор
            </button>
          </div>
        </div>

        {stats ? renderStatCards() : (
          <div style={{ textAlign: "center", padding: "20px", color: "#697782" }}>
            <Loader2 size={20} className="spin" style={{ marginBottom: "6px" }} />
            <div>Загрузка статистики зеркала…</div>
          </div>
        )}
      </div>

      {/* ── Active Jobs Panel ──────────────────────────────────────────────── */}
      {activeJobs.length > 0 && (
        <div className="panel" style={{ padding: "20px", borderLeft: "4px solid #f59e0b" }}>
          <h3 style={{ display: "flex", alignItems: "center", gap: "10px", margin: "0 0 15px 0", color: "#b45309" }}>
            <Loader2 className="spin" size={18} />
            Выполняются фоновые задачи…
          </h3>
          <div style={{ display: "flex", flexDirection: "column", gap: "14px" }}>
            {activeJobs.map((job) => (
              <div
                key={job.job_id}
                style={{
                  background: "#fffbeb",
                  padding: "14px",
                  borderRadius: "8px",
                  border: "1px solid #fef3c7"
                }}
              >
                <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: "8px" }}>
                  <div>
                    <strong style={{ fontSize: "14px", color: "#78350f" }}>
                      {job.kind === "install" && `Установка Python ${job.version ?? ""}`}
                      {job.kind === "verify" && `Проверка Python ${job.version ?? ""}`}
                      {job.kind === "verify_all" && "Глобальная проверка целостности"}
                      {!["install", "verify", "verify_all"].includes(job.kind) && job.kind}
                    </strong>
                    <div style={{ fontSize: "12px", color: "#b45309", marginTop: "2px" }}>
                      {job.current_file
                        ? <span style={{ fontFamily: "monospace" }}>{job.current_file}</span>
                        : job.message || "Подготовка…"}
                    </div>
                    {job.retry_count > 0 && (
                      <div style={{ fontSize: "11px", color: "#ef4444", marginTop: "2px" }}>
                        Попыток: {job.retry_count}
                      </div>
                    )}
                  </div>
                  <div style={{ display: "flex", alignItems: "center", gap: "8px" }}>
                    <span className={statusBadgeClass(job.status)}>{job.status}</span>
                    <button
                      className="icon-button danger-icon"
                      title="Отменить задачу"
                      onClick={() => void handleCancelJob(job.job_id)}
                    >
                      <X size={13} />
                    </button>
                  </div>
                </div>

                {/* Progress bar */}
                <div
                  style={{
                    width: "100%",
                    background: "#fef3c7",
                    height: "10px",
                    borderRadius: "5px",
                    overflow: "hidden",
                    marginBottom: "5px"
                  }}
                >
                  <div
                    style={{
                      width: `${job.progress_pct}%`,
                      background: "#f59e0b",
                      height: "100%",
                      transition: "width 0.4s ease"
                    }}
                  />
                </div>
                <div style={{ display: "flex", justifyContent: "space-between", fontSize: "11px", color: "#b45309" }}>
                  <span>
                    {job.done} / {job.total} файлов ({job.progress_pct}%)
                    {job.failed > 0 && <span style={{ color: "#ef4444", marginLeft: "8px" }}>✗ {job.failed} ошибок</span>}
                  </span>
                  {job.eta_seconds !== null && <span>Осталось {fmtEta(job.eta_seconds)}</span>}
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* ── Installed Versions Table ───────────────────────────────────────── */}
      <div className="panel" style={{ padding: "20px" }}>
        <div
          style={{
            display: "flex",
            justifyContent: "space-between",
            alignItems: "center",
            marginBottom: "16px"
          }}
        >
          <div>
            <h2>Установленные версии</h2>
            <p>Локально зеркалированные релизы Python</p>
          </div>
          <button
            className="icon-button"
            title="Обновить"
            onClick={() => void fetchVersions()}
            disabled={loadingVersions}
          >
            <RefreshCw size={15} className={loadingVersions ? "spin" : ""} />
          </button>
        </div>

        {loadingVersions && versions.length === 0 ? (
          <div style={{ textAlign: "center", padding: "40px", color: "#697782" }}>
            <Loader2 size={24} className="spin" style={{ marginBottom: "10px" }} />
            <div>Загрузка…</div>
          </div>
        ) : versions.length === 0 ? (
          <div style={{ textAlign: "center", padding: "40px", color: "#697782" }}>
            Нет зеркалированных версий Python.{" "}
            <button
              style={{ background: "none", border: "none", color: "#0b5c76", cursor: "pointer", fontWeight: 700 }}
              onClick={handleOpenDownloadModal}
            >
              Скачать сейчас →
            </button>
          </div>
        ) : (
          <div className="table" style={{ overflowX: "auto" }}>
            <table style={{ width: "100%", borderCollapse: "collapse" }}>
              <thead>
                <tr style={{ borderBottom: "2px solid #dfe6ea", color: "#697782", fontSize: "13px" }}>
                  <th style={{ padding: "10px 8px", textAlign: "left", width: "32px" }} />
                  <th style={{ padding: "10px 8px", textAlign: "left" }}>Версия</th>
                  <th style={{ padding: "10px 8px", textAlign: "left" }}>Файлы</th>
                  <th style={{ padding: "10px 8px", textAlign: "left" }}>Размер</th>
                  <th style={{ padding: "10px 8px", textAlign: "right" }}>Действия</th>
                </tr>
              </thead>
              <tbody>
                {versions.map((v) => (
                  <>
                    <tr
                      key={v.version}
                      style={{
                        borderBottom: expandedVersion === v.version ? "none" : "1px solid #eef2f3",
                        background: expandedVersion === v.version ? "#f0f9ff" : undefined
                      }}
                    >
                      {/* expand toggle */}
                      <td style={{ padding: "12px 8px" }}>
                        <button
                          className="icon-button"
                          style={{ width: "26px", height: "26px" }}
                          onClick={() => void handleToggleExpand(v.version)}
                          title="Показать файлы"
                        >
                          {expandedVersion === v.version
                            ? <ChevronDown size={13} />
                            : <ChevronRight size={13} />}
                        </button>
                      </td>
                      <td style={{ padding: "12px 8px", fontWeight: 700, color: "#1e293b" }}>
                        <span style={{ display: "flex", alignItems: "center", gap: "6px" }}>
                          <Cpu size={14} color="#0b5c76" />
                          Python {v.version}
                        </span>
                      </td>
                      <td style={{ padding: "12px 8px" }}>{v.files_count}</td>
                      <td style={{ padding: "12px 8px" }}>{v.total_size_human}</td>
                      <td style={{ padding: "12px 8px", textAlign: "right" }}>
                        <div style={{ display: "flex", gap: "5px", justifyContent: "flex-end" }}>
                          <button
                            className="icon-button"
                            title="Проверить целостность"
                            onClick={() => void handleVerifyVersion(v.version)}
                            disabled={pendingKeys.has(`pymir:verify:${v.version}`)}
                          >
                            <CheckCircle size={13} />
                          </button>
                          <button
                            className="icon-button danger-icon"
                            title="Удалить версию"
                            onClick={() => void handleDeleteVersion(v.version)}
                            disabled={pendingKeys.has(`pymir:delete:${v.version}`)}
                          >
                            <Trash2 size={13} />
                          </button>
                        </div>
                      </td>
                    </tr>

                    {/* Expanded detail row */}
                    {expandedVersion === v.version && (
                      <tr key={`${v.version}-detail`}>
                        <td
                          colSpan={5}
                          style={{
                            padding: "0 16px 16px 16px",
                            background: "#f0f9ff",
                            borderBottom: "1px solid #dfe6ea"
                          }}
                        >
                          {loadingDetail[v.version] ? (
                            <div style={{ padding: "20px", textAlign: "center", color: "#697782" }}>
                              <Loader2 size={18} className="spin" />
                            </div>
                          ) : versionDetail[v.version] ? (
                            renderFilesTable(versionDetail[v.version].files, v.version)
                          ) : null}
                        </td>
                      </tr>
                    )}
                  </>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      {/* ── Download Version Modal ─────────────────────────────────────────── */}
      {downloadModalOpen && (
        <div className="confirm-backdrop" onClick={(e) => { if (e.target === e.currentTarget) setDownloadModalOpen(false); }}>
          <div className="confirm-dialog" style={{ maxWidth: "480px" }}>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
              <h2>Скачать версию Python</h2>
              <button className="icon-button" onClick={() => setDownloadModalOpen(false)}>
                <X size={16} />
              </button>
            </div>

            <form onSubmit={handleDownloadSubmit} className="form-grid">
              <label>
                Версия Python (ручной ввод)
                <div style={{ display: "flex", gap: "8px" }}>
                  <input
                    type="text"
                    placeholder="например 3.12.3"
                    value={downloadVersionInput}
                    onChange={(e) => setDownloadVersionInput(e.target.value)}
                    list="remote-versions-list"
                  />
                  <datalist id="remote-versions-list">
                    {remoteVersions.map((rv) => (
                      <option key={rv} value={rv} />
                    ))}
                  </datalist>
                </div>
              </label>

              {loadingRemote && (
                <div style={{ display: "flex", alignItems: "center", gap: "6px", color: "#697782", fontSize: "13px" }}>
                  <Loader2 size={14} className="spin" />
                  Загрузка доступных версий с python.org…
                </div>
              )}

              {remoteVersions.length > 0 && (
                <div>
                  <div style={{ fontSize: "12px", color: "#697782", marginBottom: "8px" }}>
                    Или выберите одну или несколько версий для скачивания ({downloadVersions.length} выбрано):
                  </div>
                  <div
                    style={{
                      maxHeight: "180px",
                      overflowY: "auto",
                      display: "flex",
                      flexWrap: "wrap",
                      gap: "6px"
                    }}
                  >
                    {remoteVersions.map((rv) => {
                      const isSelected = downloadVersions.includes(rv);
                      return (
                        <button
                          key={rv}
                          type="button"
                          onClick={() => {
                            if (isSelected) {
                              setDownloadVersions(cur => cur.filter(x => x !== rv));
                            } else {
                              setDownloadVersions(cur => [...cur, rv]);
                            }
                          }}
                          style={{
                            padding: "4px 10px",
                            borderRadius: "6px",
                            border: "1px solid",
                            borderColor: isSelected ? "#0b5c76" : "#cbd5dc",
                            background: isSelected ? "#e0f2fe" : "#f8fafb",
                            color: isSelected ? "#0b5c76" : "#34424c",
                            fontSize: "12px",
                            fontWeight: isSelected ? 700 : 400,
                            cursor: "pointer",
                            display: "flex",
                            alignItems: "center",
                            gap: "4px"
                          }}
                        >
                          {isSelected && <CheckCircle size={12} />}
                          {rv}
                        </button>
                      );
                    })}
                  </div>
                </div>
              )}

              <div className="confirm-actions">
                <button type="button" className="secondary-button" onClick={() => setDownloadModalOpen(false)}>
                  Отмена
                </button>
                <button
                  type="submit"
                  className="primary-button"
                  disabled={downloadVersions.length === 0 && !downloadVersionInput.trim()}
                >
                  <Download size={15} />
                  Скачать ({downloadVersions.length + (downloadVersionInput.trim() && !downloadVersions.includes(downloadVersionInput.trim()) ? 1 : 0)})
                </button>
              </div>
            </form>
          </div>
        </div>
      )}

      {/* ── Smart Picker Modal ─────────────────────────────────────────────── */}
      {pickerOpen && (
        <div
          className="confirm-backdrop"
          onClick={(e) => { if (e.target === e.currentTarget) setPickerOpen(false); }}
        >
          <div
            className="update-log-dialog"
            style={{ maxWidth: "680px", overflowY: "auto" }}
          >
            <div className="update-log-head">
              <h2>Умный подбор Python</h2>
              <button className="icon-button" onClick={() => setPickerOpen(false)}>
                <X size={16} />
              </button>
            </div>

            {/* Filters */}
            <div
              style={{
                display: "grid",
                gridTemplateColumns: "1fr 1fr 1fr 1fr",
                gap: "10px"
              }}
            >
              <label>
                Запрос версии
                <input
                  type="text"
                  placeholder="3.12, latest…"
                  value={pickerQuery}
                  onChange={(e) => setPickerQuery(e.target.value)}
                />
              </label>
              <label>
                ОС
                <select
                  value={pickerOs}
                  onChange={(e) => setPickerOs(e.target.value)}
                  style={{
                    background: "#f8fafb",
                    border: "1px solid #cbd5dc",
                    borderRadius: "6px",
                    minHeight: "42px",
                    padding: "10px 12px",
                    width: "100%",
                    color: "#1f2937"
                  }}
                >
                  <option value="">Любая</option>
                  <option value="windows">🪟 Windows</option>
                  <option value="linux">🐧 Linux</option>
                  <option value="macos">🍎 macOS</option>
                  <option value="source">📦 Source</option>
                </select>
              </label>
              <label>
                Архитектура
                <select
                  value={pickerArch}
                  onChange={(e) => setPickerArch(e.target.value)}
                  style={{
                    background: "#f8fafb",
                    border: "1px solid #cbd5dc",
                    borderRadius: "6px",
                    minHeight: "42px",
                    padding: "10px 12px",
                    width: "100%",
                    color: "#1f2937"
                  }}
                >
                  <option value="">Любая</option>
                  <option value="amd64">x86-64 / amd64</option>
                  <option value="x86">x86 (32-бит)</option>
                  <option value="arm64">ARM64</option>
                  <option value="aarch64">aarch64</option>
                </select>
              </label>
              <label>
                Тип файла
                <select
                  value={pickerFileType}
                  onChange={(e) => setPickerFileType(e.target.value)}
                  style={{
                    background: "#f8fafb",
                    border: "1px solid #cbd5dc",
                    borderRadius: "6px",
                    minHeight: "42px",
                    padding: "10px 12px",
                    width: "100%",
                    color: "#1f2937"
                  }}
                >
                  <option value="">Любой</option>
                  <option value="installer">Установщик</option>
                  <option value="embeddable">Встраиваемый</option>
                  <option value="tarball">Tar-архив</option>
                  <option value="source">Исходный код</option>
                </select>
              </label>
            </div>

            {/* Best match highlight */}
            {pickerBest && (
              <div
                style={{
                  background: "#f0fdf4",
                  border: "2px solid #86efac",
                  borderRadius: "8px",
                  padding: "14px",
                  display: "flex",
                  justifyContent: "space-between",
                  alignItems: "center",
                  gap: "12px"
                }}
              >
                <div>
                  <div style={{ fontSize: "12px", color: "#166534", fontWeight: 700, marginBottom: "4px" }}>
                    ⭐ Лучшее совпадение
                  </div>
                  <div style={{ fontWeight: 700, color: "#14532d" }}>
                    Python {pickerBest.version} — {osEmoji(pickerBest.os_type)} {pickerBest.os_type} / {pickerBest.arch}
                  </div>
                  <div style={{ fontFamily: "monospace", fontSize: "12px", color: "#166534", marginTop: "2px" }}>
                    {pickerBest.filename}
                  </div>
                  {pickerBest.is_installed && (
                    <span className="badge badge-ok" style={{ marginTop: "6px" }}>Уже установлено</span>
                  )}
                </div>
                <button
                  className="primary-button"
                  disabled={pickerBest.is_installed}
                  onClick={() => void handlePickerInstall(pickerBest!)}
                >
                  <Download size={15} />
                  {pickerBest.is_installed ? "Установлено" : "Скачать"}
                </button>
              </div>
            )}

            {/* Suggestions list */}
            {pickerLoading ? (
              <div style={{ textAlign: "center", padding: "30px", color: "#697782" }}>
                <Loader2 size={20} className="spin" />
              </div>
            ) : pickerSuggestions.length === 0 ? (
              <div style={{ textAlign: "center", padding: "30px", color: "#697782" }}>
                Нет предложений. Попробуйте изменить фильтры.
              </div>
            ) : (
              <div style={{ display: "flex", flexDirection: "column", gap: "8px", maxHeight: "340px", overflowY: "auto" }}>
                {pickerSuggestions.map((s) => (
                  <div
                    key={`${s.version}-${s.filename}`}
                    style={{
                      display: "flex",
                      alignItems: "center",
                      justifyContent: "space-between",
                      gap: "12px",
                      padding: "10px 12px",
                      background: s.is_installed ? "#f0fdf4" : "#f8fafb",
                      border: "1px solid",
                      borderColor: s.is_installed ? "#bbf7d0" : "#dfe6ea",
                      borderRadius: "8px"
                    }}
                  >
                    <div style={{ minWidth: 0 }}>
                      <div style={{ fontWeight: 700, color: "#1e293b" }}>
                        {osEmoji(s.os_type)} Python {s.version} &nbsp;
                        <span className="badge badge-muted">{s.arch}</span>
                        &nbsp;
                        <span className="badge badge-muted">{s.file_type}</span>
                        {s.is_installed && (
                          <span className="badge badge-ok" style={{ marginLeft: "6px" }}>✓ установлено</span>
                        )}
                      </div>
                      <div
                        style={{
                          fontFamily: "monospace",
                          fontSize: "11px",
                          color: "#697782",
                          marginTop: "2px",
                          overflow: "hidden",
                          textOverflow: "ellipsis",
                          whiteSpace: "nowrap"
                        }}
                      >
                        {s.filename}
                      </div>
                    </div>
                    <button
                      className={s.is_installed ? "secondary-button" : "primary-button"}
                      style={{ flexShrink: 0 }}
                      disabled={s.is_installed}
                      onClick={() => void handlePickerInstall(s)}
                    >
                      {s.is_installed ? (
                        <><CheckCircle size={13} /> Готово</>
                      ) : (
                        <><Download size={13} /> Скачать</>
                      )}
                    </button>
                  </div>
                ))}
              </div>
            )}

            <div className="confirm-actions">
              <button className="secondary-button" onClick={() => setPickerOpen(false)}>
                Закрыть
              </button>
            </div>
          </div>
        </div>
      )}
    </section>
  );
}
