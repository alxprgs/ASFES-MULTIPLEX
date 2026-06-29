import { useState, useEffect, useCallback, useRef, FormEvent } from "react";
import {
  Database,
  Download,
  Play,
  Trash2,
  RefreshCw,
  X,
  Search,
  AlertTriangle,
  CheckCircle,
  Loader2,
  AlertOctagon,
  Eye,
  Ban,
  Unlock
} from "lucide-react";
import {
  api,
  PyPIStats,
  PyPIPackageListItem,
  PyPIJobStatus,
  PyPIPackage,
  PyPIBlocklist
} from "./api";

type ToastTone = "success" | "error" | "info" | "warning";

interface PyPIViewProps {
  pendingKeys: ReadonlySet<string>;
  pushToast: (tone: ToastTone, title: string, message?: string) => void;
  runAction: (action: () => Promise<void>, options?: { pendingKey?: string; errorTitle?: string }) => Promise<void>;
}

export function PyPIView({ pendingKeys, pushToast, runAction }: PyPIViewProps) {
  // Navigation tabs inside PyPI View
  const [activeTab, setActiveTab] = useState<"packages" | "install" | "blocklist">("packages");
  const [installMode, setInstallMode] = useState<"single" | "bulk">("single");

  // Stats
  const [stats, setStats] = useState<PyPIStats | null>(null);

  // Packages list
  const [packages, setPackages] = useState<PyPIPackageListItem[]>([]);
  const [searchQuery, setSearchQuery] = useState("");
  const [page, setPage] = useState(1);
  const [totalPackages, setTotalPackages] = useState(0);
  const perPage = 15;

  // Blocklist
  const [blocklist, setBlocklist] = useState<PyPIBlocklist | null>(null);

  // Single Install form states
  const [pkgName, setPkgName] = useState("");
  const [pkgVersion, setPkgVersion] = useState("");
  const [withDependencies, setWithDependencies] = useState(false);

  // Bulk Install form states
  const [bulkText, setBulkText] = useState("");

  // Active jobs tracking
  const [activeJobs, setActiveJobs] = useState<PyPIJobStatus[]>([]);

  // Package detail modal
  const [selectedPkg, setSelectedPkg] = useState<PyPIPackage | null>(null);
  const [detailModalOpen, setDetailModalOpen] = useState(false);
  const [pkgDetailLoading, setPkgDetailLoading] = useState(false);

  // Loading states
  const [loadingStats, setLoadingStats] = useState(false);
  const [loadingPackages, setLoadingPackages] = useState(false);
  const [loadingBlocklist, setLoadingBlocklist] = useState(false);

  // Fetch all basic stats & list
  const fetchStats = useCallback(async () => {
    setLoadingStats(true);
    try {
      const data = await api.pypiStats();
      setStats(data);
    } catch (err) {
      console.error("Failed to fetch PyPI stats:", err);
    } finally {
      setLoadingStats(false);
    }
  }, []);

  const fetchPackages = useCallback(async (query = searchQuery, pageNum = page) => {
    setLoadingPackages(true);
    try {
      const data = await api.pypiPackages({ search: query, page: pageNum, per_page: perPage });
      setPackages(data.items);
      setTotalPackages(data.total);
    } catch (err) {
      console.error("Failed to fetch PyPI packages:", err);
    } finally {
      setLoadingPackages(false);
    }
  }, [page, searchQuery]);

  const fetchBlocklist = useCallback(async () => {
    setLoadingBlocklist(true);
    try {
      const data = await api.pypiGetBlocklist();
      setBlocklist(data);
    } catch (err) {
      console.error("Failed to fetch PyPI blocklist:", err);
    } finally {
      setLoadingBlocklist(false);
    }
  }, []);

  // Poll for active jobs via WS or fallback
  const jobPollInterval = useRef<number | null>(null);
  const wsRef = useRef<WebSocket | null>(null);
  const wsFailures = useRef<number>(0);
  const usingFallback = useRef<boolean>(false);

  const activeJobsRef = useRef(activeJobs);
  useEffect(() => {
    activeJobsRef.current = activeJobs;
  }, [activeJobs]);

  useEffect(() => {
    return () => {
      if (wsRef.current) {
        wsRef.current.close();
        wsRef.current = null;
      }
      if (jobPollInterval.current) {
        window.clearInterval(jobPollInterval.current);
        jobPollInterval.current = null;
      }
    };
  }, []);

  const pollJobsFallback = useCallback(async (ids: string[]) => {
    const updatedJobs: PyPIJobStatus[] = [];
    for (const id of ids) {
      try {
        const status = await api.pypiJobStatus(id);
        updatedJobs.push(status);
      } catch {
        // Assume gone
      }
    }
    return updatedJobs;
  }, []);

  const handleJobUpdates = useCallback((updatedJobs: PyPIJobStatus[]) => {
    const nextJobs = [...activeJobsRef.current];
    let changed = false;

    for (const st of updatedJobs) {
      const idx = nextJobs.findIndex(j => j.job_id === st.job_id);
      if (st.status === "running" || st.status === "pending") {
        if (idx !== -1) {
          nextJobs[idx] = st;
        } else {
          nextJobs.push(st);
        }
        changed = true;
      } else {
        // Finished
        if (idx !== -1) {
          nextJobs.splice(idx, 1);
          changed = true;
        }
        void fetchStats();
        void fetchPackages();
        if (st.status === "done") {
          pushToast("success", `Задача выполнена`, st.message || `Успешно завершено: ${st.name}`);
        } else if (st.status === "error") {
          pushToast("error", `Ошибка задачи`, st.message || `Ошибка выполнения для: ${st.name}`);
        } else if (st.status === "cancelled") {
          pushToast("info", `Задача отменена`, `Задача ${st.name || st.job_id} была отменена`);
        }
      }
    }

    if (changed) {
      setActiveJobs(nextJobs);
    }
  }, [fetchStats, fetchPackages, pushToast]);

  useEffect(() => {
    const currentActiveIds = activeJobs.map(j => j.job_id);

    if (currentActiveIds.length === 0) {
      if (wsRef.current) {
        wsRef.current.close();
        wsRef.current = null;
      }
      if (jobPollInterval.current) {
        window.clearInterval(jobPollInterval.current);
        jobPollInterval.current = null;
      }
      return;
    }

    if (usingFallback.current) {
      if (!jobPollInterval.current) {
        jobPollInterval.current = window.setInterval(() => {
          void pollJobsFallback(activeJobsRef.current.map(j => j.job_id)).then(handleJobUpdates);
        }, 1500);
      }
      return;
    }

    // WebSocket logic
    if (!wsRef.current || wsRef.current.readyState === WebSocket.CLOSED) {
      const wsUrl = `${window.location.protocol === "https:" ? "wss:" : "ws:"}//${window.location.host}/api/pypi/jobs/ws`;
      const ws = new WebSocket(wsUrl);
      wsRef.current = ws;

      ws.onopen = () => {
        wsFailures.current = 0;
        ws.send(JSON.stringify({ action: "subscribe", job_ids: activeJobsRef.current.map(j => j.job_id) }));
      };

      ws.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data);
          if (data.type === "jobs_update" && Array.isArray(data.jobs)) {
            handleJobUpdates(data.jobs);
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
            setActiveJobs([...activeJobsRef.current]); // trigger effect re-run
          } else {
            setTimeout(() => {
              setActiveJobs([...activeJobsRef.current]); // trigger reconnect
            }, 1000);
          }
        }
      };
    } else if (wsRef.current.readyState === WebSocket.OPEN) {
      wsRef.current.send(JSON.stringify({ action: "subscribe", job_ids: currentActiveIds }));
    }
  }, [activeJobs, pollJobsFallback, handleJobUpdates]);

  const addActiveJob = (job: PyPIJobStatus) => {
    setActiveJobs(current => {
      if (current.some(j => j.job_id === job.job_id)) return current;
      return [...current, job];
    });
    pushToast("info", "Задача запущена в фоне", job.message || `Запущен процесс: ${job.kind}`);
  };

  // Initial loads
  useEffect(() => {
    void fetchStats();
    void fetchPackages();
    void fetchBlocklist();
  }, [fetchStats, fetchPackages, fetchBlocklist]);

  // Handle Search input
  const handleSearchChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const val = e.target.value;
    setSearchQuery(val);
    setPage(1);
    void fetchPackages(val, 1);
  };

  // Single Install Submit
  const handleSingleInstall = async (e: FormEvent) => {
    e.preventDefault();
    if (!pkgName.trim()) return;

    await runAction(async () => {
      const job = await api.pypiInstall(pkgName.trim(), pkgVersion.trim() || undefined, withDependencies);
      addActiveJob(job);
      setPkgName("");
      setPkgVersion("");
    }, { pendingKey: "pypi:install", errorTitle: "Не удалось запустить установку" });
  };

  // Bulk Install Submit
  const handleBulkInstall = async (e: FormEvent) => {
    e.preventDefault();
    const lines = bulkText.split("\n").map(l => l.trim()).filter(Boolean);
    if (lines.length === 0) return;

    await runAction(async () => {
      const job = await api.pypiBulkInstall(lines, withDependencies);
      addActiveJob(job);
      setBulkText("");
      setActiveTab("packages");
    }, { pendingKey: "pypi:bulk-install", errorTitle: "Не удалось запустить массовую установку" });
  };

  // Cancel Job
  const handleCancelJob = async (jobId: string) => {
    await runAction(async () => {
      await api.pypiCancelJob(jobId);
      setActiveJobs(current => current.filter(j => j.job_id !== jobId));
      void fetchStats();
      void fetchPackages();
      pushToast("info", "Задача отменена");
    }, { pendingKey: `pypi:cancel:${jobId}`, errorTitle: "Не удалось отменить задачу" });
  };

  // Block package/version
  const handleBlockPackage = async (name: string, block: boolean) => {
    await runAction(async () => {
      if (block) {
        await api.pypiBlock(name);
        pushToast("success", "Пакет заблокирован", `${name} добавлен в черный список`);
      } else {
        await api.pypiUnblockPackage(name);
        pushToast("success", "Пакет разблокирован", `${name} удален из черного списка`);
      }
      void fetchBlocklist();
      void fetchPackages();
      if (selectedPkg && selectedPkg.name === name) {
        void handleViewPackageDetail(name);
      }
    }, { pendingKey: `pypi:block:${name}`, errorTitle: "Не удалось изменить статус блокировки" });
  };

  const handleBlockVersion = async (name: string, version: string, block: boolean) => {
    await runAction(async () => {
      if (block) {
        await api.pypiBlock(name, version);
        pushToast("success", "Версия заблокирована", `${name}==${version} добавлена в черный список`);
      } else {
        await api.pypiUnblockVersion(name, version);
        pushToast("success", "Версия разблокирована", `${name}==${version} удалена из черного списка`);
      }
      void fetchBlocklist();
      if (selectedPkg && selectedPkg.name === name) {
        void handleViewPackageDetail(name);
      }
    }, { pendingKey: `pypi:block-ver:${name}:${version}`, errorTitle: "Не удалось заблокировать версию" });
  };

  // Delete package/version
  const handleDeletePackage = async (name: string) => {
    if (!window.confirm(`Вы уверены, что хотите удалить пакет ${name} и все его версии с зеркала?`)) {
      return;
    }
    await runAction(async () => {
      await api.pypiDeletePackage(name);
      pushToast("success", "Пакет удален", `Пакет ${name} успешно удален с зеркала`);
      void fetchStats();
      void fetchPackages();
      if (detailModalOpen && selectedPkg?.name === name) {
        setDetailModalOpen(false);
      }
    }, { pendingKey: `pypi:delete:${name}`, errorTitle: "Не удалось удалить пакет" });
  };

  const handleDeleteVersion = async (name: string, version: string) => {
    if (!window.confirm(`Вы уверены, что хотите удалить версию ${version} пакета ${name}?`)) {
      return;
    }
    await runAction(async () => {
      await api.pypiDeleteVersion(name, version);
      pushToast("success", "Версия удалена", `Версия ${version} пакета ${name} удалена`);
      void fetchStats();
      void handleViewPackageDetail(name);
    }, { pendingKey: `pypi:delete-ver:${name}:${version}`, errorTitle: "Не удалось удалить версию" });
  };

  // Verify Package
  const handleVerifyPackage = async (name: string) => {
    await runAction(async () => {
      const job = await api.pypiVerify(name);
      addActiveJob(job);
    }, { pendingKey: `pypi:verify:${name}`, errorTitle: "Не удалось запустить проверку пакета" });
  };

  // Verify All Storage
  const handleVerifyAll = async () => {
    await runAction(async () => {
      const job = await api.pypiVerify();
      addActiveJob(job);
    }, { pendingKey: "pypi:verify-all", errorTitle: "Не удалось запустить глобальную проверку" });
  };

  // Bulk Refresh Storage
  const handleBulkRefresh = async () => {
    if (!window.confirm("Запустить полное обновление/перескачивание всех локально сохраненных пакетов? Это может занять продолжительное время.")) {
      return;
    }
    await runAction(async () => {
      const job = await api.pypiBulkDownload();
      addActiveJob(job);
    }, { pendingKey: "pypi:bulk-download", errorTitle: "Не удалось запустить обновление хранилища" });
  };

  // View Package Detail
  const handleViewPackageDetail = async (name: string) => {
    setPkgDetailLoading(true);
    setDetailModalOpen(true);
    try {
      const pkg = await api.pypiPackage(name);
      setSelectedPkg(pkg);
    } catch (err) {
      pushToast("error", "Не удалось загрузить детали пакета", err instanceof Error ? err.message : String(err));
      setDetailModalOpen(false);
    } finally {
      setPkgDetailLoading(false);
    }
  };

  // Pagination helpers
  const totalPages = Math.ceil(totalPackages / perPage);
  const handlePrevPage = () => {
    if (page > 1) {
      setPage(page - 1);
      void fetchPackages(searchQuery, page - 1);
    }
  };
  const handleNextPage = () => {
    if (page < totalPages) {
      setPage(page + 1);
      void fetchPackages(searchQuery, page + 1);
    }
  };

  return (
    <section className="page-grid" style={{ width: "100%", display: "flex", flexDirection: "column", gap: "20px" }}>
      
      {/* 1. Statistics Cards & Action Panel */}
      <div className="panel span-2" style={{ padding: "20px" }}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", flexWrap: "wrap", gap: "15px", marginBottom: "20px" }}>
          <div>
            <h2>Локальное PyPI Зеркало</h2>
            <p>Управление пакетами, прозрачное кеширование и блокировка дистрибутивов</p>
          </div>
          <div style={{ display: "flex", gap: "10px" }}>
            <button
              className="secondary-button"
              onClick={() => void fetchStats()}
              title="Обновить статистику"
              disabled={loadingStats}
            >
              <RefreshCw size={16} className={loadingStats ? "spin" : ""} />
              Обновить
            </button>
            <button
              className="secondary-button"
              onClick={handleBulkRefresh}
              title="Докачать/обновить все версии локальных пакетов"
            >
              <Download size={16} />
              Обновить хранилище
            </button>
            <button
              className="secondary-button"
              onClick={handleVerifyAll}
              title="Проверить хэши и целостность всех файлов"
            >
              <CheckCircle size={16} />
              Проверить целостность
            </button>
          </div>
        </div>

        {stats ? (
          <div className="stats-row" style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))", gap: "15px" }}>
            <div className="stat-card" style={{ padding: "15px", background: "#f8fafb", borderRadius: "8px", border: "1px solid #dfe6ea" }}>
              <small style={{ color: "#697782", textTransform: "uppercase", fontSize: "11px", fontWeight: "bold" }}>Библиотек</small>
              <div style={{ fontSize: "24px", fontWeight: "bold", marginTop: "5px", color: "#1e293b" }}>{stats.packages_count}</div>
            </div>
            <div className="stat-card" style={{ padding: "15px", background: "#f8fafb", borderRadius: "8px", border: "1px solid #dfe6ea" }}>
              <small style={{ color: "#697782", textTransform: "uppercase", fontSize: "11px", fontWeight: "bold" }}>Версий всего</small>
              <div style={{ fontSize: "24px", fontWeight: "bold", marginTop: "5px", color: "#1e293b" }}>{stats.versions_count}</div>
            </div>
            <div className="stat-card" style={{ padding: "15px", background: "#f8fafb", borderRadius: "8px", border: "1px solid #dfe6ea" }}>
              <small style={{ color: "#697782", textTransform: "uppercase", fontSize: "11px", fontWeight: "bold" }}>Размер кэша</small>
              <div style={{ fontSize: "24px", fontWeight: "bold", marginTop: "5px", color: "#1e293b" }}>{stats.total_size_human}</div>
            </div>
            <div className="stat-card" style={{ padding: "15px", background: "#f8fafb", borderRadius: "8px", border: "1px solid #dfe6ea" }}>
              <small style={{ color: "#697782", textTransform: "uppercase", fontSize: "11px", fontWeight: "bold" }}>Заблокировано пакетов</small>
              <div style={{ fontSize: "24px", fontWeight: "bold", marginTop: "5px", color: stats.blocked_packages > 0 ? "#df4d4d" : "#1e293b" }}>
                {stats.blocked_packages}
              </div>
            </div>
            <div className="stat-card" style={{ padding: "15px", background: "#f8fafb", borderRadius: "8px", border: "1px solid #dfe6ea" }}>
              <small style={{ color: "#697782", textTransform: "uppercase", fontSize: "11px", fontWeight: "bold" }}>Активные задачи</small>
              <div style={{ fontSize: "24px", fontWeight: "bold", marginTop: "5px", color: stats.active_jobs > 0 ? "#f59e0b" : "#1e293b" }}>
                {stats.active_jobs}
              </div>
            </div>
          </div>
        ) : (
          <div style={{ textAlign: "center", padding: "20px", color: "#697782" }}>Загрузка статистики зеркала...</div>
        )}
      </div>

      {/* 2. Active Background Jobs Panel (Visible only when there are active jobs) */}
      {activeJobs.length > 0 && (
        <div className="panel span-2" style={{ padding: "20px", borderLeft: "4px solid #f59e0b" }}>
          <h3 style={{ display: "flex", alignItems: "center", gap: "10px", margin: "0 0 15px 0", color: "#b45309" }}>
            <Loader2 className="spin" size={18} />
            Выполняются фоновые задачи скачивания/проверки
          </h3>
          <div style={{ display: "flex", flexDirection: "column", gap: "15px" }}>
            {activeJobs.map(job => (
              <div key={job.job_id} style={{ background: "#fffbeb", padding: "15px", borderRadius: "8px", border: "1px solid #fef3c7" }}>
                <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "8px" }}>
                  <div>
                    <strong style={{ fontSize: "14px", color: "#78350f" }}>
                      {job.kind === "install" && `Установка ${job.name}`}
                      {job.kind === "install_all" && `Скачивание всех версий: ${job.name}`}
                      {job.kind === "bulk_install" && `Массовая установка пакетов`}
                      {job.kind === "bulk_download" && `Обновление локального кэша`}
                      {job.kind === "verify" && `Проверка целостности ${job.name}`}
                      {job.kind === "verify_all" && `Глобальная проверка целостности`}
                    </strong>
                    <div style={{ fontSize: "12px", color: "#b45309", marginTop: "2px" }}>
                      {job.message || "Подготовка..."}
                    </div>
                  </div>
                  <button
                    className="icon-button danger-icon"
                    onClick={() => void handleCancelJob(job.job_id)}
                    title="Отменить задачу"
                  >
                    <X size={14} /> Cancel
                  </button>
                </div>

                {/* Progress bar */}
                <div style={{ width: "100%", background: "#fef3c7", height: "10px", borderRadius: "5px", overflow: "hidden", marginBottom: "5px" }}>
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
                    Выполнено: {job.done} из {job.total} файлов/пакетов ({job.progress_pct}%)
                  </span>
                  {job.eta_seconds !== null && (
                    <span>Осталось примерно: {job.eta_seconds} сек.</span>
                  )}
                </div>

                {job.remaining_packages && job.remaining_packages.length > 0 && (
                  <div style={{ marginTop: "8px", fontSize: "11px", color: "#d97706" }}>
                    <strong>Очередь:</strong> {job.remaining_packages.slice(0, 5).join(", ")}
                    {job.remaining_packages.length > 5 && ` ... и еще ${job.remaining_packages.length - 5}`}
                  </div>
                )}
              </div>
            ))}
          </div>
        </div>
      )}

      {/* 3. Main Views Grid (Panels list & Install Side form) */}
      <div style={{ display: "grid", gridTemplateColumns: "2fr 1fr", gap: "20px", width: "100%" }}>
        
        {/* Left Side: Packages Directory / Blocklist */}
        <div className="panel" style={{ padding: "20px" }}>
          
          {/* Tabs header */}
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", borderBottom: "1px solid #dfe6ea", paddingBottom: "15px", marginBottom: "15px" }}>
            <div className="segmented" style={{ display: "flex", gap: "4px" }}>
              <button
                className={activeTab === "packages" ? "active" : ""}
                onClick={() => { setActiveTab("packages"); void fetchPackages(); }}
              >
                Локальные пакеты
              </button>
              <button
                className={activeTab === "blocklist" ? "active" : ""}
                onClick={() => { setActiveTab("blocklist"); void fetchBlocklist(); }}
              >
                Черный список
              </button>
            </div>

            {activeTab === "packages" && (
              <div style={{ display: "flex", alignItems: "center", gap: "8px", position: "relative" }}>
                <Search size={16} style={{ position: "absolute", left: "10px", color: "#697782" }} />
                <input
                  type="text"
                  placeholder="Поиск по имени..."
                  value={searchQuery}
                  onChange={handleSearchChange}
                  style={{
                    paddingLeft: "30px",
                    height: "34px",
                    width: "200px",
                    borderRadius: "6px",
                    border: "1px solid #dfe6ea"
                  }}
                />
              </div>
            )}
          </div>

          {/* Tab 1: Packages List */}
          {activeTab === "packages" && (
            <div>
              {loadingPackages && packages.length === 0 ? (
                <div style={{ textAlign: "center", padding: "40px", color: "#697782" }}>
                  <Loader2 className="spin" size={24} style={{ margin: "0 auto 10px" }} />
                  Загрузка пакетов...
                </div>
              ) : packages.length === 0 ? (
                <div style={{ textAlign: "center", padding: "40px", color: "#697782" }}>
                  Нет установленных пакетов. Установите библиотеку через панель справа или с помощью pip.
                </div>
              ) : (
                <div className="table-wrapper" style={{ overflowX: "auto" }}>
                  <table style={{ width: "100%", borderCollapse: "collapse" }}>
                    <thead>
                      <tr style={{ borderBottom: "2px solid #dfe6ea", textAlign: "left", color: "#697782" }}>
                        <th style={{ padding: "10px 8px" }}>Имя пакета</th>
                        <th style={{ padding: "10px 8px" }}>Версий локально</th>
                        <th style={{ padding: "10px 8px" }}>Последняя</th>
                        <th style={{ padding: "10px 8px" }}>Размер</th>
                        <th style={{ padding: "10px 8px" }}>Статус</th>
                        <th style={{ padding: "10px 8px", textAlign: "right" }}>Действия</th>
                      </tr>
                    </thead>
                    <tbody>
                      {packages.map(pkg => (
                        <tr key={pkg.name} style={{ borderBottom: "1px solid #eef2f3" }}>
                          <td style={{ padding: "12px 8px", fontWeight: "bold" }}>
                            <span
                              onClick={() => void handleViewPackageDetail(pkg.name)}
                              style={{ color: "#1d4ed8", cursor: "pointer", textDecoration: "underline" }}
                            >
                              {pkg.name}
                            </span>
                          </td>
                          <td style={{ padding: "12px 8px" }}>{pkg.versions_count}</td>
                          <td style={{ padding: "12px 8px" }}>{pkg.latest_version || "—"}</td>
                          <td style={{ padding: "12px 8px" }}>{pkg.total_size_human}</td>
                          <td style={{ padding: "12px 8px" }}>
                            {pkg.is_blocked ? (
                              <span className="badge badge-danger">Заблокирован</span>
                            ) : pkg.has_blocked_versions ? (
                              <span className="badge badge-warn">Частичный блок</span>
                            ) : (
                              <span className="badge badge-ok">Активен</span>
                            )}
                          </td>
                          <td style={{ padding: "12px 8px", textAlign: "right" }}>
                            <div style={{ display: "flex", gap: "5px", justifyContent: "flex-end" }}>
                              <button
                                className="icon-button"
                                title="Подробности и версии"
                                onClick={() => void handleViewPackageDetail(pkg.name)}
                              >
                                <Eye size={14} />
                              </button>
                              <button
                                className="icon-button"
                                title="Проверить целостность файлов"
                                onClick={() => void handleVerifyPackage(pkg.name)}
                              >
                                <CheckCircle size={14} />
                              </button>
                              <button
                                className={`icon-button ${pkg.is_blocked ? "success-icon" : "danger-icon"}`}
                                title={pkg.is_blocked ? "Разблокировать пакет" : "Заблокировать пакет"}
                                onClick={() => void handleBlockPackage(pkg.name, !pkg.is_blocked)}
                              >
                                {pkg.is_blocked ? <Unlock size={14} /> : <Ban size={14} />}
                              </button>
                              <button
                                className="icon-button danger-icon"
                                title="Удалить пакет полностью"
                                onClick={() => void handleDeletePackage(pkg.name)}
                              >
                                <Trash2 size={14} />
                              </button>
                            </div>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>

                  {/* Pagination footer */}
                  {totalPages > 1 && (
                    <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginTop: "15px" }}>
                      <span style={{ fontSize: "13px", color: "#697782" }}>
                        Страница {page} из {totalPages} (всего пакетов: {totalPackages})
                      </span>
                      <div style={{ display: "flex", gap: "5px" }}>
                        <button className="secondary-button" onClick={handlePrevPage} disabled={page <= 1}>Назад</button>
                        <button className="secondary-button" onClick={handleNextPage} disabled={page >= totalPages}>Вперед</button>
                      </div>
                    </div>
                  )}
                </div>
              )}
            </div>
          )}

          {/* Tab 2: Blocklist View */}
          {activeTab === "blocklist" && (
            <div>
              {loadingBlocklist ? (
                <div style={{ textAlign: "center", padding: "40px", color: "#697782" }}>Загрузка черного списка...</div>
              ) : !blocklist || (blocklist.blocked_packages.length === 0 && Object.keys(blocklist.blocked_versions).length === 0) ? (
                <div style={{ textAlign: "center", padding: "40px", color: "#697782" }}>
                  Черный список пуст. Вы можете заблокировать нежелательные пакеты с помощью действий в таблице или формы блокировок.
                </div>
              ) : (
                <div style={{ display: "flex", flexDirection: "column", gap: "20px" }}>
                  
                  {/* Blocked Packages Section */}
                  {blocklist.blocked_packages.length > 0 && (
                    <div>
                      <h4>Полностью заблокированные пакеты</h4>
                      <p style={{ fontSize: "12px", color: "#697782", marginBottom: "10px" }}>
                        Запросы к этим пакетам через Simple API отдадут HTTP 404. Скачивание новых версий запрещено.
                      </p>
                      <div style={{ display: "flex", flexWrap: "wrap", gap: "8px" }}>
                        {blocklist.blocked_packages.map(pkg => (
                          <div key={pkg} style={{ display: "inline-flex", alignItems: "center", gap: "8px", background: "#fee2e2", border: "1px solid #fca5a5", color: "#991b1b", padding: "6px 12px", borderRadius: "20px", fontSize: "13px", fontWeight: "bold" }}>
                            <span>{pkg}</span>
                            <button
                              onClick={() => void handleBlockPackage(pkg, false)}
                              title="Разблокировать"
                              style={{ background: "none", border: "none", cursor: "pointer", color: "#991b1b", display: "inline-flex" }}
                            >
                              <X size={14} />
                            </button>
                          </div>
                        ))}
                      </div>
                    </div>
                  )}

                  {/* Blocked Versions Section */}
                  {Object.keys(blocklist.blocked_versions).length > 0 && (
                    <div>
                      <h4>Блокировка отдельных версий</h4>
                      <p style={{ fontSize: "12px", color: "#697782", marginBottom: "10px" }}>
                        Перечисленные версии скрыты из Simple API и недоступны для скачивания.
                      </p>
                      <div style={{ display: "flex", flexDirection: "column", gap: "10px" }}>
                        {Object.entries(blocklist.blocked_versions).map(([pkg, versions]) => (
                          <div key={pkg} style={{ padding: "10px", background: "#f8fafb", border: "1px solid #dfe6ea", borderRadius: "6px" }}>
                            <strong style={{ fontSize: "13px" }}>{pkg}:</strong>
                            <div style={{ display: "flex", flexWrap: "wrap", gap: "6px", marginTop: "6px" }}>
                              {versions.map(ver => (
                                <span key={ver} style={{ display: "inline-flex", alignItems: "center", gap: "6px", background: "#ffedd5", border: "1px solid #fed7aa", color: "#c2410c", padding: "4px 8px", borderRadius: "4px", fontSize: "12px" }}>
                                  <span>{ver}</span>
                                  <button
                                    onClick={() => void handleBlockVersion(pkg, ver, false)}
                                    title="Разблокировать версию"
                                    style={{ background: "none", border: "none", cursor: "pointer", color: "#c2410c", display: "inline-flex" }}
                                  >
                                    <X size={12} />
                                  </button>
                                </span>
                              ))}
                            </div>
                          </div>
                        ))}
                      </div>
                    </div>
                  )}
                </div>
              )}
            </div>
          )}

        </div>

        {/* Right Side: Install Packages Panel */}
        <div className="panel" style={{ padding: "20px", height: "fit-content" }}>
          <div className="panel-head" style={{ display: "flex", justifyContent: "space-between", marginBottom: "16px" }}>
            <div>
              <h2>Установка пакетов</h2>
              <p>Добавить пакеты с PyPI.org</p>
            </div>
            <div className="segmented" style={{ display: "flex", gap: "4px" }}>
              <button className={installMode === "single" ? "active" : ""} onClick={() => setInstallMode("single")}>Один</button>
              <button className={installMode === "bulk" ? "active" : ""} onClick={() => setInstallMode("bulk")}>Списком</button>
            </div>
          </div>

          {/* Form Mode 1: Single Package */}
          {installMode === "single" && (
            <form onSubmit={handleSingleInstall} className="form-grid" style={{ display: "flex", flexDirection: "column", gap: "12px" }}>
              <label>
                Имя пакета (например requests)
                <input
                  type="text"
                  placeholder="Имя пакета"
                  value={pkgName}
                  onChange={(e) => setPkgName(e.target.value)}
                  required
                />
              </label>
              <label>
                Версия (необязательно)
                <input
                  type="text"
                  placeholder="например 2.31.0 (пусто = все версии)"
                  value={pkgVersion}
                  onChange={(e) => setPkgVersion(e.target.value)}
                />
              </label>
              <label style={{ display: "flex", alignItems: "center", gap: "8px", cursor: "pointer", fontWeight: "normal", margin: "4px 0" }}>
                <input
                  type="checkbox"
                  checked={withDependencies}
                  onChange={(e) => setWithDependencies(e.target.checked)}
                  style={{ width: "16px", height: "16px", minHeight: "auto", cursor: "pointer", margin: 0 }}
                />
                Устанавливать с зависимостями
              </label>
              <button
                type="submit"
                className="primary-button"
                style={{ width: "100%", marginTop: "5px" }}
                disabled={pendingKeys.has("pypi:install") || !pkgName.trim()}
              >
                {pendingKeys.has("pypi:install") ? <Loader2 size={16} className="spin" /> : <Play size={16} />}
                Скачать в кэш
              </button>
              <p style={{ fontSize: "11px", color: "#697782", marginTop: "5px", lineHeight: "1.3" }}>
                * По умолчанию, если при установке не указать конкретную версию, сервер получит метаданные и скачает все доступные релизы пакета в фоне.
              </p>
            </form>
          )}

          {/* Form Mode 2: Bulk List */}
          {installMode === "bulk" && (
            <form onSubmit={handleBulkInstall} className="form-grid" style={{ display: "flex", flexDirection: "column", gap: "12px" }}>
              <label>
                Список пакетов (каждый с новой строки)
                <textarea
                  rows={8}
                  placeholder="django&#10;flask==2.0.0&#10;numpy>=1.22"
                  value={bulkText}
                  onChange={(e) => setBulkText(e.target.value)}
                  required
                  style={{
                    width: "100%",
                    borderRadius: "6px",
                    border: "1px solid #dfe6ea",
                    padding: "10px",
                    fontFamily: "monospace",
                    fontSize: "13px"
                  }}
                />
              </label>
              <label style={{ display: "flex", alignItems: "center", gap: "8px", cursor: "pointer", fontWeight: "normal", margin: "4px 0" }}>
                <input
                  type="checkbox"
                  checked={withDependencies}
                  onChange={(e) => setWithDependencies(e.target.checked)}
                  style={{ width: "16px", height: "16px", minHeight: "auto", cursor: "pointer", margin: 0 }}
                />
                Устанавливать с зависимостями
              </label>
              <button
                type="submit"
                className="primary-button"
                style={{ width: "100%", marginTop: "5px" }}
                disabled={pendingKeys.has("pypi:bulk-install") || !bulkText.trim()}
              >
                {pendingKeys.has("pypi:bulk-install") ? <Loader2 size={16} className="spin" /> : <Play size={16} />}
                Начать фоновую установку
              </button>
              <p style={{ fontSize: "11px", color: "#697782", marginTop: "5px", lineHeight: "1.3" }}>
                * Синтаксис поддерживает точное указание версий (например <code>flask==2.0.0</code>). Для других форматов (например <code>requests</code>) будут установлены все версии.
              </p>
            </form>
          )}

          {/* Block package section directly */}
          <div style={{ marginTop: "20px", paddingTop: "20px", borderTop: "1px solid #dfe6ea" }}>
            <h4 style={{ marginBottom: "10px" }}>Быстрая блокировка</h4>
            <div style={{ display: "flex", gap: "5px" }}>
              <input
                type="text"
                placeholder="Имя пакета для блокировки"
                id="quick-block-input"
                style={{ height: "34px", flex: 1, borderRadius: "6px", border: "1px solid #dfe6ea", padding: "0 10px" }}
              />
              <button
                className="secondary-button danger-icon"
                onClick={() => {
                  const input = document.getElementById("quick-block-input") as HTMLInputElement;
                  if (input && input.value.trim()) {
                    void handleBlockPackage(input.value.trim(), true);
                    input.value = "";
                  }
                }}
              >
                Блок
              </button>
            </div>
          </div>
        </div>

      </div>

      {/* 4. Package Detail Modal (Local Versions List) */}
      {detailModalOpen && (
        <div className="confirm-backdrop" role="presentation">
          <section className="update-log-dialog" role="dialog" aria-modal="true" style={{ maxWidth: "700px", width: "100%" }}>
            
            <div className="update-log-head" style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "15px" }}>
              <div>
                <h2 style={{ margin: 0 }}>Пакет: {selectedPkg ? selectedPkg.name : "Загрузка..."}</h2>
                <p style={{ margin: "2px 0 0 0", fontSize: "13px", color: "#697782" }}>
                  {selectedPkg && `Всего версий локально: ${selectedPkg.total_versions} · Общий размер: ${selectedPkg.total_size_human}`}
                </p>
              </div>
              <button
                type="button"
                className="icon-button"
                onClick={() => { setDetailModalOpen(false); setSelectedPkg(null); }}
              >
                <X size={18} />
              </button>
            </div>

            {pkgDetailLoading ? (
              <div style={{ textAlign: "center", padding: "30px" }}>
                <Loader2 className="spin" size={24} style={{ margin: "0 auto 10px" }} />
                Загрузка информации о версиях пакета...
              </div>
            ) : selectedPkg ? (
              <div style={{ display: "flex", flexDirection: "column", gap: "15px" }}>
                
                {/* Block/Delete whole package options */}
                <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", background: "#f8fafb", padding: "10px 15px", borderRadius: "6px", border: "1px solid #dfe6ea" }}>
                  <div>
                    <strong>Глобальные операции с пакетом</strong>
                  </div>
                  <div style={{ display: "flex", gap: "10px" }}>
                    <button
                      className={`secondary-button ${selectedPkg.is_blocked ? "success-icon" : "danger-icon"}`}
                      onClick={() => void handleBlockPackage(selectedPkg.name, !selectedPkg.is_blocked)}
                    >
                      {selectedPkg.is_blocked ? <Unlock size={14} /> : <Ban size={14} />}
                      {selectedPkg.is_blocked ? "Разблокировать пакет" : "Заблокировать пакет"}
                    </button>
                    <button
                      className="secondary-button danger-icon"
                      onClick={() => void handleDeletePackage(selectedPkg.name)}
                    >
                      <Trash2 size={14} /> Удалить все файлы
                    </button>
                  </div>
                </div>

                {/* Table of local versions */}
                <div className="table-wrapper" style={{ maxHeight: "350px", overflowY: "auto" }}>
                  <table style={{ width: "100%", borderCollapse: "collapse" }}>
                    <thead>
                      <tr style={{ borderBottom: "2px solid #dfe6ea", textAlign: "left", color: "#697782" }}>
                        <th style={{ padding: "10px 8px" }}>Версия</th>
                        <th style={{ padding: "10px 8px" }}>Файлов</th>
                        <th style={{ padding: "10px 8px" }}>Размер</th>
                        <th style={{ padding: "10px 8px" }}>Статус</th>
                        <th style={{ padding: "10px 8px", textAlign: "right" }}>Действия</th>
                      </tr>
                    </thead>
                    <tbody>
                      {selectedPkg.versions.map(ver => (
                        <tr key={ver.version} style={{ borderBottom: "1px solid #eef2f3" }}>
                          <td style={{ padding: "10px 8px", fontFamily: "monospace", fontWeight: "bold" }}>
                            {ver.version}
                          </td>
                          <td style={{ padding: "10px 8px" }}>{ver.files_count}</td>
                          <td style={{ padding: "10px 8px" }}>{ver.size_human}</td>
                          <td style={{ padding: "10px 8px" }}>
                            {selectedPkg.is_blocked || ver.is_blocked ? (
                              <span className="badge badge-danger">Блокирован</span>
                            ) : (
                              <span className="badge badge-ok">Доступен</span>
                            )}
                          </td>
                          <td style={{ padding: "10px 8px", textAlign: "right" }}>
                            <div style={{ display: "flex", gap: "5px", justifyContent: "flex-end" }}>
                              <button
                                className="icon-button"
                                title="Перескачать / обновить эту версию"
                                onClick={() => runAction(async () => {
                                  const job = await api.pypiInstall(selectedPkg.name, ver.version);
                                  addActiveJob(job);
                                }, { pendingKey: `pypi:install:${selectedPkg.name}:${ver.version}`, errorTitle: "Не удалось запустить загрузку" })}
                              >
                                <RefreshCw size={12} />
                              </button>
                              <button
                                className={`icon-button ${ver.is_blocked ? "success-icon" : "danger-icon"}`}
                                title={ver.is_blocked ? "Разблокировать версию" : "Заблокировать версию"}
                                onClick={() => void handleBlockVersion(selectedPkg.name, ver.version, !ver.is_blocked)}
                              >
                                {ver.is_blocked ? <Unlock size={12} /> : <Ban size={12} />}
                              </button>
                              <button
                                className="icon-button danger-icon"
                                title="Удалить версию"
                                onClick={() => void handleDeleteVersion(selectedPkg.name, ver.version)}
                              >
                                <Trash2 size={12} />
                              </button>
                            </div>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>

                <div className="confirm-actions" style={{ display: "flex", justifyContent: "flex-end", marginTop: "10px" }}>
                  <button
                    type="button"
                    className="secondary-button"
                    onClick={() => { setDetailModalOpen(false); setSelectedPkg(null); }}
                  >
                    Закрыть
                  </button>
                </div>
              </div>
            ) : (
              <div style={{ padding: "20px", textAlign: "center" }}>Не удалось получить информацию о пакете.</div>
            )}
          </section>
        </div>
      )}

    </section>
  );
}
