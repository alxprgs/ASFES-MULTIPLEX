from __future__ import annotations

from functools import lru_cache
from pathlib import Path
import tomllib
from typing import Literal

from pydantic import BaseModel, EmailStr, Field, HttpUrl, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


BASE_DIR = Path(__file__).resolve().parents[2]


def read_project_version() -> str:
    pyproject_path = BASE_DIR / "pyproject.toml"
    try:
        with pyproject_path.open("rb") as pyproject_file:
            project_data = tomllib.load(pyproject_file)
    except (OSError, tomllib.TOMLDecodeError):
        return "0.1.0"
    return str(project_data.get("project", {}).get("version") or "0.1.0")


class AppConfig(BaseModel):
    name: str = "ASFES Multiplex"
    version: str = Field(default_factory=read_project_version)
    env: str = "development"
    dev: bool = True
    host: str = "0.0.0.0"
    port: int = 8000
    public_base_url: HttpUrl = "https://multiplex.asfes.ru"
    api_prefix: str = "/api"
    mcp_path: str = "/mcp"
    frontend_dist: Path = BASE_DIR / "frontend" / "dist"
    startup_progress: bool = True
    trusted_proxy_ips: list[str] = Field(default_factory=lambda: ["127.0.0.1", "::1"])

    @field_validator("api_prefix", "mcp_path")
    @classmethod
    def validate_prefixes(cls, value: str) -> str:
        value = value.strip()
        if not value.startswith("/"):
            raise ValueError("route prefixes must start with '/'")
        return value.rstrip("/") or "/"


class MongoConfig(BaseModel):
    uri: str = "mongodb://127.0.0.1:27017"
    database: str = "asfes_multiplex"
    connect_timeout_ms: int = 5000
    server_selection_timeout_ms: int = 5000
    max_pool_size: int = 50


class RedisConfig(BaseModel):
    mode: Literal["disabled", "runtime", "required"] = "runtime"
    url: str | None = "redis://127.0.0.1:6379/0"
    enabled_on_startup: bool = False


class SMTPConfig(BaseModel):
    enabled: bool = False
    host: str | None = None
    port: int = 587
    username: str | None = None
    password: SecretStr | None = None
    from_email: EmailStr | None = None
    use_ssl: bool = False
    starttls: bool = True
    timeout_seconds: int = 10

    @model_validator(mode="after")
    def validate_enabled_settings(self) -> "SMTPConfig":
        if not self.enabled:
            return self
        missing = [
            name
            for name, value in {
                "SMTP__HOST": self.host,
                "SMTP__FROM_EMAIL": self.from_email,
            }.items()
            if not value
        ]
        if missing:
            raise ValueError(f"SMTP is enabled but missing required settings: {', '.join(missing)}")
        return self


class RootAccountConfig(BaseModel):
    username: str = "root"
    password: SecretStr = SecretStr("ChangeMeRootPassword123!")
    email: EmailStr = "root@multiplex.asfes.ru"
    display_name: str = "Root"

    @field_validator("username")
    @classmethod
    def validate_username(cls, value: str) -> str:
        value = value.strip()
        if len(value) < 3:
            raise ValueError("ROOT__USERNAME must contain at least 3 characters")
        return value

    @model_validator(mode="after")
    def validate_password(self) -> "RootAccountConfig":
        if len(self.password.get_secret_value()) < 12:
            raise ValueError("ROOT__PASSWORD must contain at least 12 characters")
        return self


class LoggingConfig(BaseModel):
    level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    directory: Path = BASE_DIR / "runtime" / "logs"
    sqlite_path: Path = BASE_DIR / "runtime" / "multiplex_logs.db"
    verifier_interval_seconds: int = 600
    console_rich_tracebacks: bool = True


class HostOpsConfig(BaseModel):
    managed_file_roots: list[Path] = Field(default_factory=lambda: [BASE_DIR / "data", BASE_DIR / "runtime"])
    managed_log_roots: list[Path] = Field(default_factory=lambda: [BASE_DIR / "runtime" / "logs"])
    backup_directory: Path = BASE_DIR / "runtime" / "backups"
    command_timeout_seconds: int = 30
    max_output_bytes: int = 65536
    alert_poll_interval_seconds: int = 60
    executable_overrides: dict[str, str] = Field(default_factory=dict)
    provider_overrides: dict[str, str] = Field(default_factory=dict)
    database_profiles_directory: Path = BASE_DIR / "data" / "profiles" / "databases"
    vpn_profiles_directory: Path = BASE_DIR / "data" / "profiles" / "vpn"
    ssl_profiles_directory: Path = BASE_DIR / "data" / "profiles" / "ssl"
    nginx_config_paths: list[Path] = Field(default_factory=lambda: [BASE_DIR / "data" / "nginx"])
    process_allowed_executables: list[str] = Field(default_factory=list)
    port_probe_allowed_hosts: list[str] = Field(default_factory=lambda: ["127.0.0.1", "::1", "localhost"])


class SecurityConfig(BaseModel):
    api_jwt_secret: SecretStr = SecretStr("ChangeThisApiJwtSecretImmediately")
    oauth_jwt_secret: SecretStr = SecretStr("ChangeThisOauthJwtSecretImmediately")
    password_pepper: SecretStr = SecretStr("ChangeThisPasswordPepper")
    access_token_ttl_minutes: int = 15
    refresh_token_ttl_days: int = 30
    oauth_access_token_ttl_minutes: int = 30
    oauth_refresh_token_ttl_days: int = 30
    oauth_authorization_code_ttl_minutes: int = 10
    session_cookie_name: str = "multiplex_session"
    csrf_cookie_name: str = "multiplex_csrf"
    cookie_secure: bool = False
    cookie_samesite: Literal["lax", "strict", "none"] = "lax"
    allow_insecure_cookies: bool = False
    issuer: str | None = None
    api_audience: str = "multiplex-api"
    mcp_audience: str = "multiplex-mcp"


class PasswordPolicyConfig(BaseModel):
    min_length: int = 12
    forbidden_passwords: list[str] = Field(
        default_factory=lambda: [
            "password",
            "password123",
            "qwerty123",
            "admin123",
            "changeme",
            "changemerootpassword123!",
        ]
    )


class OAuthConfig(BaseModel):
    issuer_path: str = "/api/oauth"
    authorization_path: str = "/api/oauth/authorize"
    token_path: str = "/api/oauth/token"
    revocation_path: str = "/api/oauth/revoke"
    clients_path: str = "/api/oauth/clients"
    jwks_path: str = "/api/oauth/jwks"
    supported_scopes: list[str] = Field(default_factory=lambda: ["mcp", "profile"])
    require_pkce: bool = True
    allow_plain_pkce: bool = False

    @field_validator("issuer_path", "authorization_path", "token_path", "revocation_path", "clients_path", "jwks_path")
    @classmethod
    def validate_paths(cls, value: str) -> str:
        if not value.startswith("/"):
            raise ValueError("OAuth paths must start with '/'")
        return value


class RateLimitPresetConfig(BaseModel):
    login_limit: int = 5
    login_window_seconds: int = 60
    register_limit: int = 3
    register_window_seconds: int = 3600
    oauth_token_limit: int = 20
    oauth_token_window_seconds: int = 60
    rest_read_limit: int = 60
    rest_read_window_seconds: int = 60
    rest_write_limit: int = 20
    rest_write_window_seconds: int = 60
    mcp_read_limit: int = 30
    mcp_read_window_seconds: int = 60
    mcp_write_limit: int = 10
    mcp_write_window_seconds: int = 300


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_nested_delimiter="__",
        extra="ignore",
    )

    app: AppConfig = Field(default_factory=AppConfig)
    mongo: MongoConfig = Field(default_factory=MongoConfig)
    redis: RedisConfig = Field(default_factory=RedisConfig)
    smtp: SMTPConfig = Field(default_factory=SMTPConfig)
    root: RootAccountConfig = Field(default_factory=RootAccountConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    host_ops: HostOpsConfig = Field(default_factory=HostOpsConfig)
    security: SecurityConfig = Field(default_factory=SecurityConfig)
    password_policy: PasswordPolicyConfig = Field(default_factory=PasswordPolicyConfig)
    oauth: OAuthConfig = Field(default_factory=OAuthConfig)
    rate_limits: RateLimitPresetConfig = Field(default_factory=RateLimitPresetConfig)

    @model_validator(mode="after")
    def finalize_paths(self) -> "Settings":
        self.app.version = read_project_version()
        self.app.frontend_dist = self.app.frontend_dist if self.app.frontend_dist.is_absolute() else BASE_DIR / self.app.frontend_dist
        self.logging.directory = self.logging.directory if self.logging.directory.is_absolute() else BASE_DIR / self.logging.directory
        self.logging.sqlite_path = (
            self.logging.sqlite_path if self.logging.sqlite_path.is_absolute() else BASE_DIR / self.logging.sqlite_path
        )
        self.host_ops.managed_file_roots = [
            path if path.is_absolute() else BASE_DIR / path for path in self.host_ops.managed_file_roots
        ]
        self.host_ops.managed_log_roots = [
            path if path.is_absolute() else BASE_DIR / path for path in self.host_ops.managed_log_roots
        ]
        self.host_ops.backup_directory = (
            self.host_ops.backup_directory if self.host_ops.backup_directory.is_absolute() else BASE_DIR / self.host_ops.backup_directory
        )
        self.host_ops.database_profiles_directory = (
            self.host_ops.database_profiles_directory
            if self.host_ops.database_profiles_directory.is_absolute()
            else BASE_DIR / self.host_ops.database_profiles_directory
        )
        self.host_ops.vpn_profiles_directory = (
            self.host_ops.vpn_profiles_directory
            if self.host_ops.vpn_profiles_directory.is_absolute()
            else BASE_DIR / self.host_ops.vpn_profiles_directory
        )
        self.host_ops.ssl_profiles_directory = (
            self.host_ops.ssl_profiles_directory
            if self.host_ops.ssl_profiles_directory.is_absolute()
            else BASE_DIR / self.host_ops.ssl_profiles_directory
        )
        self.host_ops.nginx_config_paths = [
            path if path.is_absolute() else BASE_DIR / path for path in self.host_ops.nginx_config_paths
        ]
        if self.redis.mode != "disabled" and not self.redis.url:
            raise ValueError("REDIS__URL is required when REDIS__MODE is not 'disabled'")
        if self.app.mcp_path == self.app.api_prefix:
            raise ValueError("APP__MCP_PATH and APP__API_PREFIX must differ")
        return self

    @property
    def DEV(self) -> bool:
        return self.app.dev

    @property
    def PORT(self) -> int:
        return self.app.port

    @property
    def host(self) -> str:
        return self.app.host

    @property
    def public_base_url(self) -> str:
        return str(self.app.public_base_url).rstrip("/")

    @property
    def api_prefix(self) -> str:
        return self.app.api_prefix

    @property
    def mcp_path(self) -> str:
        return self.app.mcp_path

    @property
    def oauth_issuer(self) -> str:
        return f"{self.public_base_url}{self.oauth.issuer_path}"

    @property
    def security_issuer(self) -> str:
        return self.security.issuer or self.oauth_issuer

    @property
    def authorization_endpoint(self) -> str:
        return f"{self.public_base_url}{self.oauth.authorization_path}"

    @property
    def token_endpoint(self) -> str:
        return f"{self.public_base_url}{self.oauth.token_path}"

    @property
    def revocation_endpoint(self) -> str:
        return f"{self.public_base_url}{self.oauth.revocation_path}"

    @property
    def clients_endpoint(self) -> str:
        return f"{self.public_base_url}{self.oauth.clients_path}"

    @property
    def jwks_uri(self) -> str:
        return f"{self.public_base_url}{self.oauth.jwks_path}"

    @property
    def protected_resource_metadata_path(self) -> str:
        return f"/.well-known/oauth-protected-resource{self.mcp_path}"

    @property
    def is_production(self) -> bool:
        return self.app.env.lower() == "production" or not self.app.dev

    @property
    def access_cookie_name(self) -> str:
        return self.security.session_cookie_name

    @property
    def refresh_cookie_name(self) -> str:
        return f"{self.security.session_cookie_name}_refresh"

    @property
    def csrf_cookie_name(self) -> str:
        return self.security.csrf_cookie_name

    def _uses_default_secret_values(self) -> bool:
        insecure_values = {
            "ChangeThisApiJwtSecretImmediately",
            "ChangeThisOauthJwtSecretImmediately",
            "ChangeThisPasswordPepper",
            "ChangeMeRootPassword123!",
            "change-this-api-secret",
            "change-this-oauth-secret",
            "change-this-password-pepper",
        }
        current_values = {
            self.security.api_jwt_secret.get_secret_value(),
            self.security.oauth_jwt_secret.get_secret_value(),
            self.security.password_pepper.get_secret_value(),
            self.root.password.get_secret_value(),
        }
        return bool(current_values.intersection(insecure_values))


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
