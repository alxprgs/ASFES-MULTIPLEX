# Developing Plugins for ASFES Multiplex

ASFES Multiplex is designed so that integrating new plugins and tools is as simple and isolated as possible. To add a plugin, you do not need to modify any existing application code or configuration files—it is sufficient to create a single Python file in the `server/mcp/plugins/` directory.

---

## 1. Introduction and Architectural Overview

The ASFES Multiplex plugin system is built on auto-discovery and dynamic module loading. At startup, the server scans the plugins directory and loads all declared manifests and tools.

### Plugin Lifecycle in the System

```
[ Application Startup ]
        │
        ▼
[ PluginRegistry.load_plugins() ]
        │
        ├─► Scan server/mcp/plugins/ (pkgutil.iter_modules)
        ├─► Ignore files starting with "_" (e.g., _common.py)
        │
        ▼
[ Loading Plugin Module ]
        │
        ├─► importlib.import_module()
        ├─► Extract PLUGIN object (PluginDefinition)
        │
        ▼
[ Registration in System ]
        │
        ├─► Register permissions in PermissionCatalog
        ├─► Save plugin metadata in MongoDB (PLUGINS collection)
        ├─► Create global policies for tools (TOOL_POLICIES)
        │
        ▼
[ Initialization (Lifecycle Startup) ]
        │
        └─► Call plugin.startup(services) — if declared
        │
        ▼
[ Ready for Work / Tool Executions ]
        │
        ▼
[ Application Shutdown ]
        │
        ▼
[ Call plugin.shutdown(services) ] — if declared
```

---

## 2. Anatomy of Data Types

All data structures for plugins are defined in [models.py](../server/models.py). Below is a description of the key fields of each class.

### 2.1 PluginManifest
Describes the metadata of the plugin itself.

| Field | Type | Description | Default Value |
| :--- | :--- | :--- | :--- |
| `key` | `str` | Unique plugin identifier (snake_case). | *Required* |
| `name` | `str` | Human-readable name of the plugin for the UI. | *Required* |
| `version` | `str` | Plugin version (SemVer, e.g., `"1.0.0"`). | *Required* |
| `description` | `str` | Brief description of the plugin's purpose. | *Required* |
| `os_support` | `list[str]` | List of supported OS names. Allowed: `"linux"`, `"windows"`. | `["linux", "windows"]` |
| `enabled_by_default` | `bool` | Whether the plugin is enabled globally on first run. | `True` |
| `permissions` | `list[PermissionDefinition]` | List of permissions registered by this plugin. | `[]` |
| `required_backends` | `list[str]` | Required system CLI utilities or libraries. | `[]` |
| `providers` | `list[str]` | List of resource providers. | `[]` |

### 2.2 PermissionDefinition
Describes an access permission.

| Field | Type | Description |
| :--- | :--- | :--- |
| `key` | `str` | Unique permission string identifier (e.g., `docker.containers.read`). |
| `description` | `str` | Explanation of what this permission grants (shown in the admin panel). |

### 2.3 MCPToolManifest
Describes the metadata of a specific MCP tool.

| Field | Type | Description | Default Value |
| :--- | :--- | :--- | :--- |
| `key` | `str` | Unique tool key. Format: `<plugin_key>.<action_name>`. | *Required* |
| `name` | `str` | Tool display name. | *Required* |
| `description` | `str` | Tool description (used by AI clients to understand its function). | *Required* |
| `input_schema` | `dict` | Argument schema in JSON Schema format. | *Required* |
| `permissions` | `list[str]` | List of permission keys required to call the tool. | *Required* |
| `tags` | `list[str]` | Tags for tool categorization. | `[]` |
| `read_only` | `bool` | Whether the tool is safe for reading (does not change state). | `False` |
| `default_global_enabled` | `bool` | Whether the tool should be enabled globally by default. | `True` |
| `os_support` | `list[str]` | Supported operating systems for this tool. | `["linux", "windows"]` |
| `required_backends` | `list[str]` | Dependencies required specifically for this tool. | `[]` |
| `providers` | `list[str]` | Tool providers. | `[]` |
| `audit_redact_fields` | `list[str]` | List of keys from arguments to mask in audit logs (secrets, passwords). | `[]` |
| `audit_max_string_length` | `int` | Maximum string length for arguments in the audit log. | `512` |

> [!IMPORTANT]
> Upon first registration of a tool, its initial enabled state is calculated as: `default_global_enabled AND read_only`.
> Tools with `read_only=False` (performing write or destructive actions) will always be globally disabled by default. The administrator must explicitly enable them via the control panel UI.

### 2.4 MCPTool
Binds the tool manifest to its code implementation and availability logic.

| Field | Type | Description |
| :--- | :--- | :--- |
| `manifest` | `MCPToolManifest` | The tool manifest. |
| `handler` | `ToolHandler` | Async function processing the call. |
| `availability` | `AvailabilityHandler \| None` | Function checking availability in the current environment. |

### 2.5 PluginDefinition
Main plugin object, exported as `PLUGIN`.

| Field | Type | Description |
| :--- | :--- | :--- |
| `manifest` | `PluginManifest` | The plugin manifest. |
| `tools` | `dict[str, MCPTool]` | Dictionary of plugin tools, where the key is the `tool_key`. |
| `startup` | `Callable[[ApplicationServices], Awaitable[None]] \| None` | Async initialization hook. |
| `shutdown` | `Callable[[ApplicationServices], Awaitable[None]] \| None` | Async resource cleanup hook. |
| `availability` | `AvailabilityHandler \| None` | Function checking availability of the entire plugin. |

---

## 3. Execution Context (ToolExecutionContext)

Each tool handler receives a `context: ToolExecutionContext` object on call, providing safe access to system state and the current user:

*   `context.user`: `UserPrincipal` object
    *   `user_id`: Unique user identifier.
    *   `username`: Username.
    *   `is_root`: Superuser flag.
    *   `permissions`: List of user's permissions.
*   `context.services`: `ApplicationServices` object
    *   Provides access to all main application core services:
        *   `services.host_ops`: Executing CLI commands, checking file path access.
        *   `services.db`: Accessing MongoDB database (via `DatabaseManager`).
        *   `services.alerts`: Managing alert rules and events.
        *   `services.mailer`: Sending email notifications.
        *   `services.settings`: System settings (defined in `.env`).
        *   `services.audit`: Logging system events to the audit trail.
*   `context.request_meta`: HTTP/SSE request metadata (client IP address, User-Agent, etc.).

---

## 4. Writing a Tool Handler Function

An async tool handler function has a strict signature:
```python
async def my_handler(context: ToolExecutionContext, arguments: dict[str, Any]) -> dict[str, Any]:
    ...
```

### Error Handling Rules
If an error occurs during execution, you must raise a `RuntimeError`. The system will catch it and return it to the client as a structured MCP Tool error.

### Common Utilities `server/mcp/plugins/_common.py`
Use pre-built functions for validating arguments and paths:

*   `require_argument(arguments, "key")`: Returns the argument value or raises `RuntimeError` if it is missing or empty.
*   `string_list_argument(arguments, "key")`: Safely converts the argument to a list of strings.
*   `int_argument(arguments, "key", default)`: Extracts an integer.
*   `bool_argument(arguments, "key", default)`: Extracts a boolean.
*   `dict_argument(arguments, "key")`: Extracts a dictionary.
*   `managed_path(context, raw_path, use_logs_root=False)`: Validates and resolves the path under the `HOST_OPS__MANAGED_FILE_ROOTS` or `HOST_OPS__MANAGED_LOG_ROOTS` safety constraints. Prevents Path Traversal vulnerabilities.
*   `command_result_payload(result)`: Converts a CLI execution result (`CommandResult`) to a dictionary.

---

## 5. Availability System

Plugins and tools can disable themselves automatically if a required utility (e.g., `docker` or `nginx`) or library (`psutil`) is missing on the host OS.

### Checking via `static_availability`
The easiest way is using the `static_availability` factory:

```python
from server.mcp.plugins._common import static_availability

# Tool requires docker CLI in PATH
availability = static_availability(backend="docker")

# Library psutil is required
availability = static_availability(require_psutil=True)
```

### Custom Availability Check
If the check is more complex, write an async function:

```python
from server.models import RuntimeAvailability

async def custom_availability(services: ApplicationServices) -> RuntimeAvailability:
    if services.host_ops.is_windows:
        return RuntimeAvailability(available=True)
    return RuntimeAvailability(
        available=False, 
        reason="This plugin is only supported on Windows OS."
    )
```

---

## 6. Lifecycle Hooks (Startup / Shutdown)

If a plugin needs to initialize background tasks, connections, or periodic polling, use async hooks:

```python
import asyncio
from server.services import ApplicationServices

async def my_startup(services: ApplicationServices) -> None:
    # Start background process or initialize resources
    pass

async def my_shutdown(services: ApplicationServices) -> None:
    # Safely release resources on application termination
    pass
```

Pass these functions to the `PluginDefinition`:
```python
PLUGIN = PluginDefinition(
    manifest=...,
    tools=...,
    startup=my_startup,
    shutdown=my_shutdown,
)
```

---

## 7. Working with `host_ops`

The application core provides a secure interface for host interaction — `HostOpsService`. Access it using `context.services.host_ops`.

Main methods:
*   `await context.services.host_ops.run_backend(cmd_name, *args, check=True)`: Safely executes an external utility (if allowed in `HOST_OPS__PROCESS_ALLOWED_EXECUTABLES`).
*   `context.services.host_ops.is_linux`: Check if running on Linux.
*   `context.services.host_ops.is_windows`: Check if running on Windows.
*   `context.services.host_ops.resolve_managed_path(raw_path)`: Validate file path.

---

## 8. Plugin Examples

### Example 1: Minimal Plugin (`mail.py`)
Simple email sending via core-configured SMTP client.

```python
from __future__ import annotations
from typing import Any
from server.models import (
    MCPTool, MCPToolManifest, PermissionDefinition, 
    PluginDefinition, PluginManifest, ToolExecutionContext
)

async def send_test_email(context: ToolExecutionContext, arguments: dict[str, Any]) -> dict[str, Any]:
    recipient = arguments.get("recipient")
    subject = arguments.get("subject") or "Test email"
    body = arguments.get("body") or "Hello from Multiplex!"
    
    if not recipient:
        raise RuntimeError("The 'recipient' argument is required.")
        
    sent = await context.services.mailer.send_email(str(recipient), str(subject), str(body))
    if not sent:
        raise RuntimeError("SMTP sending is disabled or unavailable.")
        
    return {"recipient": recipient, "sent": True}

PLUGIN = PluginDefinition(
    manifest=PluginManifest(
        key="mail",
        name="Mail",
        version="1.0.0",
        description="Sending system emails.",
        permissions=[
            PermissionDefinition(key="mail.send", description="Allows sending emails")
        ],
    ),
    tools={
        "mail.send_test_email": MCPTool(
            manifest=MCPToolManifest(
                key="mail.send_test_email",
                name="Send Test Email",
                description="Sends a test email message.",
                input_schema={
                    "type": "object",
                    "required": ["recipient"],
                    "properties": {
                        "recipient": {"type": "string"},
                        "subject": {"type": "string"},
                        "body": {"type": "string"},
                    },
                    "additionalProperties": False,
                },
                permissions=["mail.send"],
                read_only=False,
                default_global_enabled=False,
            ),
            handler=send_test_email,
        )
    },
)
```

### Example 2: Plugin with `psutil` (`system_stats.py`)
Using an external library with automatic import checking.

```python
from __future__ import annotations
from typing import Any
from server.host_ops import _psutil
from server.mcp.plugins._common import static_availability
from server.models import (
    MCPTool, MCPToolManifest, PermissionDefinition, 
    PluginDefinition, PluginManifest, ToolExecutionContext
)

async def get_snapshot(context: ToolExecutionContext, arguments: dict[str, Any]) -> dict[str, Any]:
    if _psutil is None:
        raise RuntimeError("Library psutil is not installed.")
        
    return {
        "cpu_percent": _psutil.cpu_percent(interval=0.1),
        "memory": _psutil.virtual_memory()._asdict(),
    }

PLUGIN = PluginDefinition(
    manifest=PluginManifest(
        key="system_stats",
        name="System Statistics",
        version="1.0.0",
        description="Collecting CPU and memory metrics of the host.",
        permissions=[
            PermissionDefinition(key="system.stats.read", description="Allows reading host metrics")
        ],
        required_backends=["psutil"],
    ),
    tools={
        "system_stats.get_snapshot": MCPTool(
            manifest=MCPToolManifest(
                key="system_stats.get_snapshot",
                name="Get Metric Snapshot",
                description="Returns CPU and RAM statistics.",
                input_schema={"type": "object", "properties": {}, "additionalProperties": False},
                permissions=["system.stats.read"],
                read_only=True,
                required_backends=["psutil"],
            ),
            handler=get_snapshot,
            availability=static_availability(require_psutil=True),
        )
    },
)
```

### Example 3: Background Service (`alerts.py`)
Plugin managing a background loop for checking rules.

```python
from __future__ import annotations
from server.models import PluginDefinition, PluginManifest

async def alerts_startup(services) -> None:
    await services.alerts.start()

async def alerts_shutdown(services) -> None:
    await services.alerts.stop()

PLUGIN = PluginDefinition(
    manifest=PluginManifest(
        key="alerts",
        name="Alerts",
        version="1.0.0",
        description="Background control of host triggers.",
        permissions=[],
    ),
    tools={},
    startup=alerts_startup,
    shutdown=alerts_shutdown,
)
```

---

## 9. Step-by-Step Guide: Creating a Plugin in 5 Steps

1.  **Create Plugin File**: Add a file named `server/mcp/plugins/my_plugin.py`. The file name defines the plugin key.
2.  **Import Models**: Add the necessary types from `server.models`.
3.  **Write Async Handler**: Create an `async def` function to perform the actual task.
4.  **Define `PLUGIN` Object**: Create an instance of `PluginDefinition` with all manifests.
5.  **Start/Restart Server**: Changes are automatically picked up on server startup (or use hot reload).

---

## 10. Hot-Reloading Plugins (Hot Reload)

You can update plugins in real-time without fully restarting the server.
Send a POST request to:
`/api/admin/plugins/reload` with the body `{"plugin_keys": ["my_plugin"]}`.

Reload process:
1.  Calls `shutdown` hook of the active plugin (if present).
2.  Python module is reloaded in memory (`importlib.reload`).
3.  Calls `startup` hook of the new plugin version.
4.  Updates registered tools inside the FastMCP gateway.

---

## 11. API Management and Permission Filtering

### Management Endpoints (require admin permissions)
*   `GET /api/admin/plugins` — List all installed plugins.
*   `GET /api/admin/tools` — List tools and their global enablement status.
*   `POST /api/admin/plugins/{key}/enable` / `disable` — Globally enable/disable a plugin.
*   `POST /api/admin/tools/{key}/enable` / `disable` — Globally enable/disable a specific tool.
*   `POST /api/admin/users/{user_id}/tools/{key}/enable` / `disable` — Enable/disable a tool for a specific user.

### Authorization Logic when Calling a Tool
Before launching the handler, FastMCP checks requirements in the following order:
1.  Is MCP globally enabled in system settings.
2.  Is the plugin enabled in the database.
3.  Is the plugin available in the runtime environment (OS check and utilities).
4.  Is the global access policy for the tool enabled (`TOOL_POLICIES` scope="global").
5.  If the user is `root`, access is granted.
6.  If regular user:
    *   Verify presence of all permissions from `tool.manifest.permissions` in the user's profile.
    *   Verify user-specific access policy enablement (`TOOL_POLICIES` scope="user" for `user_id`).

---

## 12. Naming Conventions and Security Best Practices

*   **Files and Keys**: The filename must strictly match the plugin key in `PluginManifest` (e.g., `docker.py` -> `docker`).
*   **Permission Naming**: Format: `<plugin_key>.<entity>.<action>`. E.g., `docker.containers.stop`.
*   **Tool Naming**: Format: `<plugin_key>.<action_name>`. E.g., `docker.stop_container`.
*   **Path Safety**: Never concatenate paths manually with `+` or f-strings. Always use `managed_path()` to protect against Path Traversal.
*   **Secrets**: Do not log arguments containing passwords or tokens. Make sure to define them in `audit_redact_fields`.

---

## 13. Testing Plugins

According to project guidelines, avoid over-mocking low-level OS operations. Prefer lightweight integration tests and structural verification.

### Example Smoke Test of Plugin Structure (`tests/test_my_plugin.py`)
```python
from server.mcp.plugins.my_plugin import PLUGIN

def test_plugin_manifest():
    assert PLUGIN.manifest.key == "my_plugin"
    assert len(PLUGIN.tools) > 0
    
    # Verify permissions
    permissions = [p.key for p in PLUGIN.manifest.permissions]
    assert "my_plugin.read" in permissions
```

### Example Unit Test of Handler
```python
import pytest
from unittest.mock import AsyncMock, MagicMock
from server.mcp.plugins.my_plugin import get_status
from server.models import ToolExecutionContext, UserPrincipal

@pytest.mark.asyncio
async def test_get_status_handler():
    # Mocking context
    user = UserPrincipal(user_id="user_1", username="test_user", is_root=False)
    services = MagicMock()
    context = ToolExecutionContext(user=user, services=services, request_meta={})
    
    arguments = {"name": "test_entity"}
    
    result = await get_status(context, arguments)
    
    assert result["status"] == "ok"
    assert result["entity_name"] == "test_entity"
```

---

## 14. Plugin Developer Checklist

- [ ] Plugin file is placed in `server/mcp/plugins/` and does not start with `_`.
- [ ] Plugin key (`key`) matches the Python filename.
- [ ] All tool keys are prefixed with `<plugin_key>.`.
- [ ] State-mutating tools (`read_only=False`) have `default_global_enabled=False`.
- [ ] All permissions are declared in the plugin manifest (`manifest.permissions`).
- [ ] Input arguments are validated using `_common.py` helpers.
- [ ] Path interactions are processed strictly through the `managed_path` helper.
- [ ] Sensitive arguments are masked using `audit_redact_fields`.
- [ ] Smoke tests are written to verify manifest structure.
- [ ] Linters are executed: `.venv/Scripts/python.exe -m ruff check . --fix`.
- [ ] All pytest tests pass successfully: `.venv/Scripts/python.exe -m pytest tests`.
