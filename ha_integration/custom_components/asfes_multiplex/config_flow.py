"""Config flow for ASFES Multiplex integration.

Supports:
- ConfigFlow: initial setup with optional 2FA step
- OptionsFlow: change poll interval and account label
- ReauthFlow: re-authenticate when token expires
"""

from __future__ import annotations

from typing import Any

import httpx
import voluptuous as vol

from homeassistant.config_entries import ConfigEntry, ConfigFlow, ConfigFlowResult, OptionsFlow
from homeassistant.core import callback
from homeassistant.helpers.selector import (
    NumberSelector,
    NumberSelectorConfig,
    NumberSelectorMode,
    TextSelector,
    TextSelectorConfig,
    TextSelectorType,
)

from .api import AsfesMultiplexApi, create_api_from_credentials
from .const import (
    CONF_ACCESS_TOKEN,
    CONF_ACCOUNT_LABEL,
    CONF_HOST,
    CONF_REFRESH_TOKEN,
    CONF_SERIAL,
    DOMAIN,
    OPT_POLL_INTERVAL,
    OPT_POLL_INTERVAL_DEFAULT,
    OPT_POLL_INTERVAL_MAX,
    OPT_POLL_INTERVAL_MIN,
    API_HEALTH,
)
from .exceptions import CannotConnect, InvalidAuth


class AsfesMultiplexConfigFlow(ConfigFlow, domain=DOMAIN):
    """Handle the config flow for ASFES Multiplex."""

    VERSION = 1

    def __init__(self) -> None:
        self._host: str = ""
        self._username: str = ""
        self._password: str = ""
        self._label: str = "Home Assistant"
        self._challenge_token: str = ""
        self._auth_data: dict[str, Any] = {}

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Handle the initial step."""
        errors: dict[str, str] = {}

        if user_input is not None:
            self._host = user_input[CONF_HOST].rstrip("/")
            self._username = user_input["username"]
            self._password = user_input["password"]
            self._label = user_input.get(CONF_ACCOUNT_LABEL, "Home Assistant")

            # First check server health
            try:
                async with httpx.AsyncClient(base_url=self._host, timeout=10.0) as client:
                    resp = await client.get(API_HEALTH)
                    if resp.status_code not in (200, 401):
                        errors["base"] = "cannot_connect"
            except httpx.RequestError:
                errors["base"] = "cannot_connect"

            if not errors:
                try:
                    api, data = await create_api_from_credentials(
                        self._host, self._username, self._password, self._label
                    )
                    if data.get("challenge_required"):
                        self._challenge_token = data["challenge_token"]
                        return await self.async_step_2fa()
                    self._auth_data = data
                    self._auth_data["api"] = api
                    return await self._finish_setup(api, data)
                except InvalidAuth:
                    errors["base"] = "invalid_auth"
                except CannotConnect:
                    errors["base"] = "cannot_connect"
                except Exception:  # noqa: BLE001
                    errors["base"] = "unknown"

        schema = vol.Schema(
            {
                vol.Required(CONF_HOST, default="http://192.168.0.100:8000"): TextSelector(
                    TextSelectorConfig(type=TextSelectorType.URL)
                ),
                vol.Required("username"): TextSelector(
                    TextSelectorConfig(type=TextSelectorType.TEXT, autocomplete="username")
                ),
                vol.Required("password"): TextSelector(
                    TextSelectorConfig(type=TextSelectorType.PASSWORD)
                ),
                vol.Optional(CONF_ACCOUNT_LABEL, default="Home Assistant"): TextSelector(
                    TextSelectorConfig(type=TextSelectorType.TEXT)
                ),
            }
        )
        return self.async_show_form(
            step_id="user", data_schema=schema, errors=errors
        )

    async def async_step_2fa(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Handle 2FA verification step."""
        errors: dict[str, str] = {}

        if user_input is not None:
            totp_code = user_input["totp_code"]
            try:
                async with httpx.AsyncClient(
                    base_url=self._host, timeout=10.0
                ) as client:
                    from .const import API_AUTH_2FA  # noqa: PLC0415
                    resp = await client.post(
                        API_AUTH_2FA,
                        json={
                            "challenge_token": self._challenge_token,
                            "totp_code": totp_code,
                        },
                    )
                    if resp.status_code == 401:
                        errors["base"] = "invalid_totp"
                    else:
                        resp.raise_for_status()
                        data = resp.json()
                        api = AsfesMultiplexApi(
                            host=self._host,
                            access_token=data["access_token"],
                            refresh_token=data["refresh_token"],
                            access_token_expires_in=data.get("expires_in", 1800),
                        )
                        return await self._finish_setup(api, data)
            except httpx.RequestError:
                errors["base"] = "cannot_connect"
            except Exception:  # noqa: BLE001
                errors["base"] = "unknown"

        schema = vol.Schema(
            {
                vol.Required("totp_code"): TextSelector(
                    TextSelectorConfig(type=TextSelectorType.TEXT)
                ),
            }
        )
        return self.async_show_form(
            step_id="2fa", data_schema=schema, errors=errors
        )

    async def _finish_setup(
        self, api: AsfesMultiplexApi, auth_data: dict[str, Any]
    ) -> ConfigFlowResult:
        """Fetch serial, set unique_id, and create entry."""
        try:
            diag = await api.get_diagnostics()
            serial = diag.get("serial", "")
        except Exception:  # noqa: BLE001
            serial = ""
        finally:
            await api.close()

        if serial:
            await self.async_set_unique_id(serial)
            self._abort_if_unique_id_configured()

        label = auth_data.get("account_label", self._label)
        return self.async_create_entry(
            title=label,
            data={
                CONF_HOST: self._host,
                CONF_ACCESS_TOKEN: auth_data["access_token"],
                CONF_REFRESH_TOKEN: auth_data["refresh_token"],
                CONF_ACCOUNT_LABEL: label,
                CONF_SERIAL: serial,
            },
        )

    async def async_step_reauth(
        self, entry_data: dict[str, Any]
    ) -> ConfigFlowResult:
        """Handle re-authentication."""
        return await self.async_step_reauth_confirm()

    async def async_step_reauth_confirm(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Handle re-auth credentials form."""
        errors: dict[str, str] = {}
        reauth_entry = self._get_reauth_entry()
        self._host = reauth_entry.data[CONF_HOST]
        self._label = reauth_entry.data.get(CONF_ACCOUNT_LABEL, "Home Assistant")

        if user_input is not None:
            self._username = user_input["username"]
            self._password = user_input["password"]
            try:
                api, data = await create_api_from_credentials(
                    self._host, self._username, self._password, self._label
                )
                if data.get("challenge_required"):
                    self._challenge_token = data["challenge_token"]
                    return await self.async_step_2fa()

                await api.close()
                return self.async_update_reload_and_abort(
                    reauth_entry,
                    data_updates={
                        CONF_ACCESS_TOKEN: data["access_token"],
                        CONF_REFRESH_TOKEN: data["refresh_token"],
                    },
                )
            except InvalidAuth:
                errors["base"] = "invalid_auth"
            except CannotConnect:
                errors["base"] = "cannot_connect"
            except Exception:  # noqa: BLE001
                errors["base"] = "unknown"

        schema = vol.Schema(
            {
                vol.Required("username"): TextSelector(
                    TextSelectorConfig(type=TextSelectorType.TEXT, autocomplete="username")
                ),
                vol.Required("password"): TextSelector(
                    TextSelectorConfig(type=TextSelectorType.PASSWORD)
                ),
            }
        )
        return self.async_show_form(
            step_id="reauth_confirm", data_schema=schema, errors=errors
        )

    @staticmethod
    @callback
    def async_get_options_flow(config_entry: ConfigEntry) -> OptionsFlow:
        """Return the options flow."""
        return AsfesMultiplexOptionsFlow()


class AsfesMultiplexOptionsFlow(OptionsFlow):
    """Handle options for ASFES Multiplex integration."""

    async def async_step_init(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Manage options."""
        if user_input is not None:
            return self.async_create_entry(data=user_input)

        schema = vol.Schema(
            {
                vol.Optional(
                    OPT_POLL_INTERVAL,
                    default=self.config_entry.options.get(
                        OPT_POLL_INTERVAL, OPT_POLL_INTERVAL_DEFAULT
                    ),
                ): NumberSelector(
                    NumberSelectorConfig(
                        min=OPT_POLL_INTERVAL_MIN,
                        max=OPT_POLL_INTERVAL_MAX,
                        step=5,
                        unit_of_measurement="seconds",
                        mode=NumberSelectorMode.SLIDER,
                    )
                ),
            }
        )
        return self.async_show_form(step_id="init", data_schema=schema)
