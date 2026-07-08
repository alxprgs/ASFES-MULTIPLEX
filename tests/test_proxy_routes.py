from __future__ import annotations

import pytest
from unittest.mock import AsyncMock


@pytest.mark.asyncio
async def test_proxy_crud_and_isolation(integration_env) -> None:
    client = integration_env["client"]
    cfg = integration_env["settings"]

    # 1. Login as root
    root_login = await client.post(
        "/api/auth/login",
        json={
            "username": cfg.root.username,
            "password": cfg.root.password.get_secret_value(),
        },
    )
    assert root_login.status_code == 200
    root_access = root_login.json()["access_token"]
    root_headers = {"Authorization": f"Bearer {root_access}"}

    # 2. Add Alice user
    await client.put(
        "/api/settings/registration", headers=root_headers, json={"enabled": True}
    )
    register = await client.post(
        "/api/auth/register",
        json={
            "username": "alice",
            "password": "AlicePassword123!",
            "email": "alice@example.com",
        },
    )
    assert register.status_code == 201

    # Login as Alice
    alice_login = await client.post(
        "/api/auth/login",
        json={"username": "alice", "password": "AlicePassword123!"},
    )
    alice_access = alice_login.json()["access_token"]
    alice_headers = {"Authorization": f"Bearer {alice_access}"}

    # 3. Create proxy (Alice)
    proxy_payload = {
        "protocol": "socks5",
        "host": "1.1.1.1",
        "port": 1080,
        "username": "alice_user",
        "password": "alice_password",
        "label": "Alice Socks",
    }
    create_resp = await client.post(
        "/api/proxy/proxies", headers=alice_headers, json=proxy_payload
    )
    assert create_resp.status_code == 201
    proxy_data = create_resp.json()
    assert proxy_data["host"] == "1.1.1.1"
    assert proxy_data["username"] == "alice_user"
    assert proxy_data["label"] == "Alice Socks"
    assert "password" not in proxy_data  # Should not leak password in response

    # 4. Read proxies (Alice sees her proxy)
    list_resp = await client.get("/api/proxy/proxies", headers=alice_headers)
    assert list_resp.status_code == 200
    assert len(list_resp.json()) == 1
    assert list_resp.json()[0]["proxy_id"] == proxy_data["proxy_id"]

    # 5. Isolation: root must NOT see Alice's proxy
    root_list_resp = await client.get("/api/proxy/proxies", headers=root_headers)
    assert root_list_resp.status_code == 200
    assert len(root_list_resp.json()) == 0

    # 6. Duplicate proxy insertion by Alice should fail
    dup_resp = await client.post(
        "/api/proxy/proxies", headers=alice_headers, json=proxy_payload
    )
    assert dup_resp.status_code == 400
    assert "already exists" in dup_resp.json()["detail"]

    # 7. Add proxy from URL (Alice)
    url_payload = {
        "url": "bob_user:bob_pass@2.2.2.2:8080",
        "protocol": "http",
        "label": "Bob Http",
    }
    url_resp = await client.post(
        "/api/proxy/proxies/from-url", headers=alice_headers, json=url_payload
    )
    assert url_resp.status_code == 201
    assert url_resp.json()["host"] == "2.2.2.2"
    assert url_resp.json()["port"] == 8080
    assert url_resp.json()["username"] == "bob_user"

    # Alice now has 2 proxies
    list_resp = await client.get("/api/proxy/proxies", headers=alice_headers)
    assert len(list_resp.json()) == 2

    # 8. Export checks
    # Export URL
    p_id = proxy_data["proxy_id"]
    exp_url = await client.get(
        f"/api/proxy/proxies/{p_id}/export/url", headers=alice_headers
    )
    assert exp_url.status_code == 200
    assert exp_url.json()["url"] == "socks5://alice_user:alice_password@1.1.1.1:1080"

    # Export Lines
    exp_lines = await client.get(
        f"/api/proxy/proxies/{p_id}/export/lines", headers=alice_headers
    )
    assert exp_lines.status_code == 200
    assert exp_lines.json()["lines"] == "alice_user\nalice_password\n1.1.1.1\n1080"

    # Export TG (SOCKS5 is supported)
    exp_tg = await client.get(
        f"/api/proxy/proxies/{p_id}/export/tg?secret=dd112233", headers=alice_headers
    )
    assert exp_tg.status_code == 200
    assert (
        "tg://proxy?server=1.1.1.1&port=1080&secret=dd112233"
        in exp_tg.json()["deep_link"]
    )

    # Export TG (HTTP is not supported for TG proxy)
    bob_id = url_resp.json()["proxy_id"]
    exp_tg_err = await client.get(
        f"/api/proxy/proxies/{bob_id}/export/tg", headers=alice_headers
    )
    assert exp_tg_err.status_code == 400

    # 9. Delete proxy
    del_resp = await client.delete(f"/api/proxy/proxies/{p_id}", headers=alice_headers)
    assert del_resp.status_code == 200

    # Alice now has 1 proxy left
    list_resp = await client.get("/api/proxy/proxies", headers=alice_headers)
    assert len(list_resp.json()) == 1


@pytest.mark.asyncio
async def test_proxifier_import_export(integration_env) -> None:
    client = integration_env["client"]
    cfg = integration_env["settings"]

    # Login root
    root_login = await client.post(
        "/api/auth/login",
        json={
            "username": cfg.root.username,
            "password": cfg.root.password.get_secret_value(),
        },
    )
    headers = {"Authorization": f"Bearer {root_login.json()['access_token']}"}

    xml_content = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<ProxifierProfile version="102" platform="Windows" product_id="0" product_minver="400">
	<ProxyList>
		<Proxy id="109" type="HTTPS">
			<Authentication enabled="true">
				<Password>pwd1</Password>
				<Username>user1</Username>
			</Authentication>
			<Port>50100</Port>
			<Address>45.153.163.129</Address>
		</Proxy>
		<Proxy id="125" type="SOCKS5">
			<Authentication enabled="true">
				<Password>pwd2</Password>
				<Username>user2</Username>
			</Authentication>
			<Port>8000</Port>
			<Address>68.209.59.96</Address>
		</Proxy>
	</ProxyList>
</ProxifierProfile>
"""
    # Import
    imp_resp = await client.post(
        "/api/proxy/proxies/import/proxifier",
        headers=headers,
        json={"xml_content": xml_content},
    )
    assert imp_resp.status_code == 200
    assert imp_resp.json()["imported"] == 2

    # Check imported in DB
    list_resp = await client.get("/api/proxy/proxies", headers=headers)
    proxies = list_resp.json()
    assert len(proxies) == 2
    proxy_ids = [p["proxy_id"] for p in proxies]

    # Export Proxifier
    exp_resp = await client.post(
        "/api/proxy/proxies/export/proxifier",
        headers=headers,
        json={"proxy_ids": proxy_ids},
    )
    assert exp_resp.status_code == 200
    export_xml = exp_resp.json()["xml_content"]
    assert "45.153.163.129" in export_xml
    assert "68.209.59.96" in export_xml
    assert "user1" in export_xml
    assert "pwd2" in export_xml


@pytest.mark.asyncio
async def test_proxy_limits(integration_env, monkeypatch) -> None:
    client = integration_env["client"]
    services = integration_env["services"]
    cfg = integration_env["settings"]

    # Mock count_proxies to return 500 (simulate limit)
    monkeypatch.setattr(
        services.proxy_service, "count_proxies", AsyncMock(return_value=500)
    )

    # Login root
    root_login = await client.post(
        "/api/auth/login",
        json={
            "username": cfg.root.username,
            "password": cfg.root.password.get_secret_value(),
        },
    )
    headers = {"Authorization": f"Bearer {root_login.json()['access_token']}"}

    proxy_payload = {
        "protocol": "socks5",
        "host": "9.9.9.9",
        "port": 1080,
    }
    resp = await client.post("/api/proxy/proxies", headers=headers, json=proxy_payload)
    assert resp.status_code == 400
    assert "Proxy limit exceeded" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_proxy_check_all_background(integration_env) -> None:
    client = integration_env["client"]
    cfg = integration_env["settings"]

    # Login root
    root_login = await client.post(
        "/api/auth/login",
        json={
            "username": cfg.root.username,
            "password": cfg.root.password.get_secret_value(),
        },
    )
    headers = {"Authorization": f"Bearer {root_login.json()['access_token']}"}

    # Add a mock proxy
    proxy_payload = {
        "protocol": "socks5",
        "host": "127.0.0.1",
        "port": 9999,
    }
    await client.post("/api/proxy/proxies", headers=headers, json=proxy_payload)

    # Trigger check all background
    resp = await client.post("/api/proxy/proxies/check-all", headers=headers)
    assert resp.status_code == 200
    assert resp.json()["status"] == "started"
