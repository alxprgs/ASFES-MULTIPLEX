import pytest


@pytest.mark.asyncio
async def test_metrics_route_disabled(integration_env):
    client = integration_env["client"]
    services = integration_env["services"]
    
    # Disable prometheus
    services.observability.prometheus_enabled = False

    response = await client.get("/api/metrics")
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_metrics_route_public(integration_env):
    client = integration_env["client"]
    services = integration_env["services"]
    settings = integration_env["settings"]

    # Enable and make public
    services.observability.prometheus_enabled = True
    settings.observability.metrics_public = True

    response = await client.get("/api/metrics")
    assert response.status_code == 200
    assert "text/plain" in response.headers["content-type"]
    assert "multiplex_http_requests" in response.text


@pytest.mark.asyncio
async def test_metrics_route_private_unauthorized(integration_env):
    client = integration_env["client"]
    services = integration_env["services"]
    settings = integration_env["settings"]

    # Enable but make private
    services.observability.prometheus_enabled = True
    settings.observability.metrics_public = False

    response = await client.get("/api/metrics")
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_metrics_route_private_authorized(integration_env):
    client = integration_env["client"]
    services = integration_env["services"]
    settings = integration_env["settings"]

    # Enable but make private
    services.observability.prometheus_enabled = True
    settings.observability.metrics_public = False

    # Login as root
    login = await client.post(
        "/api/auth/login",
        json={
            "username": settings.root.username,
            "password": settings.root.password.get_secret_value(),
        },
    )
    headers = {"Authorization": f"Bearer {login.json()['access_token']}"}

    response = await client.get("/api/metrics", headers=headers)
    assert response.status_code == 200
    assert "text/plain" in response.headers["content-type"]
    assert "multiplex_http_requests" in response.text
