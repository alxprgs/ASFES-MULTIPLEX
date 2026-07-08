from __future__ import annotations

import pytest
from pydantic import SecretStr
from unittest.mock import MagicMock

from server.proxy_service import ProxyService


def test_parse_proxy_url() -> None:
    # 1. Full URL with username and password
    parsed = ProxyService.parse_proxy_url("user:pass@192.168.1.1:1080", "socks5")
    assert parsed["protocol"] == "socks5"
    assert parsed["host"] == "192.168.1.1"
    assert parsed["port"] == 1080
    assert parsed["username"] == "user"
    assert parsed["password"] == "pass"

    # 2. URL without password
    parsed = ProxyService.parse_proxy_url("user@domain.com:8080", "http")
    assert parsed["protocol"] == "http"
    assert parsed["host"] == "domain.com"
    assert parsed["port"] == 8080
    assert parsed["username"] == "user"
    assert parsed["password"] is None

    # 3. URL without credentials
    parsed = ProxyService.parse_proxy_url("127.0.0.1:80", "https")
    assert parsed["protocol"] == "https"
    assert parsed["host"] == "127.0.0.1"
    assert parsed["port"] == 80
    assert parsed["username"] is None
    assert parsed["password"] is None

    # 4. Strip protocol prefix if any
    parsed = ProxyService.parse_proxy_url("socks5://user:pass@host:1080", "socks5")
    assert parsed["host"] == "host"
    assert parsed["username"] == "user"
    assert parsed["password"] == "pass"


def test_parse_proxy_url_errors() -> None:
    with pytest.raises(ValueError, match="Port must be an integer"):
        ProxyService.parse_proxy_url("host:port", "socks5")

    with pytest.raises(ValueError, match="Port must be between 1 and 65535"):
        ProxyService.parse_proxy_url("host:70000", "socks5")

    with pytest.raises(ValueError, match="Invalid host:port format"):
        ProxyService.parse_proxy_url("host", "socks5")


def test_parse_proxifier_xml() -> None:
    xml_content = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<ProxifierProfile version="102" platform="Windows" product_id="0" product_minver="400">
    <ProxyList>
        <Proxy id="109" type="HTTPS">
            <Authentication enabled="true">
                <Password>5GL3y4JWcP</Password>
                <Username>chodop2</Username>
            </Authentication>
            <Options>48</Options>
            <Port>50100</Port>
            <Address>45.153.163.129</Address>
        </Proxy>
        <Proxy id="131" type="SOCKS5">
            <Options>48</Options>
            <Port>10808</Port>
            <Address>127.0.0.1</Address>
        </Proxy>
        <Proxy id="999" type="SOCKS4">
            <Port>1080</Port>
            <Address>1.2.3.4</Address>
        </Proxy>
    </ProxyList>
</ProxifierProfile>
"""
    proxies = ProxyService.parse_proxifier_xml(xml_content)
    assert len(proxies) == 2  # SOCKS4 must be skipped!

    p1 = proxies[0]
    assert p1["protocol"] == "https"
    assert p1["host"] == "45.153.163.129"
    assert p1["port"] == 50100
    assert p1["username"] == "chodop2"
    assert p1["password"] == "5GL3y4JWcP"

    p2 = proxies[1]
    assert p2["protocol"] == "socks5"
    assert p2["host"] == "127.0.0.1"
    assert p2["port"] == 10808
    assert p2["username"] is None
    assert p2["password"] is None


def test_export_as_proxifier_xml() -> None:
    proxies = [
        {
            "protocol": "https",
            "host": "45.153.163.129",
            "port": 50100,
            "username": "chodop2",
            "password": "pwd",
        },
        {
            "protocol": "socks5",
            "host": "127.0.0.1",
            "port": 10808,
        },
    ]

    xml = ProxyService.export_as_proxifier_xml(proxies)
    assert 'type="HTTPS"' in xml
    assert 'type="SOCKS5"' in xml
    assert "<Address>45.153.163.129</Address>" in xml
    assert "<Port>10808</Port>" in xml
    assert "<Password>pwd</Password>" in xml
    assert "<Username>chodop2</Username>" in xml

    # Verify roundtrip
    parsed = ProxyService.parse_proxifier_xml(xml)
    assert len(parsed) == 2
    assert parsed[0]["protocol"] == "https"
    assert parsed[0]["username"] == "chodop2"
    assert parsed[0]["password"] == "pwd"
    assert parsed[1]["protocol"] == "socks5"
    assert parsed[1]["password"] is None


def test_export_formats() -> None:
    proxy = {
        "protocol": "socks5",
        "host": "127.0.0.1",
        "port": 1080,
        "username": "user",
    }

    url = ProxyService.export_as_url(proxy, "plain_pass")
    assert url == "socks5://user:plain_pass@127.0.0.1:1080"

    lines = ProxyService.export_as_lines(proxy, "plain_pass")
    assert lines == "user\nplain_pass\n127.0.0.1\n1080"

    tg = ProxyService.export_as_tg_proxy(proxy, "mysecret")
    assert tg["deep_link"] == "tg://proxy?server=127.0.0.1&port=1080&secret=mysecret"
    assert (
        tg["web_url"] == "https://t.me/proxy?server=127.0.0.1&port=1080&secret=mysecret"
    )


def test_encryption_decryption() -> None:
    # Setup mock service dependencies
    mock_settings = MagicMock()
    mock_settings.security.api_jwt_secret = SecretStr(
        "my-super-secret-key-1234567890123"
    )

    srv = ProxyService(MagicMock(), mock_settings)

    plain = "secret-proxy-password"
    enc = srv.encryptor.encrypt(plain)
    assert enc != plain

    dec = srv.encryptor.decrypt(enc)
    assert dec == plain

    # None check
    assert srv.encryptor.encrypt(None) is None
    assert srv.encryptor.decrypt(None) is None

    # Error handling
    with pytest.raises(ValueError, match="Decryption failed"):
        srv.encryptor.decrypt("invalid-base64-data-foo-bar-baz")
