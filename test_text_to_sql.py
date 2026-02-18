#!/usr/bin/env python3
"""Tests for text_to_sql.py - HTTP interface and port detection"""

import os
import unittest
from unittest.mock import patch, MagicMock


class TestClickHouseSQLGeneratorInit(unittest.TestCase):
    """Test initialization and port detection"""

    @patch.dict(os.environ, {
        'OPENROUTER_API_KEY': 'test-key',
        'CLICKHOUSE_HOST': 'test-host.net',
        'CLICKHOUSE_PORT': '8443',
        'CLICKHOUSE_USER': 'user',
        'CLICKHOUSE_PASSWORD': 'pass',
        'CLICKHOUSE_DATABASE': 'testdb',
    })
    def test_http_port_8443_detected(self):
        """Port 8443 should trigger HTTP interface mode"""
        from text_to_sql import ClickHouseSQLGenerator
        gen = ClickHouseSQLGenerator()
        self.assertTrue(gen.use_http)

    @patch.dict(os.environ, {
        'OPENROUTER_API_KEY': 'test-key',
        'CLICKHOUSE_HOST': 'test-host.net',
        'CLICKHOUSE_PORT': '8123',
        'CLICKHOUSE_USER': 'user',
        'CLICKHOUSE_PASSWORD': 'pass',
        'CLICKHOUSE_DATABASE': 'testdb',
    })
    def test_http_port_8123_detected(self):
        """Port 8123 should trigger HTTP interface mode"""
        from text_to_sql import ClickHouseSQLGenerator
        gen = ClickHouseSQLGenerator()
        self.assertTrue(gen.use_http)

    @patch.dict(os.environ, {
        'OPENROUTER_API_KEY': 'test-key',
        'CLICKHOUSE_HOST': 'test-host.net',
        'CLICKHOUSE_PORT': '9440',
        'CLICKHOUSE_USER': 'user',
        'CLICKHOUSE_PASSWORD': 'pass',
        'CLICKHOUSE_DATABASE': 'testdb',
    })
    def test_native_port_9440_not_http(self):
        """Port 9440 should NOT trigger HTTP interface mode"""
        from text_to_sql import ClickHouseSQLGenerator
        gen = ClickHouseSQLGenerator()
        self.assertFalse(gen.use_http)

    @patch.dict(os.environ, {
        'OPENROUTER_API_KEY': 'test-key',
        'CLICKHOUSE_HOST': 'test-host.net',
        'CLICKHOUSE_PORT': '9000',
        'CLICKHOUSE_USER': 'user',
        'CLICKHOUSE_PASSWORD': 'pass',
        'CLICKHOUSE_DATABASE': 'testdb',
    })
    def test_native_port_9000_not_http(self):
        """Port 9000 should NOT trigger HTTP interface mode"""
        from text_to_sql import ClickHouseSQLGenerator
        gen = ClickHouseSQLGenerator()
        self.assertFalse(gen.use_http)


class TestBuildHttpUrl(unittest.TestCase):
    """Test HTTP URL construction"""

    @patch.dict(os.environ, {
        'OPENROUTER_API_KEY': 'test-key',
        'CLICKHOUSE_HOST': 'my-host.net',
        'CLICKHOUSE_PORT': '8443',
        'CLICKHOUSE_USER': 'user',
        'CLICKHOUSE_PASSWORD': 'pass',
        'CLICKHOUSE_DATABASE': 'testdb',
    })
    def test_https_url_for_port_8443(self):
        """Port 8443 should produce https:// URL"""
        from text_to_sql import ClickHouseSQLGenerator
        gen = ClickHouseSQLGenerator()
        url = gen._build_http_url()
        self.assertEqual(url, 'https://my-host.net:8443/')

    @patch.dict(os.environ, {
        'OPENROUTER_API_KEY': 'test-key',
        'CLICKHOUSE_HOST': 'my-host.net',
        'CLICKHOUSE_PORT': '8123',
        'CLICKHOUSE_USER': 'user',
        'CLICKHOUSE_PASSWORD': 'pass',
        'CLICKHOUSE_DATABASE': 'testdb',
    })
    def test_http_url_for_port_8123(self):
        """Port 8123 should produce http:// URL"""
        from text_to_sql import ClickHouseSQLGenerator
        gen = ClickHouseSQLGenerator()
        url = gen._build_http_url()
        self.assertEqual(url, 'http://my-host.net:8123/')


class TestBuildClickhouseCommand(unittest.TestCase):
    """Test clickhouse-client command building with native port fallback"""

    @patch.dict(os.environ, {
        'OPENROUTER_API_KEY': 'test-key',
        'CLICKHOUSE_HOST': 'my-host.net',
        'CLICKHOUSE_PORT': '8443',
        'CLICKHOUSE_USER': 'user',
        'CLICKHOUSE_PASSWORD': 'pass',
        'CLICKHOUSE_DATABASE': 'testdb',
    })
    def test_native_port_used_when_http_port_configured(self):
        """When port is 8443, clickhouse-client should use native port 9440"""
        from text_to_sql import ClickHouseSQLGenerator
        gen = ClickHouseSQLGenerator()
        cmd = gen._build_clickhouse_command("SELECT 1")
        # Should contain --port 9440, not 8443
        port_idx = cmd.index('--port')
        self.assertEqual(cmd[port_idx + 1], '9440')

    @patch.dict(os.environ, {
        'OPENROUTER_API_KEY': 'test-key',
        'CLICKHOUSE_HOST': 'my-host.net',
        'CLICKHOUSE_PORT': '9440',
        'CLICKHOUSE_USER': 'user',
        'CLICKHOUSE_PASSWORD': 'pass',
        'CLICKHOUSE_DATABASE': 'testdb',
    })
    def test_configured_port_used_for_native_port(self):
        """When port is 9440 (native), clickhouse-client should use it directly"""
        from text_to_sql import ClickHouseSQLGenerator
        gen = ClickHouseSQLGenerator()
        cmd = gen._build_clickhouse_command("SELECT 1")
        port_idx = cmd.index('--port')
        self.assertEqual(cmd[port_idx + 1], '9440')

    @patch.dict(os.environ, {
        'OPENROUTER_API_KEY': 'test-key',
        'CLICKHOUSE_HOST': 'my-host.net',
        'CLICKHOUSE_PORT': '8443',
        'CLICKHOUSE_NATIVE_PORT': '9441',
        'CLICKHOUSE_USER': 'user',
        'CLICKHOUSE_PASSWORD': 'pass',
        'CLICKHOUSE_DATABASE': 'testdb',
    })
    def test_custom_native_port_override(self):
        """CLICKHOUSE_NATIVE_PORT should override default 9440"""
        from text_to_sql import ClickHouseSQLGenerator
        gen = ClickHouseSQLGenerator()
        cmd = gen._build_clickhouse_command("SELECT 1")
        port_idx = cmd.index('--port')
        self.assertEqual(cmd[port_idx + 1], '9441')


class TestHostParsing(unittest.TestCase):
    """Test that https:// prefix is properly stripped from host"""

    @patch.dict(os.environ, {
        'OPENROUTER_API_KEY': 'test-key',
        'CLICKHOUSE_HOST': 'https://my-host.net',
        'CLICKHOUSE_PORT': '8443',
        'CLICKHOUSE_USER': 'user',
        'CLICKHOUSE_PASSWORD': 'pass',
        'CLICKHOUSE_DATABASE': 'testdb',
    })
    def test_https_prefix_stripped(self):
        """https:// prefix should be stripped from host"""
        from text_to_sql import ClickHouseSQLGenerator
        gen = ClickHouseSQLGenerator()
        self.assertEqual(gen.ch_host, 'my-host.net')


if __name__ == '__main__':
    unittest.main()
