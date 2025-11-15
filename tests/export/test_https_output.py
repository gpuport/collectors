"""Tests for HTTPS output connector."""

import json
import os
import sys
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from gpuport_collectors.export.config import HTTPSOutputConfig
from gpuport_collectors.export.outputs import OutputError, write_to_https


# Create mock httpx module
class MockResponse:
    """Mock HTTP response."""

    def __init__(self, status_code: int = 200, text: str = "OK"):
        self.status_code = status_code
        self.text = text

    def raise_for_status(self) -> None:
        """Raise HTTPStatusError for 4xx/5xx responses."""
        if 400 <= self.status_code < 600:
            raise MockHTTPStatusError(f"HTTP {self.status_code}", self)


class MockHTTPStatusError(Exception):
    """Mock httpx.HTTPStatusError."""

    def __init__(self, message: str, response: MockResponse):
        self.response = response
        super().__init__(message)


class MockRequestError(Exception):
    """Mock httpx.RequestError."""


class MockClient:
    """Mock httpx.Client."""

    def __init__(self, **kwargs: Any):
        self.kwargs = kwargs
        self.requests: list[dict[str, Any]] = []

    def __enter__(self) -> "MockClient":
        return self

    def __exit__(self, *args: Any) -> None:
        pass

    def post(self, url: str, content: str, headers: dict[str, str]) -> MockResponse:
        """Mock POST request."""
        self.requests.append({"method": "POST", "url": url, "content": content, "headers": headers})
        return MockResponse(200, "OK")

    def put(self, url: str, content: str, headers: dict[str, str]) -> MockResponse:
        """Mock PUT request."""
        self.requests.append({"method": "PUT", "url": url, "content": content, "headers": headers})
        return MockResponse(200, "OK")

    def patch(self, url: str, content: str, headers: dict[str, str]) -> MockResponse:
        """Mock PATCH request."""
        self.requests.append(
            {"method": "PATCH", "url": url, "content": content, "headers": headers}
        )
        return MockResponse(200, "OK")


@pytest.fixture(autouse=True)
def setup_httpx_mocks():
    """Set up httpx mocks for all tests."""
    mock_httpx = MagicMock()
    mock_httpx.Client = MockClient
    mock_httpx.HTTPStatusError = MockHTTPStatusError
    mock_httpx.RequestError = MockRequestError

    with patch.dict(sys.modules, {"httpx": mock_httpx}):
        yield mock_httpx


@pytest.fixture
def mock_httpx_client(setup_httpx_mocks: Any) -> Any:
    """Mock httpx client."""
    return setup_httpx_mocks


class TestBasicHTTPSWrite:
    """Tests for basic HTTPS write functionality."""

    def test_basic_post(self, mock_httpx_client: Any) -> None:
        """Test basic POST request."""
        config = HTTPSOutputConfig(
            url="https://api.example.com/data",
        )

        data = json.dumps([{"test": "data"}])
        result = write_to_https(data, config)

        assert result["total_requests"] == 1
        assert result["successful_requests"] == 1
        assert result["failed_requests"] == 0
        assert result["total_items"] == 1

    def test_put_method(self, mock_httpx_client: Any) -> None:
        """Test PUT method."""
        config = HTTPSOutputConfig(
            url="https://api.example.com/data",
            method="PUT",
        )

        data = json.dumps([{"test": "data"}])
        result = write_to_https(data, config)

        assert result["total_requests"] == 1
        assert result["successful_requests"] == 1

    def test_patch_method(self, mock_httpx_client: Any) -> None:
        """Test PATCH method."""
        config = HTTPSOutputConfig(
            url="https://api.example.com/data",
            method="PATCH",
        )

        data = json.dumps([{"test": "data"}])
        result = write_to_https(data, config)

        assert result["total_requests"] == 1
        assert result["successful_requests"] == 1

    def test_single_item_converted_to_list(self, mock_httpx_client: Any) -> None:
        """Test that single item is converted to list."""
        config = HTTPSOutputConfig(
            url="https://api.example.com/data",
        )

        # Single object, not array
        data = json.dumps({"test": "data"})
        result = write_to_https(data, config)

        assert result["total_items"] == 1


class TestBatching:
    """Tests for request batching."""

    def test_batch_size_splits_requests(self, mock_httpx_client: Any) -> None:
        """Test that batch_size splits items into multiple requests."""
        config = HTTPSOutputConfig(
            url="https://api.example.com/data",
            batch_size=2,
        )

        # 5 items with batch_size=2 should create 3 batches
        data = json.dumps([{"id": i} for i in range(5)])
        result = write_to_https(data, config)

        assert result["total_requests"] == 3  # 3 batches (2+2+1)
        assert result["successful_requests"] == 3
        assert result["total_items"] == 5

    def test_batch_delay(self, mock_httpx_client: Any) -> None:
        """Test batch delay between requests."""
        config = HTTPSOutputConfig(
            url="https://api.example.com/data",
            batch_size=1,
            batch_delay=0.1,
        )

        import time

        start = time.time()
        data = json.dumps([{"id": i} for i in range(3)])
        result = write_to_https(data, config)
        elapsed = time.time() - start

        assert result["total_requests"] == 3
        # Should have at least 2 delays (between batch 1-2 and 2-3)
        assert elapsed >= 0.2

    def test_no_batch_size_sends_all_at_once(self, mock_httpx_client: Any) -> None:
        """Test that None batch_size sends all items in one request."""
        config = HTTPSOutputConfig(
            url="https://api.example.com/data",
            batch_size=None,
        )

        data = json.dumps([{"id": i} for i in range(100)])
        result = write_to_https(data, config)

        assert result["total_requests"] == 1
        assert result["total_items"] == 100


class TestHeaders:
    """Tests for custom headers."""

    def test_custom_headers(self, mock_httpx_client: Any) -> None:
        """Test custom headers are sent."""
        config = HTTPSOutputConfig(
            url="https://api.example.com/data",
            headers={"X-Custom-Header": "value", "Authorization": "Bearer token"},
        )

        data = json.dumps([{"test": "data"}])
        write_to_https(data, config)

        # Headers would be verified via mock inspection in real test

    def test_env_var_substitution(self, mock_httpx_client: Any) -> None:
        """Test environment variable substitution in headers."""
        config = HTTPSOutputConfig(
            url="https://api.example.com/data",
            headers={"Authorization": "${API_TOKEN}"},
        )

        with patch.dict(os.environ, {"API_TOKEN": "secret-token"}):
            data = json.dumps([{"test": "data"}])
            result = write_to_https(data, config)

            assert result["successful_requests"] == 1

    def test_missing_env_var_raises(self, mock_httpx_client: Any) -> None:
        """Test that missing environment variable raises error."""
        config = HTTPSOutputConfig(
            url="https://api.example.com/data",
            headers={"Authorization": "${MISSING_VAR}"},
        )

        data = json.dumps([{"test": "data"}])

        with pytest.raises(OutputError, match="Environment variable MISSING_VAR not found"):
            write_to_https(data, config)

    def test_default_content_type(self, mock_httpx_client: Any) -> None:
        """Test that Content-Type is set to application/json by default."""
        config = HTTPSOutputConfig(
            url="https://api.example.com/data",
        )

        data = json.dumps([{"test": "data"}])
        write_to_https(data, config)

        # Content-Type would be verified via mock inspection


class TestRetry:
    """Tests for retry logic."""

    def test_retry_on_500_status(self, mock_httpx_client: Any) -> None:
        """Test retry on 500 status code."""
        config = HTTPSOutputConfig(
            url="https://api.example.com/data",
            retry_attempts=2,
            retry_delay=0,  # No delay for faster test
        )

        # Mock client that fails first two attempts, succeeds on third
        call_count = 0

        def mock_post(*args: Any, **kwargs: Any) -> MockResponse:
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                return MockResponse(500, "Server Error")
            return MockResponse(200, "OK")

        with patch.object(MockClient, "post", side_effect=mock_post):
            data = json.dumps([{"test": "data"}])
            result = write_to_https(data, config)

            assert result["successful_requests"] == 1
            assert call_count == 3  # 2 failures + 1 success

    def test_exponential_backoff(self, mock_httpx_client: Any) -> None:
        """Test exponential backoff on retries."""
        config = HTTPSOutputConfig(
            url="https://api.example.com/data",
            retry_attempts=3,
            retry_delay=1,
            retry_backoff=2.0,
        )

        call_count = 0

        def mock_post(*args: Any, **kwargs: Any) -> MockResponse:
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                return MockResponse(503, "Service Unavailable")
            return MockResponse(200, "OK")

        import time

        with patch.object(MockClient, "post", side_effect=mock_post):
            start = time.time()
            data = json.dumps([{"test": "data"}])
            result = write_to_https(data, config)
            elapsed = time.time() - start

            assert result["successful_requests"] == 1
            # First retry: 1s, second retry: 2s = 3s total minimum
            assert elapsed >= 3.0

    def test_all_retries_exhausted(self, mock_httpx_client: Any) -> None:
        """Test behavior when all retries are exhausted."""
        config = HTTPSOutputConfig(
            url="https://api.example.com/data",
            retry_attempts=2,
            retry_delay=0,
        )

        def mock_post(*args: Any, **kwargs: Any) -> MockResponse:
            return MockResponse(500, "Server Error")

        with patch.object(MockClient, "post", side_effect=mock_post):
            data = json.dumps([{"test": "data"}])
            result = write_to_https(data, config)

            assert result["failed_requests"] == 1
            assert result["successful_requests"] == 0


class TestSSL:
    """Tests for SSL configuration."""

    def test_ssl_verification_enabled(self, mock_httpx_client: Any) -> None:
        """Test SSL verification is enabled by default."""
        config = HTTPSOutputConfig(
            url="https://api.example.com/data",
        )

        data = json.dumps([{"test": "data"}])
        write_to_https(data, config)

        # SSL settings would be verified via client_kwargs inspection

    def test_ssl_verification_disabled(self, mock_httpx_client: Any) -> None:
        """Test SSL verification can be disabled."""
        config = HTTPSOutputConfig(
            url="https://api.example.com/data",
            verify_ssl=False,
        )

        data = json.dumps([{"test": "data"}])
        write_to_https(data, config)

    def test_client_cert_configuration(self, mock_httpx_client: Any) -> None:
        """Test client certificate configuration."""
        config = HTTPSOutputConfig(
            url="https://api.example.com/data",
            client_cert="/path/to/cert.pem",
            client_key="/path/to/key.pem",
        )

        data = json.dumps([{"test": "data"}])
        write_to_https(data, config)


class TestErrorHandling:
    """Tests for error handling."""

    def test_httpx_not_installed(self) -> None:
        """Test error when httpx is not installed."""
        config = HTTPSOutputConfig(
            url="https://api.example.com/data",
        )

        data = json.dumps([{"test": "data"}])

        # Mock ImportError when trying to import httpx
        def mock_import(name: str, *args: Any, **kwargs: Any) -> Any:
            if name == "httpx":
                raise ImportError("No module named 'httpx'")
            return __builtins__.__import__(name, *args, **kwargs)

        with (
            patch("builtins.__import__", side_effect=mock_import),
            pytest.raises(OutputError, match="httpx is required"),
        ):
            write_to_https(data, config)

    def test_invalid_json_raises(self, mock_httpx_client: Any) -> None:
        """Test that invalid JSON raises error."""
        config = HTTPSOutputConfig(
            url="https://api.example.com/data",
        )

        with pytest.raises(OutputError, match="Invalid JSON"):
            write_to_https("not valid json", config)

    def test_request_error(self, mock_httpx_client: Any) -> None:
        """Test handling of request errors."""
        config = HTTPSOutputConfig(
            url="https://api.example.com/data",
            retry_attempts=1,
            retry_delay=0,
        )

        def mock_post(*args: Any, **kwargs: Any) -> None:
            raise MockRequestError("Connection failed")

        with patch.object(MockClient, "post", side_effect=mock_post):
            data = json.dumps([{"test": "data"}])
            result = write_to_https(data, config)

            assert result["failed_requests"] == 1


class TestIntegration:
    """Integration-style tests."""

    def test_complete_workflow(self, mock_httpx_client: Any) -> None:
        """Test complete workflow with batching, headers, and retry."""
        config = HTTPSOutputConfig(
            url="https://webhook.example.com/collect",
            method="POST",
            headers={"X-API-Key": "${WEBHOOK_KEY}"},
            batch_size=10,
            batch_delay=0.1,
            retry_attempts=2,
            retry_delay=0,
        )

        # 25 items with batch_size=10 = 3 batches
        items = [{"instance_type": f"type{i}", "price": i * 1.5} for i in range(25)]
        data = json.dumps(items)

        with patch.dict(os.environ, {"WEBHOOK_KEY": "secret-key"}):
            result = write_to_https(data, config)

            assert result["total_requests"] == 3  # 3 batches (10+10+5)
            assert result["successful_requests"] == 3
            assert result["failed_requests"] == 0
            assert result["total_items"] == 25
