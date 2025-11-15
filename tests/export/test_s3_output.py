"""Tests for S3 output connector."""

import gzip
import json
import os
import sys
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from gpuport_collectors.export.config import S3OutputConfig
from gpuport_collectors.export.outputs import OutputError, write_to_s3


# Create mock boto3 and botocore modules
class MockBotoCoreError(Exception):
    """Mock BotoCoreError."""


class MockClientError(Exception):
    """Mock ClientError."""

    def __init__(self, error_response, operation_name):
        self.response = error_response
        self.operation_name = operation_name
        super().__init__(str(error_response))


@pytest.fixture(autouse=True)
def setup_boto3_mocks():
    """Set up boto3 mocks for all tests."""
    # Create mock modules
    mock_boto3 = MagicMock()
    mock_botocore = MagicMock()
    mock_botocore_exceptions = MagicMock()

    # Set up exception classes
    mock_botocore_exceptions.BotoCoreError = MockBotoCoreError
    mock_botocore_exceptions.ClientError = MockClientError

    # Install mocks in sys.modules
    with patch.dict(
        sys.modules,
        {
            "boto3": mock_boto3,
            "botocore": mock_botocore,
            "botocore.exceptions": mock_botocore_exceptions,
        },
    ):
        yield mock_boto3, mock_botocore_exceptions


@pytest.fixture
def mock_s3_client(setup_boto3_mocks):
    """Mock S3 client."""
    mock_boto3, mock_exceptions = setup_boto3_mocks
    mock_client = MagicMock()
    mock_boto3.client.return_value = mock_client
    return mock_boto3, mock_client, mock_exceptions


class TestBasicS3Upload:
    """Tests for basic S3 upload functionality."""

    def test_basic_upload(self, mock_s3_client: tuple[Any, Any, Any]) -> None:
        """Test basic S3 upload."""
        _mock_boto3, mock_client, _mock_exceptions = mock_s3_client

        config = S3OutputConfig(
            bucket="test-bucket",
            filename_pattern="test.txt",
        )

        data = "Hello, S3!"
        key = write_to_s3(data, config)

        assert key == "test.txt"
        mock_client.put_object.assert_called_once()
        call_kwargs = mock_client.put_object.call_args.kwargs
        assert call_kwargs["Bucket"] == "test-bucket"
        assert call_kwargs["Key"] == "test.txt"
        assert call_kwargs["Body"] == b"Hello, S3!"

    def test_upload_with_prefix(self, mock_s3_client: tuple[Any, Any, Any]) -> None:
        """Test S3 upload with key prefix."""
        _mock_boto3, mock_client, _mock_exceptions = mock_s3_client

        config = S3OutputConfig(
            bucket="test-bucket",
            prefix="exports/data",
            filename_pattern="test.txt",
        )

        data = "test data"
        key = write_to_s3(data, config)

        assert key == "exports/data/test.txt"
        call_kwargs = mock_client.put_object.call_args.kwargs
        assert call_kwargs["Key"] == "exports/data/test.txt"

    def test_upload_with_trailing_slash_prefix(self, mock_s3_client: tuple[Any, Any, Any]) -> None:
        """Test that trailing slash in prefix is handled correctly."""
        _mock_boto3, _mock_client, _mock_exceptions = mock_s3_client

        config = S3OutputConfig(
            bucket="test-bucket",
            prefix="exports/data/",
            filename_pattern="test.txt",
        )

        key = write_to_s3("test", config)

        assert key == "exports/data/test.txt"


class TestFilenamePatterns:
    """Tests for filename pattern substitution."""

    def test_date_time_patterns(self, mock_s3_client: tuple[Any, Any, Any]) -> None:
        """Test filename pattern with date/time placeholders."""
        _mock_boto3, _mock_client, _mock_exceptions = mock_s3_client

        config = S3OutputConfig(
            bucket="test-bucket",
            filename_pattern="export_{date}_{time}.txt",
        )

        key = write_to_s3("test", config)

        assert "export_" in key
        assert key.endswith(".txt")
        # Should contain date and time
        parts = key.replace("export_", "").replace(".txt", "").split("_")
        assert len(parts) == 2  # date and time

    def test_metadata_substitution(self, mock_s3_client: tuple[Any, Any, Any]) -> None:
        """Test filename pattern with metadata substitution."""
        _mock_boto3, _mock_client, _mock_exceptions = mock_s3_client

        config = S3OutputConfig(
            bucket="test-bucket",
            filename_pattern="{provider}_{format}_{date}.{format}",
        )

        metadata = {"provider": "AWS", "format": "json"}
        key = write_to_s3('{"test": "data"}', config, metadata)

        assert "AWS" in key
        assert key.endswith(".json")


class TestCompression:
    """Tests for gzip compression."""

    def test_gzip_compression(self, mock_s3_client: tuple[Any, Any, Any]) -> None:
        """Test gzip compression of uploaded data."""
        _mock_boto3, mock_client, _mock_exceptions = mock_s3_client

        config = S3OutputConfig(
            bucket="test-bucket",
            filename_pattern="test.txt",
            compression="gzip",
        )

        data = "Hello, compressed S3!"
        key = write_to_s3(data, config)

        assert key == "test.txt.gz"
        call_kwargs = mock_client.put_object.call_args.kwargs

        # Verify data is gzipped
        uploaded_data = call_kwargs["Body"]
        decompressed = gzip.decompress(uploaded_data).decode("utf-8")
        assert decompressed == data

    def test_gzip_preserves_extension(self, mock_s3_client: tuple[Any, Any, Any]) -> None:
        """Test that .gz extension is not duplicated."""
        _mock_boto3, _mock_client, _mock_exceptions = mock_s3_client

        config = S3OutputConfig(
            bucket="test-bucket",
            filename_pattern="test.json.gz",
            compression="gzip",
        )

        key = write_to_s3('{"test": "data"}', config)

        assert key == "test.json.gz"
        assert key.count(".gz") == 1


class TestCredentials:
    """Tests for S3 credential handling."""

    def test_default_credentials(self, mock_s3_client: tuple[Any, Any, Any]) -> None:
        """Test using default AWS credentials."""
        _mock_boto3, _mock_client, _mock_exceptions = mock_s3_client

        config = S3OutputConfig(
            bucket="test-bucket",
            filename_pattern="test.txt",
        )

        write_to_s3("test", config)

        # Should create client without explicit credentials
        mock_s3_client[0].client.assert_called_once()
        call_kwargs = mock_s3_client[0].client.call_args.kwargs
        assert "aws_access_key_id" not in call_kwargs
        assert "aws_secret_access_key" not in call_kwargs

    def test_env_credentials(self, mock_s3_client: tuple[Any, Any, Any]) -> None:
        """Test loading credentials from environment variables."""
        _mock_boto3, _mock_client, _mock_exceptions = mock_s3_client

        config = S3OutputConfig(
            bucket="test-bucket",
            filename_pattern="test.txt",
            credentials={
                "access_key_env": "MY_ACCESS_KEY",
                "secret_key_env": "MY_SECRET_KEY",
            },
        )

        with patch.dict(os.environ, {"MY_ACCESS_KEY": "test-key", "MY_SECRET_KEY": "test-secret"}):
            write_to_s3("test", config)

        call_kwargs = mock_s3_client[0].client.call_args.kwargs
        assert call_kwargs["aws_access_key_id"] == "test-key"
        assert call_kwargs["aws_secret_access_key"] == "test-secret"

    def test_session_token(self, mock_s3_client: tuple[Any, Any, Any]) -> None:
        """Test loading session token from environment."""
        _mock_boto3, _mock_client, _mock_exceptions = mock_s3_client

        config = S3OutputConfig(
            bucket="test-bucket",
            filename_pattern="test.txt",
            credentials={
                "access_key_env": "AWS_ACCESS_KEY_ID",
                "secret_key_env": "AWS_SECRET_ACCESS_KEY",
                "session_token_env": "AWS_SESSION_TOKEN",
            },
        )

        with patch.dict(
            os.environ,
            {
                "AWS_ACCESS_KEY_ID": "test-key",
                "AWS_SECRET_ACCESS_KEY": "test-secret",
                "AWS_SESSION_TOKEN": "test-token",
            },
        ):
            write_to_s3("test", config)

        call_kwargs = mock_s3_client[0].client.call_args.kwargs
        assert call_kwargs["aws_session_token"] == "test-token"


class TestS3Configuration:
    """Tests for S3-specific configuration options."""

    def test_region_configuration(self, mock_s3_client: tuple[Any, Any, Any]) -> None:
        """Test S3 region configuration."""
        _mock_boto3, _mock_client, _mock_exceptions = mock_s3_client

        config = S3OutputConfig(
            bucket="test-bucket",
            filename_pattern="test.txt",
            region="us-west-2",
        )

        write_to_s3("test", config)

        call_kwargs = mock_s3_client[0].client.call_args.kwargs
        assert call_kwargs["region_name"] == "us-west-2"

    def test_custom_endpoint(self, mock_s3_client: tuple[Any, Any, Any]) -> None:
        """Test custom endpoint URL (for S3-compatible storage)."""
        _mock_boto3, _mock_client, _mock_exceptions = mock_s3_client

        config = S3OutputConfig(
            bucket="test-bucket",
            filename_pattern="test.txt",
            endpoint_url="https://minio.example.com",
        )

        write_to_s3("test", config)

        call_kwargs = mock_s3_client[0].client.call_args.kwargs
        assert call_kwargs["endpoint_url"] == "https://minio.example.com"

    def test_storage_class(self, mock_s3_client: tuple[Any, Any, Any]) -> None:
        """Test S3 storage class configuration."""
        _mock_boto3, mock_client, _mock_exceptions = mock_s3_client

        config = S3OutputConfig(
            bucket="test-bucket",
            filename_pattern="test.txt",
            storage_class="GLACIER",
        )

        write_to_s3("test", config)

        call_kwargs = mock_client.put_object.call_args.kwargs
        assert call_kwargs["StorageClass"] == "GLACIER"

    def test_server_side_encryption(self, mock_s3_client: tuple[Any, Any, Any]) -> None:
        """Test server-side encryption configuration."""
        _mock_boto3, mock_client, _mock_exceptions = mock_s3_client

        config = S3OutputConfig(
            bucket="test-bucket",
            filename_pattern="test.txt",
            server_side_encryption="AES256",
        )

        write_to_s3("test", config)

        call_kwargs = mock_client.put_object.call_args.kwargs
        assert call_kwargs["ServerSideEncryption"] == "AES256"

    def test_acl(self, mock_s3_client: tuple[Any, Any, Any]) -> None:
        """Test ACL configuration."""
        _mock_boto3, mock_client, _mock_exceptions = mock_s3_client

        config = S3OutputConfig(
            bucket="test-bucket",
            filename_pattern="test.txt",
            acl="public-read",
        )

        write_to_s3("test", config)

        call_kwargs = mock_client.put_object.call_args.kwargs
        assert call_kwargs["ACL"] == "public-read"

    def test_custom_metadata(self, mock_s3_client: tuple[Any, Any, Any]) -> None:
        """Test custom metadata configuration."""
        _mock_boto3, mock_client, _mock_exceptions = mock_s3_client

        config = S3OutputConfig(
            bucket="test-bucket",
            filename_pattern="test.txt",
            metadata={"environment": "production", "version": "1.0"},
        )

        write_to_s3("test", config)

        call_kwargs = mock_client.put_object.call_args.kwargs
        assert call_kwargs["Metadata"] == {"environment": "production", "version": "1.0"}


class TestErrorHandling:
    """Tests for error handling."""

    def test_boto3_not_installed(self, monkeypatch: Any) -> None:
        """Test error when boto3 is not installed."""
        config = S3OutputConfig(
            bucket="test-bucket",
            filename_pattern="test.txt",
        )

        # Use pytest's monkeypatch for cleaner module removal
        monkeypatch.delitem(sys.modules, "boto3", raising=False)
        monkeypatch.delitem(sys.modules, "botocore", raising=False)
        monkeypatch.delitem(sys.modules, "botocore.exceptions", raising=False)

        with pytest.raises(OutputError, match="boto3 is required"):
            write_to_s3("test", config)

    def test_s3_client_error(self, mock_s3_client: tuple[Any, Any, Any]) -> None:
        """Test handling of S3 client errors."""
        _mock_boto3, mock_client, _mock_exceptions = mock_s3_client

        error_response = {"Error": {"Code": "NoSuchBucket", "Message": "Bucket not found"}}
        mock_client.put_object.side_effect = mock_s3_client[2].ClientError(
            error_response, "PutObject"
        )

        config = S3OutputConfig(
            bucket="nonexistent-bucket",
            filename_pattern="test.txt",
        )

        with pytest.raises(OutputError, match="Failed to write to S3"):
            write_to_s3("test", config)

    def test_network_error(self, mock_s3_client: tuple[Any, Any, Any]) -> None:
        """Test handling of network errors."""
        _mock_boto3, mock_client, _mock_exceptions = mock_s3_client

        mock_client.put_object.side_effect = mock_s3_client[2].BotoCoreError()

        config = S3OutputConfig(
            bucket="test-bucket",
            filename_pattern="test.txt",
        )

        with pytest.raises(OutputError, match="Failed to write to S3"):
            write_to_s3("test", config)


class TestIntegration:
    """Integration-style tests."""

    def test_complete_workflow(self, mock_s3_client: tuple[Any, Any, Any]) -> None:
        """Test complete upload workflow with all features."""
        _mock_boto3, mock_client, _mock_exceptions = mock_s3_client

        config = S3OutputConfig(
            bucket="production-data",
            prefix="exports/gpu-instances",
            filename_pattern="{provider}_{date}.json",
            compression="gzip",
            region="us-east-1",
            storage_class="INTELLIGENT_TIERING",
            server_side_encryption="aws:kms",
            metadata={"source": "gpu-collector", "version": "1.0"},
        )

        data = json.dumps({"instances": [{"type": "p3.2xlarge", "price": 3.06}]})
        metadata = {"provider": "AWS"}

        with patch.dict(
            os.environ, {"AWS_ACCESS_KEY_ID": "test-key", "AWS_SECRET_ACCESS_KEY": "test-secret"}
        ):
            key = write_to_s3(data, config, metadata)

        # Verify key structure
        assert key.startswith("exports/gpu-instances/AWS_")
        assert key.endswith(".json.gz")

        # Verify put_object was called with correct parameters
        call_kwargs = mock_client.put_object.call_args.kwargs
        assert call_kwargs["Bucket"] == "production-data"
        assert call_kwargs["StorageClass"] == "INTELLIGENT_TIERING"
        assert call_kwargs["ServerSideEncryption"] == "aws:kms"
        assert call_kwargs["Metadata"] == {"source": "gpu-collector", "version": "1.0"}

        # Verify data is compressed
        uploaded_data = call_kwargs["Body"]
        decompressed = gzip.decompress(uploaded_data).decode("utf-8")
        assert json.loads(decompressed) == json.loads(data)
