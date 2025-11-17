"""Output connectors for export pipeline.

Provides output connectors for writing transformed data to various destinations
(local filesystem, S3, HTTPS endpoints).
"""

import gzip
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from gpuport_collectors.export.config import HTTPSOutputConfig, LocalOutputConfig, S3OutputConfig


class OutputError(Exception):
    """Raised when output operation fails."""


def write_to_local(
    data: str, config: LocalOutputConfig, metadata: dict[str, Any] | None = None
) -> Path:
    """Write data to local filesystem.

    Args:
        data: Data to write (string)
        config: Local output configuration
        metadata: Optional metadata for filename pattern substitution

    Returns:
        Path to the written file

    Raises:
        OutputError: If write operation fails
    """
    try:
        # Create output directory
        output_dir = Path(config.path)
        if config.create_dirs:
            output_dir.mkdir(parents=True, exist_ok=True)
        elif not output_dir.exists():
            raise OutputError(f"Output directory does not exist: {output_dir}")

        # Generate filename from pattern
        filename = _apply_filename_pattern(config.filename_pattern, metadata or {})

        # Add compression extension if needed
        if config.compression == "gzip" and not filename.endswith(".gz"):
            filename += ".gz"

        output_path = output_dir / filename

        # Create intermediate directories if filename contains paths
        if config.create_dirs and "/" in filename:
            output_path.parent.mkdir(parents=True, exist_ok=True)

        # Check for overwrite protection
        if output_path.exists() and not config.overwrite:
            raise OutputError(f"File already exists and overwrite is disabled: {output_path}")

        # Write data (with optional compression)
        if config.compression == "gzip":
            _write_gzip(output_path, data)
        else:
            _write_atomic(output_path, data)

        return output_path

    except OSError as e:
        raise OutputError(f"Failed to write to local filesystem: {e}") from e


def _sanitize_path_component(value: str) -> str:
    """Sanitize a value for safe use in a filename.

    Removes path separators, parent directory references, and other
    potentially dangerous characters to prevent path traversal attacks.

    Args:
        value: Value to sanitize

    Returns:
        Sanitized string safe for use in filenames
    """
    # Remove path separators and parent directory references
    sanitized = value.replace("/", "_").replace("\\", "_").replace("..", "__")
    # Replace any other problematic characters with underscore
    return "".join(c if c.isalnum() or c in "-_." else "_" for c in sanitized)


def _apply_filename_pattern(pattern: str, metadata: dict[str, Any]) -> str:
    """Apply filename pattern substitution with security sanitization.

    Args:
        pattern: Filename pattern with placeholders
        metadata: Metadata for substitution

    Returns:
        Filename with substitutions applied and sanitized

    Raises:
        OutputError: If pattern contains unsubstituted placeholders

    Note:
        Metadata values are sanitized to prevent path traversal attacks.
        Path separators (/, \\) and parent directory references (..) are
        replaced with safe characters.
    """
    result = pattern

    # Get current datetime
    now = datetime.now(UTC)

    # Standard placeholders
    placeholders = {
        "{date}": now.strftime("%Y-%m-%d"),
        "{time}": now.strftime("%H-%M-%S"),
        "{timestamp}": now.strftime("%Y%m%d-%H%M%S"),
        "{year}": now.strftime("%Y"),
        "{month}": now.strftime("%m"),
        "{day}": now.strftime("%d"),
        "{hour}": now.strftime("%H"),
        "{minute}": now.strftime("%M"),
        "{second}": now.strftime("%S"),
    }

    # Add metadata placeholders with sanitization
    for key, value in metadata.items():
        placeholders[f"{{{key}}}"] = _sanitize_path_component(str(value))

    # Apply substitutions
    for placeholder, value in placeholders.items():
        result = result.replace(placeholder, value)

    # Check for unsubstituted placeholders
    import re

    remaining_placeholders = re.findall(r"\{([^}]+)\}", result)
    if remaining_placeholders:
        raise OutputError(
            f"Unsubstituted placeholders in filename pattern: {', '.join(remaining_placeholders)}. "
            f"Provide these in metadata or use standard placeholders: "
            f"{', '.join(p.strip('{}') for p in placeholders)}"
        )

    return result


def _write_atomic(path: Path, data: str) -> None:
    """Write data atomically using temp file + rename.

    Args:
        path: Target file path
        data: Data to write
    """
    # Write to temp file
    temp_path = path.with_suffix(path.suffix + ".tmp")
    try:
        temp_path.write_text(data, encoding="utf-8")
        # Atomic rename
        temp_path.replace(path)
    except Exception:
        # Clean up temp file on error
        if temp_path.exists():
            temp_path.unlink()
        raise


def _write_gzip(path: Path, data: str) -> None:
    """Write data with gzip compression.

    Args:
        path: Target file path (should end with .gz)
        data: Data to write
    """
    # Write to temp file
    temp_path = path.with_suffix(path.suffix + ".tmp")
    try:
        with gzip.open(temp_path, "wt", encoding="utf-8") as f:
            f.write(data)
        # Atomic rename
        temp_path.replace(path)
    except Exception:
        # Clean up temp file on error
        if temp_path.exists():
            temp_path.unlink()
        raise


def write_to_s3(data: str, config: S3OutputConfig, metadata: dict[str, Any] | None = None) -> str:
    """Write data to S3-compatible storage.

    Args:
        data: Data to write (string)
        config: S3 output configuration
        metadata: Optional metadata for filename pattern substitution

    Returns:
        S3 key (path) of the written object

    Raises:
        OutputError: If write operation fails
    """
    try:
        import boto3  # type: ignore[import-not-found]
        from botocore.exceptions import BotoCoreError, ClientError  # type: ignore[import-not-found]
    except ImportError as e:
        raise OutputError("boto3 is required for S3 output. Install with: pip install boto3") from e

    try:
        # Generate filename from pattern
        filename = _apply_filename_pattern(config.filename_pattern, metadata or {})

        # Add compression extension if needed
        if config.compression == "gzip" and not filename.endswith(".gz"):
            filename += ".gz"

        # Construct full S3 key
        key = f"{config.prefix.rstrip('/')}/{filename}" if config.prefix else filename

        # Prepare data (compress if needed)
        if config.compression == "gzip":
            # Compress data in memory
            import io

            buffer = io.BytesIO()
            with gzip.open(buffer, "wb") as f:
                f.write(data.encode("utf-8"))
            upload_data = buffer.getvalue()
        else:
            upload_data = data.encode("utf-8")

        # Get credentials from environment
        aws_access_key_id = None
        aws_secret_access_key = None
        aws_session_token = None

        if config.credentials:
            access_key_env = config.credentials.get("access_key_env", "AWS_ACCESS_KEY_ID")
            secret_key_env = config.credentials.get("secret_key_env", "AWS_SECRET_ACCESS_KEY")
            session_token_env = config.credentials.get("session_token_env")

            aws_access_key_id = os.environ.get(access_key_env)
            aws_secret_access_key = os.environ.get(secret_key_env)
            if session_token_env:
                aws_session_token = os.environ.get(session_token_env)

        # Create S3 client
        client_kwargs: dict[str, Any] = {}
        if config.region:
            client_kwargs["region_name"] = config.region
        if config.endpoint_url:
            client_kwargs["endpoint_url"] = config.endpoint_url
        if aws_access_key_id and aws_secret_access_key:
            client_kwargs["aws_access_key_id"] = aws_access_key_id
            client_kwargs["aws_secret_access_key"] = aws_secret_access_key
            if aws_session_token:
                client_kwargs["aws_session_token"] = aws_session_token

        s3_client = boto3.client("s3", **client_kwargs)

        # Prepare put_object kwargs
        put_kwargs: dict[str, Any] = {
            "Bucket": config.bucket,
            "Key": key,
            "Body": upload_data,
            "StorageClass": config.storage_class,
            "ACL": config.acl,
        }

        # Add encryption if configured
        if config.server_side_encryption:
            put_kwargs["ServerSideEncryption"] = config.server_side_encryption

        # Add custom metadata if provided
        if config.metadata:
            put_kwargs["Metadata"] = config.metadata

        # Upload to S3
        s3_client.put_object(**put_kwargs)

        return key

    except (BotoCoreError, ClientError) as e:
        raise OutputError(f"Failed to write to S3: {e}") from e
    except OSError as e:
        raise OutputError(f"Failed to prepare data for S3: {e}") from e


def write_to_https(data: str, config: HTTPSOutputConfig) -> dict[str, Any]:
    """Write data to HTTPS endpoint with retry logic and batching.

    Args:
        data: Data to write (JSON string)
        config: HTTPS output configuration

    Returns:
        Dictionary with summary: {
            'total_requests': int,
            'successful_requests': int,
            'failed_requests': int,
            'total_items': int
        }

    Raises:
        OutputError: If all retry attempts fail or httpx is not installed
    """
    try:
        import httpx
    except ImportError as e:
        raise OutputError(
            "httpx is required for HTTPS output. Install with: pip install httpx"
        ) from e

    import json
    import time

    try:
        # Parse the data (should be JSON string)
        items = json.loads(data)
        if not isinstance(items, list):
            items = [items]

        total_items = len(items)
        total_requests = 0
        successful_requests = 0
        failed_requests = 0

        # Prepare headers with environment variable substitution
        headers = {}
        if config.headers:
            for key, value in config.headers.items():
                # Simple ${VAR} substitution
                if value.startswith("${") and value.endswith("}"):
                    env_var = value[2:-1]
                    env_value = os.environ.get(env_var)
                    if env_value is None:
                        raise OutputError(
                            f"Environment variable {env_var} not found for header {key}"
                        )
                    headers[key] = env_value
                else:
                    headers[key] = value

        # Ensure Content-Type is set
        if "Content-Type" not in headers:
            headers["Content-Type"] = "application/json"

        # Create httpx client with SSL settings
        client_kwargs: dict[str, Any] = {
            "timeout": config.timeout,
            "verify": config.verify_ssl,
        }
        if config.client_cert and config.client_key:
            client_kwargs["cert"] = (config.client_cert, config.client_key)

        # Determine batching
        batch_size = config.batch_size if config.batch_size else len(items)
        batches = [items[i : i + batch_size] for i in range(0, len(items), batch_size)]

        with httpx.Client(**client_kwargs) as client:
            for batch_idx, batch in enumerate(batches):
                # Add delay between batches (skip for first batch)
                if batch_idx > 0 and config.batch_delay > 0:
                    time.sleep(config.batch_delay)

                # Prepare payload
                payload = json.dumps(batch)

                # Retry logic
                last_error = None
                for attempt in range(config.retry_attempts + 1):
                    try:
                        # Make request
                        if config.method == "POST":
                            response = client.post(config.url, content=payload, headers=headers)
                        elif config.method == "PUT":
                            response = client.put(config.url, content=payload, headers=headers)
                        else:  # PATCH
                            response = client.patch(config.url, content=payload, headers=headers)

                        total_requests += 1

                        # Check if we should retry based on status code
                        if response.status_code in config.retry_on_status:
                            last_error = OutputError(
                                f"HTTP {response.status_code}: {response.text[:200]}"
                            )
                            if attempt < config.retry_attempts:
                                # Calculate retry delay with exponential backoff
                                delay = config.retry_delay * (config.retry_backoff**attempt)
                                time.sleep(delay)
                                continue
                            # All retries exhausted
                            failed_requests += 1
                            break

                        # Success (2xx or non-retryable status)
                        response.raise_for_status()
                        successful_requests += 1
                        break

                    except httpx.HTTPStatusError as e:
                        last_error = OutputError(f"HTTP error: {e}")
                        if attempt < config.retry_attempts:
                            delay = config.retry_delay * (config.retry_backoff**attempt)
                            time.sleep(delay)
                        else:
                            failed_requests += 1

                    except httpx.RequestError as e:
                        last_error = OutputError(f"Request error: {e}")
                        if attempt < config.retry_attempts:
                            delay = config.retry_delay * (config.retry_backoff**attempt)
                            time.sleep(delay)
                        else:
                            failed_requests += 1

                # If this batch completely failed after all retries, track it
                if last_error and attempt >= config.retry_attempts:
                    # Batch failed - error already counted above
                    pass

        # Return summary
        return {
            "total_requests": total_requests,
            "successful_requests": successful_requests,
            "failed_requests": failed_requests,
            "total_items": total_items,
        }

    except json.JSONDecodeError as e:
        raise OutputError(f"Invalid JSON data: {e}") from e
    except Exception as e:
        raise OutputError(f"Failed to write to HTTPS endpoint: {e}") from e
