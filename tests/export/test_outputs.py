"""Tests for output connectors."""

import contextlib
import gzip
from pathlib import Path

import pytest

from gpuport_collectors.export.config import LocalOutputConfig
from gpuport_collectors.export.outputs import OutputError, write_to_local


@pytest.fixture
def temp_output_dir(tmp_path: Path) -> Path:
    """Create temporary output directory."""
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    return output_dir


class TestLocalOutput:
    """Tests for local filesystem output."""

    def test_write_basic(self, temp_output_dir: Path) -> None:
        """Test basic file write."""
        config = LocalOutputConfig(
            path=str(temp_output_dir),
            filename_pattern="test.txt",
        )

        data = "Hello, World!"
        output_path = write_to_local(data, config)

        assert output_path.exists()
        assert output_path.read_text() == data
        assert output_path.parent == temp_output_dir

    def test_write_with_date_pattern(self, temp_output_dir: Path) -> None:
        """Test filename pattern with date placeholders."""
        config = LocalOutputConfig(
            path=str(temp_output_dir),
            filename_pattern="export_{date}_{time}.txt",
        )

        data = "test data"
        output_path = write_to_local(data, config)

        assert output_path.exists()
        assert output_path.read_text() == data
        # Should contain date and time in filename
        assert "_" in output_path.name
        assert output_path.name.endswith(".txt")

    def test_write_with_metadata(self, temp_output_dir: Path) -> None:
        """Test filename pattern with metadata substitution."""
        config = LocalOutputConfig(
            path=str(temp_output_dir),
            filename_pattern="{provider}_{format}_{date}.{format}",
        )

        metadata = {"provider": "TestProvider", "format": "json"}
        data = '{"test": "data"}'
        output_path = write_to_local(data, config, metadata)

        assert output_path.exists()
        assert "TestProvider" in output_path.name
        assert output_path.name.endswith(".json")

    def test_write_creates_directory(self, tmp_path: Path) -> None:
        """Test directory creation when create_dirs is True."""
        nested_dir = tmp_path / "level1" / "level2" / "level3"

        config = LocalOutputConfig(
            path=str(nested_dir),
            filename_pattern="test.txt",
            create_dirs=True,
        )

        data = "test"
        output_path = write_to_local(data, config)

        assert output_path.exists()
        assert nested_dir.exists()
        assert output_path.read_text() == data

    def test_write_fails_without_create_dirs(self, tmp_path: Path) -> None:
        """Test write fails when directory doesn't exist and create_dirs is False."""
        nonexistent_dir = tmp_path / "does_not_exist"

        config = LocalOutputConfig(
            path=str(nonexistent_dir),
            filename_pattern="test.txt",
            create_dirs=False,
        )

        with pytest.raises(OutputError, match="Output directory does not exist"):
            write_to_local("test", config)

    def test_write_with_overwrite_protection(self, temp_output_dir: Path) -> None:
        """Test overwrite protection."""
        config = LocalOutputConfig(
            path=str(temp_output_dir),
            filename_pattern="test.txt",
            overwrite=False,
        )

        # First write succeeds
        write_to_local("first", config)

        # Second write fails
        with pytest.raises(OutputError, match="File already exists and overwrite is disabled"):
            write_to_local("second", config)

    def test_write_with_overwrite_enabled(self, temp_output_dir: Path) -> None:
        """Test overwrite when enabled."""
        config = LocalOutputConfig(
            path=str(temp_output_dir),
            filename_pattern="test.txt",
            overwrite=True,
        )

        # First write
        write_to_local("first", config)
        output_path = temp_output_dir / "test.txt"
        assert output_path.read_text() == "first"

        # Second write overwrites
        write_to_local("second", config)
        assert output_path.read_text() == "second"


class TestGzipCompression:
    """Tests for gzip compression."""

    def test_write_with_gzip(self, temp_output_dir: Path) -> None:
        """Test write with gzip compression."""
        config = LocalOutputConfig(
            path=str(temp_output_dir),
            filename_pattern="test.txt",
            compression="gzip",
        )

        data = "Hello, compressed world!"
        output_path = write_to_local(data, config)

        assert output_path.exists()
        assert output_path.name.endswith(".gz")

        # Verify compressed data
        with gzip.open(output_path, "rt", encoding="utf-8") as f:
            decompressed = f.read()
        assert decompressed == data

    def test_gzip_adds_extension(self, temp_output_dir: Path) -> None:
        """Test that .gz extension is added if not present."""
        config = LocalOutputConfig(
            path=str(temp_output_dir),
            filename_pattern="test.json",
            compression="gzip",
        )

        data = '{"test": "data"}'
        output_path = write_to_local(data, config)

        assert output_path.name == "test.json.gz"

    def test_gzip_preserves_extension(self, temp_output_dir: Path) -> None:
        """Test that .gz extension is not duplicated."""
        config = LocalOutputConfig(
            path=str(temp_output_dir),
            filename_pattern="test.json.gz",
            compression="gzip",
        )

        data = '{"test": "data"}'
        output_path = write_to_local(data, config)

        assert output_path.name == "test.json.gz"
        assert output_path.name.count(".gz") == 1


class TestFilenamePatterns:
    """Tests for filename pattern substitution."""

    def test_date_placeholder(self, temp_output_dir: Path) -> None:
        """Test {date} placeholder."""
        config = LocalOutputConfig(
            path=str(temp_output_dir),
            filename_pattern="export_{date}.txt",
        )

        output_path = write_to_local("test", config)
        # Should match YYYY-MM-DD format
        assert len(output_path.stem.split("_")[1]) == 10  # export_YYYY-MM-DD

    def test_time_placeholder(self, temp_output_dir: Path) -> None:
        """Test {time} placeholder."""
        config = LocalOutputConfig(
            path=str(temp_output_dir),
            filename_pattern="export_{time}.txt",
        )

        output_path = write_to_local("test", config)
        # Should match HH-MM-SS format
        assert len(output_path.stem.split("_")[1]) == 8  # export_HH-MM-SS

    def test_timestamp_placeholder(self, temp_output_dir: Path) -> None:
        """Test {timestamp} placeholder."""
        config = LocalOutputConfig(
            path=str(temp_output_dir),
            filename_pattern="export_{timestamp}.txt",
        )

        output_path = write_to_local("test", config)
        # Should match YYYYMMDD-HHMMSS format
        timestamp = output_path.stem.split("_")[1]
        assert len(timestamp) == 15  # YYYYMMDD-HHMMSS
        assert "-" in timestamp

    def test_multiple_placeholders(self, temp_output_dir: Path) -> None:
        """Test multiple placeholders in one pattern."""
        config = LocalOutputConfig(
            path=str(temp_output_dir),
            filename_pattern="{provider}_{date}_{time}.{format}",
        )

        metadata = {"provider": "AWS", "format": "json"}
        output_path = write_to_local("test", config, metadata)

        assert "AWS" in output_path.name
        assert output_path.name.endswith(".json")
        # Should have AWS_YYYY-MM-DD_HH-MM-SS.json format
        parts = output_path.stem.split("_")
        assert len(parts) == 3  # provider, date, time

    def test_year_month_day_placeholders(self, temp_output_dir: Path) -> None:
        """Test individual date component placeholders."""
        config = LocalOutputConfig(
            path=str(temp_output_dir),
            filename_pattern="{year}/{month}/{day}/export.txt",
            create_dirs=True,
        )

        output_path = write_to_local("test", config)

        # Should create nested directories
        assert output_path.exists()
        assert len(output_path.parts) >= 4  # year/month/day/export.txt


class TestAtomicWrites:
    """Tests for atomic write operations."""

    def test_atomic_write_success(self, temp_output_dir: Path) -> None:
        """Test successful atomic write."""
        config = LocalOutputConfig(
            path=str(temp_output_dir),
            filename_pattern="test.txt",
        )

        write_to_local("test data", config)

        # Temp file should not exist
        temp_files = list(temp_output_dir.glob("*.tmp"))
        assert len(temp_files) == 0

    def test_no_partial_writes_on_error(self, temp_output_dir: Path) -> None:
        """Test that no partial files are left on write error."""
        config = LocalOutputConfig(
            path=str(temp_output_dir),
            filename_pattern="test.txt",
        )

        # First successful write
        write_to_local("first", config)

        # Try to write with overwrite disabled (will fail)
        config.overwrite = False
        with contextlib.suppress(OutputError):
            write_to_local("second", config)

        # Should have only the original file, no temp files
        files = list(temp_output_dir.glob("*"))
        assert len(files) == 1
        assert files[0].name == "test.txt"
        assert files[0].read_text() == "first"


class TestErrorHandling:
    """Tests for error handling."""

    def test_invalid_path_raises_error(self) -> None:
        """Test that invalid path raises OutputError."""
        config = LocalOutputConfig(
            path="/this/path/definitely/does/not/exist/and/should/fail",
            filename_pattern="test.txt",
            create_dirs=False,
        )

        with pytest.raises(OutputError):
            write_to_local("test", config)

    def test_empty_data_writes_successfully(self, temp_output_dir: Path) -> None:
        """Test that empty data can be written."""
        config = LocalOutputConfig(
            path=str(temp_output_dir),
            filename_pattern="empty.txt",
        )

        output_path = write_to_local("", config)
        assert output_path.exists()
        assert output_path.read_text() == ""


class TestSecuritySanitization:
    """Tests for security-related sanitization."""

    def test_path_traversal_sanitized(self, temp_output_dir: Path) -> None:
        """Test that path traversal attempts in metadata are sanitized."""
        config = LocalOutputConfig(
            path=str(temp_output_dir),
            filename_pattern="{provider}_data.json",
        )

        # Attempt path traversal with ../
        metadata = {"provider": "../../etc/passwd"}
        output_path = write_to_local("test", config, metadata)

        # Should sanitize to safe filename
        assert output_path.parent == temp_output_dir
        assert ".." not in output_path.name
        assert "/" not in output_path.name
        # Each ".." becomes "__", "/" becomes "_", so "../../etc/passwd" becomes "______etc_passwd"
        assert output_path.name == "______etc_passwd_data.json"

    def test_absolute_path_sanitized(self, temp_output_dir: Path) -> None:
        """Test that absolute paths in metadata are sanitized."""
        config = LocalOutputConfig(
            path=str(temp_output_dir),
            filename_pattern="{provider}_data.json",
        )

        # Attempt absolute path
        metadata = {"provider": "/etc/passwd"}
        output_path = write_to_local("test", config, metadata)

        # Should sanitize path separators
        assert output_path.parent == temp_output_dir
        assert "/" not in output_path.name
        assert output_path.name == "_etc_passwd_data.json"

    def test_backslash_path_sanitized(self, temp_output_dir: Path) -> None:
        """Test that Windows-style paths in metadata are sanitized."""
        config = LocalOutputConfig(
            path=str(temp_output_dir),
            filename_pattern="{provider}_data.json",
        )

        # Attempt Windows path traversal
        metadata = {"provider": "..\\..\\Windows\\System32"}
        output_path = write_to_local("test", config, metadata)

        # Should sanitize backslashes
        assert output_path.parent == temp_output_dir
        assert "\\" not in output_path.name
        assert ".." not in output_path.name
        # Each ".." becomes "__", "\\" becomes "_", so "..\\.." becomes "______"
        assert output_path.name == "______Windows_System32_data.json"

    def test_special_characters_sanitized(self, temp_output_dir: Path) -> None:
        """Test that special characters in metadata are sanitized."""
        config = LocalOutputConfig(
            path=str(temp_output_dir),
            filename_pattern="{provider}_data.json",
        )

        # Include various special characters
        metadata = {"provider": "provider@#$%^&*()[]{}|;:<>?"}
        output_path = write_to_local("test", config, metadata)

        # Should replace special chars with underscores
        assert output_path.parent == temp_output_dir
        # Only alphanumeric, dash, underscore, dot should remain
        assert all(c.isalnum() or c in "-_." for c in output_path.stem.replace("_data", ""))

    def test_unsubstituted_placeholder_raises(self, temp_output_dir: Path) -> None:
        """Test that unsubstituted placeholders raise error."""
        config = LocalOutputConfig(
            path=str(temp_output_dir),
            filename_pattern="{provider}_{region}_data.json",
        )

        # Only provide provider, not region
        metadata = {"provider": "AWS"}

        with pytest.raises(OutputError, match=r"Unsubstituted placeholders.*region"):
            write_to_local("test", config, metadata)

    def test_all_placeholders_substituted(self, temp_output_dir: Path) -> None:
        """Test that all placeholders must be substituted."""
        config = LocalOutputConfig(
            path=str(temp_output_dir),
            filename_pattern="{provider}_{region}_{date}.json",
        )

        # Provide all required metadata
        metadata = {"provider": "AWS", "region": "us-east-1"}
        output_path = write_to_local("test", config, metadata)

        # Should succeed with all placeholders substituted
        assert output_path.exists()
        assert "{" not in output_path.name
        assert "}" not in output_path.name
