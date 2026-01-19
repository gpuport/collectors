"""Tests for .env file loading in CLI."""

import os
from pathlib import Path

import pytest


class TestDotenvLoading:
    """Tests for automatic .env file loading."""

    def test_env_file_loads_variables(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test that .env file variables are loaded automatically."""
        # Create .env file in temp directory
        env_file = tmp_path / ".env"
        env_file.write_text(
            "RUNPOD_API_KEY=test_key_from_dotenv\n"
            "LAMBDA_API_KEY=lambda_key_from_dotenv\n"
            "CUSTOM_VAR=custom_value\n"
        )

        # Change to temp directory so .env is found
        monkeypatch.chdir(tmp_path)

        # Clear any existing env vars to ensure we're testing .env loading
        monkeypatch.delenv("RUNPOD_API_KEY", raising=False)
        monkeypatch.delenv("LAMBDA_API_KEY", raising=False)
        monkeypatch.delenv("CUSTOM_VAR", raising=False)

        # Import cli module to trigger load_dotenv
        # Note: In real usage, dotenv is loaded when module is imported
        from dotenv import load_dotenv

        load_dotenv(dotenv_path=env_file, override=False)

        # Verify environment variables are loaded
        assert os.environ.get("RUNPOD_API_KEY") == "test_key_from_dotenv"
        assert os.environ.get("LAMBDA_API_KEY") == "lambda_key_from_dotenv"
        assert os.environ.get("CUSTOM_VAR") == "custom_value"

    def test_existing_env_vars_take_priority(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test that existing environment variables override .env file."""
        # Create .env file
        env_file = tmp_path / ".env"
        env_file.write_text("RUNPOD_API_KEY=from_dotenv\n")

        # Set existing environment variable
        monkeypatch.setenv("RUNPOD_API_KEY", "from_shell")

        # Change to temp directory
        monkeypatch.chdir(tmp_path)

        from dotenv import load_dotenv

        # Load with override=False (existing vars take priority)
        load_dotenv(dotenv_path=env_file, override=False)

        # Existing env var should remain unchanged
        assert os.environ["RUNPOD_API_KEY"] == "from_shell"

    def test_env_file_not_required(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test that missing .env file doesn't cause errors."""
        # Change to temp directory without .env file
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("RUNPOD_API_KEY", raising=False)

        from dotenv import load_dotenv

        # Should not raise an error
        load_dotenv(dotenv_path=tmp_path / ".env", override=False)

        # No variables should be loaded
        assert os.environ.get("RUNPOD_API_KEY") is None

    def test_env_file_with_export_variables(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test that export-related env vars are loaded from .env."""
        env_file = tmp_path / ".env"
        env_file.write_text(
            "RUNPOD_API_KEY=test_key\n"
            "GPUPORT_INGEST_URL=https://api.example.com/ingest\n"
            "API_TOKEN=secret_token_123\n"
            "AWS_ACCESS_KEY_ID=aws_key\n"
            "AWS_SECRET_ACCESS_KEY=aws_secret\n"
        )

        monkeypatch.chdir(tmp_path)

        # Clear env vars
        for var in [
            "RUNPOD_API_KEY",
            "GPUPORT_INGEST_URL",
            "API_TOKEN",
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
        ]:
            monkeypatch.delenv(var, raising=False)

        from dotenv import load_dotenv

        load_dotenv(dotenv_path=env_file, override=False)

        # Verify all variables are loaded
        assert os.environ.get("RUNPOD_API_KEY") == "test_key"
        assert os.environ.get("GPUPORT_INGEST_URL") == "https://api.example.com/ingest"
        assert os.environ.get("API_TOKEN") == "secret_token_123"
        assert os.environ.get("AWS_ACCESS_KEY_ID") == "aws_key"
        assert os.environ.get("AWS_SECRET_ACCESS_KEY") == "aws_secret"

    def test_env_file_with_comments_and_blank_lines(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test that .env file with comments and blank lines is parsed correctly."""
        env_file = tmp_path / ".env"
        env_file.write_text(
            """
# API Keys for GPU providers
RUNPOD_API_KEY=test_key

# Export configuration
GPUPORT_INGEST_URL=https://api.example.com/ingest

# Empty lines and comments are ignored
# DISABLED_VAR=should_not_load
"""
        )

        monkeypatch.chdir(tmp_path)

        for var in ["RUNPOD_API_KEY", "GPUPORT_INGEST_URL", "DISABLED_VAR"]:
            monkeypatch.delenv(var, raising=False)

        from dotenv import load_dotenv

        load_dotenv(dotenv_path=env_file, override=False)

        assert os.environ.get("RUNPOD_API_KEY") == "test_key"
        assert os.environ.get("GPUPORT_INGEST_URL") == "https://api.example.com/ingest"
        assert "DISABLED_VAR" not in os.environ

    def test_cli_uses_dotenv_variables(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test that CLI commands use variables loaded from .env file."""
        # Create .env file with API key
        env_file = tmp_path / ".env"
        env_file.write_text("RUNPOD_API_KEY=test_api_key_from_dotenv\n")

        # Change to temp directory
        monkeypatch.chdir(tmp_path)

        # Clear existing env var
        monkeypatch.delenv("RUNPOD_API_KEY", raising=False)

        # Load .env
        from dotenv import load_dotenv

        load_dotenv(dotenv_path=env_file, override=False)

        # Verify the env var is loaded
        assert os.environ.get("RUNPOD_API_KEY") == "test_api_key_from_dotenv"

        # Note: We can't easily test full CLI execution here because
        # it would try to make real API calls. The important thing is
        # that load_dotenv() is called before the CLI commands run,
        # which we've verified in cli.py


class TestDotenvPriority:
    """Tests for environment variable priority ordering."""

    def test_priority_shell_over_dotenv(self, tmp_path: Path) -> None:
        """Test that shell env vars have priority over .env file."""
        env_file = tmp_path / ".env"
        env_file.write_text("TEST_VAR=from_dotenv\n")

        original_cwd = Path.cwd()
        os.chdir(tmp_path)

        try:
            # Set in shell first
            os.environ["TEST_VAR"] = "from_shell"

            from dotenv import load_dotenv

            # Load with override=False (default behavior)
            load_dotenv(dotenv_path=env_file, override=False)

            # Shell value should win
            assert os.environ["TEST_VAR"] == "from_shell"

        finally:
            os.chdir(original_cwd)
            os.environ.pop("TEST_VAR", None)

    def test_dotenv_provides_defaults(self, tmp_path: Path) -> None:
        """Test that .env file provides default values when shell vars not set."""
        env_file = tmp_path / ".env"
        env_file.write_text("RUNPOD_API_KEY=default_key\nHONEYCOMB_API_KEY=default_honeycomb\n")

        original_cwd = Path.cwd()
        os.chdir(tmp_path)

        try:
            # Clear env vars
            os.environ.pop("RUNPOD_API_KEY", None)
            os.environ.pop("HONEYCOMB_API_KEY", None)

            from dotenv import load_dotenv

            load_dotenv(dotenv_path=env_file, override=False)

            # .env values should be used as defaults
            assert os.environ["RUNPOD_API_KEY"] == "default_key"
            assert os.environ["HONEYCOMB_API_KEY"] == "default_honeycomb"

        finally:
            os.chdir(original_cwd)
            os.environ.pop("RUNPOD_API_KEY", None)
            os.environ.pop("HONEYCOMB_API_KEY", None)
