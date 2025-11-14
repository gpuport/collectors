"""Tests for configuration loading and validation."""

from pathlib import Path
from tempfile import NamedTemporaryFile

import pytest
import yaml
from pydantic import ValidationError

from gpuport_collectors.config import CollectorConfig, default_config


class TestCollectorConfig:
    """Tests for CollectorConfig model."""

    def test_default_values(self):
        """Test that default values are set correctly."""
        config = CollectorConfig()
        assert config.timeout == 30
        assert config.max_retries == 3
        assert config.backoff_factor == 2.0

    def test_custom_values(self):
        """Test that custom values can be set."""
        config = CollectorConfig(timeout=60, max_retries=5, backoff_factor=1.5)
        assert config.timeout == 60
        assert config.max_retries == 5
        assert config.backoff_factor == 1.5

    def test_timeout_validation_positive(self):
        """Test that timeout must be positive."""
        with pytest.raises(ValidationError, match="greater than 0"):
            CollectorConfig(timeout=0)

        with pytest.raises(ValidationError, match="greater than 0"):
            CollectorConfig(timeout=-1)

    def test_timeout_validation_max(self):
        """Test that timeout cannot exceed 300 seconds."""
        with pytest.raises(ValidationError, match="cannot exceed 300 seconds"):
            CollectorConfig(timeout=301)

    def test_max_retries_validation(self):
        """Test max_retries validation."""
        # Should accept 0
        config = CollectorConfig(max_retries=0)
        assert config.max_retries == 0

        # Should reject negative values
        with pytest.raises(ValidationError, match="greater than or equal to 0"):
            CollectorConfig(max_retries=-1)

        # Should reject values > 10
        with pytest.raises(ValidationError, match="cannot exceed 10"):
            CollectorConfig(max_retries=11)

    def test_backoff_factor_validation(self):
        """Test that backoff_factor must be positive."""
        with pytest.raises(ValidationError, match="greater than 0"):
            CollectorConfig(backoff_factor=0)

        with pytest.raises(ValidationError, match="greater than 0"):
            CollectorConfig(backoff_factor=-1)

    def test_from_yaml_valid(self):
        """Test loading configuration from a valid YAML file."""
        yaml_content = {
            "timeout": 45,
            "max_retries": 5,
            "backoff_factor": 1.5,
        }

        with NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(yaml_content, f)
            temp_path = Path(f.name)

        try:
            config = CollectorConfig.from_yaml(temp_path)
            assert config.timeout == 45
            assert config.max_retries == 5
            assert config.backoff_factor == 1.5
        finally:
            temp_path.unlink()

    def test_from_yaml_file_not_found(self):
        """Test that from_yaml raises FileNotFoundError for missing file."""
        nonexistent_path = Path("/nonexistent/config.yaml")
        with pytest.raises(FileNotFoundError, match="Configuration file not found"):
            CollectorConfig.from_yaml(nonexistent_path)

    def test_from_yaml_invalid_data(self):
        """Test that from_yaml raises ValidationError for invalid data."""
        yaml_content = {
            "timeout": -10,  # Invalid: must be positive
            "max_retries": 3,
            "backoff_factor": 2.0,
        }

        with NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(yaml_content, f)
            temp_path = Path(f.name)

        try:
            with pytest.raises(ValidationError):
                CollectorConfig.from_yaml(temp_path)
        finally:
            temp_path.unlink()

    def test_load_defaults(self):
        """Test loading default configuration from defaults.yaml."""
        config = CollectorConfig.load_defaults()
        assert config.timeout == 30
        assert config.max_retries == 3
        assert config.backoff_factor == 2.0

    def test_default_config_global(self):
        """Test that the global default_config instance is loaded correctly."""
        assert isinstance(default_config, CollectorConfig)
        assert default_config.timeout == 30
        assert default_config.max_retries == 3
        assert default_config.backoff_factor == 2.0

    def test_model_dump(self):
        """Test that the model can be dumped to a dict."""
        config = CollectorConfig(timeout=60, max_retries=5, backoff_factor=1.5)
        data = config.model_dump()
        assert data["timeout"] == 60
        assert data["max_retries"] == 5
        assert data["backoff_factor"] == 1.5
        assert data["base_delay"] == 5.0
        assert "observability" in data
        assert data["observability"]["enabled"] is False
