"""Tests for export pipeline configuration loader."""

from pathlib import Path
from textwrap import dedent

import pytest

from gpuport_collectors.export.loader import (
    ConfigLoadError,
    load_export_config,
    substitute_env_vars,
    validate_config,
)


class TestSubstituteEnvVars:
    """Tests for environment variable substitution."""

    def test_substitute_simple_string(self, monkeypatch: pytest.MonkeyPatch):  # type: ignore[no-untyped-def]
        """Test simple environment variable substitution."""
        monkeypatch.setenv("API_KEY", "secret123")
        result = substitute_env_vars("Bearer ${API_KEY}")
        assert result == "Bearer secret123"

    def test_substitute_multiple_vars(self, monkeypatch: pytest.MonkeyPatch):  # type: ignore[no-untyped-def]
        """Test multiple environment variables in one string."""
        monkeypatch.setenv("HOST", "example.com")
        monkeypatch.setenv("PORT", "8080")
        result = substitute_env_vars("https://${HOST}:${PORT}/api")
        assert result == "https://example.com:8080/api"

    def test_substitute_in_dict(self, monkeypatch: pytest.MonkeyPatch):  # type: ignore[no-untyped-def]
        """Test substitution in dictionary values."""
        monkeypatch.setenv("TOKEN", "abc123")
        data = {"authorization": "Bearer ${TOKEN}", "accept": "application/json"}
        result = substitute_env_vars(data)
        assert result == {"authorization": "Bearer abc123", "accept": "application/json"}

    def test_substitute_in_nested_dict(self, monkeypatch: pytest.MonkeyPatch):  # type: ignore[no-untyped-def]
        """Test substitution in nested dictionaries."""
        monkeypatch.setenv("KEY", "value")
        data = {"outer": {"inner": "${KEY}"}}
        result = substitute_env_vars(data)
        assert result == {"outer": {"inner": "value"}}

    def test_substitute_in_list(self, monkeypatch: pytest.MonkeyPatch):  # type: ignore[no-untyped-def]
        """Test substitution in list items."""
        monkeypatch.setenv("ITEM", "test")
        data = ["${ITEM}", "other"]
        result = substitute_env_vars(data)
        assert result == ["test", "other"]

    def test_substitute_primitives_pass_through(self):
        """Test that primitive types pass through unchanged."""
        assert substitute_env_vars(123) == 123
        assert substitute_env_vars(45.6) == 45.6
        assert substitute_env_vars(True) is True
        assert substitute_env_vars(None) is None

    def test_substitute_missing_var_raises(self):
        """Test that missing environment variable raises ConfigLoadError."""
        with pytest.raises(ConfigLoadError, match=r"MISSING_VAR.*not defined"):
            substitute_env_vars("${MISSING_VAR}")

    def test_substitute_no_vars_returns_unchanged(self):
        """Test strings without variables pass through unchanged."""
        result = substitute_env_vars("plain string")
        assert result == "plain string"


class TestLoadExportConfig:
    """Tests for loading and validating export configuration."""

    def test_load_valid_minimal_config(self, tmp_path: Path):  # type: ignore[no-untyped-def]
        """Test loading a minimal valid configuration."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text(
            dedent("""
            version: "1.0"
            pipelines:
              - name: test_pipeline
                transformer:
                  format: json
                outputs:
                  - type: local
                    path: ./output
            """)
        )

        config = load_export_config(config_file)
        assert config.version == "1.0"
        assert len(config.pipelines) == 1
        assert config.pipelines[0].name == "test_pipeline"

    def test_load_config_with_env_vars(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):  # type: ignore[no-untyped-def]
        """Test loading configuration with environment variables."""
        monkeypatch.setenv("API_TOKEN", "secret123")
        config_file = tmp_path / "config.yaml"
        config_file.write_text(
            dedent("""
            version: "1.0"
            pipelines:
              - name: test_pipeline
                transformer:
                  format: json
                outputs:
                  - type: https
                    url: https://api.example.com
                    headers:
                      Authorization: "Bearer ${API_TOKEN}"
            """)
        )

        config = load_export_config(config_file)
        output = config.pipelines[0].outputs[0]
        assert output.type == "https"
        assert output.headers is not None
        assert output.headers["Authorization"] == "Bearer secret123"

    def test_load_config_missing_env_var_raises(self, tmp_path: Path):  # type: ignore[no-untyped-def]
        """Test that missing environment variable raises ConfigLoadError."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text(
            dedent("""
            version: "1.0"
            pipelines:
              - name: test_pipeline
                transformer:
                  format: json
                outputs:
                  - type: https
                    url: https://api.example.com
                    headers:
                      Authorization: "Bearer ${MISSING_VAR}"
            """)
        )

        with pytest.raises(ConfigLoadError, match=r"MISSING_VAR.*not defined"):
            load_export_config(config_file)

    def test_load_config_file_not_found(self, tmp_path: Path):  # type: ignore[no-untyped-def]
        """Test that missing file raises ConfigLoadError."""
        config_file = tmp_path / "nonexistent.yaml"
        with pytest.raises(ConfigLoadError, match="not found"):
            load_export_config(config_file)

    def test_load_config_path_is_directory(self, tmp_path: Path):  # type: ignore[no-untyped-def]
        """Test that directory path raises ConfigLoadError."""
        with pytest.raises(ConfigLoadError, match="not a file"):
            load_export_config(tmp_path)

    def test_load_config_invalid_yaml(self, tmp_path: Path):  # type: ignore[no-untyped-def]
        """Test that invalid YAML syntax raises ConfigLoadError."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text("invalid: yaml: syntax:")

        with pytest.raises(ConfigLoadError, match="Invalid YAML syntax"):
            load_export_config(config_file)

    def test_load_config_validation_error(self, tmp_path: Path):  # type: ignore[no-untyped-def]
        """Test that Pydantic validation errors are formatted nicely."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text(
            dedent("""
            version: "1.0"
            pipelines:
              - name: test_pipeline
                transformer:
                  format: invalid_format
                outputs: []
            """)
        )

        with pytest.raises(ConfigLoadError, match="validation failed"):
            load_export_config(config_file)


class TestValidateConfig:
    """Tests for configuration validation warnings."""

    def test_validate_no_warnings_for_valid_config(self):
        """Test that valid config produces no warnings."""
        from gpuport_collectors.export.config import (
            ExportConfig,
            FilterConfig,
            JSONTransformerConfig,
            LocalOutputConfig,
            PipelineConfig,
        )

        config = ExportConfig(
            pipelines=[
                PipelineConfig(
                    name="test",
                    filters=[FilterConfig(field="provider", operator="eq", value="test")],
                    transformer=JSONTransformerConfig(),
                    outputs=[LocalOutputConfig(path="./output")],
                )
            ]
        )
        warnings = validate_config(config)
        assert len(warnings) == 0

    def test_validate_warns_about_no_outputs(self):
        """Test warning for enabled pipeline with no outputs."""
        from gpuport_collectors.export.config import (
            ExportConfig,
            FilterConfig,
            JSONTransformerConfig,
            PipelineConfig,
        )

        config = ExportConfig(
            pipelines=[
                PipelineConfig(
                    name="test",
                    enabled=True,
                    filters=[FilterConfig(field="provider", operator="eq", value="test")],
                    transformer=JSONTransformerConfig(),
                    outputs=[],
                )
            ]
        )
        warnings = validate_config(config)
        assert len(warnings) == 1
        assert "no output destinations" in warnings[0]

    def test_validate_warns_about_relative_path_without_create_dirs(self):
        """Test warning for relative path with create_dirs=False."""
        from gpuport_collectors.export.config import (
            ExportConfig,
            FilterConfig,
            JSONTransformerConfig,
            LocalOutputConfig,
            PipelineConfig,
        )

        config = ExportConfig(
            pipelines=[
                PipelineConfig(
                    name="test",
                    filters=[FilterConfig(field="provider", operator="eq", value="test")],
                    transformer=JSONTransformerConfig(),
                    outputs=[LocalOutputConfig(path="./output", create_dirs=False)],
                )
            ]
        )
        warnings = validate_config(config)
        assert len(warnings) == 1
        assert "relative" in warnings[0]

    def test_validate_warns_about_s3_without_credentials(self):
        """Test warning for S3 output without credentials."""
        from gpuport_collectors.export.config import (
            ExportConfig,
            FilterConfig,
            JSONTransformerConfig,
            PipelineConfig,
            S3OutputConfig,
        )

        config = ExportConfig(
            pipelines=[
                PipelineConfig(
                    name="test",
                    filters=[FilterConfig(field="provider", operator="eq", value="test")],
                    transformer=JSONTransformerConfig(),
                    outputs=[S3OutputConfig(bucket="test-bucket")],
                )
            ]
        )
        warnings = validate_config(config)
        assert len(warnings) == 1
        assert "no credentials" in warnings[0]

    def test_validate_disabled_pipeline_no_warnings(self):
        """Test that disabled pipelines don't generate warnings."""
        from gpuport_collectors.export.config import (
            ExportConfig,
            JSONTransformerConfig,
            PipelineConfig,
        )

        config = ExportConfig(
            pipelines=[
                PipelineConfig(
                    name="test",
                    enabled=False,
                    filters=[],
                    transformer=JSONTransformerConfig(),
                    outputs=[],
                )
            ]
        )
        warnings = validate_config(config)
        # No warnings for disabled pipeline
        assert len(warnings) == 0
