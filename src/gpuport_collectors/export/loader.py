"""YAML configuration loader for export pipelines.

Provides functionality to load and validate export pipeline configurations from YAML files,
with support for environment variable substitution and comprehensive error reporting.
"""

import os
import re
from pathlib import Path
from typing import Any

from pydantic import ValidationError
from pydantic_yaml import parse_yaml_raw_as

from gpuport_collectors.export.config import ExportConfig


class ConfigLoadError(Exception):
    """Raised when configuration loading or validation fails."""


def substitute_env_vars(data: Any) -> Any:
    """Recursively substitute environment variables in configuration data.

    Replaces ${VAR_NAME} patterns with environment variable values.
    Raises ConfigLoadError if a referenced variable is not defined.

    Args:
        data: Configuration data structure (dict, list, str, or primitive)

    Returns:
        Configuration data with environment variables substituted

    Raises:
        ConfigLoadError: If a referenced environment variable is not defined
    """
    if isinstance(data, dict):
        return {key: substitute_env_vars(value) for key, value in data.items()}
    if isinstance(data, list):
        return [substitute_env_vars(item) for item in data]
    if isinstance(data, str):
        # Find all ${VAR_NAME} patterns
        pattern = r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}"
        matches = re.findall(pattern, data)

        # Substitute each environment variable
        result = data
        for var_name in matches:
            if var_name not in os.environ:
                raise ConfigLoadError(
                    f"Environment variable '${{{var_name}}}' referenced in configuration "
                    f"but not defined in environment"
                )
            result = result.replace(f"${{{var_name}}}", os.environ[var_name])

        return result
    # Primitives (int, float, bool, None) pass through
    return data


def load_export_config(config_path: str | Path) -> ExportConfig:
    """Load and validate export pipeline configuration from YAML file.

    Args:
        config_path: Path to YAML configuration file

    Returns:
        Validated ExportConfig instance

    Raises:
        ConfigLoadError: If file not found, invalid YAML, validation fails,
            or environment variables are missing
    """
    config_path = Path(config_path)

    # Check file exists
    if not config_path.exists():
        raise ConfigLoadError(f"Configuration file not found: {config_path}")

    if not config_path.is_file():
        raise ConfigLoadError(f"Configuration path is not a file: {config_path}")

    try:
        # Read YAML content
        yaml_content = config_path.read_text(encoding="utf-8")

        # Parse YAML into Python data structure (without validation yet)
        import yaml

        raw_data = yaml.safe_load(yaml_content)

        # Substitute environment variables
        try:
            processed_data = substitute_env_vars(raw_data)
        except ConfigLoadError:
            # Re-raise with file context
            raise

        # Convert back to YAML string for pydantic-yaml
        processed_yaml = yaml.dump(processed_data)

        # Parse and validate with Pydantic
        return parse_yaml_raw_as(ExportConfig, processed_yaml)

    except yaml.YAMLError as e:
        raise ConfigLoadError(f"Invalid YAML syntax in {config_path}: {e}") from e

    except ValidationError as e:
        # Format Pydantic validation errors nicely
        errors = []
        for error in e.errors():
            location = " -> ".join(str(loc) for loc in error["loc"])
            message = error["msg"]
            errors.append(f"  • {location}: {message}")

        error_details = "\n".join(errors)
        raise ConfigLoadError(
            f"Configuration validation failed in {config_path}:\n{error_details}"
        ) from e

    except Exception as e:
        raise ConfigLoadError(f"Failed to load configuration from {config_path}: {e}") from e


def validate_config(config: ExportConfig) -> list[str]:
    """Validate configuration for common issues and return warnings.

    Checks for potential issues that are valid but may indicate problems:
    - Pipelines with no outputs enabled
    - Output paths that may not be writable
    - S3 credentials not configured

    Args:
        config: Validated ExportConfig instance

    Returns:
        List of warning messages (empty if no issues)
    """
    warnings = []

    for pipeline in config.pipelines:
        # Check if pipeline is enabled but has no outputs
        if pipeline.enabled and not pipeline.outputs:
            warnings.append(f"Pipeline '{pipeline.name}' is enabled but has no output destinations")

        # Check for local outputs with potentially problematic paths
        for output in pipeline.outputs:
            if output.type == "local":
                output_path = Path(output.path)
                # Check if parent exists (if path is relative, check against current dir)
                if not output_path.is_absolute() and not output.create_dirs:
                    warnings.append(
                        f"Pipeline '{pipeline.name}': Local output path '{output.path}' "
                        f"is relative and create_dirs=False - may fail if directory doesn't exist"
                    )

            elif output.type == "s3":
                # Check if credentials are configured
                if not output.credentials:
                    warnings.append(
                        f"Pipeline '{pipeline.name}': S3 output has no credentials configured - "
                        f"will rely on environment/IAM role"
                    )

    return warnings
