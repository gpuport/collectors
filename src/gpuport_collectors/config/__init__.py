"""Configuration management for GPUPort collectors.

This module provides configuration loading and validation using Pydantic models,
following the constitutional requirement for type-safe settings management.
"""

from pathlib import Path
from typing import Self

import yaml
from pydantic import BaseModel, Field, field_validator


class CollectorConfig(BaseModel):
    """Default configuration settings for GPUPort collectors.

    Attributes:
        timeout: HTTP request timeout in seconds
        max_retries: Maximum number of retry attempts for failed requests
        backoff_factor: Exponential backoff multiplier for retry delays
    """

    timeout: int = Field(
        default=30,
        gt=0,
        description="HTTP request timeout in seconds",
    )
    max_retries: int = Field(
        default=3,
        ge=0,
        description="Maximum number of retry attempts",
    )
    backoff_factor: float = Field(
        default=2.0,
        gt=0,
        description="Exponential backoff multiplier for retry delays",
    )

    @field_validator("timeout")
    @classmethod
    def validate_timeout(cls, v: int) -> int:
        """Validate timeout is within reasonable bounds."""
        if v > 300:  # 5 minutes max
            msg = "Timeout cannot exceed 300 seconds"
            raise ValueError(msg)
        return v

    @field_validator("max_retries")
    @classmethod
    def validate_max_retries(cls, v: int) -> int:
        """Validate max_retries is within reasonable bounds."""
        if v > 10:
            msg = "max_retries cannot exceed 10"
            raise ValueError(msg)
        return v

    @classmethod
    def from_yaml(cls, path: Path) -> Self:
        """Load configuration from a YAML file.

        Args:
            path: Path to the YAML configuration file

        Returns:
            CollectorConfig instance with validated settings

        Raises:
            FileNotFoundError: If the config file doesn't exist
            ValueError: If the YAML is invalid or validation fails
        """
        if not path.exists():
            msg = f"Configuration file not found: {path}"
            raise FileNotFoundError(msg)

        with path.open("r") as f:
            data = yaml.safe_load(f)

        return cls(**data)

    @classmethod
    def load_defaults(cls) -> Self:
        """Load the default configuration from defaults.yaml.

        Returns:
            CollectorConfig instance with default settings
        """
        config_dir = Path(__file__).parent
        defaults_path = config_dir / "defaults.yaml"
        return cls.from_yaml(defaults_path)


# Create a global instance with default settings
default_config = CollectorConfig.load_defaults()

__all__ = ["CollectorConfig", "default_config"]
