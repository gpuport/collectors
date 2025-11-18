"""Configuration management for GPUPort collectors.

This module provides configuration loading and validation using Pydantic models,
following the constitutional requirement for type-safe settings management.
"""

from pathlib import Path
from typing import Literal, Self

import yaml
from pydantic import BaseModel, Field, field_validator


class ObservabilityConfig(BaseModel):
    """Observability configuration for logging and tracing.

    Attributes:
        enabled: Whether observability is enabled
        honeycomb_api_key: Honeycomb API key for sending telemetry
        service_name: Service name for telemetry data
        environment: Environment name (e.g., production, staging, development)
        log_level: Logging level
        exporter_protocol: OTLP exporter protocol
        exporter_endpoint: OTLP exporter endpoint
    """

    enabled: bool = Field(
        default=False,
        description="Enable observability (logging and tracing)",
    )
    honeycomb_api_key: str | None = Field(
        default=None,
        description="Honeycomb API key (can also be set via HONEYCOMB_API_KEY env var)",
    )
    service_name: str = Field(
        default="gpuport-collectors",
        description="Service name for telemetry",
    )
    environment: str = Field(
        default="development",
        description="Environment name",
    )
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = Field(
        default="INFO",
        description="Logging level",
    )
    exporter_protocol: Literal["http/protobuf", "grpc"] = Field(
        default="http/protobuf",
        description="OTLP exporter protocol",
    )
    exporter_endpoint: str = Field(
        default="https://api.honeycomb.io:443",
        description="OTLP exporter endpoint",
    )


class HttpClientConfig(BaseModel):
    """HTTP client configuration for requests.

    Attributes:
        timeout: HTTP request timeout in seconds
        max_retries: Maximum number of retry attempts for failed requests
        backoff_factor: Exponential backoff multiplier for retry delays
        base_delay: Initial delay in seconds for retry backoff
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
    base_delay: float = Field(
        default=5.0,
        gt=0,
        description="Initial delay in seconds for retry backoff",
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


class CollectorConfig(BaseModel):
    """Default configuration settings for GPUPort collectors.

    Attributes:
        http_client: HTTP client configuration
        observability: Observability configuration
    """

    model_config = {"extra": "ignore"}  # Ignore extra fields like collectors, pipelines from YAML

    http_client: HttpClientConfig = Field(
        default_factory=HttpClientConfig,
        description="HTTP client configuration",
    )
    observability: ObservabilityConfig = Field(
        default_factory=ObservabilityConfig,
        description="Observability configuration",
    )

    # Convenience properties for accessing nested config
    @property
    def timeout(self) -> int:
        """Get timeout from http_client config."""
        return self.http_client.timeout

    @property
    def max_retries(self) -> int:
        """Get max_retries from http_client config."""
        return self.http_client.max_retries

    @property
    def backoff_factor(self) -> float:
        """Get backoff_factor from http_client config."""
        return self.http_client.backoff_factor

    @property
    def base_delay(self) -> float:
        """Get base_delay from http_client config."""
        return self.http_client.base_delay

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

__all__ = ["CollectorConfig", "HttpClientConfig", "ObservabilityConfig", "default_config"]
