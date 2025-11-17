"""Export pipeline configuration models.

Configuration models for defining export pipelines that filter, transform,
and export GPU instance data.
"""

from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator


# Filter Configuration
class FilterConfig(BaseModel):
    """Configuration for filtering GPU instances.

    Filters define criteria for selecting which instances to include in the export.
    """

    field: str = Field(..., description="GPUInstance field to filter on")
    operator: Literal[
        "eq",
        "ne",
        "lt",
        "lte",
        "gt",
        "gte",
        "between",
        "in",
        "not_in",
        "regex",
        "contains",
        "starts_with",
        "is_null",
        "is_not_null",
    ] = Field(..., description="Comparison operator")
    value: Any | None = Field(
        default=None, description="Value to compare against (for single-value operators)"
    )
    values: list[Any] | None = Field(
        default=None, description="List of values (for in/not_in operators)"
    )
    min: float | None = Field(default=None, description="Minimum value (for between operator)")
    max: float | None = Field(default=None, description="Maximum value (for between operator)")

    @model_validator(mode="after")
    def validate_operator_fields(self) -> "FilterConfig":
        """Validate that required fields are provided for each operator type."""
        # Operators that require 'value' field
        if (
            self.operator
            in {
                "eq",
                "ne",
                "lt",
                "lte",
                "gt",
                "gte",
                "regex",
                "contains",
                "starts_with",
            }
            and self.value is None
        ):
            raise ValueError(f"Operator '{self.operator}' requires 'value' field")

        # Operators that require 'values' field
        if self.operator in {"in", "not_in"} and self.values is None:
            raise ValueError(f"Operator '{self.operator}' requires 'values' field")

        # 'between' operator requires both 'min' and 'max' fields
        if self.operator == "between" and (self.min is None or self.max is None):
            raise ValueError("Operator 'between' requires both 'min' and 'max' fields")

        return self


# Transformer Configuration
class JSONTransformerConfig(BaseModel):
    """Configuration for JSON transformation."""

    format: Literal["json"] = "json"
    fields: dict[str, str] | None = Field(
        default=None,
        description="Field mapping: source_field -> output_alias. If None, include all fields.",
    )
    include_raw_data: bool = Field(default=False, description="Include raw_data field in output")
    pretty_print: bool = Field(default=False, description="Pretty-print JSON output")
    flatten_nested: bool = Field(default=False, description="Flatten nested objects")
    null_handling: Literal["omit", "null", "empty"] = Field(
        default="null",
        description="How to handle null values: omit (exclude), null (JSON null), empty (empty string)",
    )


class CSVTransformerConfig(BaseModel):
    """Configuration for CSV transformation."""

    format: Literal["csv"] = "csv"
    fields: dict[str, str] = Field(
        ..., description="Field mapping: source_field -> output_alias (ordered)"
    )
    include_headers: bool = Field(default=True, description="Include header row")
    delimiter: str = Field(default=",", description="Field delimiter")
    quote_char: str = Field(default='"', description="Quote character")
    escape_char: str = Field(default="\\", description="Escape character")
    line_terminator: str = Field(default="\n", description="Line terminator")
    null_value: str = Field(default="", description="Representation for null values")


class MetricConfig(BaseModel):
    """Configuration for a single metric."""

    name: str = Field(..., description="Metric name")
    type: Literal["count", "avg", "min", "max", "sum", "unique"] = Field(
        ..., description="Aggregation type"
    )
    field: str | None = Field(
        default=None, description="Field to aggregate (required for non-count metrics)"
    )
    group_by: str | None = Field(default=None, description="Field to group by")

    @model_validator(mode="after")
    def validate_field_required(self) -> "MetricConfig":
        """Validate that field is provided for non-count metric types."""
        if self.type != "count" and self.field is None:
            raise ValueError(f"Metric type '{self.type}' requires 'field' parameter")
        return self


class MetricsTransformerConfig(BaseModel):
    """Configuration for metrics transformation."""

    format: Literal["json"] = "json"
    type: Literal["metrics"] = "metrics"
    metrics: list[MetricConfig] = Field(..., description="Metrics to compute")
    include_timestamp: bool = Field(default=True, description="Include collection timestamp")
    include_collection_info: bool = Field(
        default=True, description="Include collector name and run info"
    )


TransformerConfig = JSONTransformerConfig | CSVTransformerConfig | MetricsTransformerConfig


# Output Configuration
class LocalOutputConfig(BaseModel):
    """Configuration for local filesystem output."""

    type: Literal["local"] = "local"
    name: str | None = Field(default=None, description="Output name for logging")
    path: str = Field(..., description="Output directory path")
    filename_pattern: str = Field(
        default="{provider}_{date}_{time}.{format}",
        description="Filename pattern with placeholders",
    )
    create_dirs: bool = Field(default=True, description="Create directories if they don't exist")
    overwrite: bool = Field(default=False, description="Overwrite existing files")
    compression: Literal["none", "gzip"] = Field(default="none", description="Compression type")


class S3OutputConfig(BaseModel):
    """Configuration for S3-compatible storage output."""

    type: Literal["s3"] = "s3"
    name: str | None = Field(default=None, description="Output name for logging")
    bucket: str = Field(..., description="S3 bucket name")
    prefix: str = Field(default="", description="Key prefix (folder path)")
    region: str | None = Field(default=None, description="AWS region")
    endpoint_url: str | None = Field(
        default=None, description="Custom endpoint URL (for S3-compatible storage)"
    )
    filename_pattern: str = Field(
        default="{provider}_{timestamp}.{format}",
        description="Filename pattern with placeholders",
    )
    compression: Literal["none", "gzip"] = Field(default="none", description="Compression type")
    credentials: dict[str, str] | None = Field(
        default=None,
        description="Credential configuration (access_key_env, secret_key_env, session_token_env)",
    )
    storage_class: str = Field(default="STANDARD", description="S3 storage class")
    server_side_encryption: str | None = Field(
        default=None, description="Server-side encryption type"
    )
    acl: str = Field(default="private", description="Access control list")
    metadata: dict[str, str] | None = Field(default=None, description="Custom metadata")


class HTTPSOutputConfig(BaseModel):
    """Configuration for HTTPS endpoint output."""

    type: Literal["https"] = "https"
    name: str | None = Field(default=None, description="Output name for logging")
    url: str = Field(..., description="HTTPS endpoint URL")
    method: Literal["POST", "PUT", "PATCH"] = Field(default="POST", description="HTTP method")
    headers: dict[str, str] | None = Field(
        default=None, description="Custom headers (supports ${VAR} substitution)"
    )
    batch_size: int | None = Field(
        default=None, description="Number of instances per request (None = all at once)"
    )
    batch_delay: float = Field(default=0.0, description="Delay between batches in seconds")
    timeout: int = Field(default=30, description="Request timeout in seconds")
    retry_attempts: int = Field(default=3, description="Number of retry attempts")
    retry_delay: int = Field(default=5, description="Initial retry delay in seconds")
    retry_backoff: float = Field(default=2.0, description="Backoff multiplier for retries")
    retry_on_status: list[int] = Field(
        default_factory=lambda: [500, 502, 503, 504],
        description="HTTP status codes that trigger retry",
    )
    verify_ssl: bool = Field(default=True, description="Verify SSL certificates")
    client_cert: str | None = Field(default=None, description="Path to client certificate")
    client_key: str | None = Field(default=None, description="Path to client key")


OutputConfig = LocalOutputConfig | S3OutputConfig | HTTPSOutputConfig


# Pipeline Configuration
class PipelineConfig(BaseModel):
    """Configuration for a single export pipeline."""

    name: str = Field(..., description="Unique pipeline identifier")
    description: str | None = Field(default=None, description="Human-readable description")
    enabled: bool = Field(default=True, description="Enable/disable pipeline")
    filters: list[FilterConfig] = Field(default_factory=list, description="Filter criteria")
    transformer: TransformerConfig = Field(..., description="Transformation configuration")
    outputs: list[OutputConfig] = Field(..., description="Output destinations")


# Root Configuration
class ExportConfig(BaseModel):
    """Root export configuration."""

    version: str = Field(default="1.0", description="Configuration version")
    pipelines: list[PipelineConfig] = Field(..., description="Pipeline definitions")
