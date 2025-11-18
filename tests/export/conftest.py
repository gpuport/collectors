"""Shared fixtures for export tests."""

from typing import Any

import pytest

from gpuport_collectors.export.config import (
    CSVTransformerConfig,
    ExportConfig,
    FilterConfig,
    HTTPSOutputConfig,
    JSONTransformerConfig,
    LocalOutputConfig,
    MetricConfig,
    MetricsTransformerConfig,
    PipelineConfig,
    S3OutputConfig,
)
from gpuport_collectors.models import AvailabilityStatus, GPUInstance


@pytest.fixture
def sample_instances() -> list[GPUInstance]:
    """Create sample GPU instances for testing.

    Returns a diverse set of GPU instances for testing export pipelines:
    - Multiple providers (RunPod, Lambda)
    - Various GPU types (H100, A100, RTX 4090)
    - Different availability levels
    - Range of prices
    """
    return [
        GPUInstance(
            provider="RunPod",
            instance_type="gpu.h100.80gb",
            accelerator_name="H100",
            accelerator_count=8,
            region="us-east-1",
            availability=AvailabilityStatus.HIGH,
            price=25.0,
        ),
        GPUInstance(
            provider="RunPod",
            instance_type="gpu.a100.40gb",
            accelerator_name="A100",
            accelerator_count=4,
            region="us-west-2",
            availability=AvailabilityStatus.MEDIUM,
            price=12.5,
        ),
        GPUInstance(
            provider="RunPod",
            instance_type="gpu.rtx4090",
            accelerator_name="RTX 4090",
            accelerator_count=2,
            region="us-east-1",
            availability=AvailabilityStatus.LOW,
            price=2.5,
        ),
        GPUInstance(
            provider="Lambda",
            instance_type="gpu.a100.80gb",
            accelerator_name="A100",
            accelerator_count=8,
            region="us-east-1",
            availability=AvailabilityStatus.HIGH,
            price=15.0,
        ),
    ]


@pytest.fixture
def json_transformer_config() -> JSONTransformerConfig:
    """Standard JSON transformer configuration."""
    return JSONTransformerConfig(pretty_print=True)


@pytest.fixture
def csv_transformer_config() -> CSVTransformerConfig:
    """Standard CSV transformer configuration."""
    return CSVTransformerConfig(
        fields={
            "provider": "Provider",
            "instance_type": "Instance Type",
            "accelerator_name": "GPU",
            "price": "Price",
        }
    )


@pytest.fixture
def metrics_transformer_config() -> MetricsTransformerConfig:
    """Standard metrics transformer configuration."""
    return MetricsTransformerConfig(
        metrics=[
            MetricConfig(name="total_instances", type="count"),
            MetricConfig(name="avg_price", type="avg", field="price"),
            MetricConfig(name="min_price", type="min", field="price"),
            MetricConfig(name="max_price", type="max", field="price"),
        ]
    )


@pytest.fixture
def local_output_config(tmp_path):
    """Standard local output configuration using tmp_path."""
    return LocalOutputConfig(path=str(tmp_path), filename_pattern="output.json", overwrite=True)


@pytest.fixture
def s3_output_config() -> S3OutputConfig:
    """Standard S3 output configuration."""
    return S3OutputConfig(
        bucket="test-bucket",
        prefix="exports",
        filename_pattern="data_{date}.json",
        region="us-east-1",
    )


@pytest.fixture
def https_output_config() -> HTTPSOutputConfig:
    """Standard HTTPS output configuration."""
    return HTTPSOutputConfig(
        url="https://api.example.com/webhook",
        method="POST",
        headers={"Content-Type": "application/json"},
    )


@pytest.fixture
def provider_filter_config() -> FilterConfig:
    """Filter for RunPod provider."""
    return FilterConfig(field="provider", operator="eq", value="RunPod")


@pytest.fixture
def price_filter_config() -> FilterConfig:
    """Filter for expensive instances (>= $15)."""
    return FilterConfig(field="price", operator="gte", value=15.0)


@pytest.fixture
def availability_filter_config() -> FilterConfig:
    """Filter for high availability instances."""
    return FilterConfig(field="availability", operator="eq", value=AvailabilityStatus.HIGH)


@pytest.fixture
def simple_pipeline_config(
    json_transformer_config: JSONTransformerConfig,
    local_output_config: LocalOutputConfig,
) -> PipelineConfig:
    """Simple pipeline with JSON transformer and local output."""
    return PipelineConfig(
        name="simple-pipeline",
        transformer=json_transformer_config,
        outputs=[local_output_config],
    )


@pytest.fixture
def filtered_pipeline_config(
    json_transformer_config: JSONTransformerConfig,
    local_output_config: LocalOutputConfig,
    provider_filter_config: FilterConfig,
) -> PipelineConfig:
    """Pipeline with filtering enabled."""
    return PipelineConfig(
        name="filtered-pipeline",
        filters=[provider_filter_config],
        transformer=json_transformer_config,
        outputs=[local_output_config],
    )


@pytest.fixture
def multi_output_pipeline_config(
    json_transformer_config: JSONTransformerConfig,
    local_output_config: LocalOutputConfig,
    s3_output_config: S3OutputConfig,
) -> PipelineConfig:
    """Pipeline with multiple outputs."""
    return PipelineConfig(
        name="multi-output-pipeline",
        transformer=json_transformer_config,
        outputs=[local_output_config, s3_output_config],
    )


@pytest.fixture
def export_config(simple_pipeline_config: PipelineConfig) -> ExportConfig:
    """Basic export configuration with one pipeline."""
    return ExportConfig(pipelines=[simple_pipeline_config])


@pytest.fixture
def multi_pipeline_export_config(
    json_transformer_config: JSONTransformerConfig,
    csv_transformer_config: CSVTransformerConfig,
    metrics_transformer_config: MetricsTransformerConfig,
    tmp_path: Any,
) -> ExportConfig:
    """Export configuration with multiple pipelines of different types."""
    return ExportConfig(
        pipelines=[
            PipelineConfig(
                name="json-export",
                transformer=json_transformer_config,
                outputs=[
                    LocalOutputConfig(
                        path=str(tmp_path), filename_pattern="instances.json", overwrite=True
                    )
                ],
            ),
            PipelineConfig(
                name="csv-export",
                transformer=csv_transformer_config,
                outputs=[
                    LocalOutputConfig(
                        path=str(tmp_path), filename_pattern="instances.csv", overwrite=True
                    )
                ],
            ),
            PipelineConfig(
                name="metrics-export",
                transformer=metrics_transformer_config,
                outputs=[
                    LocalOutputConfig(
                        path=str(tmp_path), filename_pattern="metrics.json", overwrite=True
                    )
                ],
            ),
        ]
    )
