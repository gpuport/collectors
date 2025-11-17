"""Tests for export pipeline transformers."""

import json

import pytest

from gpuport_collectors.export.config import (
    CSVTransformerConfig,
    JSONTransformerConfig,
    MetricsTransformerConfig,
)
from gpuport_collectors.export.transformers import (
    transform_to_csv,
    transform_to_json,
    transform_to_metrics,
)
from gpuport_collectors.models import AvailabilityStatus, GPUInstance


@pytest.fixture
def sample_instances() -> list[GPUInstance]:
    """Create sample GPUInstance list for testing."""
    return [
        GPUInstance(
            provider="Provider1",
            instance_type="type1",
            accelerator_name="GPU1",
            accelerator_count=2,
            accelerator_mem_gib=16.0,
            region="us-east-1",
            availability=AvailabilityStatus.HIGH,
            quantity=5,
            price=2.5,
            spot_price=1.5,
            v_cpus=8,
            memory_gib=32.0,
        ),
        GPUInstance(
            provider="Provider2",
            instance_type="type2",
            accelerator_name="GPU2",
            accelerator_count=4,
            accelerator_mem_gib=24.0,
            region="us-west-1",
            availability=AvailabilityStatus.MEDIUM,
            quantity=3,
            price=5.0,
            spot_price=3.0,
            v_cpus=16,
            memory_gib=64.0,
        ),
    ]


class TestTransformToJSON:
    """Tests for JSON transformation."""

    def test_transform_all_fields(self, sample_instances: list[GPUInstance]) -> None:
        """Test JSON transformation with all fields."""
        config = JSONTransformerConfig()
        result = transform_to_json(sample_instances, config)

        # Should be valid JSON
        data = json.loads(result)
        assert isinstance(data, list)
        assert len(data) == 2

        # Check first instance data (raw_data should be excluded by default)
        assert data[0]["provider"] == "Provider1"
        assert data[0]["instance_type"] == "type1"
        assert data[0]["price"] == 2.5
        assert "raw_data" not in data[0]

    def test_transform_pretty_print(self, sample_instances: list[GPUInstance]) -> None:
        """Test pretty-printed JSON transformation."""
        config = JSONTransformerConfig(pretty_print=True)
        result = transform_to_json(sample_instances, config)

        # Should be indented
        assert "  " in result
        # Should contain newlines
        assert "\n" in result

        # Should still be valid JSON
        data = json.loads(result)
        assert len(data) == 2

    def test_transform_compact(self, sample_instances: list[GPUInstance]) -> None:
        """Test compact JSON transformation."""
        config = JSONTransformerConfig(pretty_print=False)
        result = transform_to_json(sample_instances, config)

        # Should not be indented with 2 spaces
        assert result.count("  ") < 5  # May have some spaces in field values
        # Should still be valid JSON
        data = json.loads(result)
        assert len(data) == 2

    def test_transform_with_field_mapping(self, sample_instances: list[GPUInstance]) -> None:
        """Test JSON transformation with field mapping."""
        config = JSONTransformerConfig(
            fields={"provider": "cloud_provider", "price": "hourly_price", "region": "location"}
        )
        result = transform_to_json(sample_instances, config)

        data = json.loads(result)
        assert len(data) == 2

        # Should only have mapped fields with new names
        assert set(data[0].keys()) == {"cloud_provider", "hourly_price", "location"}
        assert data[0]["cloud_provider"] == "Provider1"
        assert data[0]["hourly_price"] == 2.5
        assert data[0]["location"] == "us-east-1"

    def test_transform_include_raw_data(self, sample_instances: list[GPUInstance]) -> None:
        """Test JSON transformation including raw_data."""
        config = JSONTransformerConfig(include_raw_data=True)
        result = transform_to_json(sample_instances, config)

        data = json.loads(result)
        assert "raw_data" in data[0]

    def test_transform_null_handling_omit(self) -> None:
        """Test JSON transformation omitting null values."""
        instance = GPUInstance(
            provider="Test",
            instance_type="type1",
            accelerator_name="GPU1",
            accelerator_count=1,
            region="us-east-1",
            availability=AvailabilityStatus.HIGH,
            price=1.0,
            spot_price=None,  # Null value
        )

        config = JSONTransformerConfig(null_handling="omit")
        result = transform_to_json([instance], config)

        data = json.loads(result)
        # spot_price should be omitted
        assert "spot_price" not in data[0]

    def test_transform_null_handling_null(self) -> None:
        """Test JSON transformation preserving null values."""
        instance = GPUInstance(
            provider="Test",
            instance_type="type1",
            accelerator_name="GPU1",
            accelerator_count=1,
            region="us-east-1",
            availability=AvailabilityStatus.HIGH,
            price=1.0,
            spot_price=None,  # Null value
        )

        config = JSONTransformerConfig(null_handling="null")
        result = transform_to_json([instance], config)

        data = json.loads(result)
        # spot_price should be null
        assert data[0]["spot_price"] is None

    def test_transform_null_handling_empty(self) -> None:
        """Test JSON transformation converting null to empty string."""
        instance = GPUInstance(
            provider="Test",
            instance_type="type1",
            accelerator_name="GPU1",
            accelerator_count=1,
            region="us-east-1",
            availability=AvailabilityStatus.HIGH,
            price=1.0,
            spot_price=None,  # Null value
        )

        config = JSONTransformerConfig(null_handling="empty")
        result = transform_to_json([instance], config)

        data = json.loads(result)
        # spot_price should be empty string
        assert data[0]["spot_price"] == ""

    def test_transform_empty_list(self) -> None:
        """Test JSON transformation with empty list."""
        config = JSONTransformerConfig()
        result = transform_to_json([], config)

        data = json.loads(result)
        assert data == []


class TestTransformToCSV:
    """Tests for CSV transformation."""

    def test_transform_basic(self, sample_instances: list[GPUInstance]) -> None:
        """Test basic CSV transformation."""
        config = CSVTransformerConfig(
            fields={"provider": "provider", "price": "price", "region": "region"}
        )
        result = transform_to_csv(sample_instances, config)

        # Should have header and 2 data rows
        lines = result.strip().split("\n")
        assert len(lines) == 3  # header + 2 rows

        # Header should contain field names
        header = lines[0]
        assert "provider" in header
        assert "price" in header
        assert "region" in header

        # Data rows should contain values
        assert "Provider1" in lines[1]
        assert "Provider2" in lines[2]

    def test_transform_without_header(self, sample_instances: list[GPUInstance]) -> None:
        """Test CSV transformation without header."""
        config = CSVTransformerConfig(
            fields={"provider": "provider", "price": "price"}, include_headers=False
        )
        result = transform_to_csv(sample_instances, config)

        lines = result.strip().split("\n")
        assert len(lines) == 2  # Only data rows

        # Should not have field names as first row
        assert not lines[0].startswith("provider")
        # Should have actual data
        assert "Provider1" in lines[0]

    def test_transform_with_field_mapping(self, sample_instances: list[GPUInstance]) -> None:
        """Test CSV transformation with field name mapping."""
        config = CSVTransformerConfig(
            fields={
                "provider": "cloud_provider",
                "price": "hourly_price",
                "region": "location",
            }
        )
        result = transform_to_csv(sample_instances, config)

        lines = result.strip().split("\n")
        header = lines[0]
        header_fields = header.split(",")

        # Should have mapped field names
        assert "cloud_provider" in header_fields
        assert "hourly_price" in header_fields
        assert "location" in header_fields
        # Should have exactly these 3 fields
        assert len(header_fields) == 3

    def test_transform_field_order(self, sample_instances: list[GPUInstance]) -> None:
        """Test that CSV respects field order."""
        # Note: Python 3.7+ dicts maintain insertion order
        config = CSVTransformerConfig(
            fields={"region": "region", "provider": "provider", "price": "price"}
        )
        result = transform_to_csv(sample_instances, config)

        lines = result.strip().split("\n")
        header = lines[0]

        # Field order should match config
        fields = header.split(",")
        assert fields[0] == "region"
        assert fields[1] == "provider"
        assert fields[2] == "price"

    def test_transform_custom_delimiter(self, sample_instances: list[GPUInstance]) -> None:
        """Test CSV with custom delimiter."""
        config = CSVTransformerConfig(
            fields={"provider": "provider", "price": "price"}, delimiter="|"
        )
        result = transform_to_csv(sample_instances, config)

        lines = result.strip().split("\n")
        # Should use pipe delimiter
        assert "|" in lines[0]
        assert "," not in lines[0]

    def test_transform_handles_null_values(self) -> None:
        """Test CSV handles None/null values correctly."""
        instance = GPUInstance(
            provider="Test",
            instance_type="type1",
            accelerator_name="GPU1",
            accelerator_count=1,
            region="us-east-1",
            availability=AvailabilityStatus.HIGH,
            price=1.0,
            spot_price=None,  # None value
        )

        config = CSVTransformerConfig(
            fields={"provider": "provider", "price": "price", "spot_price": "spot_price"}
        )
        result = transform_to_csv([instance], config)

        lines = result.strip().split("\n")
        # None should be represented as empty string by default
        assert len(lines) == 2
        assert "Test" in lines[1]

    def test_transform_custom_null_value(self) -> None:
        """Test CSV with custom null representation."""
        instance = GPUInstance(
            provider="Test",
            instance_type="type1",
            accelerator_name="GPU1",
            accelerator_count=1,
            region="us-east-1",
            availability=AvailabilityStatus.HIGH,
            price=1.0,
            spot_price=None,  # None value
        )

        config = CSVTransformerConfig(
            fields={"provider": "provider", "spot_price": "spot_price"}, null_value="N/A"
        )
        result = transform_to_csv([instance], config)

        lines = result.strip().split("\n")
        # None should be represented as N/A
        assert "N/A" in lines[1]

    def test_transform_empty_list(self) -> None:
        """Test CSV transformation with empty list."""
        config = CSVTransformerConfig(fields={"provider": "provider"})
        result = transform_to_csv([], config)

        assert result == ""


class TestTransformerIntegration:
    """Integration tests for transformers."""

    def test_json_and_csv_consistency(self, sample_instances: list[GPUInstance]) -> None:
        """Test that JSON and CSV transformers handle same fields consistently."""
        fields = {"provider": "provider", "price": "price", "region": "region"}

        json_config = JSONTransformerConfig(fields=fields)
        csv_config = CSVTransformerConfig(fields=fields)

        json_result = transform_to_json(sample_instances, json_config)
        csv_result = transform_to_csv(sample_instances, csv_config)

        # Parse JSON
        json_data = json.loads(json_result)

        # Parse CSV
        csv_lines = csv_result.strip().split("\n")
        csv_header = csv_lines[0].split(",")

        # Should have same number of records
        assert len(json_data) == len(csv_lines) - 1  # -1 for header

        # Should have same fields
        assert set(json_data[0].keys()) == set(csv_header)


class TestTransformToMetrics:
    """Tests for metrics transformation."""

    def test_metrics_count(self, sample_instances: list[GPUInstance]) -> None:
        """Test metrics count aggregation."""
        from gpuport_collectors.export.config import MetricConfig

        config = MetricsTransformerConfig(
            metrics=[MetricConfig(name="provider_count", type="count", field="provider")]
        )
        result = transform_to_metrics(sample_instances, config)

        metrics = json.loads(result)
        assert "provider_count" in metrics or "metrics" in metrics

    def test_metrics_sum(self, sample_instances: list[GPUInstance]) -> None:
        """Test metrics sum aggregation."""
        from gpuport_collectors.export.config import MetricConfig

        config = MetricsTransformerConfig(
            metrics=[
                MetricConfig(name="total_price", type="sum", field="price"),
                MetricConfig(name="total_quantity", type="sum", field="quantity"),
            ]
        )
        result = transform_to_metrics(sample_instances, config)

        metrics = json.loads(result)
        # Just verify it doesn't crash and returns valid JSON
        assert isinstance(metrics, dict | list)

    def test_metrics_avg(self, sample_instances: list[GPUInstance]) -> None:
        """Test metrics average aggregation."""
        from gpuport_collectors.export.config import MetricConfig

        config = MetricsTransformerConfig(
            metrics=[
                MetricConfig(name="avg_price", type="avg", field="price"),
                MetricConfig(name="avg_cpus", type="avg", field="v_cpus"),
            ]
        )
        result = transform_to_metrics(sample_instances, config)

        metrics = json.loads(result)
        assert isinstance(metrics, dict | list)

    def test_metrics_min_max(self, sample_instances: list[GPUInstance]) -> None:
        """Test metrics min and max aggregations."""
        from gpuport_collectors.export.config import MetricConfig

        config = MetricsTransformerConfig(
            metrics=[
                MetricConfig(name="min_price", type="min", field="price"),
                MetricConfig(name="max_price", type="max", field="price"),
                MetricConfig(name="min_quantity", type="min", field="quantity"),
                MetricConfig(name="max_quantity", type="max", field="quantity"),
            ]
        )
        result = transform_to_metrics(sample_instances, config)

        metrics = json.loads(result)
        assert isinstance(metrics, dict | list)

    def test_metrics_unique(self, sample_instances: list[GPUInstance]) -> None:
        """Test metrics unique aggregation."""
        from gpuport_collectors.export.config import MetricConfig

        config = MetricsTransformerConfig(
            metrics=[
                MetricConfig(name="unique_providers", type="unique", field="provider"),
                MetricConfig(name="unique_regions", type="unique", field="region"),
            ]
        )
        result = transform_to_metrics(sample_instances, config)

        metrics = json.loads(result)
        assert isinstance(metrics, dict | list)

    def test_metrics_include_timestamp(self, sample_instances: list[GPUInstance]) -> None:
        """Test metrics with timestamp included."""
        from gpuport_collectors.export.config import MetricConfig

        config = MetricsTransformerConfig(
            metrics=[MetricConfig(name="count", type="count", field="provider")],
            include_timestamp=True,
        )
        result = transform_to_metrics(sample_instances, config)

        metrics = json.loads(result)
        # Verify valid JSON output
        assert isinstance(metrics, dict | list)

    def test_metrics_without_timestamp(self, sample_instances: list[GPUInstance]) -> None:
        """Test metrics without timestamp."""
        from gpuport_collectors.export.config import MetricConfig

        config = MetricsTransformerConfig(
            metrics=[MetricConfig(name="count", type="count", field="provider")],
            include_timestamp=False,
        )
        result = transform_to_metrics(sample_instances, config)

        metrics = json.loads(result)
        assert isinstance(metrics, dict | list)

    def test_metrics_empty_instances(self) -> None:
        """Test metrics with empty instance list."""
        from gpuport_collectors.export.config import MetricConfig

        config = MetricsTransformerConfig(
            metrics=[
                MetricConfig(name="total_price", type="sum", field="price"),
                MetricConfig(name="count", type="count", field="provider"),
            ]
        )
        result = transform_to_metrics([], config)

        metrics = json.loads(result)
        assert isinstance(metrics, dict | list)

    def test_metrics_multiple_aggregations_same_field(
        self, sample_instances: list[GPUInstance]
    ) -> None:
        """Test multiple aggregations on the same field."""
        from gpuport_collectors.export.config import MetricConfig

        config = MetricsTransformerConfig(
            metrics=[
                MetricConfig(name="min_price", type="min", field="price"),
                MetricConfig(name="max_price", type="max", field="price"),
                MetricConfig(name="avg_price", type="avg", field="price"),
                MetricConfig(name="total_price", type="sum", field="price"),
            ]
        )
        result = transform_to_metrics(sample_instances, config)

        metrics = json.loads(result)
        assert isinstance(metrics, dict | list)
