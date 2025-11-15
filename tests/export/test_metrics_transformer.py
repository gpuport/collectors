"""Tests for metrics transformer."""

import json

import pytest

from gpuport_collectors.export.config import MetricConfig, MetricsTransformerConfig
from gpuport_collectors.export.transformers import TransformerError, transform_to_metrics
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
            provider="Provider1",
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
        GPUInstance(
            provider="Provider2",
            instance_type="type3",
            accelerator_name="GPU1",
            accelerator_count=1,
            accelerator_mem_gib=8.0,
            region="us-east-1",
            availability=AvailabilityStatus.LOW,
            quantity=10,
            price=1.0,
            spot_price=0.5,
            v_cpus=4,
            memory_gib=16.0,
        ),
    ]


class TestCountMetric:
    """Tests for count metric."""

    def test_count_metric(self, sample_instances: list[GPUInstance]) -> None:
        """Test count metric."""
        config = MetricsTransformerConfig(
            metrics=[MetricConfig(name="total_instances", type="count")]
        )
        result = transform_to_metrics(sample_instances, config)

        data = json.loads(result)
        assert data["metrics"]["total_instances"] == 3

    def test_count_with_empty_list(self) -> None:
        """Test count metric with empty list."""
        config = MetricsTransformerConfig(
            metrics=[MetricConfig(name="total_instances", type="count")]
        )
        result = transform_to_metrics([], config)

        data = json.loads(result)
        assert data["metrics"]["total_instances"] == 0


class TestAverageMetric:
    """Tests for average metric."""

    def test_avg_metric(self, sample_instances: list[GPUInstance]) -> None:
        """Test average metric."""
        config = MetricsTransformerConfig(
            metrics=[MetricConfig(name="avg_price", type="avg", field="price")]
        )
        result = transform_to_metrics(sample_instances, config)

        data = json.loads(result)
        # (2.5 + 5.0 + 1.0) / 3 = 2.833...
        assert abs(data["metrics"]["avg_price"] - 2.833333) < 0.001

    def test_avg_with_null_values(self) -> None:
        """Test average metric with null values."""
        instances = [
            GPUInstance(
                provider="Test",
                instance_type="type1",
                accelerator_name="GPU1",
                accelerator_count=1,
                region="us-east-1",
                availability=AvailabilityStatus.HIGH,
                price=2.0,
                spot_price=None,  # Null value
            ),
            GPUInstance(
                provider="Test",
                instance_type="type2",
                accelerator_name="GPU2",
                accelerator_count=1,
                region="us-east-1",
                availability=AvailabilityStatus.HIGH,
                price=4.0,
                spot_price=2.0,
            ),
        ]

        config = MetricsTransformerConfig(
            metrics=[MetricConfig(name="avg_spot_price", type="avg", field="spot_price")]
        )
        result = transform_to_metrics(instances, config)

        data = json.loads(result)
        # Should only average non-null values: 2.0
        assert data["metrics"]["avg_spot_price"] == 2.0


class TestMinMaxMetrics:
    """Tests for min and max metrics."""

    def test_min_metric(self, sample_instances: list[GPUInstance]) -> None:
        """Test min metric."""
        config = MetricsTransformerConfig(
            metrics=[MetricConfig(name="min_price", type="min", field="price")]
        )
        result = transform_to_metrics(sample_instances, config)

        data = json.loads(result)
        assert data["metrics"]["min_price"] == 1.0

    def test_max_metric(self, sample_instances: list[GPUInstance]) -> None:
        """Test max metric."""
        config = MetricsTransformerConfig(
            metrics=[MetricConfig(name="max_price", type="max", field="price")]
        )
        result = transform_to_metrics(sample_instances, config)

        data = json.loads(result)
        assert data["metrics"]["max_price"] == 5.0


class TestSumMetric:
    """Tests for sum metric."""

    def test_sum_metric(self, sample_instances: list[GPUInstance]) -> None:
        """Test sum metric."""
        config = MetricsTransformerConfig(
            metrics=[MetricConfig(name="total_quantity", type="sum", field="quantity")]
        )
        result = transform_to_metrics(sample_instances, config)

        data = json.loads(result)
        assert data["metrics"]["total_quantity"] == 18  # 5 + 3 + 10


class TestUniqueMetric:
    """Tests for unique metric."""

    def test_unique_metric(self, sample_instances: list[GPUInstance]) -> None:
        """Test unique metric."""
        config = MetricsTransformerConfig(
            metrics=[MetricConfig(name="unique_gpus", type="unique", field="accelerator_name")]
        )
        result = transform_to_metrics(sample_instances, config)

        data = json.loads(result)
        assert data["metrics"]["unique_gpus"] == 2  # GPU1, GPU2


class TestGroupByMetrics:
    """Tests for group-by metrics."""

    def test_count_by_provider(self, sample_instances: list[GPUInstance]) -> None:
        """Test count metric grouped by provider."""
        config = MetricsTransformerConfig(
            metrics=[MetricConfig(name="instances_by_provider", type="count", group_by="provider")]
        )
        result = transform_to_metrics(sample_instances, config)

        data = json.loads(result)
        assert data["metrics"]["instances_by_provider"]["Provider1"] == 2
        assert data["metrics"]["instances_by_provider"]["Provider2"] == 1

    def test_avg_price_by_provider(self, sample_instances: list[GPUInstance]) -> None:
        """Test average price grouped by provider."""
        config = MetricsTransformerConfig(
            metrics=[
                MetricConfig(
                    name="avg_price_by_provider", type="avg", field="price", group_by="provider"
                )
            ]
        )
        result = transform_to_metrics(sample_instances, config)

        data = json.loads(result)
        # Provider1: (2.5 + 5.0) / 2 = 3.75
        assert abs(data["metrics"]["avg_price_by_provider"]["Provider1"] - 3.75) < 0.001
        # Provider2: 1.0
        assert data["metrics"]["avg_price_by_provider"]["Provider2"] == 1.0

    def test_sum_by_region(self, sample_instances: list[GPUInstance]) -> None:
        """Test sum metric grouped by region."""
        config = MetricsTransformerConfig(
            metrics=[
                MetricConfig(
                    name="total_quantity_by_region",
                    type="sum",
                    field="quantity",
                    group_by="region",
                )
            ]
        )
        result = transform_to_metrics(sample_instances, config)

        data = json.loads(result)
        assert data["metrics"]["total_quantity_by_region"]["us-east-1"] == 15  # 5 + 10
        assert data["metrics"]["total_quantity_by_region"]["us-west-1"] == 3

    def test_group_by_with_null_values(self) -> None:
        """Test that None values in group-by field create 'null' group."""
        instances = [
            GPUInstance(
                provider="Provider1",
                instance_type="type1",
                accelerator_name="GPU1",
                accelerator_count=1,
                region="us-east-1",
                availability=AvailabilityStatus.HIGH,
                price=1.0,
                spot_price=1.5,  # Has spot price
            ),
            GPUInstance(
                provider="Provider2",
                instance_type="type2",
                accelerator_name="GPU2",
                accelerator_count=1,
                region="us-west-1",
                availability=AvailabilityStatus.HIGH,
                price=2.0,
                spot_price=None,  # No spot price
            ),
            GPUInstance(
                provider="Provider3",
                instance_type="type3",
                accelerator_name="GPU3",
                accelerator_count=1,
                region="eu-west-1",
                availability=AvailabilityStatus.HIGH,
                price=3.0,
                spot_price=None,  # No spot price
            ),
        ]

        config = MetricsTransformerConfig(
            metrics=[
                MetricConfig(
                    name="count_by_spot_price",
                    type="count",
                    group_by="spot_price",
                )
            ]
        )
        result = transform_to_metrics(instances, config)

        data = json.loads(result)
        # Instances with spot_price=None should be in "null" group
        assert data["metrics"]["count_by_spot_price"]["null"] == 2
        assert data["metrics"]["count_by_spot_price"]["1.5"] == 1


class TestMultipleMetrics:
    """Tests for multiple metrics in one transform."""

    def test_multiple_metrics(self, sample_instances: list[GPUInstance]) -> None:
        """Test multiple metrics in one transformation."""
        config = MetricsTransformerConfig(
            metrics=[
                MetricConfig(name="total", type="count"),
                MetricConfig(name="avg_price", type="avg", field="price"),
                MetricConfig(name="min_price", type="min", field="price"),
                MetricConfig(name="max_price", type="max", field="price"),
                MetricConfig(name="total_quantity", type="sum", field="quantity"),
            ]
        )
        result = transform_to_metrics(sample_instances, config)

        data = json.loads(result)
        metrics = data["metrics"]
        assert metrics["total"] == 3
        assert abs(metrics["avg_price"] - 2.833333) < 0.001
        assert metrics["min_price"] == 1.0
        assert metrics["max_price"] == 5.0
        assert metrics["total_quantity"] == 18


class TestMetadata:
    """Tests for metadata inclusion."""

    def test_include_timestamp(self, sample_instances: list[GPUInstance]) -> None:
        """Test timestamp inclusion."""
        config = MetricsTransformerConfig(
            metrics=[MetricConfig(name="total", type="count")],
            include_timestamp=True,
        )
        result = transform_to_metrics(sample_instances, config)

        data = json.loads(result)
        assert "timestamp" in data
        # Should be ISO format
        assert "T" in data["timestamp"]
        assert "Z" in data["timestamp"] or "+" in data["timestamp"]

    def test_include_collection_info(self, sample_instances: list[GPUInstance]) -> None:
        """Test collection info inclusion."""
        config = MetricsTransformerConfig(
            metrics=[MetricConfig(name="total", type="count")],
            include_collection_info=True,
        )
        result = transform_to_metrics(sample_instances, config)

        data = json.loads(result)
        assert "collection_info" in data
        assert data["collection_info"]["total_instances"] == 3
        assert "collected_at" in data["collection_info"]

    def test_exclude_metadata(self, sample_instances: list[GPUInstance]) -> None:
        """Test metadata exclusion."""
        config = MetricsTransformerConfig(
            metrics=[MetricConfig(name="total", type="count")],
            include_timestamp=False,
            include_collection_info=False,
        )
        result = transform_to_metrics(sample_instances, config)

        data = json.loads(result)
        assert "timestamp" not in data
        assert "collection_info" not in data
        assert "metrics" in data


class TestErrorHandling:
    """Tests for error handling."""

    def test_missing_field_for_avg(self, sample_instances: list[GPUInstance]) -> None:
        """Test error when field is missing for avg metric."""
        from pydantic import ValidationError

        # Field validation now happens at model creation time
        with pytest.raises(ValidationError, match="requires 'field' parameter"):
            MetricsTransformerConfig(metrics=[MetricConfig(name="avg_price", type="avg", field=None)])

    def test_all_null_values(self) -> None:
        """Test metric with all null values."""
        instances = [
            GPUInstance(
                provider="Test",
                instance_type="type1",
                accelerator_name="GPU1",
                accelerator_count=1,
                region="us-east-1",
                availability=AvailabilityStatus.HIGH,
                price=1.0,
                spot_price=None,
            ),
            GPUInstance(
                provider="Test",
                instance_type="type2",
                accelerator_name="GPU2",
                accelerator_count=1,
                region="us-east-1",
                availability=AvailabilityStatus.HIGH,
                price=2.0,
                spot_price=None,
            ),
        ]

        config = MetricsTransformerConfig(
            metrics=[MetricConfig(name="avg_spot_price", type="avg", field="spot_price")]
        )
        result = transform_to_metrics(instances, config)

        data = json.loads(result)
        # Should return None when all values are null
        assert data["metrics"]["avg_spot_price"] is None

    def test_group_by_invalid_field_raises(self, sample_instances: list[GPUInstance]) -> None:
        """Test that group-by with invalid field raises error."""
        config = MetricsTransformerConfig(
            metrics=[
                MetricConfig(
                    name="count_by_invalid",
                    type="count",
                    group_by="nonexistent_field",
                )
            ]
        )
        with pytest.raises(TransformerError, match="does not exist"):
            transform_to_metrics(sample_instances, config)

    def test_group_by_with_empty_list(self) -> None:
        """Test that group-by with empty list doesn't fail validation."""
        config = MetricsTransformerConfig(
            metrics=[
                MetricConfig(
                    name="count_by_provider",
                    type="count",
                    group_by="provider",
                )
            ]
        )
        # Should not raise - empty list has no instances to validate
        result = transform_to_metrics([], config)

        data = json.loads(result)
        assert data["metrics"]["count_by_provider"] == {}
