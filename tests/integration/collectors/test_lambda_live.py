"""Integration tests for Lambda Labs collector with real API calls.

These tests make actual API calls to Lambda Labs and require:
- Valid LAMBDA_API_KEY environment variable
- Network connectivity
- Lambda Labs API availability

Run with: pytest tests/integration/collectors/test_lambda_live.py -m integration
"""

import os

import pytest

from gpuport_collectors.collectors.lambda_labs import LambdaLabsCollector
from gpuport_collectors.config import CollectorConfig
from gpuport_collectors.models import AvailabilityStatus


@pytest.mark.integration
@pytest.mark.requires_api
@pytest.mark.network
@pytest.mark.skipif(
    not os.getenv("LAMBDA_API_KEY"),
    reason="LAMBDA_API_KEY environment variable not set",
)
class TestLambdaLabsLiveAPI:
    """Integration tests for Lambda Labs collector using real API."""

    @pytest.fixture
    def collector(self) -> LambdaLabsCollector:
        """Create Lambda Labs collector with real API key."""
        config = CollectorConfig()
        return LambdaLabsCollector(config)

    @pytest.mark.asyncio
    async def test_fetch_real_instances(self, collector: LambdaLabsCollector) -> None:
        """Test fetching real GPU instances from Lambda Labs API.

        Verifies:
        - API connection works
        - Data is returned
        - Data matches expected schema
        - All instances have required fields
        """
        instances = await collector.fetch_instances()

        # Should return at least some instances
        assert len(instances) > 0, "Expected at least one GPU instance from Lambda Labs"

        # Verify all instances have correct provider
        assert all(i.provider == "Lambda Labs" for i in instances)

        # Verify all instances have required fields
        for instance in instances:
            assert instance.instance_type, f"Instance missing instance_type: {instance}"
            assert instance.accelerator_name, f"Instance missing accelerator_name: {instance}"
            assert instance.accelerator_count > 0, f"Invalid accelerator_count: {instance}"
            assert instance.region, f"Instance missing region: {instance}"
            assert isinstance(instance.availability, AvailabilityStatus), (
                f"Invalid availability: {instance}"
            )
            assert instance.price >= 0, f"Invalid price: {instance}"
            assert instance.collected_at > 0, f"Invalid collected_at: {instance}"

    @pytest.mark.asyncio
    async def test_multiple_regions(self, collector: LambdaLabsCollector) -> None:
        """Test that multiple regions are discovered."""
        instances = await collector.fetch_instances()

        # Lambda Labs has multiple regions
        regions = {i.region for i in instances}
        assert len(regions) > 1, f"Expected multiple regions, got: {regions}"

    @pytest.mark.asyncio
    async def test_gpu_types_available(self, collector: LambdaLabsCollector) -> None:
        """Test that various GPU types are returned."""
        instances = await collector.fetch_instances()

        # Lambda Labs offers different GPU types (A100, H100, etc.)
        gpu_types = {i.accelerator_name for i in instances}
        assert len(gpu_types) > 0, f"Expected GPU types, got: {gpu_types}"

    @pytest.mark.asyncio
    async def test_pricing_data(self, collector: LambdaLabsCollector) -> None:
        """Test that pricing data is present and reasonable."""
        instances = await collector.fetch_instances()

        # All prices should be non-negative
        for instance in instances:
            assert instance.price >= 0, f"Invalid price: ${instance.price}/hr"
            assert instance.price < 100, f"Unreasonable price: ${instance.price}/hr"
