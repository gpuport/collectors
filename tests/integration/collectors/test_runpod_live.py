"""Integration tests for RunPod collector with real API calls.

These tests make actual API calls to RunPod and require:
- Valid RUNPOD_API_KEY environment variable
- Network connectivity
- RunPod API availability

Run with: pytest tests/integration/collectors/test_runpod_live.py -m integration
"""

import os

import pytest

from gpuport_collectors.collectors.runpod import RunPodCollector
from gpuport_collectors.config import CollectorConfig, CollectorsConfig
from gpuport_collectors.models import AvailabilityStatus


@pytest.mark.integration
@pytest.mark.requires_api
@pytest.mark.network
@pytest.mark.skipif(
    not os.getenv("RUNPOD_API_KEY"), reason="RUNPOD_API_KEY environment variable not set"
)
class TestRunPodLiveAPI:
    """Integration tests for RunPod collector using real API."""

    @pytest.fixture
    def collector(self) -> RunPodCollector:
        """Create RunPod collector with real API key."""
        config = CollectorConfig()
        return RunPodCollector(config)

    @pytest.mark.asyncio
    async def test_fetch_real_instances(self, collector: RunPodCollector) -> None:
        """Test fetching real GPU instances from RunPod API.

        Verifies:
        - API connection works
        - Data is returned
        - Data matches expected schema
        - All instances have required fields
        """
        instances = await collector.fetch_instances()

        # Should return at least some instances
        assert len(instances) > 0, "Expected at least one GPU instance from RunPod"

        # Verify all instances have correct provider
        assert all(i.provider == "RunPod" for i in instances)

        # Verify all instances have required fields
        for instance in instances:
            assert instance.instance_type, f"Instance missing instance_type: {instance}"
            assert instance.accelerator_name, f"Instance missing accelerator_name: {instance}"
            assert instance.accelerator_count > 0, f"Invalid accelerator_count: {instance}"
            assert instance.region, f"Instance missing region: {instance}"
            assert isinstance(instance.availability, AvailabilityStatus)
            assert instance.price >= 0, f"Invalid price: {instance}"
            assert instance.collected_at > 0, f"Invalid collected_at: {instance}"

    @pytest.mark.asyncio
    async def test_datacenters_discovered(self, collector: RunPodCollector) -> None:
        """Test that multiple datacenters are discovered."""
        instances = await collector.fetch_instances()

        # RunPod has multiple datacenters
        regions = {i.region for i in instances}
        assert len(regions) > 1, f"Expected multiple regions, got: {regions}"

    @pytest.mark.asyncio
    async def test_multiple_gpu_types(self, collector: RunPodCollector) -> None:
        """Test that multiple GPU types are returned."""
        instances = await collector.fetch_instances()

        # RunPod offers various GPU types
        gpu_types = {i.accelerator_name for i in instances}
        assert len(gpu_types) > 1, f"Expected multiple GPU types, got: {gpu_types}"

    @pytest.mark.asyncio
    async def test_pricing_is_reasonable(self, collector: RunPodCollector) -> None:
        """Test that pricing data is within reasonable bounds."""
        instances = await collector.fetch_instances()

        # All prices should be positive and less than $1000/hour (sanity check)
        for instance in instances:
            assert 0 < instance.price < 1000, f"Unreasonable price: ${instance.price}/hr"

            # If spot price exists, it should be no higher than on-demand
            if instance.spot_price is not None:
                assert instance.spot_price <= instance.price, f"Spot price > on-demand: {instance}"

    @pytest.mark.asyncio
    async def test_availability_states(self, collector: RunPodCollector) -> None:
        """Test that instances have various availability states."""
        default_instances = await collector.fetch_instances()
        default_availabilities = {i.availability for i in default_instances}

        config = CollectorConfig(collectors=CollectorsConfig(include_unavailable=True))
        include_collector = RunPodCollector(config)
        include_instances = await include_collector.fetch_instances()
        include_availabilities = {i.availability for i in include_instances}

        assert default_availabilities, "Expected availability states from default collector"
        assert include_availabilities, "Expected availability states with include_unavailable"
        assert default_availabilities.issubset(include_availabilities), (
            "Expected include_unavailable to include at least the default availability states"
        )
