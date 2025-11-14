"""Tests for RunPod collector."""

import json
import os
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from gpuport_collectors.collectors.runpod import RunPodCollector
from gpuport_collectors.config import CollectorConfig, ObservabilityConfig
from gpuport_collectors.models import AvailabilityStatus


@pytest.fixture
def collector_config():
    """Create collector configuration for testing."""
    return CollectorConfig(
        timeout=30,
        max_retries=3,
        observability=ObservabilityConfig(enabled=False),
    )


@pytest.fixture
def runpod_collector(collector_config):
    """Create RunPod collector instance."""
    with patch.dict(os.environ, {"RUNPOD_API_KEY": "test-api-key"}):
        return RunPodCollector(collector_config)


@pytest.fixture
def sample_datacenter_response():
    """Sample datacenter discovery response."""
    return {
        "dataCenters": [
            {"id": "EU-RO-1", "name": "EU-RO-1"},
            {"id": "US-CA-1", "name": "US-CA-1"},
            {"id": "US-TX-1", "name": "US-TX-1"},
        ]
    }


@pytest.fixture
def sample_gpu_response():
    """Sample GPU availability response with per-region pricing."""
    return {
        "gpuTypes": [
            {
                "id": "NVIDIA A100 80GB PCIe",
                "displayName": "A100 80GB",
                "memoryInGb": 80,
                "cudaCores": 6912,
                "nodeGroupDatacenters": ["EU-RO-1", "US-TX-1"],
                "eu_ro_1": {
                    "stockStatus": "High",
                    "uninterruptablePrice": 1.89,
                    "minimumBidPrice": 0.79,
                    "availableGpuCounts": 10,
                },
                "us_tx_1": None,  # Unavailable in this region
            },
            {
                "id": "NVIDIA H100 80GB HBM3",
                "displayName": "H100 80GB",
                "memoryInGb": 80,
                "cudaCores": 14592,
                "nodeGroupDatacenters": ["EU-RO-1"],
                "eu_ro_1": {
                    "stockStatus": "Medium",
                    "uninterruptablePrice": 2.89,
                    "minimumBidPrice": 1.29,
                    "availableGpuCounts": 3,
                },
            },
        ]
    }


class TestRunPodCollectorInit:
    """Tests for RunPodCollector initialization."""

    def test_init_with_api_key(self, collector_config):
        """Test initialization with API key in environment."""
        with patch.dict(os.environ, {"RUNPOD_API_KEY": "test-key"}):
            collector = RunPodCollector(collector_config)
            assert collector.api_key == "test-key"
            assert collector.provider_name == "RunPod"

    def test_init_without_api_key(self, collector_config):
        """Test initialization fails without API key."""
        with (
            patch.dict(os.environ, {}, clear=True),
            pytest.raises(ValueError, match="RUNPOD_API_KEY"),
        ):
            RunPodCollector(collector_config)


class TestGPUDatacenterMapping:
    """Tests for GPU-datacenter mapping discovery."""

    async def test_get_gpu_datacenter_mapping(self, runpod_collector):
        """Test GPU type discovery with their available datacenters."""
        response = {
            "gpuTypes": [
                {
                    "id": "NVIDIA A100 80GB PCIe",
                    "displayName": "A100 80GB",
                    "memoryInGb": 80,
                    "cudaCores": 6912,
                    "nodeGroupDatacenters": [
                        {"id": "EU-RO-1", "name": "EU-RO-1"},
                        {"id": "US-TX-1", "name": "US-TX-1"},
                    ],
                },
                {
                    "id": "NVIDIA H100 80GB HBM3",
                    "displayName": "H100 80GB",
                    "memoryInGb": 80,
                    "cudaCores": 14592,
                    "nodeGroupDatacenters": [
                        {"id": "EU-RO-1", "name": "EU-RO-1"},
                    ],
                },
            ]
        }
        runpod_collector._execute_graphql = AsyncMock(return_value=response)

        mapping = await runpod_collector._get_gpu_datacenter_mapping()

        assert len(mapping) == 2
        assert "NVIDIA A100 80GB PCIe" in mapping
        assert mapping["NVIDIA A100 80GB PCIe"]["datacenters"] == ["EU-RO-1", "US-TX-1"]
        assert mapping["NVIDIA H100 80GB HBM3"]["datacenters"] == ["EU-RO-1"]

    async def test_get_gpu_datacenter_mapping_empty(self, runpod_collector):
        """Test mapping discovery with empty response."""
        runpod_collector._execute_graphql = AsyncMock(return_value={"gpuTypes": []})

        mapping = await runpod_collector._get_gpu_datacenter_mapping()

        assert mapping == {}


class TestQueryBuilding:
    """Tests for GraphQL query construction."""

    def test_build_pricing_query(self, runpod_collector):
        """Test building pricing query with datacenter aliases."""
        datacenters = ["EU-RO-1", "US-TX-1"]
        query = runpod_collector._build_pricing_query(datacenters)

        # Verify query contains GPU fields
        assert "gpuTypes" in query
        assert "displayName" in query
        assert "memoryInGb" in query

        # Verify datacenter aliases are included
        assert "eu_ro_1: lowestPrice" in query
        assert "us_tx_1: lowestPrice" in query
        assert 'dataCenterId: "EU-RO-1"' in query
        assert 'dataCenterId: "US-TX-1"' in query

    def test_get_all_datacenters_from_mapping(self, runpod_collector):
        """Test extracting unique datacenters from GPU mapping."""
        gpu_mapping = {
            "GPU1": {"datacenters": ["EU-RO-1", "US-TX-1"]},
            "GPU2": {"datacenters": ["EU-RO-1"]},
            "GPU3": {"datacenters": []},
        }

        datacenters = runpod_collector._get_all_datacenters_from_mapping(gpu_mapping)

        assert datacenters == ["EU-RO-1", "US-TX-1"]  # Sorted unique


class TestStockStatusMapping:
    """Tests for stock status mapping."""

    def test_map_stock_status_high(self, runpod_collector):
        """Test mapping High stock status."""
        assert runpod_collector._map_stock_status("High") == AvailabilityStatus.HIGH

    def test_map_stock_status_medium(self, runpod_collector):
        """Test mapping Medium stock status."""
        assert runpod_collector._map_stock_status("Medium") == AvailabilityStatus.MEDIUM

    def test_map_stock_status_low(self, runpod_collector):
        """Test mapping Low stock status."""
        assert runpod_collector._map_stock_status("Low") == AvailabilityStatus.LOW

    def test_map_stock_status_none(self, runpod_collector):
        """Test mapping None stock status."""
        assert runpod_collector._map_stock_status(None) == AvailabilityStatus.NOT_AVAILABLE

    def test_map_stock_status_unknown(self, runpod_collector):
        """Test mapping unknown stock status."""
        assert runpod_collector._map_stock_status("Unknown") == AvailabilityStatus.NOT_AVAILABLE


class TestGPUDataParsing:
    """Tests for GPU data parsing."""

    def test_parse_gpu_data_available(self, runpod_collector):
        """Test parsing GPU data with available pricing."""
        gpu = {
            "id": "NVIDIA A100 80GB PCIe",
            "displayName": "A100 80GB",
            "memoryInGb": 80,
            "cudaCores": 6912,
            "eu_ro_1": {
                "stockStatus": "High",
                "uninterruptablePrice": 1.89,
                "minimumBidPrice": 0.79,
                "availableGpuCounts": 10,
            },
        }

        gpu_mapping = {
            "NVIDIA A100 80GB PCIe": {
                "displayName": "A100 80GB",
                "memoryInGb": 80,
                "cudaCores": 6912,
                "datacenters": ["EU-RO-1"],
            }
        }

        instances = runpod_collector._parse_gpu_data(gpu, ["EU-RO-1"], gpu_mapping)

        assert len(instances) == 1
        instance = instances[0]

        assert instance.provider == "RunPod"
        assert instance.instance_type == "NVIDIA A100 80GB PCIe"
        assert instance.accelerator_name == "A100 80GB"
        assert instance.region == "EU-RO-1"
        assert instance.price == 1.89
        assert instance.spot_price == 0.79
        assert instance.availability == AvailabilityStatus.HIGH
        assert instance.quantity == 10
        assert instance.accelerator_mem_gib == 80

    def test_parse_gpu_data_unavailable(self, runpod_collector):
        """Test parsing GPU data with unavailable region."""
        gpu = {
            "id": "NVIDIA A100 80GB PCIe",
            "displayName": "A100 80GB",
            "memoryInGb": 80,
            "cudaCores": 6912,
            "us_tx_1": None,  # Unavailable
        }

        gpu_mapping = {
            "NVIDIA A100 80GB PCIe": {
                "displayName": "A100 80GB",
                "memoryInGb": 80,
                "cudaCores": 6912,
                "datacenters": ["US-TX-1"],
            }
        }

        instances = runpod_collector._parse_gpu_data(gpu, ["US-TX-1"], gpu_mapping)

        assert len(instances) == 1
        instance = instances[0]

        assert instance.region == "US-TX-1"
        assert instance.price == 0.0
        assert instance.spot_price == 0.0
        assert instance.availability == AvailabilityStatus.NOT_AVAILABLE
        assert instance.quantity == 0

    def test_parse_gpu_data_multiple_regions(self, runpod_collector):
        """Test parsing GPU data across multiple regions."""
        gpu = {
            "id": "NVIDIA A100 80GB PCIe",
            "displayName": "A100 80GB",
            "memoryInGb": 80,
            "cudaCores": 6912,
            "eu_ro_1": {
                "stockStatus": "High",
                "uninterruptablePrice": 1.89,
                "minimumBidPrice": 0.79,
                "availableGpuCounts": 10,
            },
            "us_tx_1": None,
        }

        gpu_mapping = {
            "NVIDIA A100 80GB PCIe": {
                "displayName": "A100 80GB",
                "memoryInGb": 80,
                "cudaCores": 6912,
                "datacenters": ["EU-RO-1", "US-TX-1"],
            }
        }

        instances = runpod_collector._parse_gpu_data(gpu, ["EU-RO-1", "US-TX-1"], gpu_mapping)

        assert len(instances) == 2
        # Should have one available and one unavailable
        available = [i for i in instances if i.price > 0]
        unavailable = [i for i in instances if i.price == 0]
        assert len(available) == 1
        assert len(unavailable) == 1


class TestFetchInstances:
    """Tests for complete fetch instances workflow."""

    async def test_fetch_instances(self, runpod_collector, sample_gpu_response):
        """Test complete fetch workflow with mocked API."""

        # Mock GPU-datacenter mapping discovery
        mapping_response = {
            "gpuTypes": [
                {
                    "id": "NVIDIA A100 80GB PCIe",
                    "displayName": "A100 80GB",
                    "memoryInGb": 80,
                    "cudaCores": 6912,
                    "nodeGroupDatacenters": [
                        {"id": "EU-RO-1", "name": "EU-RO-1"},
                        {"id": "US-TX-1", "name": "US-TX-1"},
                    ],
                },
                {
                    "id": "NVIDIA H100 80GB HBM3",
                    "displayName": "H100 80GB",
                    "memoryInGb": 80,
                    "cudaCores": 14592,
                    "nodeGroupDatacenters": [
                        {"id": "EU-RO-1", "name": "EU-RO-1"},
                    ],
                },
            ]
        }

        # Mock GraphQL execution to return different responses
        async def mock_execute(query, variables=None):
            if "nodeGroupDatacenters" in query:
                # GPU-datacenter mapping query
                return mapping_response
            # Pricing query
            return sample_gpu_response

        runpod_collector._execute_graphql = AsyncMock(side_effect=mock_execute)

        instances = await runpod_collector.fetch_instances()

        # Should have instances only for GPUs with available datacenters
        # A100: 2 regions, H100: 1 region
        assert len(instances) > 0

        # Verify mix of available and unavailable
        available = [i for i in instances if i.price > 0]
        unavailable = [i for i in instances if i.price == 0]

        assert len(available) > 0
        assert len(unavailable) > 0

        # Verify all instances have required fields
        for instance in instances:
            assert instance.provider == "RunPod"
            assert instance.instance_type
            assert instance.accelerator_name
            assert instance.region
            assert instance.accelerator_mem_gib > 0
            assert instance.collected_at > 0


class TestFixtureData:
    """Tests using real fixture data structure."""

    def test_parse_fixture_data(self, runpod_collector):
        """Test parsing fixture data matches expected structure."""
        fixtures_path = Path(__file__).parent.parent.parent / "fixtures"
        fixture_file = fixtures_path / "runpod_sample.json"

        with fixture_file.open() as f:
            sample_data = json.load(f)

        # Verify fixture has both available and unavailable instances
        available = [i for i in sample_data if i["price"] > 0]
        unavailable = [i for i in sample_data if i["price"] == 0]

        assert len(available) > 0, "Fixture should include available instances"
        assert len(unavailable) > 0, "Fixture should include unavailable instances"

        # Verify required fields are present
        for instance in sample_data:
            assert instance["provider"] == "RunPod"
            assert "instance_type" in instance
            assert "accelerator_name" in instance
            assert "region" in instance
            assert "price" in instance
            assert "availability" in instance
            assert "raw_data" in instance
