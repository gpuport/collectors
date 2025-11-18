"""Tests for RunPod collector."""

import asyncio
import json
import os
from pathlib import Path
from unittest.mock import AsyncMock, patch

import aiohttp
import pytest

from gpuport_collectors.collectors.runpod import RunPodCollector
from gpuport_collectors.config import CollectorConfig, HttpClientConfig, ObservabilityConfig
from gpuport_collectors.models import AvailabilityStatus


@pytest.fixture
def collector_config():
    """Create collector configuration for testing."""
    return CollectorConfig(
        http_client=HttpClientConfig(timeout=30, max_retries=3),
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


class TestDataDiscovery:
    """Tests for data discovery methods."""

    async def test_get_all_gpu_types(self, runpod_collector):
        """Test fetching all GPU types."""
        response = {
            "gpuTypes": [
                {
                    "id": "NVIDIA A100 80GB PCIe",
                    "displayName": "A100 80GB",
                    "memoryInGb": 80,
                    "cudaCores": 6912,
                },
                {
                    "id": "NVIDIA H100 80GB HBM3",
                    "displayName": "H100 80GB",
                    "memoryInGb": 80,
                    "cudaCores": 14592,
                },
            ]
        }
        runpod_collector._execute_graphql = AsyncMock(return_value=response)

        gpu_types = await runpod_collector._get_all_gpu_types()

        assert len(gpu_types) == 2
        assert gpu_types[0]["id"] == "NVIDIA A100 80GB PCIe"
        assert gpu_types[1]["id"] == "NVIDIA H100 80GB HBM3"

    async def test_get_all_datacenters(self, runpod_collector, sample_datacenter_response):
        """Test fetching all datacenters."""
        runpod_collector._execute_graphql = AsyncMock(return_value=sample_datacenter_response)

        datacenters = await runpod_collector._get_all_datacenters()

        assert len(datacenters) == 3
        assert "EU-RO-1" in datacenters
        assert "US-CA-1" in datacenters
        assert "US-TX-1" in datacenters


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

        instances = runpod_collector._parse_gpu_data(gpu, ["EU-RO-1"])

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
        """Test parsing GPU data with unavailable region (skips instance creation)."""
        gpu = {
            "id": "NVIDIA A100 80GB PCIe",
            "displayName": "A100 80GB",
            "memoryInGb": 80,
            "cudaCores": 6912,
            "us_tx_1": None,  # Unavailable
        }

        instances = runpod_collector._parse_gpu_data(gpu, ["US-TX-1"])

        # Should skip instances with no stockStatus
        assert len(instances) == 0

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
            "us_tx_1": None,  # Unavailable
        }

        instances = runpod_collector._parse_gpu_data(gpu, ["EU-RO-1", "US-TX-1"])

        # Should only create instance for EU-RO-1 (has stockStatus)
        assert len(instances) == 1
        assert instances[0].region == "EU-RO-1"
        assert instances[0].price == 1.89


class TestFetchInstances:
    """Tests for complete fetch instances workflow."""

    async def test_fetch_instances(self, runpod_collector, sample_datacenter_response):
        """Test complete fetch workflow with mocked API."""

        # Mock GPU types response
        gpu_types_response = {
            "gpuTypes": [
                {
                    "id": "NVIDIA A100 80GB PCIe",
                    "displayName": "A100 80GB",
                    "memoryInGb": 80,
                    "cudaCores": 6912,
                },
                {
                    "id": "NVIDIA H100 80GB HBM3",
                    "displayName": "H100 80GB",
                    "memoryInGb": 80,
                    "cudaCores": 14592,
                },
            ]
        }

        # Mock pricing response for individual GPUs
        pricing_response_a100 = {
            "gpuTypes": [
                {
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
                    "us_ca_1": None,
                    "us_tx_1": None,
                }
            ]
        }

        pricing_response_h100 = {
            "gpuTypes": [
                {
                    "id": "NVIDIA H100 80GB HBM3",
                    "displayName": "H100 80GB",
                    "memoryInGb": 80,
                    "cudaCores": 14592,
                    "eu_ro_1": {
                        "stockStatus": "Medium",
                        "uninterruptablePrice": 2.89,
                        "minimumBidPrice": 1.29,
                        "availableGpuCounts": 3,
                    },
                    "us_ca_1": None,
                    "us_tx_1": None,
                }
            ]
        }

        # Mock GraphQL execution to return different responses
        call_count = [0]

        async def mock_execute(query, variables=None):
            call_count[0] += 1
            if "dataCenters" in query:
                return sample_datacenter_response
            if "gpuTypes {" in query and "cudaCores" in query and "lowestPrice" not in query:
                # GPU types discovery
                return gpu_types_response
            if "NVIDIA A100" in query:
                return pricing_response_a100
            if "NVIDIA H100" in query:
                return pricing_response_h100
            return {"gpuTypes": []}

        runpod_collector._execute_graphql = AsyncMock(side_effect=mock_execute)

        instances = await runpod_collector.fetch_instances()

        # Should have instances only for regions with availability
        # A100: 1 available (EU-RO-1), H100: 1 available (EU-RO-1)
        assert len(instances) == 2

        # All instances should have availability (we skip unavailable ones now)
        available = [i for i in instances if i.price > 0]
        assert len(available) == 2

        # Verify all instances have required fields
        for instance in instances:
            assert instance.provider == "RunPod"
            assert instance.instance_type
            assert instance.accelerator_name
            assert instance.region == "EU-RO-1"  # Only available region
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

        # Verify fixture has available instances (we skip unavailable now)
        available = [i for i in sample_data if i["price"] > 0]

        assert len(available) > 0, "Fixture should include available instances"

        # Verify required fields are present
        for instance in sample_data:
            assert instance["provider"] == "RunPod"
            assert "instance_type" in instance
            assert "accelerator_name" in instance
            assert "region" in instance
            assert "price" in instance
            assert "availability" in instance
            assert "raw_data" in instance


class TestErrorHandling:
    """Tests for error handling and timeout scenarios."""

    @pytest.fixture
    def no_retry_collector(self):
        """Create RunPod collector with no retries for faster error testing."""
        config = CollectorConfig(
            http_client=HttpClientConfig(timeout=30, max_retries=0),  # No retries for faster tests
            observability=ObservabilityConfig(enabled=False),
        )
        with patch.dict(os.environ, {"RUNPOD_API_KEY": "test-api-key"}):
            return RunPodCollector(config)

    async def test_timeout_during_execute_graphql(self, no_retry_collector):
        """Test that API timeouts are handled gracefully."""
        # Mock timeout error
        no_retry_collector._execute_graphql = AsyncMock(side_effect=TimeoutError("Request timeout"))

        # Should raise TimeoutError immediately (no retries)
        with pytest.raises(asyncio.TimeoutError):
            await no_retry_collector.fetch_instances()

    async def test_http_error_during_fetch(self, no_retry_collector):
        """Test that HTTP errors are handled properly."""
        # Mock HTTP error
        no_retry_collector._execute_graphql = AsyncMock(
            side_effect=aiohttp.ClientError("Connection failed")
        )

        # Should raise ClientError immediately (no retries)
        with pytest.raises(aiohttp.ClientError):
            await no_retry_collector.fetch_instances()

    async def test_graphql_error_response(self, no_retry_collector):
        """Test handling of GraphQL error responses."""
        # Mock GraphQL error response
        no_retry_collector._execute_graphql = AsyncMock(
            side_effect=ValueError("GraphQL errors: [{'message': 'Invalid query'}]")
        )

        # Should raise ValueError immediately (no retries)
        with pytest.raises(ValueError, match="GraphQL errors"):
            await no_retry_collector.fetch_instances()

    async def test_partial_failure_during_pricing_queries(
        self, runpod_collector, sample_datacenter_response
    ):
        """Test that fetch_instances handles partial failures gracefully."""
        gpu_types_response = {
            "gpuTypes": [
                {
                    "id": "NVIDIA A100 80GB PCIe",
                    "displayName": "A100 80GB",
                    "memoryInGb": 80,
                    "cudaCores": 6912,
                },
                {
                    "id": "NVIDIA H100 80GB HBM3",
                    "displayName": "H100 80GB",
                    "memoryInGb": 80,
                    "cudaCores": 14592,
                },
            ]
        }

        # First pricing query succeeds, second fails
        pricing_response_a100 = {
            "gpuTypes": [
                {
                    "id": "NVIDIA A100 80GB PCIe",
                    "displayName": "A100 80GB",
                    "memoryInGb": 80,
                    "cudaCores": 6912,
                    "eu_ro_1": {
                        "stockStatus": "High",
                        "uninterruptablePrice": 1.89,
                        "minimumBidPrice": 0.79,
                        "availableGpuCounts": [1, 2, 3],
                    },
                    "us_ca_1": None,
                    "us_tx_1": None,
                }
            ]
        }

        call_count = [0]

        async def mock_execute(query, variables=None):
            call_count[0] += 1
            if "dataCenters" in query:
                return sample_datacenter_response
            if "gpuTypes {" in query and "cudaCores" in query and "lowestPrice" not in query:
                return gpu_types_response
            if "NVIDIA A100" in query:
                return pricing_response_a100
            if "NVIDIA H100" in query:
                # Simulate failure for H100
                raise aiohttp.ClientError("Timeout for H100")
            return {"gpuTypes": []}

        runpod_collector._execute_graphql = AsyncMock(side_effect=mock_execute)

        # Should continue processing despite one failure
        # Note: with_retry decorator on fetch_instances will retry, so this will eventually fail
        # But _fetch_gpu_pricing catches exceptions and returns None
        instances = await runpod_collector.fetch_instances()

        # Should have instances from A100 only (H100 failed)
        assert len(instances) == 1
        assert instances[0].instance_type == "NVIDIA A100 80GB PCIe"

    async def test_empty_response_handling(self, runpod_collector):
        """Test handling of empty API responses."""

        # Mock empty responses
        async def mock_execute(query, variables=None):
            if "dataCenters" in query:
                return {"dataCenters": []}
            if "gpuTypes" in query:
                return {"gpuTypes": []}
            return {}

        runpod_collector._execute_graphql = AsyncMock(side_effect=mock_execute)

        # Should return empty list without errors
        instances = await runpod_collector.fetch_instances()
        assert instances == []

    async def test_malformed_response_handling(self, runpod_collector):
        """Test handling of malformed API responses."""

        # Mock malformed response (missing expected fields)
        async def mock_execute(query, variables=None):
            if "dataCenters" in query:
                return {"dataCenters": [{"id": "EU-RO-1", "name": "EU-RO-1"}]}
            if "gpuTypes" in query and "lowestPrice" not in query:
                return {
                    "gpuTypes": [
                        {
                            "id": "NVIDIA A100 80GB PCIe",
                            "displayName": "A100 80GB",
                            "memoryInGb": 80,
                            "cudaCores": 6912,
                        }
                    ]
                }
            # Malformed pricing response (missing required fields)
            return {"gpuTypes": [{"id": "test"}]}

        runpod_collector._execute_graphql = AsyncMock(side_effect=mock_execute)

        # Should handle gracefully (skip malformed entries)
        instances = await runpod_collector.fetch_instances()
        # Should return empty list since pricing data is malformed
        assert instances == []
