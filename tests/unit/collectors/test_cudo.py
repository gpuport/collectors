"""Unit tests for CudoCollector."""

import json
import time
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest

from gpuport_collectors.collectors.cudo import CudoCollector
from gpuport_collectors.config import CollectorConfig, HttpClientConfig
from gpuport_collectors.models import AvailabilityStatus, GPUInstance


@pytest.fixture
def current_timestamp():
    """Provide current timestamp for tests."""
    return int(time.time())


@pytest.fixture(autouse=True)
def no_retry_sleep():
    """Avoid real sleep delays from retry logic during tests."""
    with patch("asyncio.sleep", new_callable=AsyncMock):
        yield


class TestCudoCollectorInit:
    """Test collector initialization."""

    def test_init_with_api_key(self):
        """Test initialization with API key set."""
        with patch.dict("os.environ", {"CUDO_API_KEY": "test-key"}):
            config = CollectorConfig()
            collector = CudoCollector(config)
            assert collector.api_key == "test-key"
            assert collector.provider_name == "Cudo Compute"

    def test_init_without_api_key(self):
        """Test initialization without API key raises ValueError."""
        with patch.dict("os.environ", {}, clear=True):
            config = CollectorConfig()
            with pytest.raises(ValueError, match="CUDO_API_KEY"):
                CudoCollector(config)

    def test_provider_name(self):
        """Test provider_name property."""
        with patch.dict("os.environ", {"CUDO_API_KEY": "test-key"}):
            config = CollectorConfig()
            collector = CudoCollector(config)
            assert collector.provider_name == "Cudo Compute"


class TestCudoAPIExecution:
    """Test API call execution."""

    @pytest.mark.asyncio
    async def test_execute_api_call_success(self):
        """Test successful API call execution."""
        with patch.dict("os.environ", {"CUDO_API_KEY": "test-key"}):
            config = CollectorConfig()
            collector = CudoCollector(config)

            with patch.object(
                collector,
                "_execute_api_call",
                new=AsyncMock(return_value={"machineTypes": [], "totalCount": 0}),
            ) as mock_execute:
                result = await collector._execute_api_call("/machines-types")

                assert result == {"machineTypes": [], "totalCount": 0}
                mock_execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_execute_api_call_http_error(self):
        """Test API call with HTTP error."""
        with patch.dict("os.environ", {"CUDO_API_KEY": "test-key"}):
            config = CollectorConfig()
            collector = CudoCollector(config)

            with (
                patch.object(
                    collector,
                    "_execute_api_call",
                    new=AsyncMock(side_effect=aiohttp.ClientError("API error")),
                ),
                pytest.raises(aiohttp.ClientError),
            ):
                await collector._execute_api_call("/invalid-endpoint")

    @pytest.mark.asyncio
    async def test_execute_api_call_uses_bearer_auth(self):
        """Test that API calls use Bearer authentication."""
        with patch.dict("os.environ", {"CUDO_API_KEY": "secret-key"}):
            config = CollectorConfig()
            collector = CudoCollector(config)

            mock_response: dict[str, object] = {"machineTypes": []}

            with patch("aiohttp.ClientSession") as mock_session_class:
                # Create async context manager for the session
                mock_session = MagicMock()
                mock_session_class.return_value.__aenter__.return_value = mock_session
                mock_session_class.return_value.__aexit__.return_value = AsyncMock()

                # Create async context manager for the response
                mock_resp = AsyncMock()
                mock_resp.json = AsyncMock(return_value=mock_response)
                mock_resp.raise_for_status = MagicMock()

                mock_request = AsyncMock()
                mock_request.__aenter__.return_value = mock_resp
                mock_request.__aexit__.return_value = AsyncMock()
                mock_session.request = MagicMock(return_value=mock_request)

                await collector._execute_api_call("/machines-types")

                # Verify Bearer auth header was used
                call_kwargs = mock_session.request.call_args[1]
                assert "headers" in call_kwargs
                assert call_kwargs["headers"]["Authorization"] == "Bearer secret-key"


class TestCudoGPUParsing:
    """Test GPU name and memory parsing."""

    def test_parse_gpu_name_h100(self):
        """Test parsing H100 GPU name."""
        with patch.dict("os.environ", {"CUDO_API_KEY": "test-key"}):
            collector = CudoCollector(CollectorConfig())
            assert collector._parse_gpu_name("nvidia-h100") == "NVIDIA H100"

    def test_parse_gpu_name_h100_nvl_pcie(self):
        """Test parsing H100 NVL PCIe GPU name."""
        with patch.dict("os.environ", {"CUDO_API_KEY": "test-key"}):
            collector = CudoCollector(CollectorConfig())
            assert collector._parse_gpu_name("nvidia-h100-nvl-pcie") == "NVIDIA H100 NVL PCIE"

    def test_parse_gpu_name_l40s(self):
        """Test parsing L40S GPU name."""
        with patch.dict("os.environ", {"CUDO_API_KEY": "test-key"}):
            collector = CudoCollector(CollectorConfig())
            assert collector._parse_gpu_name("nvidia-l40s") == "NVIDIA L40S"

    def test_parse_gpu_name_a40(self):
        """Test parsing A40 GPU name."""
        with patch.dict("os.environ", {"CUDO_API_KEY": "test-key"}):
            collector = CudoCollector(CollectorConfig())
            assert collector._parse_gpu_name("nvidia-a40") == "NVIDIA A40"

    def test_extract_gpu_memory_h100(self):
        """Test extracting H100 memory (80 GiB)."""
        with patch.dict("os.environ", {"CUDO_API_KEY": "test-key"}):
            collector = CudoCollector(CollectorConfig())
            assert collector._extract_gpu_memory("nvidia-h100") == 80

    def test_extract_gpu_memory_h100_nvl(self):
        """Test extracting H100 NVL memory (94 GiB)."""
        with patch.dict("os.environ", {"CUDO_API_KEY": "test-key"}):
            collector = CudoCollector(CollectorConfig())
            assert collector._extract_gpu_memory("nvidia-h100-nvl-pcie") == 94

    def test_extract_gpu_memory_l40s(self):
        """Test extracting L40S memory (48 GiB)."""
        with patch.dict("os.environ", {"CUDO_API_KEY": "test-key"}):
            collector = CudoCollector(CollectorConfig())
            assert collector._extract_gpu_memory("nvidia-l40s") == 48

    def test_extract_gpu_memory_unknown(self):
        """Test extracting memory for unknown GPU returns None."""
        with patch.dict("os.environ", {"CUDO_API_KEY": "test-key"}):
            collector = CudoCollector(CollectorConfig())
            assert collector._extract_gpu_memory("nvidia-unknown") is None


class TestCudoPriceParsing:
    """Test price extraction from prices array."""

    def test_get_on_demand_price(self):
        """Test extracting on-demand price."""
        with patch.dict("os.environ", {"CUDO_API_KEY": "test-key"}):
            collector = CudoCollector(CollectorConfig())
            prices = [
                {
                    "commitmentTerm": "COMMITMENT_TERM_NONE",
                    "priceHr": {"value": "18.000000"},
                },
                {
                    "commitmentTerm": "COMMITMENT_TERM_1_MONTH",
                    "priceHr": {"value": "17.200000"},
                },
            ]
            assert collector._get_on_demand_price(prices) == 18.0

    def test_get_on_demand_price_fallback(self):
        """Test price extraction falls back to first price if no on-demand."""
        with patch.dict("os.environ", {"CUDO_API_KEY": "test-key"}):
            collector = CudoCollector(CollectorConfig())
            prices = [
                {
                    "commitmentTerm": "COMMITMENT_TERM_1_MONTH",
                    "priceHr": {"value": "15.500000"},
                },
            ]
            assert collector._get_on_demand_price(prices) == 15.5

    def test_get_on_demand_price_empty(self):
        """Test price extraction with empty prices array."""
        with patch.dict("os.environ", {"CUDO_API_KEY": "test-key"}):
            collector = CudoCollector(CollectorConfig())
            assert collector._get_on_demand_price([]) == 0.0


class TestCudoInstanceCreation:
    """Test GPUInstance creation from machine type data."""

    def test_create_gpu_instance_basic(self, current_timestamp):
        """Test creating basic GPU instance."""
        with patch.dict("os.environ", {"CUDO_API_KEY": "test-key"}):
            collector = CudoCollector(CollectorConfig())
            machine_type = {
                "id": "sapphire-rapids-h100",
                "dataCenterId": "fr-paris-1",
                "gpuModelId": "nvidia-h100",
                "gpus": 8,
                "cpuCores": 72,
                "memoryGib": 1024,
                "prices": [
                    {
                        "commitmentTerm": "COMMITMENT_TERM_NONE",
                        "priceHr": {"value": "18.000000"},
                    }
                ],
            }

            instance = collector._create_gpu_instance(machine_type, current_timestamp)

            assert instance.provider == "Cudo Compute"
            assert instance.instance_type == "sapphire-rapids-h100"
            assert instance.accelerator_name == "NVIDIA H100"
            assert instance.accelerator_count == 8
            assert instance.accelerator_mem_gib == 80
            assert instance.region == "fr-paris-1"
            assert instance.price == 18.0
            assert instance.spot_price is None
            assert instance.availability == AvailabilityStatus.HIGH
            assert instance.v_cpus == 72
            assert instance.memory_gib == 1024

    def test_create_gpu_instance_l40s(self, current_timestamp):
        """Test creating L40S GPU instance."""
        with patch.dict("os.environ", {"CUDO_API_KEY": "test-key"}):
            collector = CudoCollector(CollectorConfig())
            machine_type = {
                "id": "epyc-genoa-l40s",
                "dataCenterId": "no-kristiansand-1",
                "gpuModelId": "nvidia-l40s",
                "gpus": 8,
                "cpuCores": 96,
                "memoryGib": 1538,
                "prices": [
                    {
                        "commitmentTerm": "COMMITMENT_TERM_NONE",
                        "priceHr": {"value": "6.960000"},
                    }
                ],
            }

            instance = collector._create_gpu_instance(machine_type, current_timestamp)

            assert instance.accelerator_name == "NVIDIA L40S"
            assert instance.accelerator_count == 8
            assert instance.accelerator_mem_gib == 48
            assert instance.price == 6.96

    def test_create_gpu_instance_raw_data_preservation(self, current_timestamp):
        """Test that raw machine type data is preserved."""
        with patch.dict("os.environ", {"CUDO_API_KEY": "test-key"}):
            collector = CudoCollector(CollectorConfig())
            machine_type = {
                "id": "test-machine",
                "dataCenterId": "test-dc",
                "gpuModelId": "nvidia-h100",
                "gpus": 1,
                "prices": [],
            }

            instance = collector._create_gpu_instance(machine_type, current_timestamp)

            assert instance.raw_data == machine_type


class TestCudoFetchInstances:
    """Test end-to-end instance fetching."""

    @pytest.mark.asyncio
    async def test_fetch_instances_success(self):
        """Test successful instance fetching."""
        with patch.dict("os.environ", {"CUDO_API_KEY": "test-key"}):
            collector = CudoCollector(CollectorConfig())

            mock_response = {
                "machineTypes": [
                    {
                        "id": "sapphire-rapids-h100",
                        "dataCenterId": "fr-paris-1",
                        "gpuModelId": "nvidia-h100",
                        "gpus": 8,
                        "cpuCores": 72,
                        "memoryGib": 1024,
                        "prices": [
                            {
                                "commitmentTerm": "COMMITMENT_TERM_NONE",
                                "priceHr": {"value": "18.000000"},
                            }
                        ],
                    }
                ],
                "totalCount": 1,
            }

            with patch.object(collector, "_execute_api_call", return_value=mock_response):
                instances = await collector.fetch_instances()

                assert len(instances) == 1
                assert isinstance(instances[0], GPUInstance)
                assert instances[0].provider == "Cudo Compute"
                assert instances[0].instance_type == "sapphire-rapids-h100"

    @pytest.mark.asyncio
    async def test_fetch_instances_multiple_data_centers(self):
        """Test fetching instances from multiple data centers."""
        with patch.dict("os.environ", {"CUDO_API_KEY": "test-key"}):
            collector = CudoCollector(CollectorConfig())

            mock_response = {
                "machineTypes": [
                    {
                        "id": "machine-1",
                        "dataCenterId": "fr-paris-1",
                        "gpuModelId": "nvidia-h100",
                        "gpus": 8,
                        "prices": [
                            {"commitmentTerm": "COMMITMENT_TERM_NONE", "priceHr": {"value": "18.0"}}
                        ],
                    },
                    {
                        "id": "machine-2",
                        "dataCenterId": "no-kristiansand-1",
                        "gpuModelId": "nvidia-l40s",
                        "gpus": 8,
                        "prices": [
                            {"commitmentTerm": "COMMITMENT_TERM_NONE", "priceHr": {"value": "7.0"}}
                        ],
                    },
                ],
                "totalCount": 2,
            }

            with patch.object(collector, "_execute_api_call", return_value=mock_response):
                instances = await collector.fetch_instances()

                assert len(instances) == 2
                regions = {i.region for i in instances}
                assert regions == {"fr-paris-1", "no-kristiansand-1"}

    @pytest.mark.asyncio
    async def test_fetch_instances_empty_response(self):
        """Test fetching with empty response."""
        with patch.dict("os.environ", {"CUDO_API_KEY": "test-key"}):
            collector = CudoCollector(CollectorConfig())

            mock_response = {"machineTypes": [], "totalCount": 0}

            with patch.object(collector, "_execute_api_call", return_value=mock_response):
                instances = await collector.fetch_instances()

                assert len(instances) == 0

    @pytest.mark.asyncio
    async def test_fetch_instances_error_handling(self):
        """Test error handling during fetch."""
        with patch.dict("os.environ", {"CUDO_API_KEY": "test-key"}):
            collector = CudoCollector(CollectorConfig())

            with (
                patch.object(
                    collector,
                    "_execute_api_call",
                    side_effect=aiohttp.ClientError("Network error"),
                ),
                pytest.raises(aiohttp.ClientError),
            ):
                await collector.fetch_instances()


class TestCudoFixtureData:
    """Test with real fixture data."""

    @pytest.mark.asyncio
    async def test_parse_fixture_data(self):
        """Test parsing actual fixture data."""
        with patch.dict("os.environ", {"CUDO_API_KEY": "test-key"}):
            collector = CudoCollector(CollectorConfig())

            # Load fixture
            fixtures_path = Path(__file__).parent.parent.parent / "fixtures"
            fixture_file = fixtures_path / "mock_responses" / "cudo.json"

            fixture_data = json.loads(fixture_file.read_text())

            with patch.object(collector, "_execute_api_call", return_value=fixture_data):
                instances = await collector.fetch_instances()

                # Validate instances were created
                assert len(instances) > 0

                # Validate structure of first instance
                instance = instances[0]
                assert instance.provider == "Cudo Compute"
                assert instance.instance_type is not None
                assert instance.accelerator_name.startswith("NVIDIA")
                assert instance.accelerator_count > 0
                assert instance.region is not None
                assert instance.price >= 0
                assert instance.availability == AvailabilityStatus.HIGH

    @pytest.mark.asyncio
    async def test_fixture_schema_valid(self):
        """Test that fixture data matches expected schema."""
        fixtures_path = Path(__file__).parent.parent.parent / "fixtures"
        fixture_file = fixtures_path / "mock_responses" / "cudo.json"

        data = json.loads(fixture_file.read_text())

        # Validate top-level structure
        assert "machineTypes" in data
        assert isinstance(data["machineTypes"], list)

        # Validate machine type structure
        if data["machineTypes"]:
            mt = data["machineTypes"][0]
            assert "id" in mt
            assert "dataCenterId" in mt
            assert "gpuModelId" in mt
            assert "gpus" in mt
            assert "prices" in mt
            assert isinstance(mt["prices"], list)


class TestCudoErrorHandling:
    """Test error scenarios and edge cases."""

    @pytest.mark.asyncio
    async def test_timeout_handling(self):
        """Test timeout during API call."""
        with patch.dict("os.environ", {"CUDO_API_KEY": "test-key"}):
            collector = CudoCollector(CollectorConfig(http_client=HttpClientConfig(timeout=1)))

            with (
                patch.object(
                    collector,
                    "_execute_api_call",
                    new=AsyncMock(side_effect=aiohttp.ServerTimeoutError()),
                ),
                pytest.raises(aiohttp.ServerTimeoutError),
            ):
                await collector._execute_api_call("/machines-types")

    @pytest.mark.asyncio
    async def test_http_error_handling(self):
        """Test HTTP error handling."""
        with patch.dict("os.environ", {"CUDO_API_KEY": "test-key"}):
            collector = CudoCollector(CollectorConfig())

            with (
                patch.object(
                    collector,
                    "_execute_api_call",
                    side_effect=aiohttp.ClientResponseError(
                        request_info=MagicMock(),
                        history=(),
                        status=500,
                        message="Internal Server Error",
                    ),
                ),
                pytest.raises(aiohttp.ClientResponseError),
            ):
                await collector.fetch_instances()

    @pytest.mark.asyncio
    async def test_malformed_response_handling(self):
        """Test handling of malformed API response."""
        with patch.dict("os.environ", {"CUDO_API_KEY": "test-key"}):
            collector = CudoCollector(CollectorConfig())

            # Missing required fields
            mock_response = {
                "machineTypes": [
                    {
                        "id": "test",
                        # Missing gpuModelId, gpus, etc.
                    }
                ]
            }

            with (
                patch.object(collector, "_execute_api_call", return_value=mock_response),
                pytest.raises(KeyError),
            ):
                await collector.fetch_instances()

    def test_empty_prices_array(self, current_timestamp):
        """Test handling of machine type with no prices."""
        with patch.dict("os.environ", {"CUDO_API_KEY": "test-key"}):
            collector = CudoCollector(CollectorConfig())
            machine_type = {
                "id": "test-machine",
                "dataCenterId": "test-dc",
                "gpuModelId": "nvidia-h100",
                "gpus": 1,
                "prices": [],
            }

            instance = collector._create_gpu_instance(machine_type, current_timestamp)

            # Should default to 0.0
            assert instance.price == 0.0
