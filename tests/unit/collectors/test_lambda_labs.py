"""Tests for Lambda Labs collector."""

import json
import time
from pathlib import Path
from unittest.mock import ANY, AsyncMock, MagicMock, patch

import aiohttp
import pytest

from gpuport_collectors.collectors.lambda_labs import LambdaLabsCollector
from gpuport_collectors.config import CollectorConfig, CollectorsConfig
from gpuport_collectors.models import AvailabilityStatus, GPUInstance

# Load mock API response fixture
FIXTURES_DIR = Path(__file__).parent.parent.parent / "fixtures" / "mock_responses"
with (FIXTURES_DIR / "lambda_labs.json").open() as f:
    MOCK_INSTANCE_TYPES = json.load(f)


@pytest.fixture
def lambda_collector(monkeypatch):
    """Create a Lambda Labs collector instance for testing."""
    monkeypatch.setenv("LAMBDA_API_KEY", "test-api-key")
    return LambdaLabsCollector(config=CollectorConfig())


@pytest.fixture
def lambda_collector_all(monkeypatch):
    """Create a Lambda Labs collector instance that includes unavailable instances."""
    monkeypatch.setenv("LAMBDA_API_KEY", "test-api-key")
    return LambdaLabsCollector(
        config=CollectorConfig(collectors=CollectorsConfig(include_unavailable=True))
    )


@pytest.fixture
def mock_aiohttp():
    """Mock aiohttp ClientSession for API calls."""
    with patch("aiohttp.ClientSession") as mock:
        yield mock


@pytest.fixture(autouse=True)
def no_retry_sleep():
    """Avoid real sleep delays from retry logic during tests."""
    with patch("asyncio.sleep", new_callable=AsyncMock):
        yield


class TestLambdaLabsCollectorInit:
    """Tests for Lambda Labs collector initialization."""

    def test_init_success(self, lambda_collector):
        """Test successful collector initialization with API key."""
        assert lambda_collector.api_key == "test-api-key"
        assert lambda_collector.provider_name == "Lambda Labs"

    def test_init_missing_api_key(self, monkeypatch):
        """Test that missing API key raises error."""
        # Ensure no API key is set
        monkeypatch.delenv("LAMBDA_API_KEY", raising=False)

        with pytest.raises(ValueError, match="LAMBDA_API_KEY environment variable must be set"):
            LambdaLabsCollector(config=CollectorConfig())

    def test_provider_name(self, lambda_collector):
        """Test provider name property."""
        assert lambda_collector.provider_name == "Lambda Labs"


class TestLambdaLabsAPIExecution:
    """Tests for Lambda Labs API execution."""

    @pytest.mark.asyncio
    async def test_execute_api_call_success(self, lambda_collector, mock_aiohttp):
        """Test successful API call execution."""
        mock_response = AsyncMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json = AsyncMock(return_value={"test": "data"})
        mock_response.__aenter__.return_value = mock_response
        mock_response.__aexit__ = AsyncMock(return_value=None)

        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_response)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)

        mock_aiohttp.return_value = mock_session

        result = await lambda_collector._execute_api_call("/test-endpoint")
        assert result == {"test": "data"}

    @pytest.mark.asyncio
    async def test_execute_api_call_http_error(self, lambda_collector, mock_aiohttp):
        """Test handling of HTTP errors."""
        mock_session = MagicMock()
        mock_session.get = MagicMock(side_effect=aiohttp.ClientError("API error"))
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)

        mock_aiohttp.return_value = mock_session

        with pytest.raises(aiohttp.ClientError):
            await lambda_collector._execute_api_call("/test-endpoint")

    @pytest.mark.asyncio
    async def test_execute_api_call_uses_basic_auth(self, lambda_collector, mock_aiohttp):
        """Test that API calls use Basic Auth with API key."""
        mock_response = AsyncMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json = AsyncMock(return_value={})
        mock_response.__aenter__.return_value = mock_response
        mock_response.__aexit__ = AsyncMock(return_value=None)

        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_response)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)

        mock_aiohttp.return_value = mock_session

        await lambda_collector._execute_api_call("/instance-types")

        # Verify Basic Auth was used
        call_kwargs = mock_session.get.call_args[1]
        assert "auth" in call_kwargs
        auth = call_kwargs["auth"]
        assert isinstance(auth, aiohttp.BasicAuth)
        assert auth.login == "test-api-key"
        assert auth.password == ""


class TestLambdaLabsGPUParsing:
    """Tests for GPU name and memory parsing."""

    def test_parse_gpu_name_single_gpu(self, lambda_collector):
        """Test parsing single GPU descriptions."""
        gpu_name, count = lambda_collector._parse_gpu_name("1x A10 (24 GB PCIe)")
        assert gpu_name == "NVIDIA A10"
        assert count == 1

    def test_parse_gpu_name_multiple_gpus(self, lambda_collector):
        """Test parsing multiple GPU descriptions."""
        gpu_name, count = lambda_collector._parse_gpu_name("8x A100 (80 GB SXM4)")
        assert gpu_name == "NVIDIA A100 80GB"
        assert count == 8

    def test_parse_gpu_name_a100_40gb(self, lambda_collector):
        """Test parsing A100 40GB description."""
        gpu_name, count = lambda_collector._parse_gpu_name("1x A100 (40 GB SXM4)")
        assert gpu_name == "NVIDIA A100 40GB"
        assert count == 1

    def test_parse_gpu_name_a100_80gb(self, lambda_collector):
        """Test parsing A100 80GB description."""
        gpu_name, count = lambda_collector._parse_gpu_name("1x A100 (80 GB SXM4)")
        assert gpu_name == "NVIDIA A100 80GB"
        assert count == 1

    def test_parse_gpu_name_h100(self, lambda_collector):
        """Test parsing H100 description."""
        gpu_name, count = lambda_collector._parse_gpu_name("8x H100 (80 GB SXM5)")
        assert gpu_name == "NVIDIA H100 80GB"
        assert count == 8

    def test_parse_gpu_name_rtx(self, lambda_collector):
        """Test parsing RTX description."""
        gpu_name, count = lambda_collector._parse_gpu_name("1x RTX6000 (24 GB)")
        assert gpu_name == "NVIDIA RTX6000"
        assert count == 1

    def test_extract_gpu_memory_success(self, lambda_collector):
        """Test extracting GPU memory from description."""
        memory = lambda_collector._extract_gpu_memory("1x A10 (24 GB PCIe)")
        assert memory == 24

        memory = lambda_collector._extract_gpu_memory("8x A100 (80 GB SXM4)")
        assert memory == 80

    def test_extract_gpu_memory_not_found(self, lambda_collector):
        """Test GPU memory extraction when not present."""
        memory = lambda_collector._extract_gpu_memory("No memory info")
        assert memory is None


class TestLambdaLabsInstanceCreation:
    """Tests for GPUInstance creation from Lambda Labs data."""

    def test_create_gpu_instance_basic(self, lambda_collector):
        """Test creating GPUInstance from Lambda Labs data."""
        instance_type = {
            "name": "gpu_1x_a10",
            "price_cents_per_hour": 60,
            "description": "1x A10 (24 GB PCIe)",
            "specs": {
                "vcpus": 30,
                "memory_gib": 200,
                "storage_gib": 1400,
            },
        }
        region = {"name": "us-west-1", "description": "California, USA"}
        collected_at = int(time.time())

        instance = lambda_collector._create_gpu_instance(instance_type, region, collected_at)

        assert instance.provider == "Lambda Labs"
        assert instance.instance_type == "gpu_1x_a10"
        assert instance.accelerator_name == "NVIDIA A10"
        assert instance.accelerator_count == 1
        assert instance.accelerator_mem_gib == 24
        assert instance.region == "us-west-1"
        assert instance.availability_zone == "California, USA"
        assert instance.price == 0.60  # 60 cents -> $0.60
        assert instance.spot_price is None  # Lambda Labs doesn't offer spot
        assert instance.availability == AvailabilityStatus.HIGH
        assert instance.quantity is None  # Lambda Labs doesn't expose quantity
        assert instance.v_cpus == 30
        assert instance.memory_gib == 200
        # Note: storage_gib is not in GPUInstance model schema
        assert instance.collected_at == collected_at

    def test_create_gpu_instance_multi_gpu(self, lambda_collector):
        """Test creating GPUInstance for multi-GPU instance."""
        instance_type = {
            "name": "gpu_8x_a100_80gb_sxm4",
            "price_cents_per_hour": 1200,
            "description": "8x A100 (80 GB SXM4)",
            "specs": {
                "vcpus": 120,
                "memory_gib": 800,
                "storage_gib": 11200,
            },
        }
        region = {"name": "us-west-1", "description": "California, USA"}
        collected_at = int(time.time())

        instance = lambda_collector._create_gpu_instance(instance_type, region, collected_at)

        assert instance.accelerator_name == "NVIDIA A100 80GB"
        assert instance.accelerator_count == 8
        assert instance.accelerator_mem_gib == 80
        assert instance.price == 12.00  # $12/hour
        assert instance.v_cpus == 120
        assert instance.memory_gib == 800

    def test_create_gpu_instance_price_conversion(self, lambda_collector):
        """Test price conversion from cents to dollars."""
        instance_type = {
            "name": "test_instance",
            "price_cents_per_hour": 199,
            "description": "1x H100 (80 GB PCIe)",
            "specs": {"vcpus": 26, "memory_gib": 200, "storage_gib": 1400},
        }
        region = {"name": "us-east-1", "description": "Virginia, USA"}

        instance = lambda_collector._create_gpu_instance(instance_type, region, int(time.time()))

        assert instance.price == 1.99  # 199 cents -> $1.99

    def test_create_gpu_instance_raw_data_preservation(self, lambda_collector):
        """Test that raw API data is preserved."""
        instance_type = {
            "name": "gpu_1x_a10",
            "price_cents_per_hour": 60,
            "description": "1x A10 (24 GB PCIe)",
            "specs": {"vcpus": 30, "memory_gib": 200, "storage_gib": 1400},
        }
        region = {"name": "us-west-1", "description": "California, USA"}

        instance = lambda_collector._create_gpu_instance(instance_type, region, int(time.time()))

        assert "instance_type" in instance.raw_data
        assert "region" in instance.raw_data
        assert instance.raw_data["instance_type"] == instance_type
        assert instance.raw_data["region"] == region


class TestLambdaLabsIntegration:
    """Integration tests for Lambda Labs collector."""

    @pytest.mark.asyncio
    async def test_fetch_instances_success(self, lambda_collector):
        """Test successful instance fetching."""
        # Mock the API call method directly
        lambda_collector._execute_api_call = AsyncMock(return_value=MOCK_INSTANCE_TYPES)

        instances = await lambda_collector.fetch_instances()

        # Verify we got instances
        assert isinstance(instances, list)
        assert len(instances) > 0

        # Verify instance structure
        for instance in instances:
            assert isinstance(instance, GPUInstance)
            assert instance.provider == "Lambda Labs"
            assert isinstance(instance.price, float)
            assert instance.availability in AvailabilityStatus

    @pytest.mark.asyncio
    async def test_fetch_instances_creates_per_region_instances(self, lambda_collector):
        """Test that one GPUInstance is created per (gpu_type, region) pair by default."""
        # Mock the API call method directly
        lambda_collector._execute_api_call = AsyncMock(return_value=MOCK_INSTANCE_TYPES)

        instances = await lambda_collector.fetch_instances()

        # Count expected instances based on REAL Lambda Labs API fixture
        # Real API has 20 GPU types (CPU-only entries are skipped):
        # - 9 GPU types with regions (20 total GPU+region combinations)
        # - 11 GPU types without regions (filtered by default)
        expected_count = 20
        assert len(instances) == expected_count

    @pytest.mark.asyncio
    async def test_fetch_instances_captures_unavailable(self, lambda_collector_all):
        """Test that instances without regional availability are captured with NOT_AVAILABLE status."""
        # Mock the API call method directly
        lambda_collector_all._execute_api_call = AsyncMock(return_value=MOCK_INSTANCE_TYPES)

        instances = await lambda_collector_all.fetch_instances()

        # From real API: gpu_1x_h100_pcie, gpu_4x_h100_sxm5, gpu_2x_h100_sxm5,
        # gpu_1x_rtx6000, gpu_1x_a100, gpu_2x_a100, etc. have empty regions_with_capacity_available
        # These should now appear with region="unavailable" and status=NOT_AVAILABLE
        unavailable_types = ["gpu_1x_h100_pcie", "gpu_4x_h100_sxm5", "gpu_1x_rtx6000"]
        for gpu_type in unavailable_types:
            instances_of_type = [i for i in instances if i.instance_type == gpu_type]
            assert len(instances_of_type) == 1, (
                f"{gpu_type} should have exactly 1 unavailable instance"
            )
            assert instances_of_type[0].region == "unavailable"
            assert instances_of_type[0].availability == AvailabilityStatus.NOT_AVAILABLE

    @pytest.mark.asyncio
    async def test_fetch_instances_validates_schema(self, lambda_collector):
        """Test that all returned instances validate against GPUInstance schema."""
        # Mock the API call method directly
        lambda_collector._execute_api_call = AsyncMock(return_value=MOCK_INSTANCE_TYPES)

        instances = await lambda_collector.fetch_instances()

        # Verify all instances have required fields
        for instance in instances:
            # Required fields
            assert instance.provider == "Lambda Labs"
            assert isinstance(instance.instance_type, str)
            assert isinstance(instance.accelerator_name, str)
            assert isinstance(instance.accelerator_count, int | float)  # Model defines as float
            assert isinstance(instance.price, float)
            assert instance.availability in AvailabilityStatus
            assert isinstance(instance.region, str)

            # Optional fields with correct types when present
            if instance.v_cpus is not None:
                assert isinstance(instance.v_cpus, int | float)  # Model defines as float
            if instance.memory_gib is not None:
                assert isinstance(instance.memory_gib, int | float)  # Model defines as float
            if instance.accelerator_mem_gib is not None:
                assert isinstance(
                    instance.accelerator_mem_gib, int | float
                )  # Model defines as float

    @pytest.mark.asyncio
    async def test_fetch_instances_single_api_call(self, lambda_collector):
        """Test that fetch_instances makes only ONE API call."""
        # Mock the API call method directly and track calls
        lambda_collector._execute_api_call = AsyncMock(return_value=MOCK_INSTANCE_TYPES)

        await lambda_collector.fetch_instances()

        # Verify only one API call was made
        lambda_collector._execute_api_call.assert_called_once_with("/instance-types", session=ANY)

    @pytest.mark.asyncio
    async def test_fetch_instances_empty_response(self, lambda_collector):
        """Test handling of empty API response."""
        # Mock empty API response
        lambda_collector._execute_api_call = AsyncMock(return_value={})

        instances = await lambda_collector.fetch_instances()

        # Should return empty list, not error
        assert isinstance(instances, list)
        assert len(instances) == 0

    @pytest.mark.asyncio
    async def test_fetch_instances_error_handling(self, lambda_collector):
        """Test error handling during instance fetching."""
        # Mock API error
        lambda_collector._execute_api_call = AsyncMock(side_effect=aiohttp.ClientError("API error"))

        # With @with_retry decorator, this will retry and eventually raise
        with pytest.raises(aiohttp.ClientError):
            await lambda_collector.fetch_instances()


class TestLambdaLabsFixtureData:
    """Test with real fixture data."""

    @pytest.mark.asyncio
    async def test_parse_fixture_data(self, lambda_collector):
        """Test parsing actual fixture data."""
        # Load fixture
        fixtures_path = Path(__file__).parent.parent.parent / "fixtures"
        fixture_file = fixtures_path / "mock_responses" / "lambda_labs.json"

        fixture_data = json.loads(fixture_file.read_text())

        lambda_collector._execute_api_call = AsyncMock(return_value=fixture_data)
        instances = await lambda_collector.fetch_instances()

        # Validate instances were created
        assert len(instances) > 0

        # Validate structure of first instance
        instance = instances[0]
        assert instance.provider == "Lambda Labs"
        assert instance.instance_type is not None
        assert instance.accelerator_name.startswith("NVIDIA")
        assert instance.accelerator_count > 0
        assert instance.region is not None
        assert instance.price >= 0
        assert instance.availability in [
            AvailabilityStatus.HIGH,
            AvailabilityStatus.LOW,
            AvailabilityStatus.NOT_AVAILABLE,
        ]

    @pytest.mark.asyncio
    async def test_fixture_schema_valid(self):
        """Test that fixture data matches expected schema."""
        fixtures_path = Path(__file__).parent.parent.parent / "fixtures"
        fixture_file = fixtures_path / "mock_responses" / "lambda_labs.json"

        data = json.loads(fixture_file.read_text())

        # Validate top-level structure
        assert "data" in data

        # Validate instance type structure
        # Each key is a GPU type, value has instance_type and regions_with_capacity_available
        if data.get("data"):
            for _gpu_type, gpu_data in data["data"].items():
                assert "instance_type" in gpu_data
                assert "regions_with_capacity_available" in gpu_data

                # Validate instance type structure
                it = gpu_data["instance_type"]
                assert "name" in it
                assert "description" in it
                assert "price_cents_per_hour" in it
                assert "specs" in it


class TestLambdaLabsErrorHandling:
    """Test error scenarios and edge cases."""

    @pytest.mark.asyncio
    async def test_timeout_handling(self, lambda_collector):
        """Test timeout during API call propagates through fetch_instances."""
        # Mock timeout error
        lambda_collector._execute_api_call = AsyncMock(side_effect=aiohttp.ServerTimeoutError())

        with pytest.raises(aiohttp.ServerTimeoutError):
            await lambda_collector.fetch_instances()

    @pytest.mark.asyncio
    async def test_http_error_handling(self, lambda_collector):
        """Test HTTP error handling."""
        # Mock HTTP error
        lambda_collector._execute_api_call = AsyncMock(
            side_effect=aiohttp.ClientResponseError(
                request_info=MagicMock(),
                history=(),
                status=500,
                message="Internal Server Error",
            )
        )

        with pytest.raises(aiohttp.ClientResponseError):
            await lambda_collector.fetch_instances()

    @pytest.mark.asyncio
    async def test_malformed_response_handling(self, lambda_collector):
        """Test handling of malformed API response."""
        # Missing required fields
        mock_response = {
            "data": {
                "gpu_1x_test": {
                    "instance_type": {
                        # Missing required fields like "name" and "specs"
                        "description": "test",
                    },
                    "regions_with_capacity_available": [],
                }
            }
        }

        lambda_collector._execute_api_call = AsyncMock(return_value=mock_response)

        with pytest.raises((KeyError, AttributeError)):
            await lambda_collector.fetch_instances()

    def test_missing_gpu_info(self, lambda_collector):
        """Test handling of instance type with missing GPU description."""
        # Test with empty description - function returns "NVIDIA " for empty string
        gpu_name, count = lambda_collector._parse_gpu_name("")
        assert gpu_name == "NVIDIA "
        assert count == 1  # Default count when no "x" found

        # Test with invalid format - function still adds NVIDIA prefix
        gpu_name, count = lambda_collector._parse_gpu_name("Invalid Format")
        assert gpu_name == "NVIDIA Invalid Format"
        assert count == 1  # Default count when no "x" found
