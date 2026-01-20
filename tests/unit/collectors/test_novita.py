"""Unit tests for NovitaCollector."""

import json
import time
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from gpuport_collectors.collectors.novita import NovitaCollector
from gpuport_collectors.config import CollectorConfig, CollectorsConfig
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


class TestNovitaCollectorInit:
    """Test collector initialization."""

    def test_init_with_api_key(self):
        """Test initialization with API key set."""
        with patch.dict("os.environ", {"NOVITA_API_KEY": "test-key"}):
            config = CollectorConfig()
            collector = NovitaCollector(config)
            assert collector.api_key == "test-key"
            assert collector.provider_name == "Novita AI"

    def test_init_without_api_key(self):
        """Test initialization without API key raises ValueError."""
        with patch.dict("os.environ", {}, clear=True):
            config = CollectorConfig()
            with pytest.raises(ValueError, match="NOVITA_API_KEY"):
                NovitaCollector(config)

    def test_provider_name(self):
        """Test provider_name property."""
        with patch.dict("os.environ", {"NOVITA_API_KEY": "test-key"}):
            config = CollectorConfig()
            collector = NovitaCollector(config)
            assert collector.provider_name == "Novita AI"


class TestNovitaGPUParsing:
    """Test GPU name and count parsing."""

    def test_parse_gpu_name_single(self):
        """Test parsing single GPU name."""
        with patch.dict("os.environ", {"NOVITA_API_KEY": "test-key"}):
            collector = NovitaCollector(CollectorConfig())
            gpu_name, count = collector._parse_gpu_name("H100_80GB", "H100 SXM 80GB")
            # _parse_gpu_name adds NVIDIA prefix if missing
            assert gpu_name == "NVIDIA H100 SXM 80GB"
            assert count == 1

    def test_parse_gpu_name_multi(self):
        """Test parsing multi-GPU name with count."""
        with patch.dict("os.environ", {"NOVITA_API_KEY": "test-key"}):
            collector = NovitaCollector(CollectorConfig())
            gpu_name, count = collector._parse_gpu_name("A100_40GB_8x", "A100 40GB")
            # _parse_gpu_name adds NVIDIA prefix if missing
            assert gpu_name == "NVIDIA A100 40GB"
            assert count == 8

    def test_parse_gpu_name_adds_nvidia_prefix(self):
        """Test that NVIDIA prefix is added if missing."""
        with patch.dict("os.environ", {"NOVITA_API_KEY": "test-key"}):
            collector = NovitaCollector(CollectorConfig())
            gpu_name, count = collector._parse_gpu_name("4090", "RTX 4090")
            assert "NVIDIA" in gpu_name
            assert count == 1

    def test_parse_gpu_name_keeps_nvidia_prefix(self):
        """Test that existing NVIDIA prefix is preserved."""
        with patch.dict("os.environ", {"NOVITA_API_KEY": "test-key"}):
            collector = NovitaCollector(CollectorConfig())
            gpu_name, count = collector._parse_gpu_name("H100", "NVIDIA H100")
            assert gpu_name == "NVIDIA H100"
            assert count == 1

    def test_extract_gpu_memory_from_id(self):
        """Test extracting GPU memory from product ID."""
        with patch.dict("os.environ", {"NOVITA_API_KEY": "test-key"}):
            collector = NovitaCollector(CollectorConfig())
            memory = collector._extract_gpu_memory("H100_80GB", "H100 SXM")
            assert memory == 80

    def test_extract_gpu_memory_from_name(self):
        """Test extracting GPU memory from product name."""
        with patch.dict("os.environ", {"NOVITA_API_KEY": "test-key"}):
            collector = NovitaCollector(CollectorConfig())
            memory = collector._extract_gpu_memory("4090", "RTX 4090 24GB")
            assert memory == 24

    def test_extract_gpu_memory_not_found(self):
        """Test GPU memory extraction returns None when not found."""
        with patch.dict("os.environ", {"NOVITA_API_KEY": "test-key"}):
            collector = NovitaCollector(CollectorConfig())
            memory = collector._extract_gpu_memory("unknown", "Some GPU")
            assert memory is None


class TestNovitaAvailabilityMapping:
    """Test availability status mapping."""

    def test_map_availability_true(self):
        """Test mapping available_deploy=True to HIGH."""
        with patch.dict("os.environ", {"NOVITA_API_KEY": "test-key"}):
            collector = NovitaCollector(CollectorConfig())
            status = collector._map_availability(True)
            assert status == AvailabilityStatus.HIGH

    def test_map_availability_false(self):
        """Test mapping available_deploy=False to NOT_AVAILABLE."""
        with patch.dict("os.environ", {"NOVITA_API_KEY": "test-key"}):
            collector = NovitaCollector(CollectorConfig())
            status = collector._map_availability(False)
            assert status == AvailabilityStatus.NOT_AVAILABLE


class TestNovitaInstanceCreation:
    """Test GPUInstance creation from product data."""

    def test_create_gpu_instance_basic(self, current_timestamp):
        """Test creating basic GPU instance."""
        with patch.dict("os.environ", {"NOVITA_API_KEY": "test-key"}):
            collector = NovitaCollector(CollectorConfig())

            # Mock GPUProduct
            mock_product = MagicMock()
            mock_product.id = "H100-80GB.16c128g"
            mock_product.name = "H100 SXM 80GB"
            mock_product.cpu_per_gpu = 16
            mock_product.memory_per_gpu = 128
            mock_product.available_deploy = True
            mock_product.price = 80000  # 80000 / 100000 = $0.80/hour
            mock_product.model_dump = MagicMock(
                return_value={
                    "id": "H100-80GB.16c128g",
                    "name": "H100 SXM 80GB",
                    "price": 80000,
                }
            )

            instance = collector._create_gpu_instance(mock_product, "us-west-1", current_timestamp)

            assert instance.provider == "Novita AI"
            assert instance.instance_type == "H100-80GB.16c128g"
            assert "H100" in instance.accelerator_name
            assert instance.region == "us-west-1"
            assert instance.price == 0.8  # 80000 / 100000 = $0.80/hour
            assert instance.spot_price is None
            assert instance.availability == AvailabilityStatus.HIGH
            assert instance.v_cpus == 16
            assert instance.memory_gib == 128

    def test_create_gpu_instance_unavailable(self, current_timestamp):
        """Test creating instance for unavailable product."""
        with patch.dict("os.environ", {"NOVITA_API_KEY": "test-key"}):
            collector = NovitaCollector(CollectorConfig())

            mock_product = MagicMock()
            mock_product.id = "4090"
            mock_product.name = "RTX 4090"
            mock_product.cpu_per_gpu = 16
            mock_product.memory_per_gpu = 62
            mock_product.available_deploy = False
            mock_product.price = 35000
            mock_product.model_dump = MagicMock(return_value={})

            instance = collector._create_gpu_instance(mock_product, "us-east-1", current_timestamp)

            assert instance.availability == AvailabilityStatus.NOT_AVAILABLE

    def test_create_gpu_instance_multi_gpu(self, current_timestamp):
        """Test creating instance with multiple GPUs."""
        with patch.dict("os.environ", {"NOVITA_API_KEY": "test-key"}):
            collector = NovitaCollector(CollectorConfig())

            mock_product = MagicMock()
            mock_product.id = "A100_8x"
            mock_product.name = "A100 80GB"
            mock_product.cpu_per_gpu = 96
            mock_product.memory_per_gpu = 768
            mock_product.available_deploy = True
            mock_product.price = 60000  # $600/hour for 8x A100
            mock_product.model_dump = MagicMock(return_value={})

            instance = collector._create_gpu_instance(mock_product, "eu-west-1", current_timestamp)

            assert instance.accelerator_count == 8

    def test_create_gpu_instance_price_conversion(self, current_timestamp):
        """Test that price is correctly converted from Novita price units to dollars."""
        with patch.dict("os.environ", {"NOVITA_API_KEY": "test-key"}):
            collector = NovitaCollector(CollectorConfig())

            mock_product = MagicMock()
            mock_product.id = "test"
            mock_product.name = "Test GPU"
            mock_product.cpu_per_gpu = 8
            mock_product.memory_per_gpu = 32
            mock_product.available_deploy = True
            mock_product.price = 12345000  # 12345000 / 100000 = $123.45/hour
            mock_product.model_dump = MagicMock(return_value={})

            instance = collector._create_gpu_instance(
                mock_product, "test-region", current_timestamp
            )

            assert instance.price == 123.45

    def test_create_gpu_instance_raw_data_preservation(self, current_timestamp):
        """Test that raw product data is preserved."""
        with patch.dict("os.environ", {"NOVITA_API_KEY": "test-key"}):
            collector = NovitaCollector(CollectorConfig())

            mock_product = MagicMock()
            mock_product.id = "test"
            mock_product.name = "Test"
            mock_product.cpu_per_gpu = 1
            mock_product.memory_per_gpu = 1
            mock_product.available_deploy = True
            mock_product.price = 100
            raw_data = {"id": "test", "custom_field": "value"}
            mock_product.model_dump = MagicMock(return_value=raw_data)

            instance = collector._create_gpu_instance(mock_product, "test", current_timestamp)

            assert instance.raw_data == raw_data


class TestNovitaFetchInstances:
    """Test end-to-end instance fetching."""

    @pytest.mark.asyncio
    async def test_fetch_instances_success(self):
        """Test successful instance fetching."""
        with patch.dict("os.environ", {"NOVITA_API_KEY": "test-key"}):
            collector = NovitaCollector(CollectorConfig())

            # Mock product
            mock_product = MagicMock()
            mock_product.id = "4090"
            mock_product.name = "RTX 4090 24GB"
            mock_product.cpu_per_gpu = 16
            mock_product.memory_per_gpu = 125
            mock_product.available_deploy = True
            mock_product.price = 35000
            mock_product.regions = ["US-CA-06 (California)", "EU-GER-02 (Germany)"]
            mock_product.model_dump = MagicMock(return_value={})

            # Mock SDK at the import location where it's used
            with patch("gpuport_collectors.collectors.novita.AsyncNovitaClient") as mock_client:
                mock_client_instance = AsyncMock()
                # SDK returns a list directly, not an object with .data
                mock_client_instance.gpu.products.list = AsyncMock(return_value=[mock_product])
                mock_client.return_value.__aenter__.return_value = mock_client_instance
                mock_client.return_value.__aexit__.return_value = AsyncMock()

                instances = await collector.fetch_instances()

                # Should create one instance per region
                assert len(instances) == 2
                assert all(isinstance(i, GPUInstance) for i in instances)
                assert instances[0].provider == "Novita AI"
                regions = {i.region for i in instances}
                assert regions == {"US-CA-06 (California)", "EU-GER-02 (Germany)"}

    @pytest.mark.asyncio
    async def test_fetch_instances_multiple_products(self):
        """Test fetching with multiple products."""
        with patch.dict("os.environ", {"NOVITA_API_KEY": "test-key"}):
            collector = NovitaCollector(CollectorConfig())

            # Mock multiple products
            products = []
            for i in range(3):
                mock_product = MagicMock()
                mock_product.id = f"product-{i}"
                mock_product.name = f"GPU {i}"
                mock_product.cpu_per_gpu = 16
                mock_product.memory_per_gpu = 64
                mock_product.available_deploy = True
                mock_product.price = 10000
                mock_product.regions = [f"region-{i}"]
                mock_product.model_dump = MagicMock(return_value={})
                products.append(mock_product)

            # Mock SDK to return list directly
            with patch("gpuport_collectors.collectors.novita.AsyncNovitaClient") as mock_client:
                mock_client_instance = AsyncMock()
                mock_client_instance.gpu.products.list = AsyncMock(return_value=products)
                mock_client.return_value.__aenter__.return_value = mock_client_instance
                mock_client.return_value.__aexit__.return_value = AsyncMock()

                instances = await collector.fetch_instances()

                assert len(instances) == 3

    @pytest.mark.asyncio
    async def test_fetch_instances_filters_unavailable(self):
        """Test that unavailable products are filtered by default."""
        with patch.dict("os.environ", {"NOVITA_API_KEY": "test-key"}):
            collector = NovitaCollector(CollectorConfig())

            # Mix of available and unavailable
            available_product = MagicMock()
            available_product.id = "available"
            available_product.name = "Available GPU"
            available_product.cpu_per_gpu = 16
            available_product.memory_per_gpu = 64
            available_product.available_deploy = True
            available_product.price = 10000
            available_product.regions = ["region-1"]
            available_product.model_dump = MagicMock(return_value={})

            unavailable_product = MagicMock()
            unavailable_product.id = "unavailable"
            unavailable_product.name = "Unavailable GPU"
            unavailable_product.cpu_per_gpu = 16
            unavailable_product.memory_per_gpu = 64
            unavailable_product.available_deploy = False
            unavailable_product.price = 10000
            unavailable_product.regions = ["region-2"]
            unavailable_product.model_dump = MagicMock(return_value={})

            # Mock SDK to return list directly
            with patch("gpuport_collectors.collectors.novita.AsyncNovitaClient") as mock_client:
                mock_client_instance = AsyncMock()
                mock_client_instance.gpu.products.list = AsyncMock(
                    return_value=[available_product, unavailable_product]
                )
                mock_client.return_value.__aenter__.return_value = mock_client_instance
                mock_client.return_value.__aexit__.return_value = AsyncMock()

                instances = await collector.fetch_instances()

                assert len(instances) == 1
                assert instances[0].availability != AvailabilityStatus.NOT_AVAILABLE

    @pytest.mark.asyncio
    async def test_fetch_instances_include_unavailable(self):
        """Test that unavailable products are returned when include_unavailable is enabled."""
        with patch.dict("os.environ", {"NOVITA_API_KEY": "test-key"}):
            collector = NovitaCollector(
                CollectorConfig(collectors=CollectorsConfig(include_unavailable=True))
            )

            available_product = MagicMock()
            available_product.id = "available"
            available_product.name = "Available GPU"
            available_product.cpu_per_gpu = 16
            available_product.memory_per_gpu = 64
            available_product.available_deploy = True
            available_product.price = 10000
            available_product.regions = ["region-1"]
            available_product.model_dump = MagicMock(return_value={})

            unavailable_product = MagicMock()
            unavailable_product.id = "unavailable"
            unavailable_product.name = "Unavailable GPU"
            unavailable_product.cpu_per_gpu = 16
            unavailable_product.memory_per_gpu = 64
            unavailable_product.available_deploy = False
            unavailable_product.price = 10000
            unavailable_product.regions = ["region-2"]
            unavailable_product.model_dump = MagicMock(return_value={})

            with patch("gpuport_collectors.collectors.novita.AsyncNovitaClient") as mock_client:
                mock_client_instance = AsyncMock()
                mock_client_instance.gpu.products.list = AsyncMock(
                    return_value=[available_product, unavailable_product]
                )
                mock_client.return_value.__aenter__.return_value = mock_client_instance
                mock_client.return_value.__aexit__.return_value = AsyncMock()

                instances = await collector.fetch_instances()

                assert len(instances) == 2
                assert any(
                    instance.availability == AvailabilityStatus.NOT_AVAILABLE
                    for instance in instances
                )

    @pytest.mark.asyncio
    async def test_fetch_instances_empty_response(self):
        """Test fetching with empty products list."""
        with patch.dict("os.environ", {"NOVITA_API_KEY": "test-key"}):
            collector = NovitaCollector(CollectorConfig())

            # Mock SDK to return empty list directly
            with patch("gpuport_collectors.collectors.novita.AsyncNovitaClient") as mock_client:
                mock_client_instance = AsyncMock()
                mock_client_instance.gpu.products.list = AsyncMock(return_value=[])
                mock_client.return_value.__aenter__.return_value = mock_client_instance
                mock_client.return_value.__aexit__.return_value = AsyncMock()

                instances = await collector.fetch_instances()

                assert len(instances) == 0

    @pytest.mark.asyncio
    async def test_fetch_instances_error_handling(self):
        """Test error handling during fetch."""
        with patch.dict("os.environ", {"NOVITA_API_KEY": "test-key"}):
            collector = NovitaCollector(CollectorConfig())

            with patch("gpuport_collectors.collectors.novita.AsyncNovitaClient") as mock_client:
                mock_client_instance = AsyncMock()
                mock_client_instance.gpu.products.list = AsyncMock(
                    side_effect=Exception("API Error")
                )
                mock_client.return_value.__aenter__.return_value = mock_client_instance

                with pytest.raises(Exception, match="API Error"):
                    await collector.fetch_instances()


class TestNovitaFixtureData:
    """Test with real fixture data."""

    @pytest.mark.asyncio
    async def test_parse_fixture_data(self):
        """Test parsing actual fixture data."""
        with patch.dict("os.environ", {"NOVITA_API_KEY": "test-key"}):
            collector = NovitaCollector(CollectorConfig())

            # Load fixture
            fixtures_path = Path(__file__).parent.parent.parent / "fixtures"
            fixture_file = fixtures_path / "mock_responses" / "novita.json"

            fixture_data = json.loads(fixture_file.read_text())

            # Convert fixture data to mock products
            mock_products = []
            for product_data in fixture_data["products"]:
                mock_product = MagicMock()
                for key, value in product_data.items():
                    setattr(mock_product, key, value)
                mock_product.model_dump = MagicMock(return_value=product_data)
                mock_products.append(mock_product)

            # Mock SDK to return list directly
            with patch("gpuport_collectors.collectors.novita.AsyncNovitaClient") as mock_client:
                mock_client_instance = AsyncMock()
                mock_client_instance.gpu.products.list = AsyncMock(return_value=mock_products)
                mock_client.return_value.__aenter__.return_value = mock_client_instance
                mock_client.return_value.__aexit__.return_value = AsyncMock()

                instances = await collector.fetch_instances()

                # Validate instances were created
                assert len(instances) > 0

                # Validate structure of first instance
                instance = instances[0]
                assert instance.provider == "Novita AI"
                assert instance.instance_type is not None
                assert instance.accelerator_name is not None
                assert instance.accelerator_count > 0
                assert instance.region is not None
                assert instance.price >= 0
                assert instance.availability in [
                    AvailabilityStatus.HIGH,
                    AvailabilityStatus.NOT_AVAILABLE,
                ]

    @pytest.mark.asyncio
    async def test_fixture_schema_valid(self):
        """Test that fixture data matches expected schema."""
        fixtures_path = Path(__file__).parent.parent.parent / "fixtures"
        fixture_file = fixtures_path / "mock_responses" / "novita.json"

        data = json.loads(fixture_file.read_text())

        # Validate top-level structure
        assert "products" in data
        assert isinstance(data["products"], list)

        # Validate product structure
        if data["products"]:
            product = data["products"][0]
            assert "id" in product
            assert "name" in product
            assert "cpu_per_gpu" in product
            assert "memory_per_gpu" in product
            assert "available_deploy" in product
            assert "price" in product
            assert "regions" in product
            assert isinstance(product["regions"], list)


class TestNovitaErrorHandling:
    """Test error scenarios and edge cases."""

    @pytest.mark.asyncio
    async def test_sdk_timeout(self):
        """Test handling of SDK timeout."""
        with patch.dict("os.environ", {"NOVITA_API_KEY": "test-key"}):
            collector = NovitaCollector(CollectorConfig())

            with patch("gpuport_collectors.collectors.novita.AsyncNovitaClient") as mock_client:
                mock_client_instance = AsyncMock()
                mock_client_instance.gpu.products.list = AsyncMock(
                    side_effect=TimeoutError("Request timeout")
                )
                mock_client.return_value.__aenter__.return_value = mock_client_instance

                with pytest.raises(TimeoutError):
                    await collector.fetch_instances()

    @pytest.mark.asyncio
    async def test_sdk_authentication_error(self):
        """Test handling of authentication errors."""
        with patch.dict("os.environ", {"NOVITA_API_KEY": "invalid-key"}):
            collector = NovitaCollector(CollectorConfig())

            with patch("gpuport_collectors.collectors.novita.AsyncNovitaClient") as mock_client:
                mock_client_instance = AsyncMock()
                mock_client_instance.gpu.products.list = AsyncMock(
                    side_effect=Exception("Authentication failed")
                )
                mock_client.return_value.__aenter__.return_value = mock_client_instance

                with pytest.raises(Exception, match="Authentication failed"):
                    await collector.fetch_instances()

    def test_price_none_handling(self, current_timestamp):
        """Test handling of product with None price."""
        with patch.dict("os.environ", {"NOVITA_API_KEY": "test-key"}):
            collector = NovitaCollector(CollectorConfig())

            mock_product = MagicMock()
            mock_product.id = "test"
            mock_product.name = "Test"
            mock_product.cpu_per_gpu = 1
            mock_product.memory_per_gpu = 1
            mock_product.available_deploy = True
            mock_product.price = None
            mock_product.model_dump = MagicMock(return_value={})

            instance = collector._create_gpu_instance(mock_product, "test", current_timestamp)

            # Should default to 0.0
            assert instance.price == 0.0

    @pytest.mark.asyncio
    async def test_product_with_no_regions(self):
        """Test handling of product with empty regions list."""
        with patch.dict("os.environ", {"NOVITA_API_KEY": "test-key"}):
            collector = NovitaCollector(CollectorConfig())

            mock_product = MagicMock()
            mock_product.id = "test"
            mock_product.name = "Test GPU"
            mock_product.cpu_per_gpu = 16
            mock_product.memory_per_gpu = 64
            mock_product.available_deploy = True
            mock_product.price = 10000
            mock_product.regions = []  # Empty regions
            mock_product.model_dump = MagicMock(return_value={})

            # Mock SDK to return list directly
            with patch("gpuport_collectors.collectors.novita.AsyncNovitaClient") as mock_client:
                mock_client_instance = AsyncMock()
                mock_client_instance.gpu.products.list = AsyncMock(return_value=[mock_product])
                mock_client.return_value.__aenter__.return_value = mock_client_instance
                mock_client.return_value.__aexit__.return_value = AsyncMock()

                instances = await collector.fetch_instances()

                # Should create no instances if no regions
                assert len(instances) == 0
