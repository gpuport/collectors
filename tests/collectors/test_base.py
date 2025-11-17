"""Tests for base collector functionality."""

import pytest

from gpuport_collectors.base import BaseCollector, with_retry
from gpuport_collectors.config import CollectorConfig, HttpClientConfig
from gpuport_collectors.models import AvailabilityStatus, GPUInstance


def _make_test_instance() -> GPUInstance:
    """Create a test GPU instance for testing retry behavior."""
    return GPUInstance(
        provider="test",
        instance_type="test-instance",
        accelerator_name="Test GPU",
        accelerator_count=1,
        accelerator_mem_gib=16,
        v_cpus=8,
        memory_gib=32,
        price=1.0,
        availability=AvailabilityStatus.HIGH,
        region="test-region",
    )


class TestWithRetryDecorator:
    """Tests for the with_retry decorator."""

    @pytest.mark.asyncio
    async def test_retry_success_on_first_attempt(self):
        """Test successful execution on first attempt."""
        config = CollectorConfig(http_client=HttpClientConfig(max_retries=2, base_delay=0.01))

        class TestCollector(BaseCollector):
            @property
            def provider_name(self) -> str:
                return "test"

            def __init__(self, config: CollectorConfig) -> None:
                super().__init__(config)
                self.call_count = 0

            @with_retry
            async def fetch_instances(self) -> list[GPUInstance]:
                self.call_count += 1
                return [_make_test_instance()]

        collector = TestCollector(config)
        result = await collector.fetch_instances()

        assert len(result) == 1
        assert result[0].provider == "test"
        assert collector.call_count == 1

    @pytest.mark.asyncio
    async def test_retry_success_after_failures(self):
        """Test successful execution after transient failures."""
        config = CollectorConfig(http_client=HttpClientConfig(max_retries=2, base_delay=0.01))

        class TestCollector(BaseCollector):
            @property
            def provider_name(self) -> str:
                return "test"

            def __init__(self, config: CollectorConfig) -> None:
                super().__init__(config)
                self.call_count = 0

            @with_retry
            async def fetch_instances(self) -> list[GPUInstance]:
                self.call_count += 1
                if self.call_count < 3:
                    raise RuntimeError(f"Attempt {self.call_count} failed")
                return [_make_test_instance()]

        collector = TestCollector(config)
        result = await collector.fetch_instances()

        assert len(result) == 1
        assert result[0].provider == "test"
        assert collector.call_count == 3

    @pytest.mark.asyncio
    async def test_retry_exhaustion(self):
        """Test retry exhaustion after max attempts."""
        config = CollectorConfig(http_client=HttpClientConfig(max_retries=2, base_delay=0.01))

        class TestCollector(BaseCollector):
            @property
            def provider_name(self) -> str:
                return "test"

            def __init__(self, config: CollectorConfig) -> None:
                super().__init__(config)
                self.call_count = 0

            @with_retry
            async def fetch_instances(self) -> list[GPUInstance]:
                self.call_count += 1
                raise RuntimeError("Always fails")

        collector = TestCollector(config)

        with pytest.raises(RuntimeError, match="Always fails"):
            await collector.fetch_instances()

        # max_retries=2 means 3 total attempts (initial + 2 retries)
        assert collector.call_count == 3

    @pytest.mark.asyncio
    async def test_retry_respects_exponential_backoff(self):
        """Test that retry uses exponential backoff."""
        import time

        config = CollectorConfig(
            http_client=HttpClientConfig(max_retries=2, base_delay=0.1, backoff_factor=2)
        )

        class TestCollector(BaseCollector):
            @property
            def provider_name(self) -> str:
                return "test"

            def __init__(self, config: CollectorConfig) -> None:
                super().__init__(config)
                self.call_times: list[float] = []

            @with_retry
            async def fetch_instances(self) -> list[GPUInstance]:
                self.call_times.append(time.time())
                raise RuntimeError("Test error")

        collector = TestCollector(config)

        with pytest.raises(RuntimeError):
            await collector.fetch_instances()

        # Check exponential backoff
        # First retry: ~0.1s (base_delay * 2^0)
        # Second retry: ~0.2s (base_delay * 2^1)
        assert len(collector.call_times) == 3
        delay1 = collector.call_times[1] - collector.call_times[0]
        delay2 = collector.call_times[2] - collector.call_times[1]

        # Allow some tolerance for timing
        assert delay1 >= 0.09  # ~0.1s
        assert delay2 >= 0.18  # ~0.2s


class TestBaseCollector:
    """Tests for BaseCollector initialization and configuration."""

    def test_base_collector_initialization(self):
        """Test base collector initializes with config."""
        config = CollectorConfig(http_client=HttpClientConfig(timeout=30, max_retries=5))

        class TestCollector(BaseCollector):
            @property
            def provider_name(self) -> str:
                return "test"

            async def fetch_instances(self) -> list[GPUInstance]:
                return []

        collector = TestCollector(config=config)

        assert collector.config == config
        assert collector.config.timeout == 30.0
        assert collector.config.max_retries == 5

    def test_base_collector_default_config(self):
        """Test base collector uses default config if none provided."""

        class TestCollector(BaseCollector):
            @property
            def provider_name(self) -> str:
                return "test"

            async def fetch_instances(self) -> list[GPUInstance]:
                return []

        collector = TestCollector()

        assert collector.config is not None
        assert isinstance(collector.config, CollectorConfig)

    def test_base_collector_logger_initialization(self):
        """Test base collector initializes logger."""

        class TestCollector(BaseCollector):
            @property
            def provider_name(self) -> str:
                return "test"

            async def fetch_instances(self) -> list[GPUInstance]:
                return []

        collector = TestCollector()

        # Logger should be initialized (as private _logger)
        assert hasattr(collector, "_logger")
        assert collector._logger is not None

    @pytest.mark.asyncio
    async def test_base_collector_fetch_instances_abstract(self):
        """Test that BaseCollector is abstract and cannot be instantiated."""
        with pytest.raises(TypeError, match="abstract"):
            BaseCollector()  # type: ignore[abstract]

    def test_base_collector_with_custom_timeout(self):
        """Test base collector with custom timeout configuration."""
        config = CollectorConfig(http_client=HttpClientConfig(timeout=60))

        class TestCollector(BaseCollector):
            @property
            def provider_name(self) -> str:
                return "test"

            async def fetch_instances(self) -> list[GPUInstance]:
                return []

        collector = TestCollector(config=config)

        assert collector.config.timeout == 60

    def test_base_collector_with_custom_retries(self):
        """Test base collector with custom retry configuration."""
        config = CollectorConfig(http_client=HttpClientConfig(max_retries=10))

        class TestCollector(BaseCollector):
            @property
            def provider_name(self) -> str:
                return "test"

            async def fetch_instances(self) -> list[GPUInstance]:
                return []

        collector = TestCollector(config=config)

        assert collector.config.max_retries == 10
