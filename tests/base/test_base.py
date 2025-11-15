"""Tests for BaseCollector abstract class."""

from unittest.mock import AsyncMock, patch

import pytest

from gpuport_collectors.base import BaseCollector, with_retry
from gpuport_collectors.config import CollectorConfig, HttpClientConfig
from gpuport_collectors.models import AvailabilityStatus, GPUInstance


class TestBaseCollector:
    """Tests for BaseCollector abstract class."""

    def test_cannot_instantiate_directly(self):
        """Test that BaseCollector cannot be instantiated directly."""
        with pytest.raises(TypeError, match="Can't instantiate abstract class"):
            BaseCollector()  # type: ignore[abstract]

    def test_must_implement_provider_name(self):
        """Test that subclasses must implement provider_name property."""

        class IncompleteCollector(BaseCollector):
            async def fetch_instances(self) -> list[GPUInstance]:
                return []

        with pytest.raises(TypeError, match="Can't instantiate abstract class"):
            IncompleteCollector()  # type: ignore[abstract]

    def test_must_implement_fetch_instances(self):
        """Test that subclasses must implement fetch_instances method."""

        class IncompleteCollector(BaseCollector):
            @property
            def provider_name(self) -> str:
                return "TestProvider"

        with pytest.raises(TypeError, match="Can't instantiate abstract class"):
            IncompleteCollector()  # type: ignore[abstract]

    def test_concrete_implementation_with_defaults(self):
        """Test that a complete concrete implementation can be instantiated."""

        class ConcreteCollector(BaseCollector):
            @property
            def provider_name(self) -> str:
                return "TestProvider"

            async def fetch_instances(self) -> list[GPUInstance]:
                return []

        collector = ConcreteCollector()
        assert collector.provider_name == "TestProvider"
        assert collector.config is not None
        assert collector.config.timeout == 30
        assert collector.config.max_retries == 3
        assert collector.config.backoff_factor == 2.0

    def test_concrete_implementation_with_custom_config(self):
        """Test that a collector can be initialized with custom config."""

        class ConcreteCollector(BaseCollector):
            @property
            def provider_name(self) -> str:
                return "TestProvider"

            async def fetch_instances(self) -> list[GPUInstance]:
                return []

        custom_config = CollectorConfig(
            http_client=HttpClientConfig(timeout=60, max_retries=5, backoff_factor=1.5),
        )
        collector = ConcreteCollector(config=custom_config)
        assert collector.provider_name == "TestProvider"
        assert collector.config == custom_config
        assert collector.config.timeout == 60
        assert collector.config.max_retries == 5
        assert collector.config.backoff_factor == 1.5

    async def test_fetch_instances_returns_list(self):
        """Test that fetch_instances returns a list of GPUInstance."""

        class ConcreteCollector(BaseCollector):
            @property
            def provider_name(self) -> str:
                return "TestProvider"

            async def fetch_instances(self) -> list[GPUInstance]:
                return [
                    GPUInstance(
                        provider="TestProvider",
                        instance_type="test-instance",
                        v_cpus=8.0,
                        memory_gib=32.0,
                        accelerator_name="RTX 4090",
                        accelerator_count=1.0,
                        region="US",
                        price=0.79,
                        availability=AvailabilityStatus.HIGH,
                    )
                ]

        collector = ConcreteCollector()
        instances = await collector.fetch_instances()
        assert isinstance(instances, list)
        assert len(instances) == 1
        assert isinstance(instances[0], GPUInstance)
        assert instances[0].provider == "TestProvider"
        assert instances[0].accelerator_name == "RTX 4090"

    async def test_fetch_instances_can_return_empty_list(self):
        """Test that fetch_instances can return empty list when no instances available."""

        class ConcreteCollector(BaseCollector):
            @property
            def provider_name(self) -> str:
                return "TestProvider"

            async def fetch_instances(self) -> list[GPUInstance]:
                return []

        collector = ConcreteCollector()
        instances = await collector.fetch_instances()
        assert isinstance(instances, list)
        assert len(instances) == 0

    async def test_fetch_instances_can_raise_exceptions(self):
        """Test that fetch_instances can raise exceptions for error handling."""

        class FailingCollector(BaseCollector):
            @property
            def provider_name(self) -> str:
                return "TestProvider"

            async def fetch_instances(self) -> list[GPUInstance]:
                msg = "API request failed"
                raise RuntimeError(msg)

        collector = FailingCollector()
        with pytest.raises(RuntimeError, match="API request failed"):
            await collector.fetch_instances()

    async def test_multiple_instances_returned(self):
        """Test that fetch_instances can return multiple instances."""

        class MultiInstanceCollector(BaseCollector):
            @property
            def provider_name(self) -> str:
                return "TestProvider"

            async def fetch_instances(self) -> list[GPUInstance]:
                return [
                    GPUInstance(
                        provider="TestProvider",
                        instance_type="rtx-4090",
                        v_cpus=8.0,
                        memory_gib=32.0,
                        accelerator_name="RTX 4090",
                        accelerator_count=1.0,
                        region="US",
                        price=0.79,
                        availability=AvailabilityStatus.HIGH,
                    ),
                    GPUInstance(
                        provider="TestProvider",
                        instance_type="h100-80gb",
                        v_cpus=16.0,
                        memory_gib=64.0,
                        accelerator_name="H100 80GB",
                        accelerator_count=1.0,
                        region="US",
                        price=2.49,
                        availability=AvailabilityStatus.MEDIUM,
                    ),
                ]

        collector = MultiInstanceCollector()
        instances = await collector.fetch_instances()
        assert len(instances) == 2
        assert instances[0].accelerator_name == "RTX 4090"
        assert instances[1].accelerator_name == "H100 80GB"

    def test_provider_name_is_property(self):
        """Test that provider_name is accessed as a property."""

        class ConcreteCollector(BaseCollector):
            @property
            def provider_name(self) -> str:
                return "TestProvider"

            async def fetch_instances(self) -> list[GPUInstance]:
                return []

        collector = ConcreteCollector()
        # Should be accessed as property, not method
        assert collector.provider_name == "TestProvider"
        # Verify it's a property
        assert isinstance(type(collector).provider_name, property)

    def test_default_config_not_shared_across_instances(self):
        """Test that collectors without explicit config get independent config instances.

        This verifies that mutating one collector's config doesn't affect others.
        """

        class ConcreteCollector(BaseCollector):
            @property
            def provider_name(self) -> str:
                return "TestProvider"

            async def fetch_instances(self) -> list[GPUInstance]:
                return []

        # Create two collectors without providing config
        collector1 = ConcreteCollector()
        collector2 = ConcreteCollector()

        # Store original values
        original_max_retries = collector1.config.max_retries
        original_backoff_factor = collector1.config.backoff_factor

        # Mutate collector1's config (now via http_client)
        collector1.config.http_client.max_retries = 999
        collector1.config.http_client.backoff_factor = 10.0

        # Verify collector2's config is unchanged
        assert collector2.config.max_retries == original_max_retries
        assert collector2.config.backoff_factor == original_backoff_factor

        # Verify the configs are different objects
        assert collector1.config is not collector2.config


class TestRetryDecorator:
    """Tests for with_retry decorator."""

    async def test_successful_call_no_retry(self):
        """Test that successful calls don't trigger retry logic."""

        class SuccessCollector(BaseCollector):
            @property
            def provider_name(self) -> str:
                return "TestProvider"

            @with_retry
            async def fetch_instances(self) -> list[GPUInstance]:
                return [
                    GPUInstance(
                        provider="TestProvider",
                        instance_type="test",
                        v_cpus=8.0,
                        memory_gib=32.0,
                        accelerator_name="RTX 4090",
                        accelerator_count=1.0,
                        region="US",
                        price=0.79,
                        availability=AvailabilityStatus.HIGH,
                    )
                ]

        collector = SuccessCollector()
        instances = await collector.fetch_instances()
        assert len(instances) == 1

    async def test_retry_on_failure(self):
        """Test that decorator retries on failure."""

        class RetryCollector(BaseCollector):
            def __init__(self, config: CollectorConfig | None = None) -> None:
                super().__init__(config)
                self.attempt_count = 0

            @property
            def provider_name(self) -> str:
                return "TestProvider"

            @with_retry
            async def fetch_instances(self) -> list[GPUInstance]:
                self.attempt_count += 1
                if self.attempt_count < 3:
                    msg = "Temporary failure"
                    raise RuntimeError(msg)
                return []

        # Use config with faster retries for testing
        config = CollectorConfig(
            http_client=HttpClientConfig(timeout=30, max_retries=3, backoff_factor=1.1)
        )
        collector = RetryCollector(config=config)

        with patch("asyncio.sleep", new_callable=AsyncMock):
            instances = await collector.fetch_instances()
            assert collector.attempt_count == 3
            assert instances == []

    async def test_max_retries_respected(self):
        """Test that max_retries is respected."""

        class FailingCollector(BaseCollector):
            def __init__(self, config: CollectorConfig | None = None) -> None:
                super().__init__(config)
                self.attempt_count = 0

            @property
            def provider_name(self) -> str:
                return "TestProvider"

            @with_retry
            async def fetch_instances(self) -> list[GPUInstance]:
                self.attempt_count += 1
                msg = "Always fails"
                raise RuntimeError(msg)

        config = CollectorConfig(
            http_client=HttpClientConfig(timeout=30, max_retries=2, backoff_factor=1.1)
        )
        collector = FailingCollector(config=config)

        with patch("asyncio.sleep", new_callable=AsyncMock):
            with pytest.raises(RuntimeError, match="Always fails"):
                await collector.fetch_instances()
            # Should have tried 3 times total (initial + 2 retries)
            assert collector.attempt_count == 3

    async def test_exponential_backoff_delays(self):
        """Test that exponential backoff delays are calculated correctly."""

        class FailingCollector(BaseCollector):
            @property
            def provider_name(self) -> str:
                return "TestProvider"

            @with_retry
            async def fetch_instances(self) -> list[GPUInstance]:
                msg = "Always fails"
                raise RuntimeError(msg)

        config = CollectorConfig(
            http_client=HttpClientConfig(timeout=30, max_retries=3, backoff_factor=2.0)
        )
        collector = FailingCollector(config=config)

        sleep_mock = AsyncMock()
        with patch("asyncio.sleep", sleep_mock):
            with pytest.raises(RuntimeError):
                await collector.fetch_instances()

            # Verify exponential backoff: base_delay * (backoff_factor ** attempt)
            # base_delay = 5
            # Retry 1 (attempt 0): 5 * (2.0 ** 0) = 5.0
            # Retry 2 (attempt 1): 5 * (2.0 ** 1) = 10.0
            # Retry 3 (attempt 2): 5 * (2.0 ** 2) = 20.0
            assert sleep_mock.call_count == 3
            calls = [call.args[0] for call in sleep_mock.call_args_list]
            assert calls[0] == 5.0  # First retry
            assert calls[1] == 10.0  # Second retry
            assert calls[2] == 20.0  # Third retry

    async def test_exponential_backoff_with_factor_5(self):
        """Test exponential backoff with factor 5 (matches issue spec: 5s, 25s, 125s)."""

        class FailingCollector(BaseCollector):
            @property
            def provider_name(self) -> str:
                return "TestProvider"

            @with_retry
            async def fetch_instances(self) -> list[GPUInstance]:
                msg = "Always fails"
                raise RuntimeError(msg)

        config = CollectorConfig(
            http_client=HttpClientConfig(timeout=30, max_retries=3, backoff_factor=5.0)
        )
        collector = FailingCollector(config=config)

        sleep_mock = AsyncMock()
        with patch("asyncio.sleep", sleep_mock):
            with pytest.raises(RuntimeError):
                await collector.fetch_instances()

            # Verify backoff matches issue: 5s, 25s, 125s
            # base_delay = 5
            # Retry 1 (attempt 0): 5 * (5.0 ** 0) = 5.0
            # Retry 2 (attempt 1): 5 * (5.0 ** 1) = 25.0
            # Retry 3 (attempt 2): 5 * (5.0 ** 2) = 125.0
            assert sleep_mock.call_count == 3
            calls = [call.args[0] for call in sleep_mock.call_args_list]
            assert calls[0] == 5.0
            assert calls[1] == 25.0
            assert calls[2] == 125.0

    async def test_exponential_backoff_with_factor_3(self):
        """Test exponential backoff with factor 3 (5s, 15s, 45s)."""

        class FailingCollector(BaseCollector):
            @property
            def provider_name(self) -> str:
                return "TestProvider"

            @with_retry
            async def fetch_instances(self) -> list[GPUInstance]:
                msg = "Always fails"
                raise RuntimeError(msg)

        config = CollectorConfig(
            http_client=HttpClientConfig(timeout=30, max_retries=3, backoff_factor=3.0)
        )
        collector = FailingCollector(config=config)

        sleep_mock = AsyncMock()
        with patch("asyncio.sleep", sleep_mock):
            with pytest.raises(RuntimeError):
                await collector.fetch_instances()

            # Verify backoff: 5s, 15s, 45s
            # base_delay = 5
            # Retry 1 (attempt 0): 5 * (3.0 ** 0) = 5.0
            # Retry 2 (attempt 1): 5 * (3.0 ** 1) = 15.0
            # Retry 3 (attempt 2): 5 * (3.0 ** 2) = 45.0
            assert sleep_mock.call_count == 3
            calls = [call.args[0] for call in sleep_mock.call_args_list]
            assert calls[0] == 5.0
            assert calls[1] == 15.0
            assert calls[2] == 45.0

    async def test_retry_with_different_exception_types(self):
        """Test that retry works with different exception types."""

        class MultiErrorCollector(BaseCollector):
            def __init__(self, config: CollectorConfig | None = None) -> None:
                super().__init__(config)
                self.attempt_count = 0

            @property
            def provider_name(self) -> str:
                return "TestProvider"

            @with_retry
            async def fetch_instances(self) -> list[GPUInstance]:
                self.attempt_count += 1
                if self.attempt_count == 1:
                    msg = "Network error"
                    raise ConnectionError(msg)
                if self.attempt_count == 2:
                    msg = "Timeout error"
                    raise TimeoutError(msg)
                return []

        config = CollectorConfig(
            http_client=HttpClientConfig(timeout=30, max_retries=3, backoff_factor=1.1)
        )
        collector = MultiErrorCollector(config=config)

        with patch("asyncio.sleep", new_callable=AsyncMock):
            instances = await collector.fetch_instances()
            assert collector.attempt_count == 3
            assert instances == []

    async def test_retry_doesnt_retry_on_success_after_failures(self):
        """Test that retry stops after success, not using all retries."""

        class EventualSuccessCollector(BaseCollector):
            def __init__(self, config: CollectorConfig | None = None) -> None:
                super().__init__(config)
                self.attempt_count = 0

            @property
            def provider_name(self) -> str:
                return "TestProvider"

            @with_retry
            async def fetch_instances(self) -> list[GPUInstance]:
                self.attempt_count += 1
                if self.attempt_count < 2:
                    msg = "Temporary failure"
                    raise RuntimeError(msg)
                return []

        config = CollectorConfig(
            http_client=HttpClientConfig(timeout=30, max_retries=5, backoff_factor=1.1)
        )
        collector = EventualSuccessCollector(config=config)

        with patch("asyncio.sleep", new_callable=AsyncMock) as sleep_mock:
            instances = await collector.fetch_instances()
            # Should have stopped after 2 attempts (1 failure, 1 success)
            assert collector.attempt_count == 2
            assert instances == []
            # Should have slept only once (between first failure and second attempt)
            assert sleep_mock.call_count == 1

    async def test_retry_with_zero_max_retries(self):
        """Test that with max_retries=0, no retries are attempted."""

        class FailingCollector(BaseCollector):
            def __init__(self, config: CollectorConfig | None = None) -> None:
                super().__init__(config)
                self.attempt_count = 0

            @property
            def provider_name(self) -> str:
                return "TestProvider"

            @with_retry
            async def fetch_instances(self) -> list[GPUInstance]:
                self.attempt_count += 1
                msg = "Always fails"
                raise RuntimeError(msg)

        config = CollectorConfig(
            http_client=HttpClientConfig(timeout=30, max_retries=0, backoff_factor=2.0)
        )
        collector = FailingCollector(config=config)

        with patch("asyncio.sleep", new_callable=AsyncMock) as sleep_mock:
            with pytest.raises(RuntimeError):
                await collector.fetch_instances()
            # Should have tried only once (no retries)
            assert collector.attempt_count == 1
            # Should never have slept
            assert sleep_mock.call_count == 0
