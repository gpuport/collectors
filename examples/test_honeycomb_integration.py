"""Test script to verify Honeycomb integration.

This script creates a mock collector and performs some operations to generate
logs and traces that will be sent to Honeycomb.io.
"""

import asyncio
import time

from gpuport_collectors.base import BaseCollector, with_retry
from gpuport_collectors.config import CollectorConfig, ObservabilityConfig
from gpuport_collectors.models import AvailabilityStatus, GPUInstance


class TestCollector(BaseCollector):
    """Test collector for demonstrating Honeycomb integration."""

    @property
    def provider_name(self) -> str:
        """Return the test provider name."""
        return "TestProvider"

    @with_retry
    async def fetch_instances(self) -> list[GPUInstance]:
        """Fetch some test GPU instances."""
        # Simulate some work
        await asyncio.sleep(0.5)

        # Create and return test instances
        return [
            GPUInstance(
                provider="TestProvider",
                instance_type="test-a100-instance",
                accelerator_name="NVIDIA A100",
                region="us-east-1",
                price=2.50,
                quantity=10,
                v_cpus=8,
                memory_gib=64,
                accelerator_count=1,
                accelerator_mem_gib=40,
                availability=AvailabilityStatus.HIGH,
                collected_at=int(time.time()),
            ),
            GPUInstance(
                provider="TestProvider",
                instance_type="test-h100-instance",
                accelerator_name="NVIDIA H100",
                region="us-west-2",
                price=4.50,
                quantity=5,
                v_cpus=16,
                memory_gib=128,
                accelerator_count=1,
                accelerator_mem_gib=80,
                availability=AvailabilityStatus.HIGH,
                collected_at=int(time.time()),
            ),
        ]


class FailingCollector(BaseCollector):
    """Collector that fails to test error logging and retries."""

    def __init__(self, config: CollectorConfig) -> None:
        """Initialize the failing collector."""
        super().__init__(config)
        self.attempt_count = 0

    @property
    def provider_name(self) -> str:
        """Return the test provider name."""
        return "FailingProvider"

    @with_retry
    async def fetch_instances(self) -> list[GPUInstance]:
        """Fail a few times before succeeding."""
        self.attempt_count += 1

        if self.attempt_count < 3:
            # Simulate different types of errors
            if self.attempt_count == 1:
                raise TimeoutError("Connection timeout to provider API")
            raise ValueError("Invalid response from provider API")

        # Third attempt succeeds
        return [
            GPUInstance(
                provider="FailingProvider",
                instance_type="test-v100-instance",
                accelerator_name="NVIDIA V100",
                region="eu-west-1",
                price=1.50,
                quantity=3,
                v_cpus=4,
                memory_gib=32,
                accelerator_count=1,
                accelerator_mem_gib=16,
                availability=AvailabilityStatus.HIGH,
                collected_at=int(time.time()),
            ),
        ]


async def main() -> None:
    """Run the test collectors to generate logs and traces."""
    print("🚀 Testing Honeycomb Integration\n")

    # Create configuration with observability enabled
    config = CollectorConfig(
        timeout=30,
        max_retries=3,
        backoff_factor=2.0,
        base_delay=2.0,  # Shorter delays for testing
        observability=ObservabilityConfig(
            enabled=True,
            honeycomb_api_key="lrdZql5zE5cqiIrpPzoFKD",
            service_name="gpuport-collectors-test",
            environment="test",
            log_level="INFO",
            exporter_endpoint="https://api.eu1.honeycomb.io:443",
        ),
    )

    print("📊 Configuration:")
    print(f"  • Service: {config.observability.service_name}")
    print(f"  • Environment: {config.observability.environment}")
    print(f"  • Log Level: {config.observability.log_level}")
    print(f"  • Observability: {'✅ Enabled' if config.observability.enabled else '❌ Disabled'}")
    print()

    # Test 1: Successful collection
    print("🔍 Test 1: Successful Collection")
    print("-" * 50)
    test_collector = TestCollector(config)
    instances = await test_collector.fetch_instances_with_tracing()
    print(f"✅ Successfully fetched {len(instances)} instances")
    for instance in instances:
        print(f"  • {instance.accelerator_name} in {instance.region}: ${instance.price}/hr")
    print()

    # Test 2: Collection with retries
    print("🔍 Test 2: Collection with Retries (Error Handling)")
    print("-" * 50)
    failing_collector = FailingCollector(config)
    try:
        instances = await failing_collector.fetch_instances_with_tracing()
        print(f"✅ Eventually succeeded after {failing_collector.attempt_count} attempts")
        print(f"✅ Fetched {len(instances)} instances")
        for instance in instances:
            print(f"  • {instance.accelerator_name} in {instance.region}: ${instance.price}/hr")
    except Exception as e:
        print(f"❌ Failed: {e}")
    print()

    # Test 3: Custom logging
    print("🔍 Test 3: Custom Structured Logging")
    print("-" * 50)
    from gpuport_collectors.observability import get_observability_manager

    obs_manager = get_observability_manager()
    logger = obs_manager.get_logger("test_script")

    logger.info(
        "Testing custom log message",
        provider_name="CustomProvider",
        operation="test",
        count=42,
    )
    print("✅ Sent custom log message")

    logger.warning(
        "Testing warning message",
        provider_name="CustomProvider",
        remaining_quota=10,
    )
    print("✅ Sent warning message")

    # Simulate an error
    try:
        raise RuntimeError("This is a test error for demonstration")
    except RuntimeError as e:
        logger.error(
            "Demonstrating error logging",
            error=e,
            provider_name="CustomProvider",
            context="test_integration",
        )
        print("✅ Sent error log with stack trace")
    print()

    # Test 4: Custom tracing
    print("🔍 Test 4: Custom Distributed Tracing")
    print("-" * 50)
    with obs_manager.trace_operation(
        "custom_data_processing",
        provider="TestProvider",
        record_count=100,
        processing_type="batch",
    ):
        # Simulate some processing work
        await asyncio.sleep(0.3)
        print("✅ Completed traced operation")
    print()

    # Give time for telemetry to be sent
    print("⏳ Waiting for telemetry to be sent to Honeycomb...")
    await asyncio.sleep(3)

    # Shutdown observability to flush remaining spans
    obs_manager.shutdown()

    print("\n" + "=" * 50)
    print("✨ Test Complete!")
    print("=" * 50)
    print("\n📈 View your data in Honeycomb:")
    print("   https://ui.honeycomb.io/")
    print("\n🔍 What to look for:")
    print("   • Service: gpuport-collectors-test")
    print("   • Environment: test")
    print("   • Traces for:")
    print("     - fetch_instances operations")
    print("     - custom_data_processing operation")
    print("   • Logs showing:")
    print("     - Successful fetches")
    print("     - Retry attempts with errors")
    print("     - Custom structured logs")
    print("     - Error with full stack trace")
    print()


if __name__ == "__main__":
    asyncio.run(main())
