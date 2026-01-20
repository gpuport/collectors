"""Root conftest for pytest configuration.

Provides global fixtures and configuration for both unit and integration tests.
"""

import pytest


def pytest_configure(config: pytest.Config) -> None:
    """Register custom markers for test categorization."""
    config.addinivalue_line(
        "markers",
        "unit: Unit tests (fast, no network, uses mocks)",
    )
    config.addinivalue_line(
        "markers",
        "integration: Integration tests (slow, requires network/APIs)",
    )
    config.addinivalue_line(
        "markers",
        "requires_api: Test requires valid API keys to be set",
    )
    config.addinivalue_line(
        "markers",
        "network: Test requires network access",
    )


@pytest.fixture
def sample_instances():
    """Create sample GPUInstance list for testing.

    This fixture is shared across unit and integration tests.
    """
    from gpuport_collectors.models import AvailabilityStatus, GPUInstance

    return [
        GPUInstance(
            provider="RunPod",
            instance_type="gpu.h100.80gb",
            accelerator_name="H100",
            accelerator_count=8,
            accelerator_mem_gib=80.0,
            region="us-tx-1",
            availability=AvailabilityStatus.HIGH,
            quantity=10,
            price=25.0,
            spot_price=18.0,
            v_cpus=64,
            memory_gib=512.0,
        ),
        GPUInstance(
            provider="Lambda Labs",
            instance_type="gpu_8x_a100",
            accelerator_name="A100",
            accelerator_count=8,
            accelerator_mem_gib=40.0,
            region="us-west-2",
            availability=AvailabilityStatus.MEDIUM,
            quantity=5,
            price=12.50,
            v_cpus=64,
            memory_gib=512.0,
        ),
        GPUInstance(
            provider="RunPod",
            instance_type="gpu.rtx4090",
            accelerator_name="RTX 4090",
            accelerator_count=1,
            accelerator_mem_gib=24.0,
            region="eu-ro-1",
            availability=AvailabilityStatus.LOW,
            quantity=2,
            price=2.50,
            v_cpus=8,
            memory_gib=32.0,
        ),
        GPUInstance(
            provider="Lambda Labs",
            instance_type="gpu_1x_h100",
            accelerator_name="H100",
            accelerator_count=1,
            accelerator_mem_gib=80.0,
            region="us-east-1",
            availability=AvailabilityStatus.HIGH,
            quantity=15,
            price=15.0,
            v_cpus=16,
            memory_gib=128.0,
        ),
    ]
