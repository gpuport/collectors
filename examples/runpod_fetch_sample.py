"""Fetch sample RunPod API data and save to fixtures.

This script fetches real data from RunPod API once and saves it for testing.
Run this manually when you need to update test fixtures with fresh data.

Usage:
    export RUNPOD_API_KEY="your-api-key"
    python examples/runpod_fetch_sample.py
"""

import asyncio
import json
from pathlib import Path

from gpuport_collectors.collectors.runpod import RunPodCollector
from gpuport_collectors.config import CollectorConfig, ObservabilityConfig
from gpuport_collectors.models import AvailabilityStatus


async def main() -> None:
    """Fetch sample data and save to fixtures."""
    # Create collector with observability disabled for simple output
    config = CollectorConfig(
        timeout=30,
        max_retries=3,
        observability=ObservabilityConfig(enabled=False),
    )

    collector = RunPodCollector(config)

    print("Fetching RunPod GPU data...")
    print(f"  Endpoint: {collector.GRAPHQL_ENDPOINT}")
    print()

    # Fetch data
    instances = await collector.fetch_instances()

    print(f"✅ Fetched {len(instances)} GPU instances")
    print()

    # Show some statistics
    available = [i for i in instances if i.availability != AvailabilityStatus.NOT_AVAILABLE]
    unavailable = [i for i in instances if i.availability == AvailabilityStatus.NOT_AVAILABLE]

    print(f"  Available instances: {len(available)}")
    print(f"  Unavailable instances: {len(unavailable)}")
    print()

    # Get unique GPU types and regions
    gpu_types = {i.accelerator_name for i in instances}
    regions = {i.region for i in instances}

    print(f"  GPU types: {len(gpu_types)}")
    print(f"  Regions: {len(regions)}")
    print()

    # Sample some GPUs by type
    print("Sample GPUs by availability:")
    for gpu_type in sorted(gpu_types)[:3]:  # Show first 3 types
        gpu_instances = [i for i in instances if i.accelerator_name == gpu_type]
        available_count = len(
            [i for i in gpu_instances if i.availability != AvailabilityStatus.NOT_AVAILABLE]
        )
        print(f"  {gpu_type}: {available_count}/{len(gpu_instances)} regions available")

    print()

    # Convert instances to JSON-serializable format using Pydantic's model_dump
    instances_data = [i.model_dump() for i in instances]

    # Save to fixtures (synchronously - this is a one-time utility script)
    fixtures_dir = Path(__file__).parent.parent / "fixtures"
    fixtures_dir.mkdir(exist_ok=True)

    output_file = fixtures_dir / "runpod_sample.json"
    output_file.write_text(json.dumps(instances_data, indent=2))

    print(f"💾 Saved to: {output_file}")
    print()
    print("✨ Sample data ready for testing!")


if __name__ == "__main__":
    asyncio.run(main())
