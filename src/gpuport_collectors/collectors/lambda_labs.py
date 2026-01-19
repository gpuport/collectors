"""Lambda Labs GPU collector using REST API."""

import os
import re
import time
from typing import Any

import aiohttp

from gpuport_collectors.base import BaseCollector, with_retry
from gpuport_collectors.config import CollectorConfig
from gpuport_collectors.models import AvailabilityStatus, GPUInstance


class LambdaLabsCollector(BaseCollector):
    """Collector for Lambda Labs GPU availability data.

    Uses Lambda Labs' REST API to fetch GPU availability:
    1. Single API call to /instance-types endpoint
    2. Response includes all GPU types with pricing and regional availability
    3. Create GPUInstance for each (gpu_type, region) combination

    Key advantages over other providers:
    - Single API call for all data (no need for multiple queries)
    - Availability is explicit (regions listed = available)
    - Pricing included in response (no separate pricing lookup)
    """

    API_BASE_URL = "https://cloud.lambdalabs.com/api/v1"

    def __init__(self, config: CollectorConfig) -> None:
        """Initialize Lambda Labs collector.

        Args:
            config: Collector configuration

        Raises:
            ValueError: If LAMBDA_API_KEY environment variable is not set
        """
        super().__init__(config)
        api_key = os.environ.get("LAMBDA_API_KEY")
        if not api_key:
            raise ValueError("LAMBDA_API_KEY environment variable must be set")
        self.api_key = api_key

    @property
    def provider_name(self) -> str:
        """Return provider name."""
        return "Lambda Labs"

    async def _execute_api_call(self, endpoint: str) -> dict[str, Any]:
        """Execute a REST API call to Lambda Labs API.

        Args:
            endpoint: API endpoint path (e.g., "/instance-types")

        Returns:
            JSON response data

        Raises:
            aiohttp.ClientError: On HTTP errors
        """
        url = f"{self.API_BASE_URL}{endpoint}"

        # Lambda Labs uses HTTP Basic Auth with API key as username
        auth = aiohttp.BasicAuth(self.api_key, "")

        async with (
            aiohttp.ClientSession() as session,
            session.get(
                url,
                auth=auth,
                timeout=aiohttp.ClientTimeout(total=self.config.timeout),
            ) as response,
        ):
            response.raise_for_status()
            result: dict[str, Any] = await response.json()
            return result

    def _parse_gpu_name(self, description: str) -> tuple[str, int]:
        """Parse GPU name and count from description.

        Args:
            description: Instance description (e.g., "1x A10 (24 GB PCIe)")

        Returns:
            Tuple of (gpu_name, gpu_count)

        Example:
            "1x A10 (24 GB PCIe)" -> ("NVIDIA A10", 1)
            "8x A100 (80 GB SXM4)" -> ("NVIDIA A100 80GB", 8)
        """
        # Extract count (e.g., "1x", "8x")
        parts = description.split("x", 1)
        if len(parts) == 2 and parts[0].strip().isdigit():
            count = int(parts[0].strip())
            gpu_part = parts[1].strip()
        else:
            count = 1
            gpu_part = description

        # Extract GPU model from description
        # Format: "A10 (24 GB PCIe)" or "A100 (80 GB SXM4)"
        model_part = gpu_part.split("(")[0].strip() if "(" in gpu_part else gpu_part

        # Add NVIDIA prefix and extract memory if present
        if "80 GB" in gpu_part and "A100" in model_part:
            gpu_name = f"NVIDIA {model_part} 80GB"
        elif "40 GB" in gpu_part and "A100" in model_part:
            gpu_name = f"NVIDIA {model_part} 40GB"
        elif "80 GB" in gpu_part and "H100" in model_part:
            gpu_name = f"NVIDIA {model_part} 80GB"
        else:
            gpu_name = f"NVIDIA {model_part}"

        return gpu_name, count

    def _extract_gpu_memory(self, description: str) -> int | None:
        """Extract GPU memory in GiB from description.

        Args:
            description: Instance description (e.g., "1x A10 (24 GB PCIe)")

        Returns:
            GPU memory in GiB, or None if not found
        """
        # Look for pattern like "24 GB" or "80 GB"
        match = re.search(r"(\d+)\s*GB", description)
        if match:
            return int(match.group(1))
        return None

    def _create_gpu_instance(
        self,
        instance_type: dict[str, Any],
        region: dict[str, str],
        collected_at: int,
    ) -> GPUInstance:
        """Create a GPUInstance from Lambda Labs API data.

        Args:
            instance_type: Instance type data from API
            region: Region data with availability
            collected_at: Unix timestamp when data was collected

        Returns:
            GPUInstance object
        """
        specs = instance_type["specs"]
        gpu_name, gpu_count = self._parse_gpu_name(instance_type["description"])
        gpu_memory = self._extract_gpu_memory(instance_type["description"])

        # Lambda Labs price is in cents per hour, convert to dollars
        price = instance_type["price_cents_per_hour"] / 100.0

        return GPUInstance(
            # Identification
            provider="Lambda Labs",
            instance_type=instance_type["name"],
            accelerator_name=gpu_name,
            region=region["name"],
            availability_zone=region["description"],  # Use region description as zone info
            # Pricing (Lambda Labs only offers on-demand, no spot pricing)
            price=price,
            spot_price=None,
            # Availability (if region is listed, it's available)
            availability=AvailabilityStatus.HIGH,
            quantity=None,  # Lambda Labs doesn't expose quantity
            # Hardware specs
            v_cpus=specs["vcpus"],
            memory_gib=specs["memory_gib"],
            accelerator_count=gpu_count,
            accelerator_mem_gib=gpu_memory,
            # Note: storage_gib not included - not in GPUInstance model schema
            # Metadata
            collected_at=collected_at,
            raw_data={
                "instance_type": instance_type,
                "region": region,
            },
        )

    @with_retry
    async def fetch_instances(self) -> list[GPUInstance]:
        """Fetch GPU instances from Lambda Labs API.

        Uses a single API call strategy:
        1. GET /instance-types endpoint
        2. Response includes all GPU types with specs, pricing, and regional availability
        3. Create GPUInstance for each (gpu_type, region) pair where availability exists

        Returns:
            List of GPUInstance objects
        """
        self._logger.debug(
            "Fetching instance types from Lambda Labs",
            provider_name=self.provider_name,
        )

        # Single API call gets everything
        response = await self._execute_api_call("/instance-types")
        collected_at = int(time.time())

        # API returns {"data": {...}} structure
        data = response.get("data", response)

        self._logger.debug(
            "Fetched instance types",
            provider_name=self.provider_name,
            instance_type_count=len(data),
        )

        # Parse response - create one GPUInstance per (gpu_type, region) pair
        instances: list[GPUInstance] = []
        unique_regions: set[str] = set()

        for _gpu_type_key, gpu_data in data.items():
            instance_type = gpu_data["instance_type"]
            specs = instance_type.get("specs", {})
            if specs.get("gpus") == 0:
                continue
            regions = gpu_data.get("regions_with_capacity_available", [])

            if regions:
                # Create per-region instances for available GPUs
                for region in regions:
                    instance = self._create_gpu_instance(instance_type, region, collected_at)
                    instances.append(instance)
                    unique_regions.add(region["name"])
            else:
                # GPU type exists but no regions have capacity
                # Create single instance with special marker to indicate global unavailability
                unavailable_region = {
                    "name": "unavailable",
                    "description": "No regions have capacity",
                }
                instance = self._create_gpu_instance(
                    instance_type, unavailable_region, collected_at
                )
                # Override availability status to NOT_AVAILABLE
                instance.availability = AvailabilityStatus.NOT_AVAILABLE
                instances.append(instance)

        # Log summary statistics
        available_instances = [
            i for i in instances if i.availability != AvailabilityStatus.NOT_AVAILABLE
        ]

        self._logger.info(
            "Fetched GPU availability data",
            provider_name=self.provider_name,
            instance_type_count=len(data),
            total_instances=len(instances),
            available_instances=len(available_instances),
            unique_regions=len(unique_regions),
        )

        return self._apply_availability_filter(instances)
