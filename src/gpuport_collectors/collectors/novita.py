"""Novita AI GPU collector using Novita Python SDK."""

import os
import time

from novita import AsyncNovitaClient
from novita.generated.models import GPUProduct

from gpuport_collectors.base import BaseCollector, with_retry
from gpuport_collectors.config import CollectorConfig
from gpuport_collectors.models import AvailabilityStatus, GPUInstance


class NovitaCollector(BaseCollector):
    """Collector for Novita AI GPU availability data.

    Uses Novita's Python SDK to fetch GPU availability:
    1. Call client.gpu.products.list() to get all GPU products
    2. Each product has available_deploy (bool) and regions (list)
    3. Create one GPUInstance per (product, region) combination

    Key advantages:
    - Type-safe SDK with Pydantic models
    - Async support built-in
    - Regional availability included
    - Pricing included in response

    Note: Novita API provides binary availability (yes/no) but not quantity counts.
    Similar to Lambda Labs in this regard.
    """

    def __init__(self, config: CollectorConfig) -> None:
        """Initialize Novita collector.

        Args:
            config: Collector configuration

        Raises:
            ValueError: If NOVITA_API_KEY environment variable is not set
        """
        super().__init__(config)
        self.api_key = os.environ.get("NOVITA_API_KEY")
        if not self.api_key:
            raise ValueError("NOVITA_API_KEY environment variable must be set")

    @property
    def provider_name(self) -> str:
        """Return provider name."""
        return "Novita AI"

    def _parse_gpu_name(self, product_id: str, product_name: str) -> tuple[str, int]:
        """Parse GPU name and count from product ID or name.

        Args:
            product_id: Product ID (e.g., "H100_80GB", "A100_40GB_8x")
            product_name: Product name (e.g., "NVIDIA H100 80GB")

        Returns:
            Tuple of (gpu_name, gpu_count)

        Examples:
            "H100_80GB" -> ("NVIDIA H100 80GB", 1)
            "A100_40GB_8x" -> ("NVIDIA A100 40GB", 8)
            "L40S_2x" -> ("NVIDIA L40S", 2)
        """
        # Extract GPU count from product_id (e.g., "8x" at end)
        count = 1
        id_parts = product_id.split("_")
        for part in id_parts:
            if part.endswith("x") and part[:-1].isdigit():
                count = int(part[:-1])
                break

        # Use product_name if it already has "NVIDIA" prefix, else add it
        gpu_name = product_name if "NVIDIA" in product_name.upper() else f"NVIDIA {product_name}"

        return gpu_name, count

    def _extract_gpu_memory(self, product_id: str, product_name: str) -> int | None:
        """Extract GPU memory in GiB from product ID or name.

        Args:
            product_id: Product ID (e.g., "H100_80GB")
            product_name: Product name

        Returns:
            GPU memory in GiB, or None if not found
        """
        # Look for memory in product_id (e.g., "80GB", "40GB")
        import re

        # Try product_id first
        match = re.search(r"(\d+)GB", product_id, re.IGNORECASE)
        if match:
            return int(match.group(1))

        # Try product_name
        match = re.search(r"(\d+)\s*GB", product_name, re.IGNORECASE)
        if match:
            return int(match.group(1))

        return None

    def _map_availability(self, available_deploy: bool) -> AvailabilityStatus:
        """Map Novita availability to AvailabilityStatus enum.

        Args:
            available_deploy: Whether the product can be deployed

        Returns:
            Corresponding AvailabilityStatus enum value

        Note: Novita only provides binary availability (yes/no), not granular levels.
        We map True -> HIGH since it means ready to deploy immediately.
        """
        return AvailabilityStatus.HIGH if available_deploy else AvailabilityStatus.NOT_AVAILABLE

    def _create_gpu_instance(
        self,
        product: GPUProduct,
        region: str,
        collected_at: int,
    ) -> GPUInstance:
        """Create a GPUInstance from Novita GPUProduct data.

        Args:
            product: GPUProduct from Novita SDK
            region: Region/cluster name
            collected_at: Unix timestamp when data was collected

        Returns:
            GPUInstance object
        """
        gpu_name, gpu_count = self._parse_gpu_name(product.id, product.name)
        gpu_memory = self._extract_gpu_memory(product.id, product.name)

        # Novita price is in 0.001 cents per hour (1/100000 dollars per hour)
        # Example: 67000 = $0.67/hour
        price = float(product.price) / 100000.0 if product.price else 0.0

        return GPUInstance(
            # Identification
            provider="Novita AI",
            instance_type=product.id,
            accelerator_name=gpu_name,
            region=region,
            availability_zone="",  # Novita doesn't have separate availability zones
            # Pricing (Novita only shows on-demand pricing in products API)
            price=price,
            spot_price=None,  # Not available in products API
            # Availability
            availability=self._map_availability(product.available_deploy),
            quantity=None,  # Novita doesn't expose quantity in API
            # Hardware specs
            v_cpus=product.cpu_per_gpu,
            memory_gib=product.memory_per_gpu,
            accelerator_count=gpu_count,
            accelerator_mem_gib=gpu_memory,
            # Metadata
            collected_at=collected_at,
            raw_data=product.model_dump(),
        )

    @with_retry
    async def fetch_instances(self) -> list[GPUInstance]:
        """Fetch GPU instances from Novita AI API.

        Uses the Novita Python SDK for type-safe async API access:
        1. Get all GPU products via client.gpu.products.list()
        2. For each product, create GPUInstance per region
        3. Filter to only available products by default

        Returns:
            List of GPUInstance objects (one per product-region combination)
        """
        self._logger.debug(
            "Fetching GPU products from Novita AI",
            provider_name=self.provider_name,
        )

        # Use Novita SDK for type-safe async API access
        async with AsyncNovitaClient(api_key=self.api_key) as client:
            # Get all GPU products (includes availability, pricing, regions)
            products = await client.gpu.products.list()

        collected_at = int(time.time())

        self._logger.debug(
            "Fetched GPU products",
            provider_name=self.provider_name,
            product_count=len(products),
        )

        # Create GPUInstance for each (product, region) combination
        instances: list[GPUInstance] = []
        unique_regions: set[str] = set()

        for product in products:
            # Create one instance per region where product is available
            for region in product.regions:
                instance = self._create_gpu_instance(product, region, collected_at)
                instances.append(instance)
                unique_regions.add(region)

        # Log summary statistics
        available_instances = [
            i for i in instances if i.availability != AvailabilityStatus.NOT_AVAILABLE
        ]

        self._logger.info(
            "Fetched GPU availability data",
            provider_name=self.provider_name,
            product_count=len(products),
            total_instances=len(instances),
            available_instances=len(available_instances),
            unique_regions=len(unique_regions),
        )

        return self._apply_availability_filter(instances)
