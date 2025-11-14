"""RunPod GPU collector using GraphQL API."""

import asyncio
import os
import time
from typing import Any

import aiohttp

from gpuport_collectors.base import BaseCollector, with_retry
from gpuport_collectors.config import CollectorConfig
from gpuport_collectors.models import AvailabilityStatus, GPUInstance


class RunPodCollector(BaseCollector):
    """Collector for RunPod GPU availability data.

    Uses RunPod's GraphQL API to fetch all GPU availability:
    1. Query all datacenters (39 total)
    2. Query pricing for ALL GPU types across ALL datacenters in one request
    3. Filter client-side based on which pricing responses have availability

    Note: We query all combinations because nodeGroupDatacenters is unreliable
    and can show empty arrays for GPUs that are actually available in multiple
    datacenters. See RUNPOD_NODEGROUPDATACENTERS_BUG.md for details.
    """

    GRAPHQL_ENDPOINT = "https://api.runpod.io/graphql"
    MAX_CONCURRENT_REQUESTS = 3  # Rate limiting for API calls

    def __init__(self, config: CollectorConfig) -> None:
        """Initialize RunPod collector.

        Args:
            config: Collector configuration

        Raises:
            ValueError: If RUNPOD_API_KEY environment variable is not set
        """
        super().__init__(config)
        self.api_key = os.environ.get("RUNPOD_API_KEY")
        if not self.api_key:
            raise ValueError("RUNPOD_API_KEY environment variable must be set")
        self._semaphore = asyncio.Semaphore(self.MAX_CONCURRENT_REQUESTS)

    @property
    def provider_name(self) -> str:
        """Return provider name."""
        return "RunPod"

    async def _execute_graphql(
        self, query: str, variables: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """Execute a GraphQL query against RunPod API with rate limiting.

        Args:
            query: GraphQL query string
            variables: Optional query variables

        Returns:
            GraphQL response data

        Raises:
            aiohttp.ClientError: On HTTP errors
            ValueError: On GraphQL errors
        """
        url = self.GRAPHQL_ENDPOINT
        headers = {"Authorization": f"Bearer {self.api_key}"}

        # Use semaphore for rate limiting (max concurrent requests)
        async with (
            self._semaphore,
            aiohttp.ClientSession() as session,
            session.post(
                url,
                json={"query": query, "variables": variables or {}},
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=self.config.timeout),
            ) as response,
        ):
            response.raise_for_status()
            result = await response.json()

            if "errors" in result:
                raise ValueError(f"GraphQL errors: {result['errors']}")

            data: dict[str, Any] = result.get("data", {})
            return data

    async def _get_all_datacenters(self) -> list[str]:
        """Get all RunPod datacenters.

        Returns:
            List of all datacenter IDs (39 total)
        """
        self._logger.info(
            "Fetching all datacenters",
            provider_name=self.provider_name,
        )

        query = """
        query {
          dataCenters {
            id
            name
          }
        }
        """

        data = await self._execute_graphql(query)
        datacenters = data.get("dataCenters", [])

        dc_ids = [dc["id"] for dc in datacenters]

        self._logger.info(
            "Fetched datacenters",
            provider_name=self.provider_name,
            datacenter_count=len(dc_ids),
        )

        return sorted(dc_ids)

    def _build_pricing_query(self, datacenters: list[str]) -> str:
        """Build GraphQL query for pricing across datacenters.

        Args:
            datacenters: List of datacenter IDs to query

        Returns:
            GraphQL query string
        """
        # Build datacenter aliases
        datacenter_aliases = []
        for dc in datacenters:
            alias = dc.lower().replace("-", "_")
            datacenter_aliases.append(
                f"""
            {alias}: lowestPrice(input: {{ dataCenterId: "{dc}", gpuCount: 1 }}) {{
              stockStatus
              uninterruptablePrice
              minimumBidPrice
              availableGpuCounts
            }}
            """
            )

        newline = "\n"
        return f"""
        query {{
          gpuTypes {{
            id
            displayName
            memoryInGb
            cudaCores
            {newline.join(datacenter_aliases)}
          }}
        }}
        """

    def _map_stock_status(self, status: str | None) -> AvailabilityStatus:
        """Map RunPod stock status to AvailabilityStatus enum.

        Args:
            status: RunPod stock status string ("High", "Medium", "Low", or None)

        Returns:
            Corresponding AvailabilityStatus enum value
        """
        if not status:
            return AvailabilityStatus.NOT_AVAILABLE

        status_map = {
            "High": AvailabilityStatus.HIGH,
            "Medium": AvailabilityStatus.MEDIUM,
            "Low": AvailabilityStatus.LOW,
        }

        return status_map.get(status, AvailabilityStatus.NOT_AVAILABLE)

    def _parse_gpu_data(
        self,
        gpu: dict[str, Any],
        datacenters: list[str],
    ) -> list[GPUInstance]:
        """Parse GPU data and create GPUInstance for each datacenter with availability.

        Creates instances only for datacenters where the GPU has actual availability
        (non-null stockStatus in pricing response). This filters out the ~95% of
        GPU-datacenter combinations that don't have availability.

        Args:
            gpu: GPU type data from GraphQL response with pricing
            datacenters: List of all datacenters queried

        Returns:
            List of GPUInstance objects (one per available region only)
        """
        instances = []
        collected_at = int(time.time())

        # Check each datacenter for availability
        for dc in datacenters:
            # Convert datacenter ID to alias format
            alias = dc.lower().replace("-", "_")
            pricing = gpu.get(alias)

            # Skip if no pricing data or no stock status (means not available)
            if not pricing or not pricing.get("stockStatus"):
                continue

            # Get pricing data (0.0 if unavailable or None)
            price = pricing.get("uninterruptablePrice")
            price = price if price is not None else 0.0

            spot_price = pricing.get("minimumBidPrice")
            spot_price = spot_price if spot_price is not None else 0.0

            stock_status = pricing.get("stockStatus")

            # availableGpuCounts can be a list or None
            quantity_raw = pricing.get("availableGpuCounts")
            if isinstance(quantity_raw, list):
                quantity = sum(quantity_raw) if quantity_raw else 0
            elif quantity_raw is not None:
                quantity = int(quantity_raw)
            else:
                quantity = 0

            instance = GPUInstance(
                # Identification
                provider="RunPod",
                instance_type=gpu["id"],
                accelerator_name=gpu["displayName"],
                region=dc,
                # Pricing
                price=price,
                spot_price=spot_price,
                # Availability
                availability=self._map_stock_status(stock_status),
                quantity=quantity,
                # Hardware specs (RunPod doesn't expose CPU/memory per GPU)
                v_cpus=None,
                memory_gib=None,
                accelerator_count=1,
                accelerator_mem_gib=gpu["memoryInGb"] if gpu.get("memoryInGb", 0) > 0 else None,
                # Metadata
                collected_at=collected_at,
                raw_data={
                    "gpu_type": {
                        "id": gpu["id"],
                        "displayName": gpu["displayName"],
                        "memoryInGb": gpu["memoryInGb"],
                        "cudaCores": gpu.get("cudaCores"),
                    },
                    "pricing": pricing,
                    "datacenter": dc,
                },
            )

            instances.append(instance)

        return instances

    async def _get_all_gpu_types(self) -> list[dict[str, Any]]:
        """Get all GPU types.

        Returns:
            List of GPU type dictionaries with id, displayName, memoryInGb, cudaCores
        """
        self._logger.info(
            "Fetching all GPU types",
            provider_name=self.provider_name,
        )

        query = """
        query {
          gpuTypes {
            id
            displayName
            memoryInGb
            cudaCores
          }
        }
        """

        data = await self._execute_graphql(query)
        gpu_types = data.get("gpuTypes", [])

        self._logger.info(
            "Fetched GPU types",
            provider_name=self.provider_name,
            gpu_type_count=len(gpu_types),
        )

        gpu_types_list: list[dict[str, Any]] = gpu_types
        return gpu_types_list

    async def _fetch_gpu_pricing(
        self, gpu_id: str, datacenters: list[str]
    ) -> dict[str, Any] | None:
        """Fetch pricing for a single GPU type across all datacenters.

        Args:
            gpu_id: GPU type ID to query
            datacenters: List of all datacenter IDs

        Returns:
            GPU data with pricing for all datacenters, or None if query fails
        """
        query = self._build_pricing_query(datacenters)

        # Modify query to filter for specific GPU type
        query = query.replace("gpuTypes {", f'gpuTypes(input: {{ id: "{gpu_id}" }}) {{')

        try:
            data = await self._execute_graphql(query)
            gpu_types = data.get("gpuTypes", [])

            if gpu_types:
                gpu_data: dict[str, Any] = gpu_types[0]
                return gpu_data
            self._logger.warning(
                "No data returned for GPU type",
                provider_name=self.provider_name,
                gpu_id=gpu_id,
            )
            return None
        except Exception as e:
            self._logger.error(
                "Failed to fetch pricing for GPU type",
                provider_name=self.provider_name,
                gpu_id=gpu_id,
                error=e,
            )
            return None

    @with_retry
    async def fetch_instances(self) -> list[GPUInstance]:
        """Fetch GPU instances from RunPod API.

        Uses a per-GPU-type query strategy:
        1. Get all GPU types (41 total)
        2. Get all datacenters (39 total)
        3. For each GPU type, query pricing across all datacenters (41 queries)
           - Each query has 39 datacenter aliases (manageable complexity)
           - Max 3 concurrent queries via semaphore

        This approach is simple, maintainable, and avoids excessive query complexity
        while ensuring we don't miss any available GPUs.

        Returns:
            List of GPUInstance objects (only for available combinations)
        """
        # Step 1: Get all GPU types
        gpu_types = await self._get_all_gpu_types()

        if not gpu_types:
            self._logger.warning(
                "No GPU types discovered",
                provider_name=self.provider_name,
            )
            return []

        # Step 2: Get all datacenters
        datacenters = await self._get_all_datacenters()

        if not datacenters:
            self._logger.warning(
                "No datacenters discovered",
                provider_name=self.provider_name,
            )
            return []

        # Step 3: Query pricing for each GPU type across all datacenters
        self._logger.info(
            "Fetching GPU pricing data",
            provider_name=self.provider_name,
            gpu_type_count=len(gpu_types),
            datacenter_count=len(datacenters),
            total_queries=len(gpu_types),
        )

        # Create tasks for each GPU type (semaphore limits concurrency)
        tasks = [self._fetch_gpu_pricing(gpu["id"], datacenters) for gpu in gpu_types]

        # Execute all queries with max 3 concurrent
        gpu_pricing_results = await asyncio.gather(*tasks)

        # Step 4: Parse all results and create instances
        instances: list[GPUInstance] = []
        for gpu_data in gpu_pricing_results:
            if gpu_data:
                instances.extend(self._parse_gpu_data(gpu_data, datacenters))

        # Log summary statistics
        available_instances = [
            i for i in instances if i.availability != AvailabilityStatus.NOT_AVAILABLE
        ]

        self._logger.info(
            "Fetched GPU availability data",
            provider_name=self.provider_name,
            gpu_type_count=len(gpu_types),
            datacenter_count=len(datacenters),
            total_instances=len(instances),
            available_instances=len(available_instances),
        )

        return instances
