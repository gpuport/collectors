"""Base collector class for all provider implementations.

This module defines the abstract base class that all provider-specific collectors
must extend, ensuring consistent interface and behavior across all providers.
"""

import asyncio
import logging
from abc import ABC, abstractmethod
from collections.abc import Callable
from functools import wraps
from typing import Any, TypeVar

from gpuport_collectors.config import CollectorConfig, default_config
from gpuport_collectors.models import GPUInstance

logger = logging.getLogger(__name__)

T = TypeVar("T")


def with_retry(func: Callable[..., Any]) -> Callable[..., Any]:
    """Decorator that adds retry logic with exponential backoff to async functions.

    This decorator automatically retries failed async function calls using exponential
    backoff. The retry behavior is configured via the collector's config object.

    The backoff delay follows the pattern: base_delay * (backoff_factor ^ attempt_number)
    For example, with base_delay=5 and backoff_factor=3:
    - Retry 1: 5s (5 * 3^0)
    - Retry 2: 15s (5 * 3^1)
    - Retry 3: 45s (5 * 3^2)

    Args:
        func: The async function to wrap with retry logic

    Returns:
        Wrapped function with retry behavior

    Example:
        ```python
        class MyCollector(BaseCollector):
            @with_retry
            async def fetch_instances(self) -> list[GPUInstance]:
                # This will automatically retry on failure
                return await self._fetch_from_api()
        ```
    """

    @wraps(func)
    async def wrapper(self: "BaseCollector", *args: Any, **kwargs: Any) -> Any:
        last_exception: Exception | None = None
        base_delay = 5  # Initial delay in seconds

        for attempt in range(self.config.max_retries + 1):
            try:
                return await func(self, *args, **kwargs)
            except Exception as e:
                last_exception = e

                # If this was the last attempt, raise the exception
                if attempt >= self.config.max_retries:
                    logger.error(
                        f"{self.provider_name}: All {self.config.max_retries + 1} "
                        f"attempts failed. Last error: {e}"
                    )
                    raise

                # Calculate exponential backoff delay
                delay = base_delay * (self.config.backoff_factor**attempt)
                logger.warning(
                    f"{self.provider_name}: Attempt {attempt + 1} failed: {e}. "
                    f"Retrying in {delay}s..."
                )

                await asyncio.sleep(delay)

        # This should never be reached, but satisfies type checker
        if last_exception:
            raise last_exception
        msg = "Unexpected state in retry logic"
        raise RuntimeError(msg)

    return wrapper


class BaseCollector(ABC):
    """Abstract base class for all GPU instance collectors.

    This class defines the interface that all provider-specific collectors must
    implement. Each provider (RunPod, Lambda Labs, Vast.ai, etc.) should extend
    this class and implement the required methods.

    Attributes:
        config: Configuration settings for timeout, retries, etc.
        provider_name: Name of the provider (must be implemented by subclass)
    """

    def __init__(self, config: CollectorConfig | None = None) -> None:
        """Initialize the collector with optional configuration.

        Args:
            config: Optional configuration override. If not provided, uses default
                   configuration from defaults.yaml
        """
        self.config = config or default_config

    @property
    @abstractmethod
    def provider_name(self) -> str:
        """Return the name of the provider.

        This must be implemented by each provider-specific collector.

        Returns:
            Provider name (e.g., "RunPod", "Lambda Labs", "Vast.ai")
        """

    @abstractmethod
    async def fetch_instances(self) -> list[GPUInstance]:
        """Fetch and return all available GPU instances from the provider.

        This is the main method that provider-specific collectors must implement.
        It should:
        1. Fetch data from the provider's API or website
        2. Parse and normalize the data to GPUInstance models
        3. Handle errors gracefully (timeouts, rate limits, schema changes)
        4. Return a list of validated GPUInstance objects

        The method must be async and should respect the timeout and retry settings
        from self.config.

        You can apply the @with_retry decorator to automatically retry failed requests:

        Example:
            ```python
            class RunPodCollector(BaseCollector):
                @property
                def provider_name(self) -> str:
                    return "RunPod"

                @with_retry
                async def fetch_instances(self) -> list[GPUInstance]:
                    # Fetch from RunPod API
                    # Parse and normalize data
                    # Return list of GPUInstance objects
                    return instances
            ```

        Returns:
            List of normalized GPUInstance objects representing available GPU
            instances from this provider

        Raises:
            Exception: Implementation-specific exceptions for network errors,
                      parsing failures, etc. Callers should handle these gracefully
                      to ensure one provider failure doesn't block others.
        """


__all__ = ["BaseCollector", "with_retry"]
