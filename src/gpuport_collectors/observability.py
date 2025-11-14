"""Observability infrastructure for logging and tracing with Honeycomb.io.

This module provides structured logging and distributed tracing capabilities
using OpenTelemetry with Honeycomb.io as the backend.
"""

import logging
import os
import traceback
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any

from opentelemetry import trace
from opentelemetry._logs import set_logger_provider
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from gpuport_collectors.config import ObservabilityConfig

logger = logging.getLogger(__name__)


class StructuredLogger:
    """Structured logger that formats logs with required fields.

    All logs include:
    - timestamp: ISO 8601 format
    - provider_name: Name of the provider (if applicable)
    - error_message: Error message (for errors)
    - stack_trace: Stack trace (for errors)
    - Additional context fields
    """

    def __init__(
        self,
        name: str,
        config: ObservabilityConfig,
        honeycomb_handler: logging.Handler | None = None,
    ) -> None:
        """Initialize the structured logger.

        Args:
            name: Logger name (usually module name)
            config: Observability configuration
            honeycomb_handler: Optional OpenTelemetry logging handler for Honeycomb
        """
        self.logger = logging.getLogger(name)
        self.logger.setLevel(getattr(logging, config.log_level))
        self.config = config

        # Add console handler for local output
        if not self.logger.handlers:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(getattr(logging, config.log_level))
            self.logger.addHandler(console_handler)

        # Add Honeycomb handler if provided and not already attached
        if honeycomb_handler and honeycomb_handler not in self.logger.handlers:
            self.logger.addHandler(honeycomb_handler)

    def _format_message(self, message: str, **context: Any) -> str:
        """Format log message with structured context.

        Args:
            message: Base log message
            **context: Additional context fields

        Returns:
            Formatted log message with context
        """
        timestamp = datetime.now(UTC).isoformat()
        parts = [f"timestamp={timestamp}"]

        if "provider_name" in context:
            parts.append(f"provider={context.pop('provider_name')}")

        parts.append(f"msg={message}")

        for key, value in context.items():
            parts.append(f"{key}={value}")

        return " ".join(parts)

    def info(self, message: str, **context: Any) -> None:
        """Log info message with structured context.

        Args:
            message: Log message
            **context: Additional context fields
        """
        self.logger.info(self._format_message(message, **context))

    def warning(self, message: str, **context: Any) -> None:
        """Log warning message with structured context.

        Args:
            message: Log message
            **context: Additional context fields
        """
        self.logger.warning(self._format_message(message, **context))

    def error(
        self,
        message: str,
        error: Exception | None = None,
        **context: Any,
    ) -> None:
        """Log error message with structured context and stack trace.

        Args:
            message: Error message
            error: Exception object (optional)
            **context: Additional context fields
        """
        if error:
            context["error_type"] = type(error).__name__
            context["error_message"] = str(error)
            context["stack_trace"] = "".join(
                traceback.format_exception(type(error), error, error.__traceback__)
            )

        self.logger.error(self._format_message(message, **context))

    def debug(self, message: str, **context: Any) -> None:
        """Log debug message with structured context.

        Args:
            message: Log message
            **context: Additional context fields
        """
        self.logger.debug(self._format_message(message, **context))


class ObservabilityManager:
    """Manager for observability infrastructure (logging and tracing).

    This class initializes and manages OpenTelemetry tracing with Honeycomb.io
    and provides structured logging capabilities.
    """

    def __init__(self, config: ObservabilityConfig) -> None:
        """Initialize observability infrastructure.

        Args:
            config: Observability configuration
        """
        self.config = config
        self._tracer_provider: TracerProvider | None = None
        self._logger_provider: LoggerProvider | None = None
        self._honeycomb_handler: LoggingHandler | None = None
        self._initialized = False

    def initialize(self) -> None:
        """Initialize OpenTelemetry tracing and logging with Honeycomb.io.

        This sets up:
        - Resource attributes (service name, environment)
        - OTLP exporters to Honeycomb (traces and logs)
        - Tracer provider
        - Logger provider
        - Batch processors for traces and logs
        """
        if not self.config.enabled:
            logger.info("Observability is disabled")
            return

        if self._initialized:
            logger.warning("Observability already initialized")
            return

        # Get API key from config or environment
        api_key = self.config.honeycomb_api_key or os.environ.get("HONEYCOMB_API_KEY")

        if not api_key:
            logger.warning(
                "Honeycomb API key not configured. "
                "Set honeycomb_api_key in config or HONEYCOMB_API_KEY env var. "
                "Observability will be disabled."
            )
            return

        # Create resource with service metadata
        resource = Resource.create(
            {
                "service.name": self.config.service_name,
                "deployment.environment": self.config.environment,
            }
        )

        # Create OTLP trace exporter to Honeycomb
        trace_exporter = OTLPSpanExporter(
            endpoint=f"{self.config.exporter_endpoint}/v1/traces",
            headers={"x-honeycomb-team": api_key},
        )

        # Create and configure tracer provider
        self._tracer_provider = TracerProvider(resource=resource)
        self._tracer_provider.add_span_processor(BatchSpanProcessor(trace_exporter))

        # Set as global tracer provider
        trace.set_tracer_provider(self._tracer_provider)

        # Create OTLP log exporter to Honeycomb
        log_exporter = OTLPLogExporter(
            endpoint=f"{self.config.exporter_endpoint}/v1/logs",
            headers={"x-honeycomb-team": api_key},
        )

        # Create and configure logger provider
        self._logger_provider = LoggerProvider(resource=resource)
        self._logger_provider.add_log_record_processor(BatchLogRecordProcessor(log_exporter))

        # Set as global logger provider
        set_logger_provider(self._logger_provider)

        # Create logging handler for Honeycomb
        self._honeycomb_handler = LoggingHandler(
            level=getattr(logging, self.config.log_level),
            logger_provider=self._logger_provider,
        )

        self._initialized = True
        logger.info(
            f"Observability initialized: service={self.config.service_name}, "
            f"environment={self.config.environment}"
        )

    def shutdown(self) -> None:
        """Shutdown observability infrastructure and flush pending spans and logs."""
        if self._tracer_provider:
            self._tracer_provider.shutdown()  # type: ignore[no-untyped-call]
        if self._logger_provider:
            self._logger_provider.shutdown()  # type: ignore[no-untyped-call]
        if self._initialized:
            self._initialized = False
            logger.info("Observability shutdown")

    def get_tracer(self, name: str) -> trace.Tracer:
        """Get a tracer for creating spans.

        Args:
            name: Tracer name (usually module name)

        Returns:
            OpenTelemetry tracer instance
        """
        return trace.get_tracer(name)

    def get_logger(self, name: str) -> StructuredLogger:
        """Get a structured logger that outputs to both console and Honeycomb.

        Args:
            name: Logger name (usually module name)

        Returns:
            Structured logger instance
        """
        return StructuredLogger(name, self.config, self._honeycomb_handler)

    @contextmanager
    def trace_operation(
        self,
        operation_name: str,
        **attributes: Any,
    ) -> Any:
        """Context manager for tracing an operation.

        Args:
            operation_name: Name of the operation being traced
            **attributes: Additional attributes to add to the span

        Yields:
            The current span

        Example:
            ```python
            with obs_manager.trace_operation("fetch_instances", provider="RunPod"):
                # Your code here
                pass
            ```
        """
        if not self._initialized:
            # If not initialized, just yield without tracing
            yield None
            return

        tracer = self.get_tracer(__name__)
        with tracer.start_as_current_span(operation_name) as span:
            # Add attributes to span
            for key, value in attributes.items():
                span.set_attribute(key, str(value))

            try:
                yield span
            except Exception as e:
                # Record exception in span
                span.set_attribute("error", True)
                span.set_attribute("error.type", type(e).__name__)
                span.set_attribute("error.message", str(e))
                span.set_attribute(
                    "error.stack_trace",
                    "".join(traceback.format_exception(type(e), e, e.__traceback__)),
                )
                raise


# Global observability manager instance
_observability_manager: ObservabilityManager | None = None


def get_observability_manager(
    config: ObservabilityConfig | None = None,
) -> ObservabilityManager:
    """Get the global observability manager instance.

    Args:
        config: Optional configuration to initialize with

    Returns:
        Global observability manager instance
    """
    global _observability_manager

    if _observability_manager is None:
        if config is None:
            from gpuport_collectors.config import default_config

            config = default_config.observability

        _observability_manager = ObservabilityManager(config)
        _observability_manager.initialize()

    return _observability_manager


__all__ = [
    "ObservabilityManager",
    "StructuredLogger",
    "get_observability_manager",
]
