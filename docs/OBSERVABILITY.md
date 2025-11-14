# Observability with Honeycomb.io

GPUPort Collectors includes comprehensive observability infrastructure for logging and distributed tracing using [Honeycomb.io](https://honeycomb.io) and [OpenTelemetry](https://opentelemetry.io/).

## Features

- **Structured Logging**: All logs include structured context fields (timestamp, provider name, error messages, stack traces)
- **Log Export**: Logs are sent to both Honeycomb.io and local console for easy debugging
- **Distributed Tracing**: Automatic tracing of collector operations with OpenTelemetry
- **Honeycomb Integration**: Direct integration with Honeycomb.io for visualization and analysis
- **Configurable**: Easily enable/disable and configure via YAML or environment variables

## Quick Start

### 1. Enable Observability

Edit your `defaults.yaml` or create a custom configuration file:

```yaml
observability:
  enabled: true
  honeycomb_api_key: "your-api-key-here"  # Or use HONEYCOMB_API_KEY env var
  service_name: "gpuport-collectors"
  environment: "production"
  log_level: "INFO"
```

Alternatively, set the API key via environment variable:

```bash
export HONEYCOMB_API_KEY="your-api-key-here"
```

### 2. Use in Your Code

The observability infrastructure is automatically initialized when you create a collector:

```python
from gpuport_collectors.base import BaseCollector
from gpuport_collectors.config import CollectorConfig

# Load configuration with observability enabled
config = CollectorConfig.from_yaml("config.yaml")

# Collector will automatically use observability
class MyCollector(BaseCollector):
    def __init__(self):
        super().__init__(config)

    @property
    def provider_name(self) -> str:
        return "MyProvider"

    async def fetch_instances(self):
        # Logging and tracing happens automatically
        ...
```

## Configuration

### Configuration Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | boolean | `false` | Enable/disable observability |
| `honeycomb_api_key` | string | `null` | Honeycomb API key (can also use `HONEYCOMB_API_KEY` env var) |
| `service_name` | string | `"gpuport-collectors"` | Service name for telemetry |
| `environment` | string | `"development"` | Environment name (production, staging, etc.) |
| `log_level` | string | `"INFO"` | Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL) |
| `exporter_protocol` | string | `"http/protobuf"` | OTLP exporter protocol (http/protobuf or grpc) |
| `exporter_endpoint` | string | `"https://api.honeycomb.io:443"` | OTLP exporter endpoint (use `https://api.eu1.honeycomb.io:443` for EU) |

### Example Configurations

**US Region (Default)**:
```yaml
observability:
  enabled: true
  honeycomb_api_key: null  # Set via HONEYCOMB_API_KEY
  service_name: "gpuport-collectors"
  environment: "production"
  log_level: "INFO"
```

**EU Region**:
```yaml
observability:
  enabled: true
  honeycomb_api_key: null  # Set via HONEYCOMB_API_KEY
  service_name: "gpuport-collectors"
  environment: "production"
  log_level: "INFO"
  exporter_endpoint: "https://api.eu1.honeycomb.io:443"
```

## Structured Logging

All logs include structured context for easy filtering and analysis.

### Dual Output: Console + Honeycomb

When observability is enabled, logs are automatically sent to **both**:
1. **Local console** - For immediate visibility during development and debugging
2. **Honeycomb.io** - For persistent storage, search, and correlation with traces

This dual output gives you the best of both worlds: real-time local feedback and powerful cloud-based analysis.

### Log Format

All logs include structured context for easy filtering and analysis:

```python
# Automatically included fields:
# - timestamp: ISO 8601 format
# - provider_name: Name of the provider
# - error_type: Type of exception (for errors)
# - error_message: Error message (for errors)
# - stack_trace: Full stack trace (for errors)

# Example log output:
# timestamp=2024-01-15T10:30:45.123Z provider=RunPod msg=Fetching instances
# timestamp=2024-01-15T10:30:46.456Z provider=RunPod msg=Successfully fetched instances instance_count=25
# timestamp=2024-01-15T10:30:50.789Z provider=LambdaLabs msg=Retry attempt failed, retrying attempt=1 retry_delay=5.0 error_type=TimeoutError
```

### Using the Logger Directly

You can also use the structured logger directly in your code:

```python
from gpuport_collectors.observability import get_observability_manager

obs_manager = get_observability_manager()
logger = obs_manager.get_logger(__name__)

# Info logging
logger.info("Processing data", provider_name="MyProvider", count=100)

# Warning logging
logger.warning("Rate limit approaching", provider_name="MyProvider", remaining=10)

# Error logging with exception
try:
    raise ValueError("Invalid configuration")
except ValueError as e:
    logger.error("Configuration error", error=e, provider_name="MyProvider")
```

### Appropriate Log Levels

- **DEBUG**: Development and troubleshooting
- **INFO**: Production normal operations
- **WARNING**: Production unusual but handled situations
- **ERROR**: Production errors requiring attention

### Add Context to Logs

Always include relevant context when logging:

```python
logger.info(
    "Fetching instances",
    provider_name=self.provider_name,
    timeout=self.config.timeout,
    max_retries=self.config.max_retries,
)
```

## Distributed Tracing

Observability automatically creates traces for collector operations:

### Automatic Tracing

The `fetch_instances_with_tracing` method creates distributed traces, and if your `fetch_instances` method uses the `@with_retry` decorator, all retry attempts will be visible within the trace:

```python
from gpuport_collectors.base import BaseCollector, with_retry

class MyCollector(BaseCollector):
    @property
    def provider_name(self) -> str:
        return "MyProvider"

    @with_retry  # Retries with structured logging (visible in traces when using fetch_instances_with_tracing)
    async def fetch_instances(self):
        # Your implementation here
        return instances

# Use the tracing wrapper to create spans
collector = MyCollector()
instances = await collector.fetch_instances_with_tracing()  # Creates trace span
```

**How it works:**
- `fetch_instances_with_tracing()` creates a trace span for the entire fetch operation
- `@with_retry` provides automatic retry logic with structured logging
- All retry attempts happen within the trace span, making them visible in Honeycomb

### Manual Tracing

You can also add custom traces to specific operations:

```python
from gpuport_collectors.observability import get_observability_manager

obs_manager = get_observability_manager()

# Trace a custom operation
with obs_manager.trace_operation("parse_gpu_data", provider="MyProvider", gpu_count=50):
    # Your code here
    parsed_data = parse_gpu_data(raw_data)
```

### Trace Attributes

Traces automatically include:
- Operation name
- Provider name
- Custom attributes you specify
- Error information (if an exception occurs)
- Instance counts (for fetch operations)

## Viewing Data in Honeycomb

Once configured, all logs and traces are sent to Honeycomb.io where you can:

1. **View Traces**: See the complete timeline of collector operations
2. **Analyze Errors**: Query for errors and see full stack traces
3. **Monitor Performance**: Track operation durations and retry patterns
4. **Debug Issues**: Follow the flow of execution across retries and errors

### Example Queries

In Honeycomb, you can run queries like:

- "Show all fetch operations that took longer than 10 seconds"
- "Show errors grouped by provider"
- "Show retry patterns for failed operations"
- "Show instance counts over time by provider"

## Performance Impact

The observability infrastructure is designed for minimal performance impact:

- **Logging**: Asynchronous, non-blocking
- **Tracing**: Batch span export to minimize network calls
- **Overhead**: Typically <1% CPU and memory overhead
- **Disabled State**: Zero overhead when `enabled: false`

## Related Documentation

- [OpenTelemetry Python Documentation](https://opentelemetry.io/docs/instrumentation/python/)
- [Honeycomb Documentation](https://docs.honeycomb.io/)
- [Honeycomb OpenTelemetry Integration](https://docs.honeycomb.io/send-data/opentelemetry/)
