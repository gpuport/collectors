# GPU Data Exports

Flexible pipeline system for filtering, transforming, and exporting GPU instance data to multiple destinations.

## Quick Start

Create `export.yaml`:

```yaml
pipelines:
  - name: daily-export
    filters:
      - field: availability
        operator: ne
        value: NOT_AVAILABLE
    transformer:
      format: json
      pretty_print: true
    outputs:
      - type: local
        path: ./data
        filename_pattern: "gpus_{date}.json"
```

Run export:

```bash
# Activate virtual environment (if not already activated)
source .venv/bin/activate

# Run export
gpuport-collectors export --config export.yaml --api-key YOUR_KEY

# Or use uv run without activating
uv run gpuport-collectors export --config export.yaml --api-key YOUR_KEY
```

## Architecture

```text
GPU Instances → Filter → Transform → Output
```

Each pipeline:
- **Filters** instances based on conditions
- **Transforms** to JSON, CSV, or metrics
- **Outputs** to local files, S3, or HTTPS
- Handles partial failures gracefully

## Configuration

### Basic Pipeline

```yaml
pipelines:
  - name: pipeline-name           # Required
    enabled: true                 # Optional (default: true)
    filters: []                   # Optional filter conditions
    transformer: {}               # Required transformer config
    outputs: []                   # Required output destinations
```

### Multiple Pipelines

```yaml
pipelines:
  # Full export
  - name: full-data
    transformer:
      format: json
    outputs:
      - type: local
        path: ./exports
        filename_pattern: "full_{timestamp}.json"

  # Expensive GPUs only
  - name: expensive-gpus
    filters:
      - field: price
        operator: gte
        value: 10.0
    transformer:
      format: csv
      fields:
        provider: "Provider"
        price: "Price ($/hr)"
    outputs:
      - type: local
        path: ./reports
        filename_pattern: "expensive_{date}.csv"
```

## Filters

### Operators

```yaml
filters:
  # Equality
  - field: provider
    operator: eq                  # equals
    value: RunPod

  - field: provider
    operator: ne                  # not equals
    value: AWS

  # Comparison
  - field: price
    operator: lt                  # less than
    value: 5.0

  - field: price
    operator: gte                 # greater than or equal
    value: 1.0

  - field: price
    operator: between             # range (inclusive)
    min: 1.0
    max: 5.0

  # Set membership
  - field: availability
    operator: in
    values: [HIGH, MEDIUM]

  - field: region
    operator: not_in
    values: [deprecated-1, deprecated-2]

  # Pattern matching
  - field: region
    operator: regex
    value: "^EU-.*"

  - field: instance_type
    operator: contains
    value: "A100"

  - field: instance_type
    operator: starts_with
    value: "gpu"

  # Null checks
  - field: spot_price
    operator: is_null

  - field: spot_price
    operator: is_not_null
```

### Available Fields

- `provider`, `instance_type`, `accelerator_name`, `region`
- `accelerator_count`, `accelerator_mem_gib`, `quantity`
- `price`, `spot_price`, `v_cpus`, `memory_gib`
- `availability` (NOT_AVAILABLE, LOW, MEDIUM, HIGH)
- `collected_at` (Unix timestamp)

## Transformers

### JSON

```yaml
transformer:
  format: json
  pretty_print: true              # Indent output (default: false)
  fields:                         # Optional field mapping
    provider: "cloud_provider"
    price: "hourly_cost"
  include_raw_data: false         # Include API raw data (default: false)
  null_handling: "null"           # "null", "omit", or "empty"
```

### CSV

```yaml
transformer:
  format: csv
  fields:                         # Required, order preserved
    provider: "Provider"
    instance_type: "Instance Type"
    price: "Price"
  include_headers: true           # Header row (default: true)
  delimiter: ","                  # Field delimiter
  null_value: ""                  # Null representation
```

### Metrics

```yaml
transformer:
  format: metrics
  metrics:
    - name: total
      type: count

    - name: avg_price
      type: avg                   # avg, min, max, sum
      field: price

    - name: by_provider
      type: count
      group_by: provider

    - name: unique_gpus
      type: unique
      field: accelerator_name

  include_timestamp: true         # Add timestamp
  include_collection_info: false  # Add collector metadata
```

**Metric types:** `count`, `avg`, `min`, `max`, `sum`, `unique`

## Outputs

### Local Filesystem

```yaml
outputs:
  - type: local
    path: "./data/exports"
    filename_pattern: "gpus_{date}.json"
    create_dirs: true             # Create missing dirs (default: true)
    overwrite: false              # Overwrite files (default: false)
    compression: "none"           # "none" or "gzip"
```

**Filename placeholders:** `{date}`, `{time}`, `{timestamp}`, `{year}`, `{month}`, `{day}`, `{hour}`, `{minute}`, `{second}`

### S3-Compatible Storage

```yaml
outputs:
  - type: s3
    bucket: "my-bucket"
    prefix: "data/"               # Optional key prefix
    region: "us-east-1"
    endpoint_url: null            # For MinIO, R2, etc.
    filename_pattern: "gpus_{timestamp}.json"
    compression: "gzip"           # "none" or "gzip"

    # Credentials from environment
    credentials:
      access_key_env: AWS_ACCESS_KEY_ID
      secret_key_env: AWS_SECRET_ACCESS_KEY
      session_token_env: AWS_SESSION_TOKEN  # Optional

    # Advanced options
    storage_class: "STANDARD"     # STANDARD, GLACIER, etc.
    server_side_encryption: null  # AES256, aws:kms
    acl: "private"                # private, public-read, etc.
    metadata:                     # Custom metadata
      pipeline: "daily-export"
```

### HTTPS Webhooks

```yaml
outputs:
  - type: https
    url: "https://api.example.com/webhooks/gpu-data"
    method: "POST"                # POST, PUT, PATCH

    # Headers support env var substitution
    headers:
      Authorization: "Bearer ${API_TOKEN}"
      Content-Type: "application/json"

    # Batching
    batch_size: 100               # Instances per request (default: all)
    batch_delay: 1.0              # Delay between batches (seconds)

    # Retry configuration
    retry_attempts: 3             # Number of retries
    retry_delay: 5                # Initial delay (seconds)
    retry_backoff: 2.0            # Backoff multiplier
    retry_on_status: [500, 502, 503, 504]

    # SSL/TLS
    verify_ssl: true              # Verify certificates
    client_cert: null             # Client cert path
    client_key: null              # Client key path

    timeout: 30                   # Request timeout (seconds)
```

## CLI Usage

### Installation

For development:

```bash
cd collectors
uv pip install -e .
```

For production:

```bash
pip install gpuport-data-collectors
```

### Commands

```bash
# If virtual environment is activated
source .venv/bin/activate
gpuport-collectors export --config export.yaml --api-key YOUR_KEY
gpuport-collectors validate --config export.yaml

# Or without activating (using uv run)
uv run gpuport-collectors export --config export.yaml --api-key YOUR_KEY

# With environment variables
export RUNPOD_API_KEY=your_key
gpuport-collectors export --config export.yaml

# Verbose logging
gpuport-collectors export --config export.yaml --verbose
```

## Programmatic Usage

```python
from gpuport_collectors.export import execute_pipelines, load_export_config
from gpuport_collectors.collectors.runpod import RunPodCollector
from gpuport_collectors.config import CollectorConfig

# Load configuration
config = load_export_config("export.yaml")

# Collect instances
collector = RunPodCollector(config=CollectorConfig())
instances = await collector.fetch_instances()

# Execute pipelines
results = execute_pipelines(instances, config)

# Check results
for result in results:
    print(f"{result.pipeline_name}: {result.success}")
    print(f"  Input: {result.input_count}")
    print(f"  Filtered: {result.filtered_count}")
    print(f"  Outputs: {result.successful_outputs}/{result.output_count}")
    print(f"  Duration: {result.duration_seconds:.2f}s")
```

## Complete Example

```yaml
pipelines:
  # High-availability GPUs to S3
  - name: production-export
    filters:
      - field: availability
        operator: in
        values: [HIGH, MEDIUM]
      - field: price
        operator: lt
        value: 10.0

    transformer:
      format: json
      fields:
        provider: Provider
        accelerator_name: GPU
        price: PricePerHour
      pretty_print: true

    outputs:
      - type: s3
        bucket: "gpu-data-prod"
        prefix: "high-availability/"
        region: "us-east-1"
        filename_pattern: "{date}/gpus_{time}.json.gz"
        compression: "gzip"
        credentials:
          access_key_env: AWS_ACCESS_KEY_ID
          secret_key_env: AWS_SECRET_ACCESS_KEY

      - type: local
        path: "./backups"
        filename_pattern: "backup_{timestamp}.json"

  # EU regions CSV report
  - name: eu-regions-report
    filters:
      - field: region
        operator: regex
        value: "^eu-.*"

    transformer:
      format: csv
      fields:
        provider: Provider
        instance_type: Instance
        region: Region
        price: Price
      include_headers: true

    outputs:
      - type: local
        path: "./reports"
        filename_pattern: "eu-gpus_{date}.csv"

  # Daily metrics to webhook
  - name: daily-metrics
    transformer:
      format: metrics
      metrics:
        - name: total_instances
          type: count
        - name: avg_price
          type: avg
          field: price
        - name: providers
          type: unique
          field: provider
      include_timestamp: true

    outputs:
      - type: https
        url: "https://metrics.example.com/api/gpu-stats"
        headers:
          Authorization: "Bearer ${METRICS_API_TOKEN}"
        retry_attempts: 3
```

## Best Practices

1. **Use environment variables for secrets** - Never hardcode credentials
2. **Enable compression for S3** - Reduces storage costs
3. **Batch HTTPS requests** - Avoid overwhelming endpoints
4. **Test with `--validate-only`** - Verify config before running
5. **Use descriptive pipeline names** - Easier debugging and monitoring
6. **Include timestamps in filenames** - Avoid overwrite conflicts
7. **Monitor pipeline failures** - Check result.success in production

## Error Handling

- **Individual output failures** don't stop the pipeline
- **Pipeline failures** are tracked in `PipelineResult.error`
- **Disabled pipelines** are skipped (enabled=false)
- **Filtered count** shows how many instances passed filters
- **Timing metrics** help identify bottlenecks (filter_duration, transform_duration, output_duration)

## See Also

- [Configuration Schema](../src/gpuport_collectors/export/config.py)
- [Pipeline Implementation](../src/gpuport_collectors/export/pipeline.py)
- [Integration Tests](../tests/export/test_integration.py)
