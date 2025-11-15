# Export Pipeline Configuration Design

## Overview

Export pipelines allow flexible filtering, transformation, and routing of GPU instance collection data to multiple destinations. Pipelines are defined once and can be reused across all collectors.

## YAML Configuration Format

### Complete Example

```yaml
# export_config.yaml - Export pipeline configuration
version: "1.0"

# Global defaults (optional)
defaults:
  # Default transformer settings
  transformer:
    format: json
    include_metadata: true
    timestamp_format: iso8601

  # Default output settings
  outputs:
    compression: gzip
    retry_attempts: 3
    timeout: 30

# Pipeline definitions
pipelines:
  # Pipeline 1: High-availability GPUs to production S3
  - name: high_availability_production
    description: "Export high-availability GPUs (Medium/High) to production S3 bucket"
    enabled: true

    # Filters define which instances to include
    filters:
      - field: availability
        operator: in
        values: [HIGH, MEDIUM]
      - field: price
        operator: lt
        value: 5.0

    # Transformer defines output format and field mapping
    transformer:
      format: json
      fields:
        # Map: source_field -> output_alias
        provider: Provider
        instance_type: InstanceType
        accelerator_name: GPUType
        accelerator_count: GPUCount
        accelerator_mem_gib: GPUMemoryGB
        region: Region
        availability: AvailabilityLevel
        price: OnDemandPriceUSD
        spot_price: SpotPriceUSD
        v_cpus: vCPUs
        memory_gib: SystemMemoryGB
        collected_at: CollectedTimestamp
      include_raw_data: false
      flatten_nested: true

    # Multiple output targets
    outputs:
      - type: s3
        name: production_s3
        bucket: gpu-availability-prod
        prefix: high-availability/
        region: us-east-1
        filename_pattern: "{provider}_{date}_{time}.json.gz"
        compression: gzip
        credentials:
          access_key_env: AWS_ACCESS_KEY_ID
          secret_key_env: AWS_SECRET_ACCESS_KEY

      - type: local
        name: local_backup
        path: ./exports/high-availability
        filename_pattern: "{provider}_{date}_{time}.json"
        create_dirs: true

  # Pipeline 2: EU regions CSV export
  - name: eu_regions_csv
    description: "Export EU region GPUs as CSV for analysis"
    enabled: true

    filters:
      - field: region
        operator: regex
        value: "^(EU|eu)-.*"
      - field: availability
        operator: ne
        value: NOT_AVAILABLE

    transformer:
      format: csv
      fields:
        provider: Provider
        instance_type: Instance
        accelerator_name: GPU
        region: Datacenter
        availability: Status
        price: Price
        spot_price: SpotPrice
      include_headers: true
      delimiter: ","
      quote_char: "\""

    outputs:
      - type: local
        path: ./exports/eu-regions
        filename_pattern: "eu_gpus_{date}.csv"

      - type: https
        name: analytics_webhook
        url: https://analytics.example.com/api/gpu-data
        method: POST
        headers:
          Authorization: "Bearer ${ANALYTICS_API_TOKEN}"
          Content-Type: "text/csv"
        retry_on_failure: true

  # Pipeline 3: RunPod-specific export with all data
  - name: runpod_full_export
    description: "Export all RunPod data with raw metadata"
    enabled: true

    filters:
      - field: provider
        operator: eq
        value: RunPod

    transformer:
      format: json
      # If no fields specified, include all fields
      include_raw_data: true
      pretty_print: true

    outputs:
      - type: local
        path: ./exports/runpod
        filename_pattern: "runpod_full_{timestamp}.json"

      - type: s3
        bucket: gpu-data-archive
        prefix: runpod/full/
        region: us-west-2
        filename_pattern: "{date}/runpod_{time}.json.gz"
        compression: gzip

  # Pipeline 4: Low-cost GPUs to multiple destinations
  - name: budget_gpus
    description: "Export budget-friendly GPUs (< $2/hr)"
    enabled: true

    filters:
      - field: price
        operator: lt
        value: 2.0
      - field: price
        operator: gt
        value: 0.0  # Exclude free tier / errors
      - field: availability
        operator: in
        values: [HIGH, MEDIUM, LOW]

    transformer:
      format: json
      fields:
        provider: provider
        instance_type: instance_type
        accelerator_name: gpu_name
        region: region
        price: price_per_hour
        availability: availability
        collected_at: timestamp

    outputs:
      - type: local
        path: ./exports/budget
        filename_pattern: "budget_gpus_{date}.json"

      - type: https
        url: https://api.example.com/v1/gpu-prices
        method: POST
        headers:
          X-API-Key: "${API_KEY}"
        batch_size: 100  # Send in batches of 100 instances

      - type: s3
        bucket: budget-gpu-tracker
        prefix: daily-exports/
        filename_pattern: "{date}.json.gz"
        compression: gzip

  # Pipeline 5: Metrics and summary data
  - name: collection_metrics
    description: "Export collection run metrics"
    enabled: true

    # No filters = process all instances for aggregation

    transformer:
      format: json
      # Special transformer type for metrics
      type: metrics
      metrics:
        - name: total_instances
          type: count
        - name: instances_by_provider
          type: count
          group_by: provider
        - name: instances_by_availability
          type: count
          group_by: availability
        - name: average_price
          type: avg
          field: price
        - name: min_price
          type: min
          field: price
        - name: max_price
          type: max
          field: price
        - name: providers_list
          type: unique
          field: provider
      include_timestamp: true

    outputs:
      - type: https
        url: https://metrics.example.com/api/gpu-collection
        method: POST
        headers:
          Authorization: "Bearer ${METRICS_TOKEN}"

      - type: local
        path: ./exports/metrics
        filename_pattern: "metrics_{timestamp}.json"

# Output target templates (reusable configurations)
output_templates:
  production_s3:
    type: s3
    bucket: gpu-availability-prod
    region: us-east-1
    compression: gzip
    credentials:
      access_key_env: AWS_ACCESS_KEY_ID
      secret_key_env: AWS_SECRET_ACCESS_KEY

  analytics_webhook:
    type: https
    url: https://analytics.example.com/api/gpu-data
    method: POST
    headers:
      Authorization: "Bearer ${ANALYTICS_API_TOKEN}"
    retry_attempts: 3
    timeout: 30
```

## Configuration Reference

### Pipeline Structure

```yaml
pipelines:
  - name: string              # Unique pipeline identifier
    description: string       # Human-readable description
    enabled: boolean          # Enable/disable pipeline (default: true)
    filters: []               # List of filter conditions
    transformer: {}           # Transformation configuration
    outputs: []               # List of output targets
```

### Filters

#### Filter Operators

```yaml
filters:
  # Equality
  - field: provider
    operator: eq              # equals
    value: RunPod

  - field: provider
    operator: ne              # not equals
    value: AWS

  # Comparison (numeric)
  - field: price
    operator: lt              # less than
    value: 5.0

  - field: price
    operator: lte             # less than or equal
    value: 5.0

  - field: price
    operator: gt              # greater than
    value: 0.5

  - field: price
    operator: gte             # greater than or equal
    value: 0.5

  # Set membership
  - field: availability
    operator: in              # value in list
    values: [HIGH, MEDIUM]

  - field: region
    operator: not_in          # value not in list
    values: [DEPRECATED_1, DEPRECATED_2]

  # Pattern matching
  - field: region
    operator: regex           # matches regex pattern
    value: "^EU-.*"

  - field: instance_type
    operator: contains        # contains substring
    value: "A100"

  - field: instance_type
    operator: starts_with     # starts with prefix
    value: "NVIDIA"

  # Null checks
  - field: spot_price
    operator: is_null         # field is null

  - field: spot_price
    operator: is_not_null     # field is not null

  # Range (numeric)
  - field: price
    operator: between         # value between min and max (inclusive)
    min: 1.0
    max: 5.0
```

#### Available Fields for Filtering

All GPUInstance model fields:
- `provider` (string)
- `instance_type` (string)
- `accelerator_name` (string)
- `accelerator_count` (int)
- `accelerator_mem_gib` (float, nullable)
- `region` (string)
- `availability` (enum: NOT_AVAILABLE, LOW, MEDIUM, HIGH)
- `quantity` (int)
- `price` (float)
- `spot_price` (float)
- `v_cpus` (int, nullable)
- `memory_gib` (float, nullable)
- `collected_at` (int, unix timestamp)

### Transformer

#### JSON Format

```yaml
transformer:
  format: json

  # Field mapping (source -> output alias)
  fields:
    provider: Provider
    instance_type: InstanceType
    price: PricePerHour

  # Options
  include_raw_data: false     # Include raw_data field (default: false)
  include_metadata: true      # Include collection metadata (default: true)
  flatten_nested: true        # Flatten nested objects (default: false)
  pretty_print: false         # Pretty-print JSON (default: false)
  null_handling: omit         # omit|null|empty - how to handle null values
```

#### CSV Format

```yaml
transformer:
  format: csv

  # Field mapping (order matters for CSV!)
  fields:
    provider: Provider
    instance_type: Instance
    price: Price

  # CSV options
  include_headers: true       # Include header row (default: true)
  delimiter: ","              # Field delimiter (default: ",")
  quote_char: "\""            # Quote character (default: "\"")
  escape_char: "\\"           # Escape character (default: "\\")
  line_terminator: "\n"       # Line terminator (default: "\n")
  null_value: "NULL"          # How to represent null (default: empty string)
```

#### Metrics Transformer

```yaml
transformer:
  format: json
  type: metrics

  metrics:
    # Count aggregations
    - name: total_count
      type: count

    - name: by_provider
      type: count
      group_by: provider

    # Numeric aggregations
    - name: avg_price
      type: avg
      field: price

    - name: min_price
      type: min
      field: price

    - name: max_price
      type: max
      field: price

    - name: sum_quantity
      type: sum
      field: quantity

    # Distinct values
    - name: unique_providers
      type: unique
      field: provider

    - name: unique_regions
      type: unique
      field: region

  include_timestamp: true
  include_collection_info: true  # Include collector name, run ID, etc.
```

### Output Targets

#### Local Filesystem

```yaml
outputs:
  - type: local
    name: local_export        # Optional name for logging
    path: ./exports/data      # Output directory
    filename_pattern: "{provider}_{date}_{time}.json"
    create_dirs: true         # Create directories if missing (default: true)
    overwrite: false          # Overwrite existing files (default: false)
    compression: none         # none|gzip|bzip2|xz (default: none)
```

#### S3-Compatible Storage

```yaml
outputs:
  - type: s3
    name: s3_export

    # S3 configuration
    bucket: my-bucket-name
    prefix: path/to/data/     # Optional prefix (default: "")
    region: us-east-1         # AWS region
    endpoint_url: null        # Custom endpoint for S3-compatible (MinIO, etc.)

    # File configuration
    filename_pattern: "{provider}_{timestamp}.json"
    compression: gzip         # none|gzip (default: none)

    # Credentials (environment variables)
    credentials:
      access_key_env: AWS_ACCESS_KEY_ID
      secret_key_env: AWS_SECRET_ACCESS_KEY
      session_token_env: AWS_SESSION_TOKEN  # Optional

    # Advanced options
    storage_class: STANDARD   # STANDARD|GLACIER|etc. (default: STANDARD)
    server_side_encryption: AES256  # AES256|aws:kms|null (default: null)
    acl: private              # private|public-read|etc. (default: private)
    metadata:                 # Custom metadata
      pipeline: high_availability
      environment: production
```

#### HTTPS Endpoint

```yaml
outputs:
  - type: https
    name: webhook_export

    # HTTP configuration
    url: https://api.example.com/v1/gpu-data
    method: POST              # POST|PUT|PATCH (default: POST)

    # Headers (supports environment variable substitution)
    headers:
      Authorization: "Bearer ${API_TOKEN}"
      Content-Type: "application/json"
      X-Pipeline: "high-availability"

    # Request options
    batch_size: 100           # Send instances in batches (default: all at once)
    batch_delay: 1.0          # Delay between batches in seconds (default: 0)
    timeout: 30               # Request timeout in seconds (default: 30)

    # Retry configuration
    retry_attempts: 3         # Number of retries (default: 3)
    retry_delay: 5            # Initial retry delay in seconds (default: 5)
    retry_backoff: 2.0        # Backoff multiplier (default: 2.0)
    retry_on_status: [500, 502, 503, 504]  # Retry on these status codes

    # SSL/TLS
    verify_ssl: true          # Verify SSL certificates (default: true)
    client_cert: null         # Path to client certificate (optional)
    client_key: null          # Path to client key (optional)
```

### Filename Patterns

Supported placeholders:
- `{provider}` - Provider name (e.g., "RunPod")
- `{date}` - Date in YYYY-MM-DD format
- `{time}` - Time in HH-MM-SS format
- `{timestamp}` - Unix timestamp
- `{datetime}` - ISO 8601 datetime (YYYY-MM-DDTHH-MM-SS)
- `{pipeline}` - Pipeline name
- `{format}` - Output format (json, csv)

Examples:
- `{provider}_{date}.json` → `RunPod_2025-11-14.json`
- `data_{timestamp}.csv.gz` → `data_1700000000.csv.gz`
- `{pipeline}/{date}/{time}.json` → `high_availability/2025-11-14/14-30-00.json`

## Usage Examples

### Running with Export Configuration

```bash
# Run collector with export configuration
gpuport-collector run runpod --export-config export_config.yaml

# Run with specific pipelines only
gpuport-collector run runpod \
  --export-config export_config.yaml \
  --pipelines high_availability_production,eu_regions_csv

# Dry run (validate config without exporting)
gpuport-collector run runpod \
  --export-config export_config.yaml \
  --dry-run
```

### Programmatic Usage

```python
from gpuport_collectors.export import ExportPipeline, load_export_config

# Load configuration
config = load_export_config("export_config.yaml")

# Get instances from collector
instances = await collector.fetch_instances()

# Run all pipelines
for pipeline_config in config.pipelines:
    if pipeline_config.enabled:
        pipeline = ExportPipeline(pipeline_config)
        await pipeline.process(instances)
```

## Advanced Features

### Environment Variable Substitution

Values can reference environment variables using `${VAR_NAME}` syntax:

```yaml
outputs:
  - type: https
    url: https://api.example.com/v1/data
    headers:
      Authorization: "Bearer ${API_TOKEN}"
      X-Environment: "${ENVIRONMENT}"
```

### Conditional Pipelines

```yaml
pipelines:
  - name: production_only
    enabled: "${ENVIRONMENT}" == "production"  # Only run in production
    # ...
```

### Pipeline Dependencies

```yaml
pipelines:
  - name: aggregate_metrics
    depends_on: [high_availability_production, eu_regions_csv]
    # Runs after specified pipelines complete
```

### Error Handling

```yaml
defaults:
  error_handling:
    on_filter_error: skip      # skip|fail|log (default: skip)
    on_transform_error: skip   # skip|fail|log (default: skip)
    on_output_error: log       # skip|fail|log (default: log)
    continue_on_error: true    # Continue processing other pipelines on error
```

## Best Practices

1. **Use descriptive pipeline names** - Makes logs and monitoring easier
2. **Enable/disable with `enabled` flag** - Better than commenting out
3. **Use output templates** - Reduce duplication for common destinations
4. **Test with dry-run first** - Validate configuration before real export
5. **Use compression for large datasets** - Especially for S3 uploads
6. **Batch HTTPS requests** - Avoid overwhelming endpoints
7. **Include metadata in exports** - Helps with debugging and auditing
8. **Use environment variables for secrets** - Never hardcode credentials
9. **Monitor pipeline failures** - Set up alerts for output errors
10. **Version your config** - Track changes to export configurations

## Configuration Validation

The system validates:
- Required fields are present
- Field types are correct
- Operator-value combinations are valid
- Output configurations are complete
- Filename patterns are valid
- Referenced environment variables exist (warning only)
- Filter fields exist in GPUInstance model
- Transformer field mappings are valid

## Future Enhancements

Potential additions:
- Database outputs (PostgreSQL, MongoDB, etc.)
- Message queue outputs (Kafka, RabbitMQ, etc.)
- Custom transformer plugins
- Conditional transformations
- Data enrichment from external sources
- Schema validation for outputs
- Incremental exports (only new/changed instances)
