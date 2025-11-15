# Configuration Examples

This directory contains example configuration files for GPUPort Collectors.

## Files

### Collector Configuration

- **`collector-config.yaml`** - Global collector settings (timeouts, retries, observability)
- **`runpod-provider.yaml`** - RunPod-specific provider configuration

### Export Pipeline Configuration

- **`export-basic.yaml`** - Simple export examples (local JSON and CSV)
- **`export-advanced.yaml`** - Advanced pipelines with filtering, multiple outputs, and metrics

## Quick Start

### 1. Basic Collection (RunPod)

Collect GPU data from RunPod without export:

```bash
# Activate virtual environment
source .venv/bin/activate

# Set your API key
export RUNPOD_API_KEY=your_api_key

# Run the collector (displays results in console)
gpuport-collectors run runpod
```

### 2. Collection + Basic Export

Collect data and export using simple pipelines:

```bash
# Activate virtual environment
source .venv/bin/activate

# Set your API key
export RUNPOD_API_KEY=your_api_key

# Run collection with basic export
gpuport-collectors run runpod --export-config examples/export-basic.yaml
```

This will:
- Export all GPU instances to `./data/exports/gpu-instances_2025-01-15.json`
- Export available GPUs to `./data/reports/available-gpus_2025-01-15.csv`

### 3. Collection + Advanced Export (Production)

Use advanced pipelines with multiple outputs (S3, webhooks, metrics):

```bash
# Activate virtual environment
source .venv/bin/activate

# Set all required environment variables
export RUNPOD_API_KEY=your_api_key
export AWS_ACCESS_KEY_ID=your_aws_key
export AWS_SECRET_ACCESS_KEY=your_aws_secret
export API_TOKEN=your_webhook_token
export METRICS_TOKEN=your_metrics_token

# Validate configuration first (optional)
gpuport-collectors validate --config examples/export-advanced.yaml

# Run with advanced export
gpuport-collectors run runpod --export-config examples/export-advanced.yaml
```

## Customization

Copy any example file and modify it for your needs:

```bash
# Copy and customize
cp examples/export-basic.yaml my-export-config.yaml

# Edit the file
vim my-export-config.yaml

# Use your custom config
gpuport-collectors export --config my-export-config.yaml
```

## Environment Variables

Common environment variables used in examples:

- `RUNPOD_API_KEY` - RunPod API key for data collection
- `AWS_ACCESS_KEY_ID` - AWS access key for S3 output
- `AWS_SECRET_ACCESS_KEY` - AWS secret key for S3 output
- `AWS_SESSION_TOKEN` - (Optional) AWS session token
- `API_TOKEN` - Generic API token for webhook authentication
- `METRICS_TOKEN` - API token for metrics endpoint
- `HONEYCOMB_API_KEY` - (Optional) Honeycomb.io API key for observability

## Documentation

For complete documentation, see:

- [Export Pipeline Documentation](../docs/EXPORTS.md)
- [Collector Configuration](../src/gpuport_collectors/config/defaults.yaml)

## Support

For issues or questions, please open an issue on GitHub.
