# Configuration Guide

## Configuration Methods

GPU Port collectors support three configuration methods with priority ordering.

### Priority Order (Highest to Lowest)

1. **Command-line arguments** - Highest priority
2. **Environment variables** - Middle priority
3. **.env file** - Lowest priority (defaults)

**Example**: If `RUNPOD_API_KEY` is set in all three places:
- CLI flag `--api-key` wins over environment variable
- Environment variable wins over `.env` file value
- `.env` file provides the default if neither CLI nor env var is set

---

## 1. Command-Line Arguments

Pass configuration directly via CLI flags.

### Collection Commands

```bash
# RunPod with API key
gpuport-collectors run runpod --api-key YOUR_KEY

# Lambda Labs with export
gpuport-collectors run lambdalabs \
  --api-key YOUR_KEY \
  --export-config examples/export-backend.yaml

# Verbose mode for debugging
gpuport-collectors run runpod --api-key YOUR_KEY --verbose

# Quiet mode (errors only)
gpuport-collectors run runpod --api-key YOUR_KEY --quiet
```

### Export Commands

```bash
# Validate export configuration
gpuport-collectors validate --config export.yaml

# Export with specific provider
gpuport-collectors export \
  --config export.yaml \
  --provider runpod \
  --api-key YOUR_KEY
```

### Common Flags

| Flag | Short | Description |
|------|-------|-------------|
| `--api-key` | `-k` | Provider API key |
| `--export-config` | `-e` | Path to export YAML file |
| `--verbose` | `-v` | Enable debug logging |
| `--quiet` | `-q` | Suppress non-error output |
| `--config` | `-c` | Export configuration path |

---

## 2. Environment Variables

Set configuration in your shell or CI/CD environment.

### Setting Environment Variables

**In Shell (Linux/macOS)**:
```bash
export RUNPOD_API_KEY=your_key_here
export LAMBDA_API_KEY=your_key_here
gpuport-collectors run runpod
```

**In Shell (Windows PowerShell)**:
```powershell
$env:RUNPOD_API_KEY="your_key_here"
$env:LAMBDA_API_KEY="your_key_here"
gpuport-collectors run runpod
```

**Inline (Single Command)**:
```bash
RUNPOD_API_KEY=your_key gpuport-collectors run runpod
```

### Provider API Keys

| Variable | Provider | Required | Description |
|----------|----------|----------|-------------|
| `RUNPOD_API_KEY` | RunPod | Yes | RunPod GraphQL API authentication |
| `LAMBDA_API_KEY` | Lambda Labs | Yes | Lambda Labs REST API (HTTP Basic Auth) |
| `CUDO_API_KEY` | Cudo Compute | Yes | Cudo REST API Bearer token |
| `NOVITA_API_KEY` | Novita AI | Yes | Novita SDK API key |

### Observability (Optional)

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `HONEYCOMB_API_KEY` | No | None | Honeycomb.io API key for OpenTelemetry traces/logs |

### HTTP Export (User-Defined)

These are custom variables you define in your export YAML using `${VAR}` syntax:

| Variable | Common Usage | Description |
|----------|--------------|-------------|
| `API_TOKEN` | Authentication | Bearer token for HTTP endpoints |
| Any custom name | Headers, URLs | Any `${VAR_NAME}` in export config |

### S3/R2 Export (Optional)

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `AWS_ACCESS_KEY_ID` | Conditional* | None | AWS/R2 access key |
| `AWS_SECRET_ACCESS_KEY` | Conditional* | None | AWS/R2 secret key |
| `AWS_SESSION_TOKEN` | No | None | AWS temporary session token |

*Required only if not using IAM roles or AWS CLI credentials

---

## 3. .env File (Recommended)

The **easiest and safest** way to manage configuration.

### Quick Start

1. **Copy the example file**:
   ```bash
   cp .env.example .env
   ```

2. **Edit `.env` with your values**:
   ```bash
   # GPU Provider API Keys
   RUNPOD_API_KEY=your_runpod_api_key_here
   LAMBDA_API_KEY=your_lambda_api_key_here

   # HTTP Export
   GPUPORT_INGEST_URL=https://api.example.com/ingest
   API_TOKEN=your_secret_token
   ```

3. **Run commands** (automatically loads `.env`):
   ```bash
   gpuport-collectors run runpod
   ```

---

## Export Pipeline Configuration

Export pipelines are configured via YAML files.

### Basic Export Config

```yaml
# examples/export-basic.yaml
pipelines:
  - name: backend-export
    enabled: true

    transformer:
      format: json
      fields:
        provider: provider
        accelerator_name: gpu_model
        price: price_usd

    outputs:
      - type: https
        url: "${GPUPORT_INGEST_URL}"
        method: POST
        headers:
          Authorization: "Bearer ${API_TOKEN}"
        batch_size: 100
        retry_attempts: 3
```

### Environment Variable Substitution

Use `${VAR_NAME}` syntax in YAML for any string value:

```yaml
# URL
url: "${EXPORT_URL:-http://localhost:3000}"  # With default

# Headers
headers:
  Authorization: "Bearer ${API_TOKEN}"
  X-API-Key: "${API_KEY}"

# Paths
path: "${DATA_DIR}/exports"

# S3 credentials
credentials:
  access_key_env: "AWS_ACCESS_KEY_ID"  # Variable name (not value)
  secret_key_env: "AWS_SECRET_ACCESS_KEY"
```

**Error Handling**: If a referenced variable is not set, you'll get a clear error:
```
ConfigLoadError: Environment variable '${API_TOKEN}' referenced in
configuration but not defined in environment
```
