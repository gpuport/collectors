"""Command-line interface for GPU data collection and export."""

import asyncio
import logging
import sys
from pathlib import Path

import click

from gpuport_collectors.collectors.runpod import RunPodCollector
from gpuport_collectors.export.loader import load_export_config, validate_config
from gpuport_collectors.export.pipeline import execute_pipelines
from gpuport_collectors.observability import get_observability_manager

# Get structured logger
obs = get_observability_manager()
logger = obs.get_logger("cli")


def print_summary(message: str) -> None:
    """Print a summary message directly to stdout (not as JSON log).

    Used for human-readable summary reports that should not be formatted as JSON.
    """
    print(message)  # noqa: T201


@click.group()
def cli() -> None:
    """GPU data collection and export CLI."""


@cli.group()
def run() -> None:
    """Run data collection from GPU providers."""


@run.command()
@click.option(
    "--export-config",
    "-e",
    type=click.Path(exists=True, path_type=Path),
    help="Path to export pipeline configuration YAML file",
)
@click.option(
    "--api-key",
    "-k",
    envvar="RUNPOD_API_KEY",
    help="RunPod API key (or set RUNPOD_API_KEY env var)",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Enable verbose logging",
)
def runpod(
    export_config: Path | None,
    api_key: str | None,
    verbose: bool,
) -> None:
    """Collect GPU data from RunPod.

    This command:
    1. Collects GPU instance data from RunPod API
    2. Optionally exports data using configured pipelines

    Example:
        gpuport-collectors run runpod --api-key YOUR_KEY
        gpuport-collectors run runpod --export-config examples/export-basic.yaml
        gpuport-collectors run runpod --export-config export.yaml --api-key YOUR_KEY
    """
    # Configure logging level
    if verbose:
        logger.logger.setLevel(logging.DEBUG)
        logger.debug("Verbose logging enabled")

    # Validate API key
    if not api_key:
        error_msg = "RunPod API key required. Set RUNPOD_API_KEY or use --api-key"
        logger.error(error_msg)
        click.echo(f"Error: {error_msg}", err=True)
        sys.exit(1)

    # Set API key in environment for RunPodCollector
    import os

    os.environ["RUNPOD_API_KEY"] = api_key

    # Load collector configuration
    from gpuport_collectors.config import CollectorConfig

    collector_config = CollectorConfig()

    # Create collector instance
    collector = RunPodCollector(config=collector_config)

    # Collect GPU instances
    logger.debug("Collecting GPU instances from RunPod...")
    instances = asyncio.run(collector.fetch_instances())
    logger.info(f"Collected {len(instances)} GPU instance(s)")

    if not instances:
        logger.warning("No instances collected")

    # If export config provided, run export pipelines
    if export_config:
        logger.debug(f"Loading export configuration from {export_config}")
        export_cfg = load_export_config(export_config)
        logger.debug(f"Loaded configuration with {len(export_cfg.pipelines)} pipeline(s)")

        # Validate configuration and show warnings
        warnings = validate_config(export_cfg)
        if warnings:
            logger.warning("Configuration validation warnings:")
            for warning in warnings:
                logger.warning(f"  • {warning}")

        # Execute export pipelines
        logger.debug("Executing export pipelines...")
        results = execute_pipelines(instances, export_cfg)

        # Report results
        print_summary("\n" + "=" * 60)
        print_summary("PIPELINE EXECUTION SUMMARY")
        print_summary("=" * 60)

        total_successful = 0
        total_failed = 0

        for result in results:
            if not result.enabled:
                print_summary(f"\n{result.pipeline_name}: DISABLED")
                continue

            status = "✓ SUCCESS" if result.success else "✗ FAILED"
            print_summary(f"\n{result.pipeline_name}: {status}")
            print_summary(f"  Input: {result.input_count} instances")
            print_summary(f"  Filtered: {result.filtered_count} instances")
            print_summary(
                f"  Outputs: {result.successful_outputs} successful, {result.failed_outputs} failed"
            )

            if result.duration_seconds is not None:
                print_summary(f"  Duration: {result.duration_seconds:.3f}s")

            if result.error:
                print_summary(f"  Error: {result.error}")
                total_failed += 1
            else:
                total_successful += 1

            # Show output details
            if result.outputs:
                print_summary("  Output details:")
                for output in result.outputs:
                    output_status = "✓" if output.get("success") else "✗"
                    output_name = output.get("name") or output.get("type", "unknown")
                    print_summary(f"    {output_status} {output_name}")
                    if output.get("path"):
                        print_summary(f"      Path: {output['path']}")
                    if not output.get("success"):
                        print_summary(f"      Error: {output.get('error')}")

        print_summary("\n" + "=" * 60)
        print_summary(
            f"TOTAL: {total_successful} successful, {total_failed} failed out of {len([r for r in results if r.enabled])} enabled pipelines"
        )
        print_summary("=" * 60 + "\n")

        # Exit with appropriate code
        if total_failed > 0:
            sys.exit(1)
    else:
        # Just display collected data
        logger.info("\nCollected GPU instances:")
        for instance in instances[:10]:  # Show first 10
            logger.info(
                f"  • {instance.provider} - {instance.instance_type} - "
                f"{instance.accelerator_name} x{instance.accelerator_count} - "
                f"${instance.price:.2f}/hr - {instance.availability.value}"
            )
        if len(instances) > 10:
            logger.info(f"  ... and {len(instances) - 10} more")


@cli.command()
@click.option(
    "--config",
    "-c",
    type=click.Path(exists=True, path_type=Path),
    required=True,
    help="Path to export configuration YAML file",
)
@click.option(
    "--provider",
    "-p",
    type=click.Choice(["runpod"], case_sensitive=False),
    default="runpod",
    help="GPU provider to collect from (default: runpod)",
)
@click.option(
    "--api-key",
    "-k",
    envvar="RUNPOD_API_KEY",
    help="Provider API key (or set RUNPOD_API_KEY env var)",
)
@click.option(
    "--validate-only",
    is_flag=True,
    help="Only validate configuration without executing pipelines",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Enable verbose logging",
)
def export(
    config: Path,
    provider: str,
    api_key: str | None,
    validate_only: bool,
    verbose: bool,
) -> None:
    """Collect GPU data and execute export pipelines (legacy command).

    This command:
    1. Collects GPU instance data from the specified provider
    2. Loads the export configuration
    3. Executes all enabled export pipelines
    4. Outputs results and metrics

    Example:
        gpuport-collectors export --config export.yaml --api-key YOUR_KEY
    """
    # Configure logging level
    if verbose:
        logger.logger.setLevel(logging.DEBUG)
        logger.debug("Verbose logging enabled")

    logger.debug(f"Loading export configuration from {config}")

    try:
        # Load and validate configuration
        export_config = load_export_config(config)
        logger.debug(f"Loaded configuration with {len(export_config.pipelines)} pipeline(s)")

        # Validate configuration and show warnings
        warnings = validate_config(export_config)
        if warnings:
            logger.warning("Configuration validation warnings:")
            click.echo("Configuration validation warnings:", err=True)
            for warning in warnings:
                logger.warning(f"  • {warning}")
                click.echo(f"  • {warning}", err=True)

        if validate_only:
            logger.info("Configuration validation complete (--validate-only mode)")
            logger.info("✓ Configuration is valid")
            click.echo("Configuration validation complete (--validate-only mode)")
            click.echo("✓ Configuration is valid")
            sys.exit(0)

        # Create collector instance
        if provider == "runpod":
            if not api_key:
                logger.error("RunPod API key required. Set RUNPOD_API_KEY or use --api-key")
                sys.exit(1)
            # Set API key in environment for RunPodCollector
            import os

            os.environ["RUNPOD_API_KEY"] = api_key
            from gpuport_collectors.config import CollectorConfig

            collector = RunPodCollector(config=CollectorConfig())
        else:
            logger.error(f"Unsupported provider: {provider}")
            sys.exit(1)

        # Collect GPU instances
        logger.debug(f"Collecting GPU instances from {provider}...")
        instances = asyncio.run(collector.fetch_instances())
        logger.info(f"Collected {len(instances)} GPU instance(s)")

        if not instances:
            logger.warning("No instances collected - pipelines will process empty data")

        # Execute export pipelines
        logger.debug("Executing export pipelines...")
        results = execute_pipelines(instances, export_config)

        # Report results
        print_summary("\n" + "=" * 60)
        print_summary("PIPELINE EXECUTION SUMMARY")
        print_summary("=" * 60)

        total_successful = 0
        total_failed = 0

        for result in results:
            if not result.enabled:
                print_summary(f"\n{result.pipeline_name}: DISABLED")
                continue

            status = "✓ SUCCESS" if result.success else "✗ FAILED"
            print_summary(f"\n{result.pipeline_name}: {status}")
            print_summary(f"  Input: {result.input_count} instances")
            print_summary(f"  Filtered: {result.filtered_count} instances")
            print_summary(
                f"  Outputs: {result.successful_outputs} successful, {result.failed_outputs} failed"
            )

            if result.duration_seconds is not None:
                print_summary(f"  Duration: {result.duration_seconds:.3f}s")
                if result.filter_duration is not None:
                    print_summary(f"    Filter: {result.filter_duration:.3f}s")
                    print_summary(f"    Transform: {result.transform_duration:.3f}s")
                    print_summary(f"    Output: {result.output_duration:.3f}s")

            if result.error:
                print_summary(f"  Error: {result.error}")
                total_failed += 1
            else:
                total_successful += 1

            # Show output details
            if result.outputs:
                print_summary("  Output details:")
                for output in result.outputs:
                    output_status = "✓" if output.get("success") else "✗"
                    output_name = output.get("name") or output.get("type", "unknown")
                    print_summary(f"    {output_status} {output_name}")
                    if output.get("path"):
                        print_summary(f"      Path: {output['path']}")
                    elif output.get("key"):
                        print_summary(f"      S3: {output.get('bucket')}/{output['key']}")
                    elif output.get("url"):
                        print_summary(f"      URL: {output['url']}")
                        if output.get("total_requests"):
                            print_summary(
                                f"      Requests: {output['successful_requests']}/{output['total_requests']}"
                            )
                    if not output.get("success"):
                        print_summary(f"      Error: {output.get('error')}")

        print_summary("\n" + "=" * 60)
        print_summary(
            f"TOTAL: {total_successful} successful, {total_failed} failed out of {len([r for r in results if r.enabled])} enabled pipelines"
        )
        print_summary("=" * 60 + "\n")

        # Exit with appropriate code
        if total_failed > 0:
            sys.exit(1)

    except Exception as e:
        # Pass exception to logger when verbose, enabling stack trace logging
        logger.error("Export failed", error=e if verbose else None)
        sys.exit(1)


@cli.command()
@click.option(
    "--config",
    "-c",
    type=click.Path(exists=True, path_type=Path),
    required=True,
    help="Path to export configuration YAML file",
)
def validate(config: Path) -> None:
    """Validate an export configuration file.

    Checks the configuration for:
    - Valid YAML syntax
    - Required fields
    - Type correctness
    - Common mistakes (e.g., missing filters, no outputs)

    Example:
        gpuport-collectors validate --config export.yaml
    """
    logger.info(f"Validating configuration: {config}")

    try:
        # Load configuration
        export_config = load_export_config(config)
        logger.info("✓ Configuration loaded successfully")
        logger.info(f"  Pipelines: {len(export_config.pipelines)}")

        # Validate and show warnings
        warnings = validate_config(export_config)

        if warnings:
            logger.warning("\nValidation warnings:")
            for warning in warnings:
                logger.warning(f"  • {warning}")
        else:
            logger.info("✓ No validation warnings")

        # Show pipeline summary
        print_summary("\nPipeline summary:")
        for pipeline in export_config.pipelines:
            enabled_status = "✓ enabled" if pipeline.enabled else "✗ disabled"
            print_summary(f"  {pipeline.name}: {enabled_status}")
            print_summary(f"    Filters: {len(pipeline.filters) if pipeline.filters else 0}")
            # Get transformer format (all transformers have a format field)
            transformer_format = getattr(pipeline.transformer, "format", "unknown")
            print_summary(f"    Transformer: {transformer_format}")
            print_summary(f"    Outputs: {len(pipeline.outputs)}")
            for output in pipeline.outputs:
                print_summary(f"      - {output.type}: {output.name or 'unnamed'}")

        print_summary("\n✓ Configuration is valid")

    except Exception as e:
        logger.error(f"Validation failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    cli()
