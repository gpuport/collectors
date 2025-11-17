"""Tests for CLI commands and output formatting."""

from pathlib import Path
from unittest.mock import patch

from click.testing import CliRunner

from gpuport_collectors.cli import cli, print_summary


class TestPrintSummary:
    """Tests for the print_summary helper function."""

    def test_print_summary_outputs_to_stdout(self, capsys):
        """Test that print_summary outputs plain text to stdout."""
        message = "Test summary message"
        print_summary(message)

        captured = capsys.readouterr()
        assert captured.out == f"{message}\n"
        assert captured.err == ""

    def test_print_summary_not_json_formatted(self, capsys):
        """Test that print_summary does NOT output JSON format."""
        message = "Pipeline execution complete"
        print_summary(message)

        captured = capsys.readouterr()
        # Should be plain text, not JSON with timestamp/level
        assert "timestamp" not in captured.out
        assert "level" not in captured.out
        assert captured.out == f"{message}\n"

    def test_print_summary_with_special_characters(self, capsys):
        """Test print_summary handles special characters correctly."""
        message = "✓ SUCCESS: 100% complete"
        print_summary(message)

        captured = capsys.readouterr()
        assert message in captured.out


class TestRunPodCommand:
    """Tests for the runpod command."""

    def test_runpod_missing_api_key_error(self):
        """Test runpod command fails with clear error when API key missing."""
        runner = CliRunner()
        result = runner.invoke(cli, ["run", "runpod"])

        # Should exit with error code when API key is missing
        assert result.exit_code == 1
        # Error message should appear in command output
        assert "RunPod API key required" in result.output

    def test_runpod_verbose_flag_enables_debug(self):
        """Test --verbose flag enables debug logging."""
        runner = CliRunner()
        with (
            patch("gpuport_collectors.cli.RunPodCollector"),
            patch("gpuport_collectors.cli.asyncio.run") as mock_run,
        ):
            # Mock the collector to return empty instances
            mock_run.return_value = []

            result = runner.invoke(cli, ["run", "runpod", "--api-key", "test-key", "--verbose"])

            # Should succeed
            assert result.exit_code == 0

    @patch("gpuport_collectors.cli.asyncio.run")
    @patch("gpuport_collectors.cli.RunPodCollector")
    def test_runpod_displays_collected_instances(self, mock_collector, mock_asyncio_run):
        """Test runpod command displays collected instances in summary."""
        # Mock collected instances
        mock_asyncio_run.return_value = []  # Empty list for simplicity

        runner = CliRunner()
        result = runner.invoke(cli, ["run", "runpod", "--api-key", "test-key"])

        # Should complete successfully
        assert result.exit_code == 0

    @patch("gpuport_collectors.cli.asyncio.run")
    @patch("gpuport_collectors.cli.RunPodCollector")
    @patch("gpuport_collectors.cli.execute_pipelines")
    @patch("gpuport_collectors.cli.load_export_config")
    def test_runpod_with_export_config(
        self, mock_load_config, mock_execute, mock_collector, mock_asyncio_run
    ):
        """Test runpod command with export configuration."""
        from gpuport_collectors.export.config import (
            ExportConfig,
            JSONTransformerConfig,
            LocalOutputConfig,
            PipelineConfig,
        )
        from gpuport_collectors.export.pipeline import PipelineResult
        from gpuport_collectors.models import AvailabilityStatus, GPUInstance

        # Mock instances
        mock_instances = [
            GPUInstance(
                provider="runpod",
                instance_type="GPU-1X-A100",
                accelerator_name="NVIDIA A100",
                accelerator_count=1,
                accelerator_mem_gib=80,
                v_cpus=8,
                memory_gib=64,
                price=1.50,
                availability=AvailabilityStatus.HIGH,
                region="US-NY-1",
            )
        ]
        mock_asyncio_run.return_value = mock_instances

        # Mock export config
        export_config = ExportConfig(
            pipelines=[
                PipelineConfig(
                    name="test-export",
                    transformer=JSONTransformerConfig(),
                    outputs=[LocalOutputConfig(path="./output")],
                )
            ]
        )
        mock_load_config.return_value = export_config

        # Mock pipeline results
        mock_execute.return_value = [
            PipelineResult(
                pipeline_name="test-export",
                enabled=True,
                input_count=1,
                filtered_count=1,
                output_count=1,
                outputs=[{"success": True}],
            )
        ]

        runner = CliRunner()
        with runner.isolated_filesystem():
            # Create export config file
            config_file = Path("export.yaml")
            config_file.write_text("pipelines: []")

            result = runner.invoke(
                cli,
                [
                    "run",
                    "runpod",
                    "--api-key",
                    "test-key",
                    "--export-config",
                    str(config_file),
                ],
            )

            # Should complete successfully when config is valid
            assert result.exit_code == 0


class TestExportCommand:
    """Tests for the export command."""

    def test_export_missing_api_key_error(self):
        """Test export command fails when API key missing."""
        runner = CliRunner()
        with runner.isolated_filesystem():
            config_file = Path("export.yaml")
            config_file.write_text("pipelines: []")

            result = runner.invoke(cli, ["export", "--config", str(config_file)])

            # Should exit with error code when API key is missing
            assert result.exit_code == 1

    @patch("gpuport_collectors.cli.load_export_config")
    @patch("gpuport_collectors.cli.validate_config")
    def test_export_validate_only_mode(self, mock_validate, mock_load_config):
        """Test export --validate-only mode."""
        from gpuport_collectors.export.config import ExportConfig

        mock_load_config.return_value = ExportConfig(pipelines=[])
        mock_validate.return_value = []  # No warnings

        runner = CliRunner()
        with runner.isolated_filesystem():
            config_file = Path("export.yaml")
            config_file.write_text("pipelines: []")

            result = runner.invoke(cli, ["export", "--config", str(config_file), "--validate-only"])

            # Should succeed with valid config
            assert result.exit_code == 0

    @patch("gpuport_collectors.cli.load_export_config")
    @patch("gpuport_collectors.cli.validate_config")
    def test_export_displays_validation_warnings(self, mock_validate, mock_load_config):
        """Test export command displays validation warnings."""
        from gpuport_collectors.export.config import ExportConfig

        mock_load_config.return_value = ExportConfig(pipelines=[])
        mock_validate.return_value = [
            "Pipeline 'test' has no output destinations",
            "S3 output has no credentials configured",
        ]

        runner = CliRunner()
        with runner.isolated_filesystem():
            config_file = Path("export.yaml")
            config_file.write_text("pipelines: []")

            result = runner.invoke(cli, ["export", "--config", str(config_file), "--validate-only"])

            # Should succeed even with warnings (warnings don't cause failure)
            assert result.exit_code == 0

    @patch("gpuport_collectors.cli.asyncio.run")
    @patch("gpuport_collectors.cli.RunPodCollector")
    @patch("gpuport_collectors.cli.execute_pipelines")
    @patch("gpuport_collectors.cli.load_export_config")
    def test_export_pipeline_failure_exit_code(
        self, mock_load_config, mock_execute, mock_collector, mock_asyncio_run
    ):
        """Test export command exits with code 1 when pipelines fail."""
        from gpuport_collectors.export.config import ExportConfig
        from gpuport_collectors.export.pipeline import PipelineResult

        mock_asyncio_run.return_value = []  # No instances
        mock_load_config.return_value = ExportConfig(pipelines=[])

        # Mock failed pipeline
        mock_execute.return_value = [
            PipelineResult(
                pipeline_name="test-export",
                enabled=True,
                error="Failed to write output",
                input_count=0,
                filtered_count=0,
                output_count=1,
                outputs=[{"success": False}],
            )
        ]

        runner = CliRunner()
        with runner.isolated_filesystem():
            config_file = Path("export.yaml")
            config_file.write_text("pipelines: []")

            result = runner.invoke(
                cli, ["export", "--config", str(config_file), "--api-key", "test-key"]
            )

            # Should exit with error code when pipeline fails
            assert result.exit_code == 1


class TestValidateCommand:
    """Tests for the validate command."""

    @patch("gpuport_collectors.cli.load_export_config")
    @patch("gpuport_collectors.cli.validate_config")
    def test_validate_success(self, mock_validate, mock_load_config):
        """Test validate command with valid configuration."""
        from gpuport_collectors.export.config import (
            ExportConfig,
            JSONTransformerConfig,
            LocalOutputConfig,
            PipelineConfig,
        )

        config = ExportConfig(
            pipelines=[
                PipelineConfig(
                    name="test-pipeline",
                    transformer=JSONTransformerConfig(),
                    outputs=[LocalOutputConfig(path="./output")],
                )
            ]
        )
        mock_load_config.return_value = config
        mock_validate.return_value = []  # No warnings

        runner = CliRunner()
        with runner.isolated_filesystem():
            config_file = Path("export.yaml")
            config_file.write_text("pipelines: []")

            result = runner.invoke(cli, ["validate", "--config", str(config_file)])

            # Just check success and key output
            assert result.exit_code == 0
            assert "✓ Configuration is valid" in result.output
            assert "test-pipeline" in result.output

    @patch("gpuport_collectors.cli.load_export_config")
    @patch("gpuport_collectors.cli.validate_config")
    def test_validate_with_warnings(self, mock_validate, mock_load_config):
        """Test validate command displays warnings."""
        from gpuport_collectors.export.config import ExportConfig

        mock_load_config.return_value = ExportConfig(pipelines=[])
        mock_validate.return_value = ["Pipeline 'test' has no output destinations"]

        runner = CliRunner()
        with runner.isolated_filesystem():
            config_file = Path("export.yaml")
            config_file.write_text("pipelines: []")

            result = runner.invoke(cli, ["validate", "--config", str(config_file)])

            # Should succeed even with warnings (warnings don't cause failure)
            assert result.exit_code == 0

    def test_validate_invalid_file(self):
        """Test validate command with non-existent file."""
        runner = CliRunner()
        result = runner.invoke(cli, ["validate", "--config", "nonexistent.yaml"])

        # Should fail with error
        assert result.exit_code == 1

    @patch("gpuport_collectors.cli.load_export_config")
    @patch("gpuport_collectors.cli.validate_config")
    def test_validate_pipeline_summary(self, mock_validate, mock_load_config):
        """Test validate command displays pipeline summary."""
        from gpuport_collectors.export.config import (
            CSVTransformerConfig,
            ExportConfig,
            FilterConfig,
            LocalOutputConfig,
            PipelineConfig,
        )

        config = ExportConfig(
            pipelines=[
                PipelineConfig(
                    name="filtered-csv",
                    enabled=True,
                    filters=[FilterConfig(field="provider", operator="eq", value="runpod")],
                    transformer=CSVTransformerConfig(fields={"provider": "Provider"}),
                    outputs=[
                        LocalOutputConfig(path="./output", name="csv-output"),
                        LocalOutputConfig(path="./backup", name="backup-output"),
                    ],
                ),
                PipelineConfig(
                    name="disabled-pipeline",
                    enabled=False,
                    transformer=CSVTransformerConfig(fields={"price": "Price"}),
                    outputs=[LocalOutputConfig(path="./output")],
                ),
            ]
        )
        mock_load_config.return_value = config
        mock_validate.return_value = []

        runner = CliRunner()
        with runner.isolated_filesystem():
            config_file = Path("export.yaml")
            config_file.write_text("pipelines: []")

            result = runner.invoke(cli, ["validate", "--config", str(config_file)])

            assert result.exit_code == 0
            assert "Pipeline summary:" in result.output
            assert "filtered-csv: ✓ enabled" in result.output
            assert "Filters: 1" in result.output
            assert "Transformer: csv" in result.output
            assert "Outputs: 2" in result.output
            assert "csv-output" in result.output
            assert "backup-output" in result.output
            assert "disabled-pipeline: ✗ disabled" in result.output
