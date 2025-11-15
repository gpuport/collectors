"""Integration tests for complete export pipeline flows."""

import json
from pathlib import Path

from gpuport_collectors.export.config import (
    CSVTransformerConfig,
    ExportConfig,
    FilterConfig,
    JSONTransformerConfig,
    LocalOutputConfig,
    MetricConfig,
    MetricsTransformerConfig,
    PipelineConfig,
)
from gpuport_collectors.export.pipeline import execute_pipelines
from gpuport_collectors.models import AvailabilityStatus, GPUInstance


class TestEndToEndPipelineFlows:
    """Test complete export pipeline workflows."""

    def test_multi_pipeline_multi_format(
        self, sample_instances: list[GPUInstance], tmp_path: Path
    ) -> None:
        """Test multiple pipelines with different formats executing in parallel."""
        config = ExportConfig(
            pipelines=[
                PipelineConfig(
                    name="json-export",
                    transformer=JSONTransformerConfig(pretty_print=True),
                    outputs=[
                        LocalOutputConfig(
                            path=str(tmp_path), filename_pattern="instances.json", overwrite=True
                        )
                    ],
                ),
                PipelineConfig(
                    name="csv-export",
                    transformer=CSVTransformerConfig(
                        fields={
                            "provider": "Provider",
                            "instance_type": "Instance Type",
                            "price": "Price",
                        }
                    ),
                    outputs=[
                        LocalOutputConfig(
                            path=str(tmp_path), filename_pattern="instances.csv", overwrite=True
                        )
                    ],
                ),
                PipelineConfig(
                    name="metrics-export",
                    transformer=MetricsTransformerConfig(
                        metrics=[
                            MetricConfig(name="total_instances", type="count"),
                            MetricConfig(name="avg_price", type="avg", field="price"),
                            MetricConfig(name="min_price", type="min", field="price"),
                            MetricConfig(name="max_price", type="max", field="price"),
                        ]
                    ),
                    outputs=[
                        LocalOutputConfig(
                            path=str(tmp_path), filename_pattern="metrics.json", overwrite=True
                        )
                    ],
                ),
            ]
        )

        # Execute all pipelines
        results = execute_pipelines(sample_instances, config)

        # Verify all pipelines succeeded
        assert len(results) == 3
        assert all(r.success for r in results)
        assert all(r.enabled for r in results)

        # Verify JSON export
        json_file = tmp_path / "instances.json"
        assert json_file.exists()
        json_data = json.loads(json_file.read_text())
        assert len(json_data) == 4
        assert json_data[0]["provider"] == "RunPod"

        # Verify CSV export
        csv_file = tmp_path / "instances.csv"
        assert csv_file.exists()
        csv_content = csv_file.read_text()
        assert "Provider,Instance Type,Price" in csv_content
        assert "RunPod,gpu.h100.80gb,25.0" in csv_content

        # Verify metrics export
        metrics_file = tmp_path / "metrics.json"
        assert metrics_file.exists()
        metrics_data = json.loads(metrics_file.read_text())
        assert metrics_data["metrics"]["total_instances"] == 4
        assert metrics_data["metrics"]["avg_price"] == 13.75  # (25+12.5+2.5+15)/4
        assert metrics_data["metrics"]["min_price"] == 2.5
        assert metrics_data["metrics"]["max_price"] == 25.0

    def test_filtered_pipeline_workflow(
        self, sample_instances: list[GPUInstance], tmp_path: Path
    ) -> None:
        """Test filtering then exporting workflow."""
        config = ExportConfig(
            pipelines=[
                PipelineConfig(
                    name="runpod-only",
                    filters=[FilterConfig(field="provider", operator="eq", value="RunPod")],
                    transformer=JSONTransformerConfig(),
                    outputs=[
                        LocalOutputConfig(
                            path=str(tmp_path), filename_pattern="runpod.json", overwrite=True
                        )
                    ],
                ),
                PipelineConfig(
                    name="high-availability",
                    filters=[
                        FilterConfig(
                            field="availability", operator="eq", value=AvailabilityStatus.HIGH
                        )
                    ],
                    transformer=JSONTransformerConfig(),
                    outputs=[
                        LocalOutputConfig(
                            path=str(tmp_path), filename_pattern="high-avail.json", overwrite=True
                        )
                    ],
                ),
                PipelineConfig(
                    name="expensive",
                    filters=[FilterConfig(field="price", operator="gte", value=15.0)],
                    transformer=JSONTransformerConfig(),
                    outputs=[
                        LocalOutputConfig(
                            path=str(tmp_path), filename_pattern="expensive.json", overwrite=True
                        )
                    ],
                ),
            ]
        )

        results = execute_pipelines(sample_instances, config)

        # Verify results
        assert len(results) == 3
        assert all(r.success for r in results)

        # RunPod only should have 3 instances
        assert results[0].filtered_count == 3
        runpod_data = json.loads((tmp_path / "runpod.json").read_text())
        assert len(runpod_data) == 3
        assert all(i["provider"] == "RunPod" for i in runpod_data)

        # High availability should have 2 instances
        assert results[1].filtered_count == 2
        high_avail_data = json.loads((tmp_path / "high-avail.json").read_text())
        assert len(high_avail_data) == 2

        # Expensive should have 2 instances (H100 at 25.0 and Lambda A100 at 15.0)
        assert results[2].filtered_count == 2
        expensive_data = json.loads((tmp_path / "expensive.json").read_text())
        assert len(expensive_data) == 2
        assert all(i["price"] >= 15.0 for i in expensive_data)

    def test_pipeline_with_disabled_pipeline(
        self, sample_instances: list[GPUInstance], tmp_path: Path
    ) -> None:
        """Test that disabled pipelines are skipped while enabled ones run."""
        config = ExportConfig(
            pipelines=[
                PipelineConfig(
                    name="enabled-pipeline",
                    enabled=True,
                    transformer=JSONTransformerConfig(),
                    outputs=[
                        LocalOutputConfig(
                            path=str(tmp_path), filename_pattern="enabled.json", overwrite=True
                        )
                    ],
                ),
                PipelineConfig(
                    name="disabled-pipeline",
                    enabled=False,
                    transformer=JSONTransformerConfig(),
                    outputs=[
                        LocalOutputConfig(
                            path=str(tmp_path), filename_pattern="disabled.json", overwrite=True
                        )
                    ],
                ),
            ]
        )

        results = execute_pipelines(sample_instances, config)

        # Verify results
        assert len(results) == 2
        assert results[0].enabled is True
        assert results[0].success is True
        assert results[1].enabled is False

        # Enabled pipeline should create output
        assert (tmp_path / "enabled.json").exists()

        # Disabled pipeline should NOT create output
        assert not (tmp_path / "disabled.json").exists()

    def test_multi_output_per_pipeline(
        self, sample_instances: list[GPUInstance], tmp_path: Path
    ) -> None:
        """Test pipeline with multiple outputs."""
        config = ExportConfig(
            pipelines=[
                PipelineConfig(
                    name="multi-output",
                    transformer=JSONTransformerConfig(),
                    outputs=[
                        LocalOutputConfig(
                            path=str(tmp_path), filename_pattern="output1.json", overwrite=True
                        ),
                        LocalOutputConfig(
                            path=str(tmp_path), filename_pattern="output2.json", overwrite=True
                        ),
                        LocalOutputConfig(
                            path=str(tmp_path / "subdir"),
                            filename_pattern="output3.json",
                            overwrite=True,
                            create_dirs=True,
                        ),
                    ],
                ),
            ]
        )

        results = execute_pipelines(sample_instances, config)

        # Verify all outputs created
        assert results[0].success is True
        assert results[0].output_count == 3
        assert results[0].successful_outputs == 3
        assert results[0].failed_outputs == 0

        assert (tmp_path / "output1.json").exists()
        assert (tmp_path / "output2.json").exists()
        assert (tmp_path / "subdir" / "output3.json").exists()

        # All outputs should have same data
        data1 = json.loads((tmp_path / "output1.json").read_text())
        data2 = json.loads((tmp_path / "output2.json").read_text())
        data3 = json.loads((tmp_path / "subdir" / "output3.json").read_text())
        assert data1 == data2 == data3
        assert len(data1) == 4


class TestRealWorldScenarios:
    """Test realistic export scenarios."""

    def test_daily_export_workflow(
        self, sample_instances: list[GPUInstance], tmp_path: Path
    ) -> None:
        """Simulate a daily export workflow with multiple formats and purposes."""
        config = ExportConfig(
            pipelines=[
                # Full data export for backup
                PipelineConfig(
                    name="daily-backup",
                    transformer=JSONTransformerConfig(pretty_print=True, include_raw_data=True),
                    outputs=[
                        LocalOutputConfig(
                            path=str(tmp_path),
                            filename_pattern="backup_{date}.json",
                            overwrite=False,
                        )
                    ],
                ),
                # CSV for data analysis
                PipelineConfig(
                    name="analysis-csv",
                    transformer=CSVTransformerConfig(
                        fields={
                            "provider": "provider",
                            "instance_type": "instance_type",
                            "accelerator_name": "gpu",
                            "accelerator_count": "gpu_count",
                            "region": "region",
                            "price": "price",
                            "availability": "availability",
                        }
                    ),
                    outputs=[
                        LocalOutputConfig(
                            path=str(tmp_path),
                            filename_pattern="analysis_{date}.csv",
                            overwrite=False,
                        )
                    ],
                ),
                # Metrics for monitoring
                PipelineConfig(
                    name="daily-metrics",
                    transformer=MetricsTransformerConfig(
                        metrics=[
                            MetricConfig(name="total", type="count"),
                            MetricConfig(name="avg_price", type="avg", field="price"),
                            MetricConfig(name="providers", type="unique", field="provider"),
                        ],
                        include_timestamp=True,
                    ),
                    outputs=[
                        LocalOutputConfig(
                            path=str(tmp_path),
                            filename_pattern="metrics_{date}.json",
                            overwrite=False,
                        )
                    ],
                ),
            ]
        )

        results = execute_pipelines(sample_instances, config)

        # All pipelines should succeed
        assert all(r.success for r in results)
        assert all(r.enabled for r in results)

        # Verify timing metrics are captured
        for result in results:
            assert result.duration_seconds is not None
            assert result.filter_duration is not None
            assert result.transform_duration is not None
            assert result.output_duration is not None

        # Verify all files created with date pattern
        import datetime

        today = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d")

        backup_file = tmp_path / f"backup_{today}.json"
        csv_file = tmp_path / f"analysis_{today}.csv"
        metrics_file = tmp_path / f"metrics_{today}.json"

        assert backup_file.exists()
        assert csv_file.exists()
        assert metrics_file.exists()

        # Verify backup has raw_data
        backup_data = json.loads(backup_file.read_text())
        assert len(backup_data) == 4
        # raw_data should be empty dict but present
        assert all("raw_data" in item for item in backup_data)

        # Verify CSV has correct columns
        csv_content = csv_file.read_text()
        assert "provider,instance_type,gpu,gpu_count,region,price,availability" in csv_content

        # Verify metrics has timestamp
        metrics_data = json.loads(metrics_file.read_text())
        assert "timestamp" in metrics_data
        assert metrics_data["metrics"]["total"] == 4
        assert metrics_data["metrics"]["providers"] == 2  # RunPod and Lambda
