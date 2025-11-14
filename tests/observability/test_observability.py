"""Tests for observability infrastructure (logging and tracing)."""

import logging
from unittest.mock import Mock, patch

import pytest

from gpuport_collectors.config import ObservabilityConfig
import gpuport_collectors.observability


class TestStructuredLogger:
    """Tests for the StructuredLogger class."""

    def test_logger_initialization(self):
        """Test that logger is properly initialized."""
        config = ObservabilityConfig(log_level="DEBUG")
        logger = gpuport_collectors.observability.StructuredLogger("test_logger", config)

        assert logger.logger.level == logging.DEBUG
        assert logger.config == config

    def test_format_message_basic(self):
        """Test basic message formatting."""
        config = ObservabilityConfig()
        logger = gpuport_collectors.observability.StructuredLogger("test", config)

        msg = logger._format_message("test message", key1="value1", key2="value2")

        assert "msg=test message" in msg
        assert "timestamp=" in msg
        assert "key1=value1" in msg
        assert "key2=value2" in msg

    def test_format_message_with_provider(self):
        """Test message formatting with provider_name."""
        config = ObservabilityConfig()
        logger = StructuredLogger("test", config)

        msg = logger._format_message("test message", provider_name="TestProvider")

        assert "provider=TestProvider" in msg
        assert "timestamp=" in msg

    def test_info_logging(self, caplog):
        """Test info level logging."""
        config = ObservabilityConfig(log_level="INFO")
        logger = StructuredLogger("test", config)

        with caplog.at_level(logging.INFO):
            logger.info("test info", key="value")

        assert "msg=test info" in caplog.text
        assert "key=value" in caplog.text

    def test_warning_logging(self, caplog):
        """Test warning level logging."""
        config = ObservabilityConfig(log_level="WARNING")
        logger = StructuredLogger("test", config)

        with caplog.at_level(logging.WARNING):
            logger.warning("test warning", provider_name="TestProvider")

        assert "msg=test warning" in caplog.text
        assert "provider=TestProvider" in caplog.text

    def test_error_logging_without_exception(self, caplog):
        """Test error logging without exception object."""
        config = ObservabilityConfig(log_level="ERROR")
        logger = StructuredLogger("test", config)

        with caplog.at_level(logging.ERROR):
            logger.error("test error", provider_name="TestProvider")

        assert "msg=test error" in caplog.text
        assert "provider=TestProvider" in caplog.text

    def test_error_logging_with_exception(self, caplog):
        """Test error logging with exception object."""
        config = ObservabilityConfig(log_level="ERROR")
        logger = StructuredLogger("test", config)

        try:
            raise ValueError("test exception")
        except ValueError as e:
            with caplog.at_level(logging.ERROR):
                logger.error("operation failed", error=e, provider_name="TestProvider")

        assert "msg=operation failed" in caplog.text
        assert "error_type=ValueError" in caplog.text
        assert "error_message=test exception" in caplog.text
        assert "stack_trace=" in caplog.text


class TestObservabilityManager:
    """Tests for the ObservabilityManager class."""

    def test_manager_initialization(self):
        """Test that manager is properly initialized."""
        config = ObservabilityConfig(enabled=False)
        manager = ObservabilityManager(config)

        assert manager.config == config
        assert manager._initialized is False

    def test_initialize_when_disabled(self, caplog):
        """Test initialization when observability is disabled."""
        config = ObservabilityConfig(enabled=False)
        manager = ObservabilityManager(config)

        with caplog.at_level(logging.INFO):
            manager.initialize()

        assert manager._initialized is False
        assert "Observability is disabled" in caplog.text

    def test_initialize_without_api_key(self, caplog):
        """Test initialization without Honeycomb API key."""
        config = ObservabilityConfig(enabled=True, honeycomb_api_key=None)
        manager = ObservabilityManager(config)

        with patch.dict("os.environ", {}, clear=True), caplog.at_level(logging.WARNING):
            manager.initialize()

        assert manager._initialized is False
        assert "Honeycomb API key not configured" in caplog.text

    @patch("gpuport_collectors.observability.OTLPSpanExporter")
    @patch("gpuport_collectors.observability.TracerProvider")
    @patch("gpuport_collectors.observability.trace.set_tracer_provider")
    def test_initialize_with_api_key(
        self, mock_set_provider, mock_tracer_provider, mock_exporter, caplog
    ):
        """Test successful initialization with API key."""
        config = ObservabilityConfig(
            enabled=True,
            honeycomb_api_key="test-key",
            service_name="test-service",
            environment="test",
        )
        manager = ObservabilityManager(config)

        with caplog.at_level(logging.INFO):
            manager.initialize()

        assert manager._initialized is True
        assert "Observability initialized" in caplog.text
        mock_exporter.assert_called_once()
        mock_tracer_provider.assert_called_once()
        mock_set_provider.assert_called_once()

    def test_initialize_idempotent(self, caplog):
        """Test that initialize can be called multiple times safely."""
        config = ObservabilityConfig(enabled=True, honeycomb_api_key="test-key")
        manager = ObservabilityManager(config)

        with (
            patch("gpuport_collectors.observability.OTLPSpanExporter"),
            patch("gpuport_collectors.observability.TracerProvider"),
        ):
            manager.initialize()

            with caplog.at_level(logging.WARNING):
                manager.initialize()

        assert "Observability already initialized" in caplog.text

    def test_get_logger(self):
        """Test getting a structured logger."""
        config = ObservabilityConfig()
        manager = ObservabilityManager(config)

        logger = manager.get_logger("test_logger")

        assert isinstance(logger, StructuredLogger)
        assert logger.config == config

    @patch("gpuport_collectors.observability.trace.get_tracer")
    def test_get_tracer(self, mock_get_tracer):
        """Test getting a tracer."""
        config = ObservabilityConfig()
        manager = ObservabilityManager(config)

        manager.get_tracer("test_tracer")

        mock_get_tracer.assert_called_once_with("test_tracer")

    def test_trace_operation_when_not_initialized(self):
        """Test trace_operation context manager when not initialized."""
        config = ObservabilityConfig(enabled=False)
        manager = ObservabilityManager(config)

        # Should not raise an exception
        with manager.trace_operation("test_op", key="value") as span:
            assert span is None

    @patch("gpuport_collectors.observability.trace.get_tracer")
    def test_trace_operation_when_initialized(self, mock_get_tracer):
        """Test trace_operation context manager when initialized."""
        config = ObservabilityConfig(enabled=True, honeycomb_api_key="test-key")
        manager = ObservabilityManager(config)

        mock_span = Mock()
        mock_context = Mock()
        mock_context.__enter__ = Mock(return_value=mock_span)
        mock_context.__exit__ = Mock(return_value=False)

        mock_tracer = Mock()
        mock_tracer.start_as_current_span = Mock(return_value=mock_context)
        mock_get_tracer.return_value = mock_tracer

        with (
            patch("gpuport_collectors.observability.OTLPSpanExporter"),
            patch("gpuport_collectors.observability.TracerProvider"),
        ):
            manager.initialize()

            with manager.trace_operation("test_op", provider="TestProvider"):
                pass

        mock_span.set_attribute.assert_any_call("provider", "TestProvider")

    @patch("gpuport_collectors.observability.trace.get_tracer")
    def test_trace_operation_with_exception(self, mock_get_tracer):
        """Test trace_operation records exceptions."""
        config = ObservabilityConfig(enabled=True, honeycomb_api_key="test-key")
        manager = gpuport_collectors.observability.ObservabilityManager(config)

        mock_span = Mock()
        mock_context = Mock()
        mock_context.__enter__ = Mock(return_value=mock_span)
        mock_context.__exit__ = Mock(return_value=False)

        mock_tracer = Mock()
        mock_tracer.start_as_current_span = Mock(return_value=mock_context)
        mock_get_tracer.return_value = mock_tracer

        with (
            patch("gpuport_collectors.observability.OTLPSpanExporter"),
            patch("gpuport_collectors.observability.TracerProvider"),
        ):
            manager.initialize()

            with pytest.raises(ValueError), manager.trace_operation("test_op"):
                raise ValueError("test error")

        # Verify exception attributes were set
        mock_span.set_attribute.assert_any_call("error", True)
        mock_span.set_attribute.assert_any_call("error.type", "ValueError")
        mock_span.set_attribute.assert_any_call("error.message", "test error")


class TestGetObservabilityManager:
    """Tests for the get_observability_manager function."""

    def test_get_manager_singleton(self):
        """Test that get_observability_manager returns a singleton."""
        # Reset the global manager

        gpuport_collectors.observability._observability_manager = None

        config = ObservabilityConfig(enabled=False)
        manager1 = gpuport_collectors.observability.get_observability_manager(config)
        manager2 = gpuport_collectors.observability.get_observability_manager()

        assert manager1 is manager2

    def test_get_manager_with_default_config(self):
        """Test getting manager with default config."""

        gpuport_collectors.observability._observability_manager = None

        manager = gpuport_collectors.observability.get_observability_manager()

        assert isinstance(manager, gpuport_collectors.observability.ObservabilityManager)
        assert manager.config is not None
