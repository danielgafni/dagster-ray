"""Tests for RayResource lifecycle hook methods (on_create, on_ready, on_connect, on_cleanup)."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest
from pydantic import PrivateAttr

from dagster_ray._base.resources import RayResource
from dagster_ray.configs import Lifecycle


class MockRayResource(RayResource):
    """A concrete RayResource subclass for testing lifecycle hooks."""

    _mock_fail_create: bool = PrivateAttr(default=False)
    _mock_fail_wait: bool = PrivateAttr(default=False)

    @property
    def host(self) -> str:
        return "127.0.0.1"

    @property
    def name(self) -> str:
        return "test-cluster"

    def create(self, context):
        if self._mock_fail_create:
            raise RuntimeError("create failed")
        self._name = "test-cluster"

    def wait(self, context):
        if self._mock_fail_wait:
            raise RuntimeError("wait failed")
        self._host = "127.0.0.1"

    def connect(self, context):
        self._context = MagicMock()
        return self._context

    def delete(self, context):
        pass


class TrackingRayResource(MockRayResource):
    """Tracks hook invocations for assertion."""

    _hook_calls: list = PrivateAttr(default_factory=list)

    @property
    def hook_calls(self) -> list[str]:
        return self._hook_calls

    def on_create(self, context):
        self._hook_calls.append("on_create")
        super().on_create(context)

    def on_ready(self, context):
        self._hook_calls.append("on_ready")
        super().on_ready(context)

    def on_connect(self, context):
        self._hook_calls.append("on_connect")
        super().on_connect(context)

    def on_cleanup(self, context, *, deleted: bool):
        self._hook_calls.append(f"on_cleanup(deleted={deleted})")
        super().on_cleanup(context, deleted=deleted)


def _make_context() -> Any:
    """Create a mock InitResourceContext with a logger."""
    context = MagicMock()
    context.log = MagicMock()
    context.run_id = "test-run-id"
    context.dagster_run = MagicMock()
    return context


class TestHookInvocationOrder:
    def test_full_lifecycle_calls_hooks_in_order(self):
        resource = TrackingRayResource(lifecycle=Lifecycle(cleanup="always"))
        context = _make_context()

        resource._create(context)
        resource._wait(context)
        resource._connect(context)
        resource.cleanup(context, None)

        assert resource.hook_calls == [
            "on_create",
            "on_ready",
            "on_connect",
            "on_cleanup(deleted=True)",
        ]

    def test_yield_for_execution_calls_hooks_in_order(self):
        resource = TrackingRayResource(lifecycle=Lifecycle(cleanup="always"))
        context = _make_context()

        gen = resource.yield_for_execution(context)
        yielded = gen.__enter__()
        assert yielded is resource
        gen.__exit__(None, None, None)

        assert resource.hook_calls == [
            "on_create",
            "on_ready",
            "on_connect",
            "on_cleanup(deleted=True)",
        ]


class TestHooksSkippedWhenPhaseSkipped:
    def test_no_hooks_when_lifecycle_disabled(self):
        resource = TrackingRayResource(lifecycle=Lifecycle(create=False, wait=False, connect=False, cleanup="never"))
        context = _make_context()

        gen = resource.yield_for_execution(context)
        gen.__enter__()
        gen.__exit__(None, None, None)

        # on_cleanup still fires (with deleted=False) since cleanup always runs
        assert resource.hook_calls == ["on_cleanup(deleted=False)"]

    def test_on_create_skipped_when_already_created(self):
        resource = TrackingRayResource()
        resource._name = "already-created"  # mark as created
        context = _make_context()

        resource._create(context)
        assert "on_create" not in resource.hook_calls

    def test_on_ready_skipped_when_already_ready(self):
        resource = TrackingRayResource()
        resource._name = "test"
        resource._host = "127.0.0.1"  # mark as ready
        context = _make_context()

        resource._wait(context)
        assert "on_ready" not in resource.hook_calls

    def test_on_connect_skipped_when_already_connected(self):
        resource = TrackingRayResource()
        resource._name = "test"
        resource._host = "127.0.0.1"
        resource._context = MagicMock()  # mark as connected
        context = _make_context()

        resource._connect(context)
        assert "on_connect" not in resource.hook_calls


class TestOnCleanupDeletedFlag:
    def test_deleted_true_when_cleanup_always(self):
        resource = TrackingRayResource(lifecycle=Lifecycle(cleanup="always"))
        context = _make_context()

        resource._create(context)
        resource.cleanup(context, None)

        assert "on_cleanup(deleted=True)" in resource.hook_calls

    def test_deleted_false_when_cleanup_never(self):
        resource = TrackingRayResource(lifecycle=Lifecycle(cleanup="never"))
        context = _make_context()

        resource._create(context)
        resource.cleanup(context, None)

        assert "on_cleanup(deleted=False)" in resource.hook_calls

    def test_deleted_true_on_exception_with_exception(self):
        resource = TrackingRayResource(lifecycle=Lifecycle(cleanup="on_exception"))
        context = _make_context()
        exc = RuntimeError("boom")

        resource._create(context)
        resource.cleanup(context, exc)

        assert "on_cleanup(deleted=True)" in resource.hook_calls

    def test_deleted_false_on_exception_without_exception(self):
        resource = TrackingRayResource(lifecycle=Lifecycle(cleanup="on_exception"))
        context = _make_context()

        resource._create(context)
        resource.cleanup(context, None)

        assert "on_cleanup(deleted=False)" in resource.hook_calls


class TestSubclassOverride:
    def test_override_without_super(self):
        class CustomResource(MockRayResource):
            _custom_messages: list = PrivateAttr(default_factory=list)

            def on_create(self, context):
                self._custom_messages.append("custom create")

        resource = CustomResource()
        context = _make_context()

        resource._create(context)

        assert resource._custom_messages == ["custom create"]
        # Default log message should NOT have been called
        context.log.info.assert_not_called()

    def test_override_with_super(self):
        class CustomResource(MockRayResource):
            _custom_messages: list = PrivateAttr(default_factory=list)

            def on_ready(self, context):
                super().on_ready(context)
                self._custom_messages.append("custom ready")

        resource = CustomResource()
        context = _make_context()

        resource._create(context)
        resource._wait(context)

        assert resource._custom_messages == ["custom ready"]


class TestDefaultHookMessages:
    def test_on_create_default_message(self):
        resource = MockRayResource()
        context = _make_context()

        resource._create(context)

        context.log.info.assert_called_with("Created test-cluster.")

    def test_on_create_uses_creation_verb(self):
        resource = MockRayResource()
        resource._creation_verb = "Using"
        context = _make_context()

        resource.on_create(context)

        context.log.info.assert_called_with("Using test-cluster.")

    def test_on_connect_default_message(self):
        resource = MockRayResource()
        resource._name = "test-cluster"
        resource._host = "127.0.0.1"
        context = _make_context()

        resource._connect(context)

        context.log.info.assert_called_with("Connected to test-cluster via Ray Client")

    def test_on_cleanup_deleted_message(self):
        resource = MockRayResource(lifecycle=Lifecycle(cleanup="always"))
        resource._name = "test-cluster"
        context = _make_context()

        resource.on_cleanup(context, deleted=True)

        context.log.info.assert_called_with('Deleted test-cluster according to cleanup policy "always"')

    def test_on_cleanup_no_message_when_not_deleted(self):
        resource = MockRayResource()
        context = _make_context()

        resource.on_cleanup(context, deleted=False)

        context.log.info.assert_not_called()

    def test_on_ready_default_is_noop(self):
        resource = MockRayResource()
        context = _make_context()

        resource.on_ready(context)

        context.log.info.assert_not_called()


class TestHooksNotCalledOnFailure:
    def test_on_create_not_called_on_create_failure(self):
        resource = TrackingRayResource()
        resource._mock_fail_create = True
        context = _make_context()

        with pytest.raises(RuntimeError, match="create failed"):
            resource._create(context)

        assert "on_create" not in resource.hook_calls

    def test_on_ready_not_called_on_wait_failure(self):
        resource = TrackingRayResource()
        resource._mock_fail_wait = True
        context = _make_context()

        resource._create(context)

        with pytest.raises(RuntimeError, match="wait failed"):
            resource._wait(context)

        assert "on_create" in resource.hook_calls
        assert "on_ready" not in resource.hook_calls
