"""Griffe extension to support the kuberay_docs decorator."""

from __future__ import annotations

import griffe

COMMON_KUBERAY_DOCSTRING = """
Info:
    Image defaults to `dagster/image` run tag.

Tip:
    Make sure `ray[full]` is available in the image.
"""


class KubeRayDocsExtension(griffe.Extension):
    """Extension to support @kuberay_docs decorator."""

    def __init__(self):
        raise RuntimeError()
        super().__init__()

    def on_class(self, *, cls: griffe.Class, **kwargs) -> None:
        """Process classes that have the kuberay_docs decorator."""
        # Check if the class is decorated with kuberay_docs
        for decorator in cls.decorators:
            if self._is_kuberay_docs_decorator(decorator):
                self._apply_kuberay_docs(cls)
                break

    def _is_kuberay_docs_decorator(self, decorator: griffe.Decorator) -> bool:
        """Check if decorator is kuberay_docs."""
        # Check for exact path match
        if decorator.callable_path == "dagster_ray.kuberay.resources.base.kuberay_docs":
            return True
        # Also check for shortened paths (when imported)
        if decorator.callable_path and decorator.callable_path.endswith("kuberay_docs"):
            return True
        return False

    def _apply_kuberay_docs(self, cls: griffe.Class) -> None:
        """Apply kuberay_docs decorator logic to class docstring."""
        if cls.docstring and cls.docstring.value:
            # Append the common docstring to existing docstring
            original_value = cls.docstring.value.rstrip()
            new_value = original_value + COMMON_KUBERAY_DOCSTRING
            cls.docstring.value = new_value
        else:
            # Class doesn't have a docstring, add the common one
            cls.docstring = griffe.Docstring(
                COMMON_KUBERAY_DOCSTRING.lstrip(),
                lineno=cls.lineno or 1,
            )
