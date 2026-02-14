"""Griffe extensions for dagster-ray documentation."""

from __future__ import annotations

import json
from typing import Any

import griffe
from griffe import Object


def _highlight_json(json_str: str) -> str:
    """Apply JSON syntax highlighting."""
    from pygments import highlight
    from pygments.formatters import HtmlFormatter
    from pygments.lexers import JsonLexer

    lexer = JsonLexer()
    formatter = HtmlFormatter(nowrap=True, cssclass="highlight")
    return highlight(json_str, lexer, formatter)


class ConfigSchemaExtension(griffe.Extension):
    """Extension to display Dagster configuration schemas in documentation."""

    def __init__(self, display_format: str = "json") -> None:
        """Initialize the extension.

        Args:
            display_format: Format to display schemas ('json', 'table', or 'both')
        """
        super().__init__()
        self.display_format = display_format

    def on_object(self, obj: Object, **kwargs: Any) -> None:
        """Process objects to extract configuration schemas."""
        # Check if this is a dagster configurable object
        if self._is_dagster_configurable(obj):
            schema_info = self._extract_config_schema(obj)
            if schema_info and schema_info.get("parameters"):
                # Add schema parameters as members
                self._add_schema_members(obj, schema_info)

                # For functions that are actually decorated classes (like @executor),
                # change the kind to class so mkdocstrings shows members properly
                if obj.kind == griffe.Kind.FUNCTION:
                    obj.kind = griffe.Kind.CLASS

                    # Remove function signature to make it more class-like
                    if hasattr(obj, "signature"):
                        original_signature = obj.signature
                        obj.signature = None

                        # Add a __call__ method instead to preserve the function nature
                        if obj.parameters:
                            call_method = griffe.Function(name="__call__", lineno=obj.lineno, parent=obj)
                            call_method.signature = original_signature
                            call_method.docstring = griffe.Docstring("Call the executor function.")
                            obj.members["__call__"] = call_method

    def on_package_loaded(self, **kwargs: Any) -> None:
        """Hook called when a package is loaded - allows dynamic inspection."""
        # This allows us to inspect the actual live objects during documentation build
        pkg = kwargs.get("pkg")
        if pkg:
            self._process_package_objects(pkg)

    def _process_package_objects(self, pkg: Object) -> None:
        """Process all objects in the package to find our targets."""

        def visit_object(obj: Object) -> None:
            if self._is_dagster_configurable(obj):
                schema_info = self._extract_config_schema(obj)
                if schema_info and schema_info.get("parameters"):
                    # Add as members instead of storing in extra
                    self._add_schema_members(obj, schema_info)

            # Recursively visit members with error handling
            try:
                if hasattr(obj, "members") and obj.members:
                    for member in obj.members.values():
                        visit_object(member)
            except Exception:
                # Skip problematic objects that cause alias resolution errors
                pass

        visit_object(pkg)

    def _is_dagster_configurable(self, obj: Object) -> bool:
        """Check if object is a Dagster configurable using runtime type checking."""
        try:
            # Try to get the actual runtime object for type checking
            runtime_obj = self._get_runtime_object(obj)
            if runtime_obj is None:
                # Try fallback detection
                return self._fallback_detection(obj)

            # Import Dagster types for checking
            from dagster._core.definitions.configurable import ConfigurableDefinition
            from dagster._serdes import ConfigurableClass

            # Check using Dagster's approach: isinstance for ConfigurableDefinition or issubclass for ConfigurableClass
            return isinstance(runtime_obj, ConfigurableDefinition) or (
                isinstance(runtime_obj, type) and issubclass(runtime_obj, ConfigurableClass)
            )
        except Exception:
            # Fallback to string-based detection if runtime object can't be accessed
            return self._fallback_detection(obj)

    def _get_runtime_object(self, obj: Object):
        """Try to get the actual runtime object from a griffe Object."""
        try:
            # Get the module path and object name
            if hasattr(obj, "path"):
                module_path = ".".join(obj.path.split(".")[:-1])  # Remove the object name
                object_name = obj.path.split(".")[-1]
            else:
                return None

            # Import the module and get the object
            import importlib

            module = importlib.import_module(module_path)
            return getattr(module, object_name, None)
        except Exception:
            return None

    def _fallback_detection(self, obj: Object) -> bool:
        """Fallback detection using string patterns when runtime checking fails."""
        # Check for executor decorator
        if obj.kind == griffe.Kind.FUNCTION and hasattr(obj, "decorators") and obj.decorators:
            for decorator in obj.decorators:
                if hasattr(decorator, "value"):
                    decorator_str = str(decorator.value)
                    if any(pattern in decorator_str for pattern in ["executor", "dagster.executor", "dg.executor"]):
                        return True

        # Check for RunLauncher inheritance
        if obj.kind == griffe.Kind.CLASS and hasattr(obj, "bases"):
            for base in obj.bases:
                base_str = str(base)
                if any(pattern in base_str for pattern in ["RunLauncher", "ConfigurableClass"]):
                    return True

        # Check for resource decorator
        if obj.kind == griffe.Kind.FUNCTION and hasattr(obj, "decorators") and obj.decorators:
            for decorator in obj.decorators:
                if hasattr(decorator, "value"):
                    decorator_str = str(decorator.value)
                    if any(pattern in decorator_str for pattern in ["resource", "dagster.resource", "dg.resource"]):
                        return True

        return False

    def _extract_config_schema(self, obj: Object) -> dict[str, Any] | None:
        """Extract configuration schema from the object."""
        try:
            # Use dynamic import to get live objects during doc build
            if obj.name == "ray_executor":
                return self._extract_ray_executor_schema()
            elif obj.name == "RayRunLauncher":
                return self._extract_ray_run_launcher_schema()
        except Exception as e:
            return {"title": f"Configuration Schema for {obj.name}", "error": f"Could not extract schema: {e}"}

        return None

    def _extract_ray_executor_schema(self) -> dict[str, Any]:
        """Extract schema for ray_executor."""
        try:
            from dagster_ray.core.executor import _RAY_EXECUTOR_CONFIG_SCHEMA, RayExecutorConfig

            # Get the full Dagster config schema (includes ray + retries + tag_concurrency_limits)
            dagster_schema = self._dagster_schema_to_dict(_RAY_EXECUTOR_CONFIG_SCHEMA)

            # Also get the JSON schema for the ray section
            json_schema = RayExecutorConfig.model_json_schema()

            return {
                "title": "Ray Executor Configuration",
                "description": "Configuration schema for the ray_executor. This includes the main 'ray' section plus Dagster's built-in retry and concurrency configurations.",
                "config_class": "RayExecutorConfig (ray section) + Dagster retries/concurrency",
                "schema": json_schema,
                "schema_json": json.dumps(json_schema, indent=2),
                "schema_html": _highlight_json(json.dumps(json_schema, indent=2)),
                "parameters": self._format_parameters(json_schema),
            }
        except ImportError as e:
            return {
                "title": "Ray Executor Configuration",
                "description": "Configuration schema for the ray_executor",
                "error": f"Could not import schema: {e}",
            }

    def _extract_ray_run_launcher_schema(self) -> dict[str, Any]:
        """Extract schema for RayRunLauncher."""
        try:
            from dagster_ray.core.run_launcher import RayLauncherConfig

            # Generate JSON schema
            schema = RayLauncherConfig.model_json_schema()

            return {
                "title": "Ray Run Launcher Configuration",
                "description": "Configuration schema for the RayRunLauncher",
                "config_class": "RayLauncherConfig",
                "schema": schema,
                "schema_json": json.dumps(schema, indent=2),
                "schema_html": _highlight_json(json.dumps(schema, indent=2)),
                "parameters": self._format_parameters(schema),
            }
        except ImportError:
            return {
                "title": "Ray Run Launcher Configuration",
                "description": "Configuration schema for the RayRunLauncher",
                "error": "Could not import RayLauncherConfig",
            }

    def _format_parameters(self, schema: dict[str, Any]) -> list[dict[str, Any]]:
        """Format schema properties into a readable parameter list."""
        parameters = []

        properties = schema.get("properties", {})
        required = set(schema.get("required", []))

        for param_name, param_info in properties.items():
            param_data = {
                "name": param_name,
                "type": self._json_schema_type_to_python(param_info.get("type", "unknown")),
                "required": param_name in required,
                "description": param_info.get("description", ""),
                "default": param_info.get("default"),
            }

            # Handle complex types
            if param_info.get("type") == "array":
                items_info = param_info.get("items", {})
                item_type = self._json_schema_type_to_python(items_info.get("type", "unknown"))
                param_data["type"] = f"list[{item_type}]"
            elif param_info.get("type") == "object":
                param_data["type"] = "dict"
            elif "anyOf" in param_info:
                # Handle union types
                types = [self._json_schema_type_to_python(item.get("type", "unknown")) for item in param_info["anyOf"]]
                param_data["type"] = " | ".join(types)

            parameters.append(param_data)

        return parameters

    def _json_schema_type_to_python(self, json_type: str) -> str:
        """Convert JSON Schema type names to Python type names."""
        type_mapping = {
            "string": "str",
            "integer": "int",
            "number": "float",
            "boolean": "bool",
            "array": "list",
            "object": "dict",
            "null": "None",
        }
        return type_mapping.get(json_type, json_type)

    def _make_type_annotation_with_crossrefs(self, type_str: str) -> str:
        """Convert type string for proper annotation display."""
        # Just return the type string as-is - mkdocstrings handles cross-references automatically
        # for standard Python types when they appear in annotations
        return type_str

    def _format_type_with_crossrefs(self, type_str: str) -> str:
        """Format type string with cross-references for documentation display."""

        # Handle union types (e.g., "str | None")
        if " | " in type_str:
            parts = type_str.split(" | ")
            formatted_parts = [self._format_single_type_with_crossref(part.strip()) for part in parts]
            return " | ".join(formatted_parts)

        return self._format_single_type_with_crossref(type_str)

    def _format_single_type_with_crossref(self, type_str: str) -> str:
        """Format a single type with cross-references."""
        # Handle generic types like dict[str, Any], list[float], etc.
        if "[" in type_str and "]" in type_str:
            # Extract the base type and the generic parameters
            base_type = type_str.split("[")[0]
            inner_part = type_str[len(base_type) + 1 : -1]  # Remove base_type[ and ]

            # Format the base type with cross-reference
            formatted_base = self._add_type_crossref(base_type)

            # Handle the inner types (can be comma-separated like dict[str, Any])
            if "," in inner_part:
                # Multiple type parameters (e.g., dict[str, Any])
                inner_types = [self._format_single_type_with_crossref(t.strip()) for t in inner_part.split(",")]
                formatted_inner = ", ".join(inner_types)
            else:
                # Single type parameter (e.g., list[str])
                formatted_inner = self._format_single_type_with_crossref(inner_part)

            return f"{formatted_base}[{formatted_inner}]"

        # Simple type, just add cross-reference
        return self._add_type_crossref(type_str)

    def _add_type_crossref(self, type_name: str) -> str:
        """Add cross-reference to a type name."""
        # Let's just return the type name as-is for now
        # mkdocstrings should handle cross-references automatically in many cases
        return type_name

    def _dagster_schema_to_dict(self, schema) -> dict[str, Any]:
        """Convert Dagster config schema to a dict representation."""
        try:
            # Handle the schema based on Dagster's approach
            if isinstance(schema, dict):
                # This is already a dict, likely the merged schema from executor
                result = {}
                for key, value in schema.items():
                    if hasattr(value, "config_type"):
                        # This is a Field with a config_type
                        result[key] = self._config_type_to_dict(value.config_type)
                    elif hasattr(value, "_schema_dict"):
                        result[key] = value._schema_dict
                    else:
                        result[key] = {"type": str(type(value)), "repr": str(value)}
                return result
            elif hasattr(schema, "config_type"):
                return self._config_type_to_dict(schema.config_type)
            else:
                return {"type": str(type(schema)), "repr": str(schema)}
        except Exception as e:
            return {"error": f"Could not convert schema: {e}"}

    def _config_type_to_dict(self, config_type) -> dict[str, Any]:
        """Convert a Dagster config type to dict, inspired by Dagster's approach."""
        try:
            # Based on Dagster's config type system
            result = {
                "type": self._get_config_type_name(config_type),
                "description": getattr(config_type, "description", None),
            }

            # Handle different config type kinds
            if hasattr(config_type, "fields") and config_type.fields:
                # Shape/Selector types - similar to Dagster's field discovery
                fields = {}
                for field_name, field in config_type.fields.items():
                    field_info = {
                        "type": self._get_config_type_name(field.config_type),
                        "is_required": field.is_required,
                        "description": field.description if hasattr(field, "description") else None,
                    }

                    # Handle default values like Dagster does
                    if hasattr(field, "default_value") and not field.is_required:
                        default = field.default_value
                        # Check if default is not the sentinel value (using string comparison as fallback)
                        if default is not None and str(default) != "@":
                            field_info["default"] = (
                                str(default) if not isinstance(default, (str, int, float, bool)) else default
                            )

                    # Recursively handle nested fields
                    if hasattr(field.config_type, "fields"):
                        field_info["nested_fields"] = self._config_type_to_dict(field.config_type)["fields"]

                    fields[field_name] = field_info

                result["fields"] = fields

            elif hasattr(config_type, "inner_type"):
                # Array/list types
                result["inner_type"] = self._get_config_type_name(config_type.inner_type)

            elif hasattr(config_type, "type_params"):
                # Union types
                result["type_params"] = [self._get_config_type_name(t) for t in config_type.type_params]

            return result

        except Exception as e:
            return {"error": f"Could not convert config type: {e}", "type": str(type(config_type))}

    def _get_config_type_name(self, config_type) -> str:
        """Get a readable name for a Dagster config type."""
        try:
            if hasattr(config_type, "key"):
                key = config_type.key
                # Clean up type names for better readability
                return self._clean_type_name(key)
            elif hasattr(config_type, "__name__"):
                return config_type.__name__
            else:
                return str(type(config_type).__name__)
        except:
            return "unknown"

    def _clean_type_name(self, type_name: str) -> str:
        """Clean up Dagster type names for better readability."""
        # Convert complex Dagster types to more readable forms
        replacements = {
            "Noneable.StringSourceType": "str | None",
            "Noneable.Map.String.StringSourceType": "dict[str, str] | None",
            "Noneable.Map.String.Any": "dict[str, Any] | None",
            "Noneable.IntSourceType": "int | None",
            "Noneable.FloatSourceType": "float | None",
            "StringSourceType": "str",
            "IntSourceType": "int",
            "FloatSourceType": "float",
            "BoolSourceType": "bool",
            "Map.String.FloatSourceType": "dict[str, float]",
        }

        for old, new in replacements.items():
            if type_name.startswith(old):
                return new

        # Handle generic patterns
        if "Noneable." in type_name:
            inner = type_name.replace("Noneable.", "")
            return f"{self._clean_type_name(inner)} | None"

        return type_name

    def _format_dagster_parameters(self, schema: dict[str, Any]) -> list[dict[str, Any]]:
        """Format Dagster schema into parameter list, based on actual schema structure."""
        parameters = []

        # The schema structure from debug: top-level keys are sections like 'ray', 'retries', 'tag_concurrency_limits'
        for section_name, section_info in schema.items():
            if isinstance(section_info, dict) and "fields" in section_info:
                # Extract fields from this section
                self._extract_section_fields(section_info["fields"], section_name, parameters)
            elif isinstance(section_info, dict) and section_info.get("type") == "object" and "fields" in section_info:
                # Direct fields in this section
                self._extract_section_fields(section_info["fields"], section_name, parameters)

        return parameters

    def _extract_section_fields(self, fields: dict[str, Any], section_name: str, parameters: list, prefix=""):
        """Extract fields from a section."""
        for field_name, field_info in fields.items():
            # Create parameter name with section prefix
            if prefix:
                param_name = f"{prefix}.{field_name}"
            else:
                param_name = f"{section_name}.{field_name}" if section_name != "ray" else field_name

            param_data = {
                "name": param_name,
                "type": field_info.get("type", "unknown"),
                "required": field_info.get("is_required", False),
                "description": field_info.get("description", "") or "",
                "default": field_info.get("default"),
            }

            # Handle type params for unions
            if "type_params" in field_info:
                param_data["type"] = " | ".join(field_info["type_params"])

            # Handle inner type for arrays
            if "inner_type" in field_info:
                param_data["type"] = f"Array[{field_info['inner_type']}]"

            # Handle special types like Noneable
            if field_info.get("type") == "Noneable":
                param_data["type"] = "Optional"

            parameters.append(param_data)

            # Handle nested fields recursively
            if "nested_fields" in field_info:
                self._extract_section_fields(field_info["nested_fields"], section_name, parameters, param_name)

    def _add_schema_members(self, obj: Object, schema_info: dict[str, Any]) -> None:
        """Add configuration schema parameters as individual members of the object."""
        from griffe import Attribute, Kind

        # Add "Fields:" section to the main object's docstring (only once)
        if schema_info["parameters"] and obj.docstring:
            # Get existing docstring content
            existing_content = obj.docstring.value if obj.docstring.value else ""

            # Only add Fields section if it doesn't already exist
            if "Fields:" not in existing_content:
                # Create Fields section as a vertical list with cross-references and types
                fields_lines = ["", "Fields:", ""]
                for param in schema_info["parameters"]:
                    field_name = param["name"]
                    field_type = self._format_type_with_crossrefs(param["type"])
                    # Create cross-reference to the attribute and show type in parentheses
                    # Use bullet points to avoid code block formatting
                    fields_lines.append(f"- [{field_name}][{obj.path}.{field_name}] ({field_type})")

                fields_section = "\n".join(fields_lines)

                # Update the docstring
                obj.docstring = griffe.Docstring(existing_content + fields_section)

        # Add each parameter as a direct member of the object
        for param in schema_info["parameters"]:
            param_attr = Attribute(name=param["name"], lineno=1, parent=obj)
            param_attr.kind = Kind.ATTRIBUTE
            param_attr.annotation = self._make_type_annotation_with_crossrefs(param["type"])

            # Create clean docstring with just the description and default value
            doc_parts = []

            # Add description first
            if param.get("description"):
                doc_parts.append(param["description"])

            # Only add default value if it exists (don't mention required/optional status)
            if not param.get("required"):
                default_val = param.get("default")
                if default_val is not None:
                    doc_parts.append(f"Default: `{default_val}`")

            param_attr.docstring = griffe.Docstring("\n\n".join(doc_parts)) if doc_parts else None

            # Add as direct member of the main object
            obj.members[param["name"]] = param_attr

        # Also add a JSON schema member if we have schema data for advanced users
        if "schema" in schema_info:
            schema_member = Attribute(name="__config_schema__", lineno=1, parent=obj)
            schema_member.kind = Kind.ATTRIBUTE
            schema_member.annotation = "dict"

            # Create docstring with foldable JSON schema
            doc_parts = []
            doc_parts.append("")
            doc_parts.append("<details>")
            doc_parts.append("<summary>JSON Schema</summary>")
            doc_parts.append("")
            doc_parts.append("```json")
            doc_parts.append(schema_info["schema_json"])
            doc_parts.append("```")
            doc_parts.append("")
            doc_parts.append("</details>")

            schema_member.docstring = griffe.Docstring("\n".join(doc_parts))
            obj.members["__config_schema__"] = schema_member
