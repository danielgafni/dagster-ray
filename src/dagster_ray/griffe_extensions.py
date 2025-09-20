"""Griffe extensions for dagster-ray documentation."""

from __future__ import annotations

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
        is_configurable = self._is_dagster_configurable(obj)

        if is_configurable:
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
        """Extract configuration schema from any configurable object."""
        try:
            # Get the actual runtime object
            runtime_obj = self._get_runtime_object(obj)
            if runtime_obj is None:
                return None

            # Try to extract schema using Dagster's config system
            schema_dict = self._extract_schema_from_runtime_object(runtime_obj, obj)
            if schema_dict:
                return {
                    "title": f"Configuration Schema for {obj.name}",
                    "description": f"Configuration schema for {obj.name}",
                    "parameters": schema_dict,
                }

        except Exception as e:
            return {"title": f"Configuration Schema for {obj.name}", "error": f"Could not extract schema: {e}"}

        return None

    def _extract_schema_from_runtime_object(self, runtime_obj, griffe_obj: Object) -> list[dict[str, Any]] | None:
        """Extract schema from any Dagster configurable object using Dagster's official detection."""
        import inspect

        try:
            # Import Dagster's config detection utilities
            from dagster import Field
            from dagster._config.pythonic_config import (
                ConfigurableResource,
                ConfigurableResourceFactory,
                infer_schema_from_config_class,
            )
            from dagster._core.definitions.configurable import ConfigurableDefinition
            from dagster._serdes import ConfigurableClass

            obj = runtime_obj
            config_field = None

            # Follow Dagster's official detection pattern
            if isinstance(obj, ConfigurableDefinition):
                if obj.config_schema:
                    config_field = obj.config_schema.as_field()
            elif inspect.isclass(obj) and (
                issubclass(obj, ConfigurableResource) or issubclass(obj, ConfigurableResourceFactory)
            ):
                config_field = infer_schema_from_config_class(obj)
            elif isinstance(obj, type) and issubclass(obj, ConfigurableClass):
                config_field = Field(obj.config_type())
            elif inspect.isfunction(obj):
                # Handle function-based configurables (like @executor decorated functions)
                try:
                    # Try to call with empty config to get the configurable object
                    instance = obj([])
                    if hasattr(instance, "config_schema") and instance.config_schema:
                        config_field = instance.config_schema.as_field()
                except:
                    pass

            if config_field:
                # Convert the Dagster config field to our parameter format
                return self._dagster_field_to_parameters(config_field)

        except Exception:
            pass

        return None

    def _dagster_field_to_parameters(self, field) -> list[dict[str, Any]]:
        """Convert a Dagster config field to parameter list using Dagster's approach."""
        parameters = []

        def extract_field_info(field_obj, field_name=None, prefix=""):
            """Recursively extract field information."""
            if field_name:
                param_name = f"{prefix}.{field_name}" if prefix else field_name

                # Get type representation using Dagster's type_repr logic
                type_str = self._dagster_type_repr(field_obj.config_type)

                param_data = {
                    "name": param_name,
                    "type": type_str,
                    "required": field_obj.is_required,
                    "description": field_obj.description or "",
                }

                # Add default value if provided
                if field_obj.default_provided:
                    param_data["default"] = field_obj.default_value

                parameters.append(param_data)

            # Recurse into subfields if they exist
            if hasattr(field_obj.config_type, "fields") and field_obj.config_type.fields:
                for name, subfield in field_obj.config_type.fields.items():
                    subfield_prefix = f"{prefix}.{field_name}" if prefix and field_name else (field_name or "")
                    extract_field_info(subfield, name, subfield_prefix)

        # Start extraction
        extract_field_info(field)
        return parameters

    def _dagster_type_repr(self, config_type) -> str:
        """Generate human-readable type representation following Dagster's approach."""
        try:
            from dagster import BoolSource, IntSource, StringSource
            from dagster._config.config_type import (
                ConfigTypeKind,
            )

            # Use given name if possible
            if hasattr(config_type, "given_name") and config_type.given_name:
                return config_type.given_name

            # Handle special source types
            if config_type == StringSource:
                return "str"
            elif config_type == BoolSource:
                return "bool"
            elif config_type == IntSource:
                return "int"
            elif config_type.kind == ConfigTypeKind.ANY:
                return "Any"
            elif config_type.kind == ConfigTypeKind.SCALAR:
                scalar_name = config_type.scalar_kind.name.lower()
                # Fix common scalar type names
                if scalar_name == "float":
                    return "float"
                elif scalar_name == "int":
                    return "int"
                elif scalar_name == "string":
                    return "str"
                elif scalar_name == "bool":
                    return "bool"
                return scalar_name
            elif config_type.kind == ConfigTypeKind.ENUM:
                values = ", ".join(str(val) for val in config_type.config_values)
                return f"Enum[{values}]"
            elif config_type.kind == ConfigTypeKind.ARRAY:
                inner_type = self._dagster_type_repr(config_type.inner_type)
                return f"list[{inner_type}]"
            elif config_type.kind == ConfigTypeKind.SELECTOR:
                # For selectors, show the available options if possible
                if hasattr(config_type, "fields") and config_type.fields:
                    options = list(config_type.fields.keys())
                    if len(options) <= 3:
                        return f"Literal[{', '.join(repr(opt) for opt in options)}]"
                    else:
                        return f"Literal[{', '.join(repr(opt) for opt in options[:3])}, ...]"
                return "dict"  # Fallback to dict since selectors are dict-like
            elif config_type.kind == ConfigTypeKind.STRICT_SHAPE:
                return "dict"
            elif config_type.kind == ConfigTypeKind.PERMISSIVE_SHAPE:
                return "dict"
            elif config_type.kind == ConfigTypeKind.MAP:
                return "dict"
            elif config_type.kind == ConfigTypeKind.SCALAR_UNION:
                scalar_type = self._dagster_type_repr(config_type.scalar_type)
                non_scalar_type = self._dagster_type_repr(config_type.non_scalar_type)
                return f"{scalar_type} | {non_scalar_type}"
            elif config_type.kind == ConfigTypeKind.NONEABLE:
                inner_type = self._dagster_type_repr(config_type.inner_type)
                return f"{inner_type} | None"
            else:
                return "unknown"
        except Exception:
            return "unknown"

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
        """Add configuration schema documentation to the object's docstring."""

        if not schema_info["parameters"] or not obj.docstring:
            return

        # Get existing docstring content
        existing_content = obj.docstring.value if obj.docstring.value else ""

        # Only add configuration section if it doesn't already exist
        # Look for the exact pattern we add: "Configuration:" as a standalone section
        if "\nConfiguration:\n" not in existing_content:
            # Create YAML-style nested configuration documentation
            config_lines = ["", "Configuration:", "", "```yaml"]

            # Group parameters by their structure
            structure = self._build_config_structure(schema_info["parameters"])

            # Format as nested YAML-style structure
            yaml_lines = self._format_yaml_structure(structure, 0)
            config_lines.extend(yaml_lines)

            config_lines.append("```")
            config_section = "\n".join(config_lines)

            # Update the docstring
            obj.docstring = griffe.Docstring(existing_content + config_section)

    def _build_config_structure(self, parameters: list[dict[str, Any]]) -> dict:
        """Build nested structure from flat parameter list."""
        structure = {}

        # First, identify ALL parameter paths that have nested children
        has_children = set()
        all_paths = [param["name"] for param in parameters]

        for path in all_paths:
            # Check if any other path starts with this path + "."
            for other_path in all_paths:
                if other_path.startswith(path + "."):
                    has_children.add(path)
                    break

        for param in parameters:
            param_name = param["name"]
            parts = param_name.split(".")

            if param_name in has_children:
                # This parameter has nested children, skip it - we'll show the nested structure instead
                continue

            # Build the nested structure
            current = structure
            for i, part in enumerate(parts[:-1]):
                if part not in current:
                    current[part] = {}
                current = current[part]

            # Add the final parameter
            final_key = parts[-1]
            current[final_key] = param

        return structure

    def _format_yaml_structure(self, structure: dict, indent_level: int) -> list[str]:
        """Format the structure as proper YAML with type and description comments."""
        lines = []
        indent = "  " * indent_level

        for key, value in structure.items():
            if isinstance(value, dict) and "type" in value:
                # This is a leaf parameter - show it with its value
                param_type = value["type"]
                required = value.get("required", True)
                default = value.get("default")
                description = value.get("description", "")

                # Format as YAML with type and description as comment (keep description intact on one line)
                comment_parts = []
                comment_parts.append(f"({param_type})")
                if description:
                    # Keep description intact but on one line
                    clean_desc = description.replace("\n", " ").strip()
                    comment_parts.append(clean_desc)

                comment = " # " + " - ".join(comment_parts) if comment_parts else ""

                # Show proper YAML value
                if default is not None:
                    if isinstance(default, str):
                        yaml_value = f'"{default}"'
                    elif isinstance(default, bool):
                        yaml_value = str(default).lower()
                    elif default == {}:
                        yaml_value = "{}"
                    elif default == []:
                        yaml_value = "[]"
                    elif isinstance(default, dict):
                        yaml_value = "# has default configuration"
                    elif isinstance(default, list):
                        yaml_value = "# has default list"
                    else:
                        yaml_value = str(default)
                elif not required:
                    yaml_value = "null  # optional"
                else:
                    yaml_value = "# required"

                lines.append(f"{indent}{key}: {yaml_value}{comment}")

            elif isinstance(value, dict):
                # This is a nested section - always expand it
                lines.append(f"{indent}{key}:")
                lines.extend(self._format_yaml_structure(value, indent_level + 1))

        return lines

    def _format_config_parameter_simple(self, param: dict[str, Any]) -> list[str]:
        """Format a single configuration parameter for documentation."""
        lines = []

        # Parameter name and type
        param_name = param["name"]
        param_type = param["type"]
        optional_indicator = " (optional)" if not param.get("required") else ""

        lines.append(f"#### {param_name}{optional_indicator}")
        lines.append("")
        lines.append(f"Type: {param_type}")
        lines.append("")

        # Description
        if param.get("description"):
            lines.append(param["description"])
            lines.append("")

        # Default value
        if not param.get("required") and param.get("default") is not None:
            default_val = param["default"]
            lines.append(f"Default: `{default_val}`")
            lines.append("")

        return lines
