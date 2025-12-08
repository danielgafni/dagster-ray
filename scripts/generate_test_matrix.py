#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "packaging",
# ]
# ///
"""
Generate optimized test matrix for CI.

This script reduces the number of test jobs by ensuring each component version
is tested at least once without testing all possible combinations (full cartesian product).
"""

import json
import sys
from typing import NamedTuple

from packaging.version import Version


class VersionConfig(NamedTuple):
    """Version configuration without OS."""

    py: str
    ray: str
    dagster: str
    kuberay: str

    def is_valid(self) -> bool:
        """
        Check if this configuration is valid based on dependency constraints.

        Returns True if valid, False if the combination is incompatible.
        """
        # Python 3.12+ requires Ray >= 2.37.0
        if Version(self.py) >= Version("3.12") and Version(self.ray) < Version("2.37.0"):
            return False

        # Add more validation rules here as needed

        return True

    def with_os(self, os: str) -> "TestConfig":
        """Create a TestConfig by adding an OS to this VersionConfig."""
        return TestConfig(os=os, **self._asdict())


class TestConfig(NamedTuple):
    """Complete test configuration including OS."""

    os: str
    py: str
    ray: str
    dagster: str
    kuberay: str


# Component versions
OS_VERSIONS = ["ubuntu-latest"]
PYTHON_VERSIONS = ["3.12", "3.11"]
RAY_VERSIONS = ["2.49.2", "2.46.0"]
DAGSTER_VERSIONS = ["1.11.13", "1.11.6", "1.11.1", "1.10.21"]
KUBERAY_VERSIONS = ["1.4.0", "1.3.0", "1.2.2"]

# Component registry for DRY iteration
COMPONENTS = {
    "os": OS_VERSIONS,
    "py": PYTHON_VERSIONS,
    "ray": RAY_VERSIONS,
    "dagster": DAGSTER_VERSIONS,
    "kuberay": KUBERAY_VERSIONS,
}


def get_extremes(versions: list[str]) -> tuple[str, str]:
    """Get both latest and oldest versions using proper version comparison."""
    sorted_versions = sorted(versions, key=Version)
    return sorted_versions[-1], sorted_versions[0]


def generate_matrix() -> list[TestConfig]:
    """
    Generate test matrix with smart heuristic ensuring each version appears at most once:

    1. Generate base configurations (OS-agnostic)
       - Latest everything
       - Each component version individually with latest of everything else
       - Oldest combination
    2. Apply each base configuration to all OSes
    3. Deduplicate via set
    """
    # Get version components (exclude OS)
    version_components = {k: v for k, v in COMPONENTS.items() if k != "os"}

    # Get latest and oldest for each component
    extremes = {name: get_extremes(versions) for name, versions in version_components.items()}
    latest_versions = {name: latest for name, (latest, _) in extremes.items()}
    oldest_versions = {name: oldest for name, (_, oldest) in extremes.items()}

    # Generate base configurations (without OS), validating each before adding
    base_configs: set[VersionConfig] = set()

    # 1. Latest everything (if valid)
    latest_config = VersionConfig(**latest_versions)
    if latest_config.is_valid():
        base_configs.add(latest_config)

    # Track which versions have been used
    used_versions: dict[str, set[str]] = {name: {latest_versions[name]} for name in version_components}

    # 2. Each non-latest version with latest of everything else (if valid)
    for component_name, component_versions in version_components.items():
        for version in component_versions:
            if version not in used_versions[component_name]:
                config_dict = latest_versions.copy()
                config_dict[component_name] = version
                # Validate before adding
                config = VersionConfig(**config_dict)
                if config.is_valid():
                    base_configs.add(config)
                    used_versions[component_name].add(version)

    # 3. Oldest combination (if valid)
    oldest_config = VersionConfig(**oldest_versions)
    if oldest_config.is_valid():
        base_configs.add(oldest_config)

    # Apply each base configuration to all OSes
    configs: set[TestConfig] = {version_config.with_os(os) for os in OS_VERSIONS for version_config in base_configs}

    matrix = sorted(configs)

    # Verify all component versions are covered
    for component_name, component_versions in version_components.items():
        used_in_matrix = {getattr(config, component_name) for config in matrix}
        assert set(component_versions).issubset(used_in_matrix), (
            f"Not all {component_name} versions are in the matrix. Missing: {set(component_versions) - used_in_matrix}"
        )

    return matrix


def main():
    dry_run = "--dry-run" in sys.argv

    matrix = generate_matrix()

    # Calculate total possible combinations
    total_combinations = (
        len(OS_VERSIONS) * len(PYTHON_VERSIONS) * len(RAY_VERSIONS) * len(DAGSTER_VERSIONS) * len(KUBERAY_VERSIONS)
    )

    if dry_run:
        print(f"Generated {len(matrix)} test configurations (down from {total_combinations}):\n")
        for i, config in enumerate(matrix, 1):
            print(
                f"{i:2d}. {config.os}, Python {config.py}, Ray {config.ray}, "
                f"Dagster {config.dagster}, KubeRay {config.kuberay}"
            )
        print(f"\nTotal jobs: {len(matrix)} (reduced from {total_combinations})")

        # Verify all versions are covered
        print("\nCoverage check:")
        # Map field names to display names
        field_info = [
            ("os", "OS versions", None),
            ("py", "Python versions", Version),
            ("ray", "Ray versions", Version),
            ("dagster", "Dagster versions", Version),
            ("kuberay", "KubeRay versions", Version),
        ]
        for field, label, sort_key in field_info:
            unique_values = set(getattr(c, field) for c in matrix)
            sorted_values = sorted(unique_values, key=sort_key) if sort_key else sorted(unique_values)
            print(f"  {label}: {sorted_values}")
    else:
        # Output JSON for GitHub Actions - convert namedtuples to dicts
        output = {"include": [config._asdict() for config in matrix]}
        print(json.dumps(output))


if __name__ == "__main__":
    main()
