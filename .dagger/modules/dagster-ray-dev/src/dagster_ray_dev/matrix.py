import tomllib
from collections import Counter
from typing import NamedTuple

from packaging.specifiers import SpecifierSet
from packaging.version import Version


class Versions(NamedTuple):
    python: str
    ray: str
    dagster: str
    kuberay: str


def _excluded(versions: Versions, excludes: list[dict[str, str]]) -> bool:
    return any(
        all(Version(getattr(versions, name)) in SpecifierSet(spec) for name, spec in exclude.items())
        for exclude in excludes
    )


class Matrix(NamedTuple):
    kubernetes: str
    versions: list[Versions]


def expand(config_toml: str) -> Matrix:
    """Expand a test-matrix.toml into the version combinations to test (see the file for the rules)."""
    config = tomllib.loads(config_toml)
    excludes: list[dict[str, str]] = config.get("exclude", [])
    components = {name: sorted(config[name], key=Version) for name in Versions._fields}
    latest = Versions(**{name: versions[-1] for name, versions in components.items()})
    oldest = Versions(**{name: versions[0] for name, versions in components.items()})

    jobs = [
        v
        for v in dict.fromkeys([latest, oldest] + [latest._replace(kuberay=k) for k in components["kuberay"]])
        if not _excluded(v, excludes)
    ]
    # Other versions are spread over KubeRay versions (fewest jobs first, newest on ties), so the per-KubeRay checks
    # get similar numbers of jobs.
    load = Counter(v.kuberay for v in jobs)
    kuberay_newest_first = components["kuberay"][::-1]
    for name in ("python", "ray", "dagster"):
        for version in components[name]:
            if version == getattr(latest, name):
                continue
            for kuberay in sorted(kuberay_newest_first, key=load.__getitem__):
                candidate = latest._replace(**{name: version, "kuberay": kuberay})
                if candidate not in jobs and not _excluded(candidate, excludes):
                    jobs.append(candidate)
                    load[kuberay] += 1
                    break
    matrix = sorted(jobs)

    for name, versions in components.items():
        missing = set(versions) - {getattr(v, name) for v in matrix}
        if missing:
            raise ValueError(f"{name} versions {sorted(missing)} are excluded from every combination")
    return Matrix(kubernetes=config["kubernetes"], versions=matrix)
