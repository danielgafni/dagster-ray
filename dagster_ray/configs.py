from typing import List, Optional

from dagster import Config


class ContainerConfig(Config):
    image: Optional[str] = None


class RuntimeEnvConfig(Config):
    working_dir: Optional[str] = None
    pip: Optional[List[str]] = None
    container: Optional[ContainerConfig] = None
