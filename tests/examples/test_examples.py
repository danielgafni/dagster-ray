import shutil
import subprocess
import sys
from pathlib import Path

import pytest

EXAMPLES_DIR = Path(__file__).parent.parent.parent / "examples"
LOCAL_EXAMPLES_DIR = EXAMPLES_DIR / "local"


RUN_LAUNCHER_EXAMPLE_DIR = LOCAL_EXAMPLES_DIR / "run_launcher"
EXECUTOR_EXAMPLE_DIR = LOCAL_EXAMPLES_DIR / "executor"
RUN_LAUNCHER_AND_EXECUTOR_EXAMPLE_DIR = LOCAL_EXAMPLES_DIR / "run_launcher_and_executor"


@pytest.mark.parametrize(
    "example_dir", [RUN_LAUNCHER_EXAMPLE_DIR, EXECUTOR_EXAMPLE_DIR, RUN_LAUNCHER_AND_EXECUTOR_EXAMPLE_DIR]
)
def test_ray_run_launcher(local_ray_address: str, example_dir: Path, tmp_path_factory):
    dagster_home = tmp_path_factory.mktemp("dagster_home")

    # copy dagter.yaml from example_dir to dagster_home

    shutil.copy(example_dir / "dagster.yaml", dagster_home / "dagster.yaml")

    # TODO: rewrite this test in Dagster's Python API instead of calling the CLI
    # the CLI command doesn't actually use the RunLauncher!

    subprocess.run(
        f"""cd {example_dir} && {sys.executable} -m dagster job execute -f {example_dir / "definitions.py"} -j my_job
        """,
        check=True,
        shell=True,
        env={
            "DAGSTER_HOME": str(dagster_home),
        },
    )
