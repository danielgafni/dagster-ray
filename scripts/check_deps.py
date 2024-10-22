from argparse import ArgumentParser

from packaging.version import Version

parser = ArgumentParser()

parser.add_argument("--python", type=Version, help="Python version")
parser.add_argument("--dagster", type=Version, help="Dagster version")
parser.add_argument("--ray", type=Version, help="Dagster version")


def check_python_312(
    python: Version,
    dagster: Version,
    ray: Version,
):
    if python >= Version("3.12") and ray < Version("2.37.0"):
        raise RuntimeError("Ray version must be >=2.37.0 for Python >=3.12")


def main():
    args = parser.parse_args()
    check_python_312(
        python=args.python,
        dagster=args.dagster,
        ray=args.ray,
    )


if __name__ == "__main__":
    main()
