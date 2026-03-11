#!/usr/bin/env python
"""Deploy zensical-built docs with mike's versioned gh-pages machinery.

Builds docs with zensical, then uses mike's Python API to commit
the built site into a versioned directory on the gh-pages branch.
"""

import argparse
import os
import subprocess

from mike import commands, git_utils, mkdocs_utils
from mike.mkdocs_utils import docs_version_var


def main():
    parser = argparse.ArgumentParser(
        description="Build docs with zensical and deploy with mike versioning",
    )
    parser.add_argument("version", help="version to deploy this build to")
    parser.add_argument("aliases", nargs="*", help="additional aliases for this build")
    parser.add_argument("-u", "--update-aliases", action="store_true")
    parser.add_argument("-p", "--push", action="store_true")
    parser.add_argument("-b", "--branch", default="gh-pages")
    parser.add_argument("-r", "--remote", default="origin")
    parser.add_argument("-F", "--config-file", default=None)
    args = parser.parse_args()

    # Build with zensical, setting the version env var that mike normally sets
    env = os.environ.copy()
    env[docs_version_var] = args.version
    subprocess.run(["zensical", "build", "--clean"], check=True, env=env)

    # Load mkdocs config (mike needs this for site_dir, use_directory_urls, etc.)
    cfg = mkdocs_utils.load_config(args.config_file)

    # Deploy using mike's git machinery — site_dir is already populated by zensical
    with commands.deploy(
        cfg,
        args.version,
        aliases=args.aliases,
        update_aliases=args.update_aliases,
        branch=args.branch,
    ):
        pass  # nothing to do — zensical already built into site_dir

    if args.push:
        git_utils.push_branch(args.remote, args.branch)


if __name__ == "__main__":
    main()
