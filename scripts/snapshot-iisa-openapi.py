#!/usr/bin/env python3
"""Write the snapshot of IISA's API that dipper's IISA client is tested against.

Dipper asks IISA which indexers to offer agreements to and which indexers accept them, through
the client in dipper-iisa/src/http_client.rs. Tests beside it compare what the client sends and
reads with the OpenAPI schema this script writes, so the build fails when the client drifts from
the recording committed here. IISA drifting from that recording fails nothing by itself: the
scheduled iisa-drift workflow records IISA's main branch afresh with this script, into a file of
its own given by --output, and runs the same tests against it.

Run it from the root of this repo, giving it a checkout of IISA
(graphprotocol/subgraph-dips-indexer-selection). It needs `uv`:

    scripts/snapshot-iisa-openapi.py ~/github/subgraph-dips-indexer-selection

The script reads the schema from IISA's app without starting a server, in IISA's own locked
environment, and refuses a checkout with uncommitted changes, so the snapshot always holds what
the commit it names serves.
"""

import argparse
import json
import os
import pathlib
import subprocess
import sys

SNAPSHOT = pathlib.Path("dipper-iisa/src/http_client/iisa_openapi.json")

# Run inside IISA's environment: build the app's schema the way FastAPI serves it at /openapi.json.
READ_SCHEMA = (
    "import json, sys\n"
    "from iisa.iisa_http_endpoints import app\n"
    "json.dump(app.openapi(), sys.stdout)\n"
)


def git(iisa, *args):
    result = subprocess.run(
        ["git", "-C", str(iisa), *args], check=True, capture_output=True, text=True
    )
    return result.stdout.strip()


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("iisa", type=pathlib.Path, help="a checkout of IISA")
    parser.add_argument(
        "--output",
        type=pathlib.Path,
        default=SNAPSHOT,
        help=f"where to write the recording (default: {SNAPSHOT}, the copy the tests read)",
    )
    arguments = parser.parse_args()
    iisa = arguments.iisa.resolve()

    if git(iisa, "status", "--porcelain"):
        sys.exit(f"{iisa} has uncommitted changes: the snapshot must match a commit")
    commit = git(iisa, "rev-parse", "HEAD")

    # Importing the app would otherwise leave compiled files in the checkout.
    env = dict(os.environ, PYTHONDONTWRITEBYTECODE="1")
    result = subprocess.run(
        ["uv", "run", "--locked", "--quiet", "python", "-c", READ_SCHEMA],
        cwd=iisa,
        env=env,
        check=True,
        stdout=subprocess.PIPE,
        text=True,
    )
    snapshot = {
        "written_by": "scripts/snapshot-iisa-openapi.py: do not edit it by hand",
        "iisa_commit": commit,
        "schema": json.loads(result.stdout),
    }
    arguments.output.write_text(json.dumps(snapshot, indent=2, sort_keys=True) + "\n")
    print(f"wrote {arguments.output} from IISA {commit[:8]}")


if __name__ == "__main__":
    main()
