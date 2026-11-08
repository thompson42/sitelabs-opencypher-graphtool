# Copyright 2026 S. Alex Thompson
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Copy docs/query-catalogue.md into a project that uses the engine, as it is at the tag the
project pins (its requirements.txt), so the project documents the openCypher it actually runs.

    python scripts/install_catalogue.py ../sitelabs-graph-database-conda           # write the copy
    python scripts/install_catalogue.py --check ../sitelabs-partitioned-graph      # is it current?

The copy is <project>/docs/query-catalogue.md: whatever the project keeps above the marker line
(its docs menu, written by its own docs tool), the marker, then this repository's file at that tag,
unchanged. The marker names the tag and the sha256 of what follows it; the project's tests check
both, so a copy edited by hand, or left behind when the pin moves, fails them. Change the catalogue
here, release a tag, move the project's pin, then run this.
"""
import argparse
import hashlib
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SOURCE = "docs/query-catalogue.md"
PIN = re.compile(r"sitelabs-opencypher-graphtool\.git@(v\d+\.\d+\.\d+)\b")
MARKER = re.compile(r"^<!-- query-catalogue: sitelabs-opencypher-graphtool (v\d+\.\d+\.\d+) sha256:([0-9a-f]{64})\b.*-->\n",
                    re.M)


def pinned_tag(project: Path) -> str:
    req = project / "requirements.txt"
    m = PIN.search(req.read_text()) if req.exists() else None
    if m is None:
        sys.exit(f"{req}: no 'sitelabs-opencypher-graphtool.git@vX.Y.Z' pin")
    return m.group(1)


def catalogue_at(tag: str) -> str:
    out = subprocess.run(["git", "-C", str(ROOT), "show", f"{tag}:{SOURCE}"], capture_output=True, text=True)
    if out.returncode != 0:
        sys.exit(f"{SOURCE} at {tag}: {out.stderr.strip()} (git fetch --tags? the catalogue starts at v0.1.5)")
    return out.stdout


def marker(tag: str, body: str) -> str:
    digest = hashlib.sha256(body.encode()).hexdigest()
    return (f"<!-- query-catalogue: sitelabs-opencypher-graphtool {tag} sha256:{digest}. A copy of that "
            f"tag's {SOURCE}: change it there, then run its scripts/install_catalogue.py. -->\n")


def build(project: Path) -> tuple[Path, str]:
    tag = pinned_tag(project)
    body = catalogue_at(tag)
    dest = project / "docs" / "query-catalogue.md"
    old = dest.read_text() if dest.exists() else ""
    m = MARKER.search(old)
    head = old[:m.start()] if m else ""
    return dest, head + marker(tag, body) + body


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("project", type=Path, help="the project's directory")
    ap.add_argument("--check", action="store_true", help="change nothing; exit 1 if the copy is out of date")
    a = ap.parse_args()
    dest, text = build(a.project.resolve())
    current = dest.read_text() if dest.exists() else None
    if current == text:
        print(f"{dest}: up to date")
        return 0
    if a.check:
        print(f"{dest}: out of date (run python scripts/install_catalogue.py {a.project})")
        return 1
    dest.write_text(text)
    print(f"{dest}: written, from {pinned_tag(a.project.resolve())}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
