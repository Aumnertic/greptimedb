#!/usr/bin/env python3
# Copyright 2023 Greptime Team
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

"""Analyze stopped remote-write scenario manifests, one region at a time."""

import argparse
import json
import subprocess
import tomllib
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--work-dir", required=True, type=Path)
    parser.add_argument("--analyzer", required=True, type=Path)
    parser.add_argument("--cases-root", required=True, type=Path)
    args = parser.parse_args()
    errors = 0
    for case_dir in sorted(args.work_dir.iterdir() if args.work_dir.exists() else []):
        if not any((case_dir / target / "datanode-0" / "data").is_dir() for target in ("base", "candidate")):
            continue
        prepared = case_dir / "prepare-remote.json"
        prepared_ok = False
        if prepared.exists():
            info = json.loads(prepared.read_text(encoding="utf-8"))
            case_path = Path(info["case_path"])
            prepared_ok = info.get("status") == "ok"
        else:
            case_path = args.cases_root / case_dir.name / "case.toml"
        if not case_path.is_file():
            continue
        with case_path.open("rb") as case_file:
            case = tomllib.load(case_file)
        if case.get("scenario", {}).get("kind") != "prom_remote_write_then_query":
            continue
        start = case["scenario"]["remote_write"]["start_unix_millis"]
        output = case_dir / "metadata_count_analysis_report.txt"
        status = "completed preparation" if prepared_ok else "INCOMPLETE: preparation did not finish successfully"
        sections = [f"Case: {case_path}\nPreparation: {status}\n"]
        for target in ("base", "candidate"):
            root = case_dir / target / "datanode-0" / "data"
            manifest_dirs = sorted(path for path in root.glob("data/**/manifest") if path.is_dir())
            sections.append(f"\n--- {target}: {root} ---\n")
            if not manifest_dirs:
                sections.append("No region manifests found\n")
                if prepared_ok:
                    errors += 1
            for manifest_dir in manifest_dirs:
                region = manifest_dir.parent.relative_to(root)
                result = subprocess.run(
                    [str(args.analyzer), "--data-home", str(root), "--region-dir", str(region), "--start-ms", str(start)],
                    text=True, capture_output=True, check=False,
                )
                sections.append(f"\n=== Region directory: {region} ===\n{result.stdout}")
                if result.returncode:
                    errors += 1
                    sections.append(f"ERROR: analyzer exited {result.returncode}\n{result.stderr}\n")
        output.write_text("".join(sections), encoding="utf-8")
        print(f"Wrote {output}")
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
