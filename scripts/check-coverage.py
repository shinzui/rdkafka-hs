#!/usr/bin/env python3
"""Fail closed on unassigned exports, missing tests and stale evidence."""

import argparse
import hashlib
import json
import sys
from pathlib import Path
from coverage_common import ROOT, compiler_exports, fingerprint

# Original repair owners remain valid; the expanded qualification plans own
# transferred obligations. EP-20 consumes evidence rather than producing it.
OWNERS = frozenset(range(8, 20))


def validate(matrix, exports, evidence, allow_open):
    errors = []
    if matrix.get("schema_version") != 1:
        errors.append("unsupported matrix schema")
    if matrix.get("exports") != exports:
        errors.append("public exports changed: reconcile compiler-derived exports and contract rows")
    symbols = {e["symbol"] for entries in exports.values() for e in entries}
    assigned = set()
    ids = set()
    registered = set() if evidence is None else set(evidence["tests"])
    for row in matrix.get("contracts", []):
        rid = row.get("id", "<missing ID>")
        if rid in ids:
            errors.append(f"duplicate row {rid}")
        ids.add(rid)
        for key in ("symbols", "behavior", "inputs", "expected", "modes", "ownership", "owner", "tests", "status"):
            if key not in row or (key not in ("symbols", "tests") and not row[key]):
                errors.append(f"{rid}: missing {key}")
        if row.get("owner") not in OWNERS:
            errors.append(f"{rid}: invalid child owner")
        if "previous_owner" in row and (row["previous_owner"] not in range(8, 16) or row.get("owner") not in range(16, 20)):
            errors.append(f"{rid}: invalid ownership transfer")
        for symbol in row.get("symbols", []):
            if symbol not in symbols:
                errors.append(f"{rid}: unknown symbol {symbol}")
            assigned.add(symbol)
        status = row.get("status")
        if status not in ("unimplemented", "tested", "not-applicable"):
            errors.append(f"{rid}: invalid status")
        if status == "unimplemented" and not allow_open:
            errors.append(f"{rid}: open (EP-{row.get('owner')})")
        if status == "not-applicable" and not row.get("rationale"):
            errors.append(f"{rid}: missing source-backed non-applicability rationale")
        tests = row.get("tests", [])
        if status == "tested":
            if not tests or not evidence:
                errors.append(f"{rid}: no test evidence")
            if row.get("fingerprint") != fingerprint():
                errors.append(f"{rid}: stale source evidence")
        for test in tests:
            if test not in registered:
                errors.append(f"{rid}: unregistered test {test}")
    for symbol in sorted(symbols - assigned):
        errors.append(f"unassigned public symbol {symbol}")
    for required in ("F1", "F2", "F3", "F4", "F5", "F6"):
        if required not in ids:
            errors.append(f"missing defect row {required}")
    if evidence is None:
        errors.append("missing measured evidence: run just coverage")
    else:
        if evidence.get("fingerprint") != fingerprint():
            errors.append("measured evidence is stale: run just coverage")
        if set(evidence.get("suites", [])) != {"rdkafka-hs-test", "rdkafka-hs-integration"}:
            errors.append("coverage requires both suites")
        artifacts = evidence.get("artifacts", {})
        if not isinstance(artifacts, dict) or not artifacts:
            errors.append("missing artifact checksums")
            artifacts = {}
        for artifact, checksum in artifacts.items():
            path = ROOT / artifact
            if not path.is_file():
                errors.append(f"missing coverage artifact {artifact}")
            elif hashlib.sha256(path.read_bytes()).hexdigest() != checksum:
                errors.append(f"modified coverage artifact {artifact}")
        if evidence.get("measured_native_wrapper_ticks", 0) <= 0:
            errors.append("missing hand-written .chs coverage")
        if not evidence.get("tests"):
            errors.append("empty registered-test evidence")
        runtime_tests = {name for name in registered if ".runtime-contract." in name}
        runtime = [run for run in evidence.get("supplemental_runs", []) if run.get("kind") == "nonthreaded-runtime"]
        if len(runtime) != 1 or not runtime_tests:
            errors.append("missing nonthreaded runtime coverage")
        else:
            supplement = runtime[0]
            way = supplement.get("rts_way")
            if not isinstance(way, str) or "thr" in way:
                errors.append("runtime supplement is not nonthreaded")
            if set(supplement.get("tests", [])) != runtime_tests or supplement.get("passed") != len(runtime_tests):
                errors.append("runtime supplement does not cover every registered runtime contract")
            if supplement.get("tix") not in artifacts:
                errors.append("missing runtime coverage artifact")
        exceptions = matrix.get("miss_classifications", {})
        for miss in evidence.get("misses", []):
            if miss.get("owner") not in OWNERS:
                errors.append(f"unclassified coverage miss: {miss}")
            if miss.get("contract") not in ids:
                errors.append(f"coverage miss has no contract row: {miss}")
            if not miss.get("generated"):
                key = f"{miss['module']}:{miss['line']}:{miss['label']}"
                classification = exceptions.get(key, {})
                if classification:
                    source = ROOT / miss.get("source", "")
                    if (classification.get("source") != miss.get("source") or not source.is_file()
                            or classification.get("source_sha256") != hashlib.sha256(source.read_bytes()).hexdigest()):
                        errors.append(f"stale source classification: {key}")
                if not allow_open and (classification.get("status") not in ("unreachable", "inapplicable") or not classification.get("rationale")):
                    errors.append(f"unexplained hand-written coverage miss: {key} (EP-{miss.get('owner')})")
    return errors


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--allow-open", action="store_true")
    parser.add_argument("--builddir", default="dist-newstyle")
    parser.add_argument("--evidence", default="dist-coverage/evidence.json")
    args = parser.parse_args()
    try:
        matrix = json.loads((ROOT / "docs/testing/coverage-matrix.json").read_text())
        evidence_path = ROOT / args.evidence
        evidence = json.loads(evidence_path.read_text()) if evidence_path.exists() else None
        errors = validate(matrix, compiler_exports(args.builddir), evidence, args.allow_open)
    except (ValueError, KeyError, OSError) as err:
        errors = [str(err)]
    for error in errors:
        print(error, file=sys.stderr)
    if errors:
        return 1
    print(f"Coverage inventory valid ({len(matrix['contracts'])} rows, allow_open={args.allow_open})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
