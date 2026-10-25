#!/usr/bin/env python3
"""Fail closed on unassigned exports, missing tests and stale evidence."""

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path
from coverage_common import ROOT, compiler_exports, fingerprint, qualification_context_errors, source_scope_errors

# Original repair owners remain valid; the expanded qualification plans own
# transferred obligations. EP-20 consumes evidence rather than producing it.
OWNERS = frozenset(range(8, 20))


REQUIRED_TUPLES = {
    'darwin-arm64-ghc9124-native215-redpanda2621-current': ('9.12.4', '2.15.0', {'complete-normal-and-mapped', 'native-invalid-access-complement'}),
    'linux-arm64-ghc9141-native2160-apache440': ('9.14.1', '2.16.0', {'released-linux-unit-build', 'native-ownership-diagnostics', 'actual-nonthreaded-runtime', 'apache-topic-configuration-reject'}),
    'linux-arm64-ghc9141-native2160-apache440-18': ('9.14.1', '2.16.0', {'distributed-failure-recovery'}),
    'linux-arm64-ghc9141-native2160-apache440-19': ('9.14.1', '2.16.0', {'authentication-tls-acl-reconnect'}),
}
REQUIRED_CONTROLS = {
    'linux-invalid-access': (99, 'Invalid read'),
    'linux-header-retention': (99, 'rd_kafka_headers_new'),
    'darwin-invalid-access': (-6, 'AddressSanitizer'),
    'producer-allocation-cleanup': (1, 'callback capture remained reachable after release'),
}


def artifact_errors(artifact):
    if not isinstance(artifact, dict):
        return ['missing artifact identity']
    name, checksum = artifact.get('path'), artifact.get('sha256')
    if not isinstance(name, str) or Path(name).is_absolute() or '..' in Path(name).parts:
        return ['invalid artifact path']
    path = ROOT / name
    if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != checksum:
        return ['missing or modified compatibility artifact: ' + name]
    return []


def passed_log_errors(log, counts):
    errors = artifact_errors(log)
    if not errors:
        text = (ROOT / log['path']).read_text()
        if [int(n) for n in re.findall(r'All (\d+) tests passed', text)] != counts or re.search(r': FAIL(?:\s|$)', text):
            errors.append('compatibility log does not prove expected nonzero tests: ' + log['path'])
    return errors


def scenario_tests(sid, coverage):
    primary = coverage.get("primary_tests", [])
    if sid == "complete-normal-and-mapped":
        return primary
    if sid == "released-linux-unit-build":
        return [name for name in primary if name.startswith("rdkafka-hs.")]
    if sid == "actual-nonthreaded-runtime":
        return [name for name in primary if ".runtime-contract." in name]
    if sid == "apache-topic-configuration-reject":
        return [name for name in primary if "unsupported configuration follows" in name]
    if sid in ("native-ownership-diagnostics", "native-invalid-access-complement"):
        return [name for name in primary if any(part in name for part in (".producer-regressions.", ".callback-lifetime.", ".transaction-errors.", ".lifecycle."))
                or (".public-contract." in name and any(part in name for part in (".messages.", ".partition-list-ownership.", ".metadata-contract.", ".diagnostics.")))]
    mode = {"distributed-failure-recovery": "--broker-qualification", "authentication-tls-acl-reconnect": "--authentication-qualification"}.get(sid)
    contexts = [context for context in coverage.get("qualification_contexts", []) if context.get("mode") == mode]
    return contexts[0].get("tests", []) if len(contexts) == 1 else []


def validate_compatibility(matrix, report, coverage):
    errors = []
    candidate = fingerprint()
    if report.get('schema_version') != 1 or report.get('status') != 'passed' or report.get('fingerprint') != candidate:
        errors.append('missing, incomplete or stale comprehensive evidence')
    qualification = matrix.get('qualification', {})
    if qualification.get('candidate_fingerprint') != candidate or qualification.get('status') != 'passed':
        errors.append('incomplete or stale compatibility matrix')
    tuples = qualification.get('tuples', [])
    if {t.get('id') for t in tuples} != set(REQUIRED_TUPLES) or len(tuples) != len(REQUIRED_TUPLES):
        errors.append('missing or duplicate required tuple')
    for row in tuples:
        rid = row.get('id')
        if rid not in REQUIRED_TUPLES:
            errors.append('unknown compatibility tuple')
            continue
        ghc, native, required = REQUIRED_TUPLES[rid]
        if row.get('status') != 'passed' or row.get('required') is not True:
            errors.append(rid + ': required tuple unavailable or not passed')
        errors.extend(source_scope_errors(row.get('source', {}), candidate))
        environment = row.get('environment', {})
        if environment.get('ghc') != ghc or environment.get('native_version') != native or environment.get('loaded_native_identity', {}).get('version') != native:
            errors.append(rid + ': wrong actual compiler/loaded native version')
        if any(not environment.get(key) for key in ('os', 'kernel', 'architecture', 'cabal', 'broker_version', 'broker_topology')):
            errors.append(rid + ': missing actual environment')
        if environment.get('architecture') not in ('arm64', 'aarch64') or ('Darwin' if rid.startswith('darwin') else 'Linux') not in environment.get('kernel', ''):
            errors.append(rid + ': wrong actual OS/architecture')
        configuration = row.get('configuration_artifact')
        found = artifact_errors(configuration)
        errors.extend(found)
        if not found:
            if not row.get('test_configuration') or json.loads((ROOT / configuration['path']).read_text()) != row.get('test_configuration'):
                errors.append(rid + ': stale test configuration')
        scenarios = row.get('scenarios', [])
        if {s.get('id') for s in scenarios} != required or len(scenarios) != len(required):
            errors.append(rid + ': missing required scenario')
        for scenario in scenarios:
            sid = rid + '/' + str(scenario.get('id'))
            tests = scenario.get('tests', [])
            if scenario.get('status') != 'passed' or scenario.get('required') is not True or not scenario.get('command'):
                errors.append(sid + ': incomplete required scenario')
            if not tests or len(tests) != len(set(tests)) or scenario.get('passed') != len(tests):
                errors.append(sid + ': zero, duplicate or incomplete test identities')
            if set(tests) != set(scenario_tests(scenario.get('id'), coverage)):
                errors.append(sid + ': tests differ from complete registered scenario')
            counts = scenario.get('log_counts', [len(tests)])
            if not counts or any(not isinstance(n, int) or n <= 0 for n in counts) or sum(counts) != len(tests):
                errors.append(sid + ': invalid expected log counts')
            errors.extend(passed_log_errors(scenario.get('log'), counts))
            if scenario.get('id') == 'native-ownership-diagnostics':
                reports = scenario.get('diagnostic_reports', [])
                if not reports:
                    errors.append(sid + ': missing native memory reports')
                for memory_report in reports:
                    found = artifact_errors(memory_report)
                    errors.extend(found)
                    if not found:
                        text = (ROOT / memory_report['path']).read_text()
                        if ('ERROR SUMMARY: 0 errors' not in text
                                or not re.search(r'definitely lost:\s+0 bytes', text)
                                or not re.search(r'indirectly lost:\s+0 bytes', text)
                                or 'rd_kafka_headers_new' in text):
                            errors.append(sid + ': native memory finding or retained headers')
            if scenario.get('id') == 'native-invalid-access-complement' and not artifact_errors(scenario.get('log')):
                if 'ERROR: AddressSanitizer' in (ROOT / scenario['log']['path']).read_text():
                    errors.append(sid + ': native invalid-access finding')
            if not scenario.get('artifacts'):
                errors.append(sid + ': missing scenario artifacts')
            for artifact in scenario.get('artifacts', {}).values():
                errors.extend(artifact_errors(artifact))
    controls = report.get('controls', {})
    if set(controls) != set(REQUIRED_CONTROLS):
        errors.append('missing required ownership diagnostic control')
    for name, (code, signature) in REQUIRED_CONTROLS.items():
        control = controls.get(name, {})
        log = control.get('log')
        if control.get('exit') != code or control.get('status') != 'detected':
            errors.append(name + ': diagnostic control did not detect its fault')
        found = artifact_errors(log)
        errors.extend(found)
        if not found and signature not in (ROOT / log['path']).read_text():
            errors.append(name + ': intended diagnostic absent')
    package = report.get('package', {})
    errors.extend(artifact_errors(package.get('archive')))
    errors.extend(source_scope_errors(package.get('source', {}), candidate, require_archive=True))
    primary = coverage.get('primary_tests', [])
    if not primary or package.get('tests') != primary or package.get('passed') != len(primary):
        errors.append('clean package did not execute every primary test')
    counts = package.get('log_counts', [])
    if len(counts) != 2 or any(not isinstance(n, int) or n <= 0 for n in counts) or sum(counts) != len(primary):
        errors.append('invalid clean-package suite counts')
    errors.extend(passed_log_errors(package.get('log'), counts))
    example = package.get('example', {})
    example_log = example.get('log')
    found = artifact_errors(example_log)
    errors.extend(found)
    if not package.get('built_examples') or example.get('records') != 4 or example.get('committed_offset') != 4:
        errors.append('missing packaged example execution')
    if not found and 'EXAMPLE VERIFIED: four exact records, ordered offsets, fancy header and committed offset 4' not in (ROOT / example_log['path']).read_text():
        errors.append('packaged example exact assertions absent')
    if not report.get('artifacts'):
        errors.append('missing comprehensive artifacts')
    for artifact in report.get('artifacts', {}).values():
        errors.extend(artifact_errors(artifact))
    return errors


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
        contexts = evidence.get("qualification_contexts", [])
        modes = set()
        for context in contexts:
            errors.extend(qualification_context_errors(context, fingerprint()))
            if context.get("mode") in modes:
                errors.append("duplicate qualification context")
            modes.add(context.get("mode"))
            if not set(context.get("tests", [])) <= registered:
                errors.append("qualification context tests are not registered in evidence")
            if any(artifacts.get(name) != checksum for name, checksum in context.get("artifacts", {}).items()):
                errors.append("qualification artifacts are absent from aggregate evidence")
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
    parser.add_argument("--compatibility", action="store_true", help="require complete tuples, controls and clean package replay")
    args = parser.parse_args()
    try:
        matrix = json.loads((ROOT / "docs/testing/coverage-matrix.json").read_text())
        evidence_path = ROOT / args.evidence
        evidence = json.loads(evidence_path.read_text()) if evidence_path.exists() else None
        errors = validate(matrix, compiler_exports(args.builddir), evidence, args.allow_open)
        if args.compatibility:
            if args.allow_open:
                errors.append("full compatibility qualification cannot allow open coverage")
            compatibility = json.loads((ROOT / "docs/testing/compatibility-matrix.json").read_text())
            report = json.loads((ROOT / "docs/testing/comprehensive-validation-evidence.json").read_text())
            errors.extend(validate_compatibility(compatibility, report, evidence or {}))
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
