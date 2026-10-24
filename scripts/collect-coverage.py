#!/usr/bin/env python3
"""Run both suites on one instrumented candidate and retain source maps."""

import hashlib
import json
import os
import re
import subprocess
import sys
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from coverage_common import ROOT, components, fingerprint, run

SUITES = ("rdkafka-hs-test", "rdkafka-hs-integration")
BUILD = "dist-coverage"
OUTPUT = ROOT / BUILD / "reports"


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def collect():
    OUTPUT.mkdir(parents=True, exist_ok=True)
    before = fingerprint()
    command = ["cabal", "test", *SUITES, "-f", "integration", "--enable-coverage", "--builddir=" + BUILD,
               "--ghc-options=-fforce-recomp -F -pgmF scripts/coverage-preprocess.py", "--test-show-details=direct"]
    # A previous run must never contribute ticks. Only this build directory is
    # touched; retain the old logs while replacing its machine evidence.
    for tix in (ROOT / BUILD).rglob("*.tix"):
        tix.unlink()
    log = OUTPUT / "test.log"
    with log.open("w") as stream:
        subprocess.run(command, cwd=ROOT, stdout=stream, stderr=subprocess.STDOUT, check=True)
    if fingerprint() != before:
        raise ValueError("candidate changed during coverage execution; discard this run")
    cs = components(BUILD)
    library = cs["lib"] / "build"
    mixfiles = sorted(p for p in library.rglob("*.mix") if "vanilla" in p.parts)
    mixdirs = sorted({p.parent.parent for p in mixfiles})
    if not mixdirs:
        raise ValueError("missing library MIX data")
    modules = sorted({p.stem for directory in mixdirs for p in directory.rglob("*.mix")})
    expected_modules = sorted(str(p.relative_to(ROOT / "src")).rsplit(".", 1)[0].replace("/", ".")
                              for p in (ROOT / "src").rglob("*") if p.suffix in (".hs", ".chs"))
    if modules != expected_modules:
        raise ValueError("library MIX module set does not match hand-written sources")
    include = ["--include=" + m for m in modules]
    mixargs = ["--hpcdir=" + str(d) for d in mixdirs]
    zeros = OUTPUT / "unlinked-zero.tix"
    zero_args = [arg for p in mixfiles for arg in (p.parent.name + "/" + p.stem, str(p))]
    run("runghc", "-package=hpc", "scripts/coverage-zero.hs", str(zeros), *zero_args)
    tixfiles = []
    tests = []
    artifacts = [log, zeros]
    for suite in SUITES:
        matches = list(cs["test:" + suite].rglob(suite + ".tix"))
        if len(matches) != 1 or not matches[0].stat().st_size:
            raise ValueError(f"missing or ambiguous TIX data for {suite}")
        expanded = OUTPUT / (suite + ".tix")
        run("hpc", "sum", "--union", *include, "--output=" + str(expanded), str(matches[0]), str(zeros))
        tixfiles.append(expanded)
        artifacts.append(expanded)
        binary = cs["test:" + suite] / "build" / suite / suite
        probe_env = dict(os.environ, HPCTIXFILE=str(OUTPUT / (suite + "-probe.tix")))
        names = subprocess.check_output([str(binary), "--list-tests", "+RTS", "--read-tix-file=no", "-RTS"], cwd=ROOT, env=probe_env, text=True).splitlines()
        root = "rdkafka-hs" if suite == "rdkafka-hs-test" else suite
        if not names or any(not n.startswith(root + ".") for n in names):
            raise ValueError(f"unexpected test list for {suite}")
        tests.extend(names)
        report = OUTPUT / (suite + ".txt")
        report.write_text(run("hpc", "report", *mixargs, *include, "--per-module", "--decl-list", str(expanded)))
        artifacts.extend([matches[0], report])
    counts = [int(n) for n in re.findall(r"All (\d+) tests passed", log.read_text())]
    if len(counts) != 2 or sum(counts) != len(tests):
        raise ValueError("suite pass counts do not match the complete registered test list")
    combined = OUTPUT / "combined.tix"
    run("hpc", "sum", "--union", *include, "--output=" + str(combined), *map(str, tixfiles))
    artifacts.append(combined)
    for name, options in (("combined.txt", ["--per-module", "--decl-list"]), ("combined.xml", ["--xml-output", "--per-module"])):
        path = OUTPUT / name
        path.write_text(run("hpc", "report", *mixargs, *options, str(combined)))
        artifacts.append(path)
    ticks = run("hpc", "show", *mixargs, str(combined))
    (OUTPUT / "ticks.txt").write_text(ticks)
    artifacts.append(OUTPUT / "ticks.txt")
    mapping_path = library / "Kafka/Internal/RdKafka.hs.coverage-map.json"
    mapping = json.loads(mapping_path.read_text())
    artifacts.append(mapping_path)
    matrix = json.loads((ROOT / "docs/testing/coverage-matrix.json").read_text())
    symbol_rows = {s: r for r in matrix["contracts"] for s in r["symbols"]}
    misses = []
    native_ticks = 0
    module_ticks = {module: 0 for module in modules}
    classified_metrics = {"handwritten": {}, "generated": {}}
    unused_functions = []
    for line in ticks.splitlines():
        match = re.fullmatch(r'\s*(\d+)\s+(\d+)\s+[^ ]+/(Kafka[\w.]+)\s+(\d+):([\d:-]+)\s+(.+)', line)
        if not match:
            raise ValueError("unrecognised HPC tick/source format: " + line)
        index, hits, module, source_line, _, label = match.groups()
        module_ticks[module] += 1
        source_line = int(source_line)
        source = "src/" + module.replace(".", "/") + ".hs"
        generated = False
        if module == "Kafka.Internal.RdKafka":
            if source_line < 1 or source_line > len(mapping) or mapping[source_line - 1] is None:
                raise ValueError("unmapped native binding tick")
            origin = mapping[source_line - 1]
            generated = origin["generated"]
            source, source_line = origin["file"], origin["line"]
            if not generated:
                native_ticks += 1
        if not source or not (ROOT / source).is_file():
            raise ValueError("missing source for HPC tick")
        kind = label.split(" [", 1)[0]
        category = "generated" if generated else "handwritten"
        metric = classified_metrics[category].setdefault(kind, {"ticks": 0, "hit": 0})
        metric["ticks"] += 1
        metric["hit"] += int(int(hits) > 0)
        if int(hits) == 0 and label.startswith("TopLevelBox") and not generated:
            unused_functions.append(dict(module=module, source=source, line=source_line, label=label))
        if int(hits) == 0:
            declaration = re.search(r'TopLevelBox \["([^"]+)"', label)
            symbol = module + "." + declaration[1] if declaration else None
            row = symbol_rows.get(symbol)
            fallback = "OWN-binding-coverage" if module == "Kafka.Internal.RdKafka" else "OWN-source-coverage"
            misses.append(dict(module=module, source=source, line=source_line, tick=int(index), label=label,
                               generated=generated, contract=row["id"] if row else fallback, owner=row["owner"] if row else 14))
    if native_ticks == 0:
        raise ValueError("HPC did not measure hand-written .chs wrappers; refuse inflated report")
    for module in modules:
        path = ROOT / "src" / (module.replace(".", "/") + ".hs")
        if path.exists() and re.search(r"^[a-z][\w']*\s*::", path.read_text(), re.M) and module_ticks[module] == 0:
            raise ValueError("HPC omitted hand-written functions in " + module)
    for name, data in (("classified-metrics.json", classified_metrics), ("unexercised-functions.json", unused_functions)):
        path = OUTPUT / name
        path.write_text(json.dumps(data, indent=2) + "\n")
        artifacts.append(path)
    misses_path = OUTPUT / "misses.json"
    misses_path.write_text(json.dumps(misses, indent=2) + "\n")
    artifacts.append(misses_path)
    native_binary = cs["test:rdkafka-hs-test"] / "build/rdkafka-hs-test/rdkafka-hs-test"
    loaded_native = subprocess.check_output([str(native_binary), "--native-version", "+RTS", "--read-tix-file=no", "-RTS"], cwd=ROOT,
                                            env=dict(os.environ, HPCTIXFILE=str(OUTPUT / "native-probe.tix")), text=True).strip()
    admin = os.getenv("RDKAFKA_TEST_ADMIN", "http://127.0.0.1:39644")
    with urllib.request.urlopen(admin + "/v1/brokers", timeout=5) as response:
        broker_versions = [{"node_id": b["node_id"], "version": b["version"]} for b in json.load(response)]
    if not broker_versions:
        raise ValueError("missing actual broker version")
    evidence = dict(test_configuration={key: os.getenv(key) for key in ("RDKAFKA_TEST_BROKER", "RDKAFKA_TEST_ADMIN", "RDKAFKA_TEST_RPK", "RDKAFKA_TEST_UNKNOWN_TOPIC_CONFIG_POLICY")}, schema_version=1, revision=run("git", "rev-parse", "HEAD").strip(), fingerprint=before,
                    at=datetime.now(timezone.utc).isoformat(), command=command, suites=list(SUITES), tests=tests,
                    passed_test_count=sum(counts), toolchain=run("ghc", "--numeric-version").strip(), cabal=run("cabal", "--numeric-version").strip(),
                    loaded_native=loaded_native, broker_versions=broker_versions, native=run("pkg-config", "--modversion", "rdkafka").strip(),
                    broker=os.getenv("RDKAFKA_TEST_BROKER", "127.0.0.1:39092"),
                    misses=misses, module_tick_counts=module_ticks, measured_native_wrapper_ticks=native_ticks,
                    artifacts={str(p.relative_to(ROOT)): digest(p) for p in artifacts})
    if fingerprint() != before:
        raise ValueError("candidate changed during report generation")
    (ROOT / BUILD / "evidence.json").write_text(json.dumps(evidence, indent=2) + "\n")
    print(f"Coverage evidence: {BUILD}/evidence.json; {len(tests)} registered tests; {native_ticks} native-wrapper ticks")


if __name__ == "__main__":
    try:
        collect()
    except (ValueError, OSError, subprocess.CalledProcessError) as error:
        print(f"Coverage collection failed: {error}; see {OUTPUT}/test.log", file=sys.stderr)
        sys.exit(1)
