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
from coverage_common import compiler_generated_declaration, library_hpc_includes, ROOT, components, fingerprint, run

SUITES = ("rdkafka-hs-test", "rdkafka-hs-integration")
BUILD = "dist-coverage"
OUTPUT = ROOT / BUILD / "reports"


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def runtime_coverage():
    build = BUILD + "-nonthreaded"
    build_log = OUTPUT / "nonthreaded-build.log"
    command = ["cabal", "build", "rdkafka-hs-integration", "-f", "integration", "--enable-coverage", "--builddir=" + build,
               "--ghc-options=-fforce-recomp -F -pgmF scripts/coverage-preprocess.py -single-threaded -with-rtsopts=-T"]
    with build_log.open("w") as stream:
        subprocess.run(command, cwd=ROOT, stdout=stream, stderr=subprocess.STDOUT, check=True)
    component = components(build)["test:rdkafka-hs-integration"]
    binary = component / "build/rdkafka-hs-integration/rdkafka-hs-integration"
    info = subprocess.check_output([str(binary), "+RTS", "--info"], cwd=ROOT, text=True)
    way = re.search(r'\("RTS way", "([^"]+)"\)', info)
    if way is None or "thr" in way[1]:
        raise ValueError("runtime supplement did not link a nonthreaded executable")
    info_log = OUTPUT / "nonthreaded-rts.txt"
    info_log.write_text(info)
    tix = OUTPUT / "nonthreaded-runtime.tix"
    tix.unlink(missing_ok=True)
    environment = dict(os.environ, HPCTIXFILE=str(tix))
    options = ["-p", "/runtime-contract/", "+RTS", "--read-tix-file=no", "-RTS"]
    names = subprocess.check_output([str(binary), "--list-tests", *options], cwd=ROOT, text=True,
                                    env=dict(environment, HPCTIXFILE=str(OUTPUT / "nonthreaded-list.tix"))).splitlines()
    log = OUTPUT / "nonthreaded-runtime.log"
    with log.open("w") as stream:
        subprocess.run([str(binary), *options], cwd=ROOT, stdout=stream, stderr=subprocess.STDOUT, check=True, env=environment)
    counts = re.findall(r"All (\d+) tests passed", log.read_text())
    if not names or counts != [str(len(names))] or not tix.is_file():
        raise ValueError("missing complete nonthreaded runtime test coverage")
    # The alternative RTS branch is in Consumer.hs. Generated c2hs output has
    # different MIX hashes between these builds, so never combine its indices
    # across builds. Retain the full raw artifact, and let hpc verify that the
    # ordinary Consumer module is identical when it is merged below.
    consumer_tix = OUTPUT / "nonthreaded-consumer.tix"
    mixfiles = (components(BUILD)["lib"] / "build").rglob("Kafka.Consumer.mix")
    includes = library_hpc_includes([path for path in mixfiles if "vanilla" in path.parts])
    if len(includes) != 1:
        raise ValueError("missing unambiguous Consumer library MIX")
    run("hpc", "sum", "--union", *includes, "--output=" + str(consumer_tix), str(tix))
    evidence = dict(kind="nonthreaded-runtime", rts_way=way[1], tests=names, passed=len(names),
                    command=[str(binary), *options], build_command=command, merged_modules=["Kafka.Consumer"],
                    raw_tix=str(tix.relative_to(ROOT)), tix=str(consumer_tix.relative_to(ROOT)))
    return evidence, [build_log, info_log, log, tix, consumer_tix]


def collect():
    OUTPUT.mkdir(parents=True, exist_ok=True)
    before = fingerprint()
    command = ["cabal", "test", *SUITES, "-f", "integration", "--enable-coverage", "--builddir=" + BUILD,
               "--ghc-options=-fforce-recomp -F -pgmF scripts/coverage-preprocess.py", "--test-show-details=direct",
               "--test-options=--quickcheck-tests=1000 --quickcheck-replay=20261024"]
    # A previous run must never contribute ticks. Only this build directory is
    # touched; retain the old logs while replacing its machine evidence.
    for tix in (ROOT / BUILD).rglob("*.tix"):
        tix.unlink()
    child_directory = ROOT / BUILD / "children"
    child_directory.mkdir(exist_ok=True)
    log = OUTPUT / "test.log"
    with log.open("w") as stream:
        subprocess.run(command, cwd=ROOT, stdout=stream, stderr=subprocess.STDOUT, check=True,
                       env=dict(os.environ, RDKAFKA_TEST_CHILD_HPC_DIR=str(child_directory)))
    runtime, runtime_artifacts = runtime_coverage()
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
    # Unit tests compile the internal owner in isolation. Select the library
    # package explicitly so its test-local copy cannot inflate library coverage.
    include = library_hpc_includes(mixfiles)
    mixargs = ["--hpcdir=" + str(d) for d in mixdirs]
    zeros = OUTPUT / "unlinked-zero.tix"
    zero_args = [arg for p in mixfiles for arg in (p.parent.name + "/" + p.stem, str(p))]
    run("runghc", "-package=hpc", "scripts/coverage-zero.hs", str(zeros), *zero_args)
    tixfiles = []
    tests = []
    artifacts = [log, zeros, *runtime_artifacts]
    children = sorted(child_directory.glob("*.tix"))
    for mode in ("global", "message"):
        if len(list(child_directory.glob("producer-" + mode + "-*.tix"))) != 1:
            raise ValueError("missing or ambiguous successful producer callback child coverage: " + mode)
    for kind in ("error", "log", "stats", "rebalance", "commit"):
        for mode in ("async", "sync"):
            if len(list(child_directory.glob("lifetime-" + kind + "-" + mode + "-*.tix"))) != 1:
                raise ValueError("missing or ambiguous callback lifetime child coverage: " + kind + "/" + mode)
    artifacts.extend(children)
    for kind in ("metadata", "groups"):
        if len(list(child_directory.glob("metadata-" + kind + "-*.tix"))) != 1:
            raise ValueError("missing or ambiguous metadata cancellation child coverage: " + kind)
    for protocol in ("cooperative", "eager"):
        for operation in ("assign", "revoke"):
            for mode in ("async", "sync"):
                for failure in (("native", "decode") if protocol == "cooperative" else ("native",)):
                    prefix = "rebalance-" + protocol + "-" + operation + "-" + mode + "-" + failure
                    if len(list(child_directory.glob(prefix + "-*.tix"))) != 1:
                        raise ValueError("missing or ambiguous rebalance error child coverage: " + prefix)
    for suite in SUITES:
        matches = list(cs["test:" + suite].rglob(suite + ".tix"))
        if len(matches) != 1 or not matches[0].stat().st_size:
            raise ValueError(f"missing or ambiguous TIX data for {suite}")
        expanded = OUTPUT / (suite + ".tix")
        child_tix = [*map(str, children), runtime["tix"]] if suite == "rdkafka-hs-integration" else []
        run("hpc", "sum", "--union", *include, "--output=" + str(expanded), str(matches[0]), str(zeros), *child_tix)
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
    if set(runtime["tests"]) != {name for name in tests if ".runtime-contract." in name}:
        raise ValueError("nonthreaded runtime coverage does not match the registered runtime contracts")
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
    source_lines = {}
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
        if not source or not (ROOT / source).is_file():
            raise ValueError("missing source for HPC tick")
        if not generated:
            if source not in source_lines:
                source_lines[source] = (ROOT / source).read_text().splitlines()
            lines = source_lines[source]
            if not 1 <= source_line <= len(lines):
                raise ValueError("coverage tick has no source line")
            generated = compiler_generated_declaration(lines[source_line - 1])
        if module == "Kafka.Internal.RdKafka" and not generated:
            native_ticks += 1
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
            if row is None and module == "Kafka.Internal.CallbackOwner":
                row = next(r for r in matrix["contracts"] if r["id"] == "F5")
            fallback = "OWN-binding-coverage" if module == "Kafka.Internal.RdKafka" else "OWN-source-coverage"
            if row is None:
                row = next(r for r in matrix["contracts"] if r["id"] == fallback)
            misses.append(dict(module=module, source=source, line=source_line, tick=int(index), label=label,
                               generated=generated, contract=row["id"], owner=row["owner"]))
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
                    passed_test_count=sum(counts), supplemental_runs=[runtime], toolchain=run("ghc", "--numeric-version").strip(), cabal=run("cabal", "--numeric-version").strip(),
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
