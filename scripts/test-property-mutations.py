#!/usr/bin/env python3
"""Prove generated producer laws shrink and replay semantic failures."""

import hashlib
import json
import re
import subprocess
import tarfile
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from coverage_common import ROOT, fingerprint, run

TARGET = "src/Kafka/Producer/ProducerProperties.hs"


def changed(source, old, new):
    if source.count(old) != 1:
        raise ValueError("property mutation must have one target: " + old)
    return source.replace(old, new, 1)


def main():
    candidate = fingerprint()
    base = ROOT / "dist-property-mutations"
    base.mkdir(exist_ok=True)
    output = Path(tempfile.mkdtemp(prefix=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ-"), dir=base))
    subprocess.run(["cabal", "sdist", "--output-directory=" + str(output)], cwd=ROOT, check=True)
    archive_path, = output.glob("*.tar.gz")
    with tarfile.open(archive_path) as archive:
        archive.extractall(output / "source", filter="data")
    package, = (output / "source").iterdir()
    (package / "cabal.project").write_text("packages: .\n")
    source = (package / TARGET).read_text()
    variants = [
        ("pure-control", source, "rdkafka-hs-test", "/Producer properties generated/", None),
        ("callback-control", source, "rdkafka-hs-integration", "/producer-properties/", None),
        ("global-bias-reversed", changed(source, "M.union k2 k1", "M.union k1 k2"), "rdkafka-hs-test", "/program follows last-write semantics/", "program follows last-write semantics"),
        ("topic-bias-reversed", changed(source, "M.union t2 t1", "M.union t1 t2"), "rdkafka-hs-test", "/program follows last-write semantics/", "program follows last-write semantics"),
        ("log-level-bias-reversed", changed(source, "ll2 `mplus` ll1", "ll1 `mplus` ll2"), "rdkafka-hs-test", "/program follows last-write semantics/", "program follows last-write semantics"),
        ("callback-order-reversed", changed(source, "cb1 `mplus` cb2", "cb2 `mplus` cb1"), "rdkafka-hs-integration", "/producer-properties/", "callback composition preserves order and identity"),
    ]
    results = []
    for name, text, suite, pattern, expected in variants:
        (package / TARGET).write_text(text)
        build_log = output / (name + ".build.log")
        with build_log.open("w") as log:
            subprocess.run(["cabal", "build", suite, "-f", "integration"], cwd=package, stdout=log, stderr=subprocess.STDOUT, check=True)
        binary = subprocess.check_output(["cabal", "list-bin", suite, "-f", "integration", "-v0"], cwd=package, text=True).strip()
        command = [binary, "-p", pattern, "--quickcheck-replay=20261024", "--quickcheck-tests=1000", "+RTS", "-N2", "-RTS"]
        result = subprocess.run(command, cwd=package, capture_output=True, text=True, timeout=120)
        test_log = output / (name + ".test.log")
        test_log.write_text(result.stdout + result.stderr)
        token = None
        replay_command = None
        if expected:
            if result.returncode != 1 or not re.search(re.escape(expected) + r":\s+FAIL", result.stdout) or "Falsified" not in result.stdout:
                raise ValueError("generated law did not catch mutation: " + str(test_log))
            match = re.search(r'Use --quickcheck-replay="([^"]+)"', result.stdout)
            if not match:
                raise ValueError("missing counterexample replay token: " + str(test_log))
            token = match[1]
            replay_command = ["--quickcheck-replay=" + token if a.startswith("--quickcheck-replay=") else a for a in command]
            replay = subprocess.run(replay_command, cwd=package, capture_output=True, text=True, timeout=120)
            replay_log = output / (name + ".replay.log")
            replay_log.write_text(replay.stdout + replay.stderr)
            if replay.returncode != 1 or not re.search(re.escape(expected) + r":\s+FAIL", replay.stdout) or "Falsified" not in replay.stdout:
                raise ValueError("counterexample did not replay: " + str(replay_log))
        elif result.returncode:
            raise ValueError("unchanged property control failed: " + str(test_log))
        results.append(dict(mutation=name, compiled=True, exit=result.returncode, command=command,
                            build_log=str(build_log.relative_to(ROOT)), test_log=str(test_log.relative_to(ROOT)),
                            replay_token=token, replay_command=replay_command))
        print(name + ": " + ("shrunk failure replayed" if expected else "control passed"), flush=True)
    (package / TARGET).write_text(source)
    if fingerprint() != candidate:
        raise ValueError("candidate changed during property mutations")
    evidence = dict(revision=run("git", "rev-parse", "HEAD").strip(), fingerprint=candidate,
                    archive_sha256=hashlib.sha256(archive_path.read_bytes()).hexdigest(), results=results)
    (output / "results.json").write_text(json.dumps(evidence, indent=2) + "\n")
    print("Property mutation evidence: " + str(output / "results.json"))


if __name__ == "__main__":
    main()
