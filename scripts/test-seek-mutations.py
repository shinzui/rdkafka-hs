#!/usr/bin/env python3
"""Compile and run semantic seek/rewind mutations in an isolated source archive."""

import hashlib
import json
import re
import subprocess
import tarfile
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from coverage_common import ROOT, fingerprint, run

CONSUMER = "src/Kafka/Consumer.hs"
CONVERT = "src/Kafka/Consumer/Convert.hs"


def replace_once(source, old, new):
    if source.count(old) != 1:
        raise ValueError("mutation target must occur exactly once: " + old)
    return source.replace(old, new, 1)


def mutations(sources):
    c, conversion = sources[CONSUMER], sources[CONVERT]
    selection = "pure $ KafkaResponseError <$> find substantive errors"
    return [
        ("member-errors-dropped", CONSUMER, replace_once(c, selection, "pure Nothing"), "unassigned member reports native error"),
        ("error-order-reversed", CONSUMER, replace_once(c, selection, "pure $ KafkaResponseError <$> find substantive (reverse errors)"), "mixed errors follow actual request order"),
        ("pending-always-normalized", CONSUMER, replace_once(c, "(timeout /= 0 || code /= RdKafkaRespErrInProgress)", "code /= RdKafkaRespErrInProgress"), "nonzero-timeout preserves residual pending"),
        ("pending-never-normalized", CONSUMER, replace_once(c, "(timeout /= 0 || code /= RdKafkaRespErrInProgress)", "True"), "zero-timeout all pending promises initiation only"),
        ("message-commit-forgets-next-offset", CONVERT, replace_once(conversion, "PartitionOffset $ moff + 1", "PartitionOffset moff"), "rewind message commit resumes after committed message"),
        ("rewind-decrements-committed-offset", CONSUMER, replace_once(c, "let ps' = map checkOffsets ps", "let ps' = map (\\p -> case tpOffset p of { PartitionOffset n -> p {tpOffset = PartitionOffset (n - 1)}; _ -> p }) (map checkOffsets ps)"), "rewind uses committed next offset exactly"),
        ("rewind-discards-seek-error", CONSUMER, replace_once(c, "              seekPartitions c ps' to", "              seekPartitions c ps' to >> pure Nothing"), "rewind propagates assignment change after committed lookup"),
    ]


def main():
    candidate, revision = fingerprint(), run("git", "rev-parse", "HEAD").strip()
    base = ROOT / "dist-seek-mutations"
    base.mkdir(exist_ok=True)
    output = Path(tempfile.mkdtemp(prefix=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ-"), dir=base))
    subprocess.run(["cabal", "sdist", "--output-directory=" + str(output)], cwd=ROOT, check=True)
    archive_path, = output.glob("*.tar.gz")
    with tarfile.open(archive_path) as archive:
        archive.extractall(output / "source", filter="data")
    package, = (output / "source").iterdir()
    (package / "cabal.project").write_text("packages: .\n")
    sources = {path: (package / path).read_text() for path in (CONSUMER, CONVERT)}
    variants = [("unchanged-control", None, None, None), *mutations(sources)]
    results = []
    for name, changed_file, changed_source, test in variants:
        for path, source in sources.items():
            (package / path).write_text(changed_source if path == changed_file else source)
        build_log, test_log = output / (name + ".build.log"), output / (name + ".test.log")
        with build_log.open("w") as stream:
            subprocess.run(["cabal", "build", "rdkafka-hs-integration", "-f", "integration"], cwd=package, stdout=stream, stderr=subprocess.STDOUT, check=True)
        binary = subprocess.check_output(["cabal", "list-bin", "rdkafka-hs-integration", "-f", "integration", "-v0"], cwd=package, text=True).strip()
        pattern = "/seek-partitions/"
        if test:
            pattern = "/seek-partitions/&&/" + test + "/"
        command = [binary, "-p", pattern, "+RTS", "-N2", "-RTS"]
        result = subprocess.run(command, cwd=package, capture_output=True, text=True, timeout=120)
        test_log.write_text(result.stdout + result.stderr)
        failed_test = test and result.returncode == 1 and re.search(re.escape(test) + r":\s+FAIL", result.stdout)
        if test and not failed_test:
            raise ValueError("mutation did not fail its intended regression: " + name + "; see " + str(test_log))
        if not test and result.returncode != 0:
            raise ValueError("unchanged control failed: " + str(test_log))
        results.append(dict(mutation=name, compiled=True, exit=result.returncode, test=test,
                            command=command, build_log=str(build_log.relative_to(ROOT)), test_log=str(test_log.relative_to(ROOT))))
        print(name + ": " + ("control passed" if not test else "intended regression failed"), flush=True)
    for path, source in sources.items():
        (package / path).write_text(source)
    if fingerprint() != candidate:
        raise ValueError("candidate changed during seek mutations")
    evidence = dict(revision=revision, fingerprint=candidate, native=run("pkg-config", "--modversion", "rdkafka").strip(),
                    archive_sha256=hashlib.sha256(archive_path.read_bytes()).hexdigest(), results=results)
    (output / "results.json").write_text(json.dumps(evidence, indent=2) + "\n")
    print("Mutation evidence: " + str(output / "results.json"))


if __name__ == "__main__":
    main()
