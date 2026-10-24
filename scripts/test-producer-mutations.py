#!/usr/bin/env python3
"""Compile and run semantic producer mutations in an isolated source archive."""

import hashlib
import json
import os
import re
import subprocess
import tarfile
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from coverage_common import ROOT, fingerprint, run

PRODUCER = "src/Kafka/Producer.hs"
CALLBACKS = "src/Kafka/Producer/Callbacks.hs"
CONVERT = "src/Kafka/Producer/Convert.hs"


def replace_once(source, old, new):
    if source.count(old) != 1:
        raise ValueError("mutation target must occur exactly once: " + old)
    return source.replace(old, new, 1)


def mutations(sources):
    p, c, conversion = sources[PRODUCER], sources[CALLBACKS], sources[CONVERT]
    apply = "  setAllKafkaConfValues kc (KafkaProps (ppTopicProps pps))"
    empty = "withBS (Just bs) f | BS.null bs = allocaBytes 1 $ \\p -> f p 0\n"
    release = "                  freeStablePtr callbackPtr\n"
    delayed_release = replace_once(p, release, "")
    delayed_release = replace_once(delayed_release, "                  res <- handleProduceErrT code\n", "                  res <- handleProduceErrT code\n" + release)
    return [
        ("topic-settings-dropped", PRODUCER, replace_once(p, apply, ""), "explicit topic timeout expires an accepted message", False),
        ("rejection-root-retained", PRODUCER, replace_once(p, release, ""), "queue-full rejection releases callback capture without invoking it", False),
        ("accepted-root-freed", PRODUCER, replace_once(p, "then pure (Right ())", "then freeStablePtr callbackPtr >> pure (Right ())"), "accepted callback retains its capture until a purged report completes", True),
        ("enqueue-mask-removed", PRODUCER, replace_once(p, "\\topicName -> mask_ $ do", "\\topicName -> do"), "cancellation after native acceptance retains callback until its report", True),
        ("global-exception-skips-message", CALLBACKS, replace_once(c, "outcome <- try @SomeException (restore (callback rep))", "outcome <- (Right <$> restore (callback rep)) :: IO (Either SomeException ())"), "throwing global callback preserves per-message delivery and releases capture", False),
        ("delivery-root-retained", CALLBACKS, replace_once(c, "bracket (pure $ castPtrToStablePtr cbPtr) freeStablePtr", "bracket (pure $ castPtrToStablePtr cbPtr) (const (pure ()))"), "accepted callback retains its capture until a purged report completes", False),
        ("empty-bytes-become-absent", PRODUCER, replace_once(p, empty, ""), "byte-exact round trip with NoCompression", False),
        ("input-validation-dropped", PRODUCER, replace_once(p, "case recordError msg of", "case Nothing of"), "rejects before native enqueue: NUL topic name", False),
        ("global-overrides-topic", PRODUCER, replace_once(p, apply, apply + "\n  setAllKafkaConfValues kc (KafkaProps (ppKafkaProps pps))"), "actual client configuration: explicit topic wins duplicate", False),
        ("headers-dropped", PRODUCER, replace_once(p, "rdKafkaMessageProduceVa k (hdrs ++ opts)", "rdKafkaMessageProduceVa k opts"), "byte-exact round trip with NoCompression", False),
        ("rejection-release-after-decoding", PRODUCER, delayed_release, "native error decoding failure releases rejected callback capture", False),
        ("nonnull-success-code-accepted", PRODUCER, replace_once(p, "pure $ Left (ImmediateError (maybe KafkaInvalidReturnValue id res))", "pure $ maybe (Right ()) (Left . ImmediateError) res"), "non-null native success-code error rejects and releases capture", False),
        ("unknown-error-replaced-by-errno", CONVERT, replace_once(conversion, "handleProduceErrT RdKafkaRespErrNoError", "handleProduceErrT RdKafkaRespErrUnknown = Just . kafkaRespErr <$> getErrno\nhandleProduceErrT RdKafkaRespErrNoError"), "unknown native error code is preserved instead of reading errno", False),
    ]


def main():
    candidate, revision = fingerprint(), run("git", "rev-parse", "HEAD").strip()
    base = ROOT / "dist-producer-mutations"
    base.mkdir(exist_ok=True)
    output = Path(tempfile.mkdtemp(prefix=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ-"), dir=base))
    subprocess.run(["cabal", "sdist", "--output-directory=" + str(output)], cwd=ROOT, check=True)
    archive_path, = output.glob("*.tar.gz")
    with tarfile.open(archive_path) as archive:
        archive.extractall(output / "source", filter="data")
    package, = (output / "source").iterdir()
    (package / "cabal.project").write_text("packages: .\n")
    sources = {path: (package / path).read_text() for path in (PRODUCER, CALLBACKS, CONVERT)}
    variants = [("unchanged-control", None, None, None, False), *mutations(sources)]
    results = []
    for name, changed_file, changed_source, test, safety_probe in variants:
        for path, source in sources.items():
            (package / path).write_text(changed_source if path == changed_file else source)
        build_log, test_log = output / (name + ".build.log"), output / (name + ".test.log")
        with build_log.open("w") as stream:
            subprocess.run(["cabal", "build", "rdkafka-hs-integration", "-f", "integration"], cwd=package, stdout=stream, stderr=subprocess.STDOUT, check=True)
        binary = subprocess.check_output(["cabal", "list-bin", "rdkafka-hs-integration", "-f", "integration", "-v0"], cwd=package, text=True).strip()
        pattern = "/producer-regressions/"
        if test:
            pattern = "/producer-regressions/&&/" + test + "/"
        command = [binary, "-p", pattern, "+RTS", "-N2", "-RTS"]
        environment = dict(os.environ)
        if safety_probe:
            environment["RDKAFKA_TEST_PRODUCER_SAFETY_PROBE"] = name
        result = subprocess.run(command, cwd=package, capture_output=True, text=True, timeout=120, env=environment)
        test_log.write_text(result.stdout + result.stderr)
        failed_test = test and result.returncode == 1 and re.search(re.escape(test) + r":\s+FAIL", result.stdout)
        marker = "producer safety probe: native enqueue accepted (" + name + ")"
        failed_safety = safety_probe and result.returncode < 0 and marker in result.stderr
        if test and not (failed_test or failed_safety):
            raise ValueError("mutation did not fail its intended regression: " + name + "; see " + str(test_log))
        if not test and result.returncode != 0:
            raise ValueError("unchanged control failed: " + str(test_log))
        results.append(dict(mutation=name, compiled=True, exit=result.returncode, test=test, safety_probe=safety_probe,
                            command=command, build_log=str(build_log.relative_to(ROOT)), test_log=str(test_log.relative_to(ROOT))))
        print(name + ": " + ("control passed" if not test else "intended regression failed"), flush=True)
    for path, source in sources.items():
        (package / path).write_text(source)
    if fingerprint() != candidate:
        raise ValueError("candidate changed during producer mutations")
    evidence = dict(revision=revision, fingerprint=candidate, native=run("pkg-config", "--modversion", "rdkafka").strip(),
                    archive_sha256=hashlib.sha256(archive_path.read_bytes()).hexdigest(), results=results)
    (output / "results.json").write_text(json.dumps(evidence, indent=2) + "\n")
    print("Mutation evidence: " + str(output / "results.json"))


if __name__ == "__main__":
    main()
