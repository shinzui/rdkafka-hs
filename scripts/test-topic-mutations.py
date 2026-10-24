#!/usr/bin/env python3
"""Prove topic regressions reject semantic mutations in an isolated sdist."""

import json
import hashlib
from coverage_common import fingerprint, run
import re
import subprocess
import tarfile
import tempfile
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def replace_once(text, old, new):
    if text.count(old) != 1:
        raise ValueError("mutation target must occur exactly once: " + old)
    return text.replace(old, new, 1)


def section(text, start, end, change):
    first = text.index(start)
    last = text.index(end, first)
    return text[:first] + change(text[first:last]) + text[last:]


def mutations(source):
    no_config = section(source, "withNewTopic t action =", "withOldTopic ::", lambda body: re.sub(
        r"configured <- [\s\S]*?(?=\n\s*case configured of)", "configured <- pure (Right ())", body, count=1))
    old_acquisition = section(source, "withNewTopic t action =", "withOldTopic ::", lambda _: "withNewTopic t = withUnsafeOne t mkNewTopicUnsafe rdKafkaNewTopicDestroy\n\n")
    old_acquisition = section(old_acquisition, "mkNewTopicUnsafe topic =", "-- Keep the client alive", lambda _: """mkNewTopicUnsafe topic = do
  allocated <- newRdKafkaNewTopicUnsafe (T.unpack $ unTopicName $ topicName topic) (unPartitionCount $ topicPartitionCount topic) (unReplicationFactor $ topicReplicationFactor topic)
  case first (KafkaError . T.pack) allocated of
    Left err -> pure (Left err)
    Right native -> do
      configured <- whileRight (uncurry $ rdKafkaNewTopicSetConfig native) (M.toList $ topicConfig topic)
      pure $ case configured of
        Left err -> Left (KafkaResponseError err)
        Right () -> Right native

""")
    no_queue = re.sub(r"bracket \(newRdKafkaQueue kafka\)\s+finalizeForeignPtr", r"bracket (newRdKafkaQueue kafka) (\_ -> pure ())", source, count=1)
    no_options = re.sub(r"bracket \(newRdKAdminOptions kafka RdKafkaAdminOpAny\)\s+finalizeForeignPtr", r"bracket (newRdKAdminOptions kafka RdKafkaAdminOpAny) (\_ -> pure ())", source, count=1)
    no_delete = section(source, "withOldTopic tName transform =", "oldTopicPtr ::", lambda body: replace_once(body, "(mapM_ finalizeForeignPtr)", "(const (pure ()))"))
    no_event = section(source, "waitForResponse topic fromEvent", "singletonList ::", lambda body: replace_once(body, "(mapM_ finalizeForeignPtr)", "(const (pure ()))"))
    no_error = replace_once(source, "requestError <- rdKafkaEventError event", "requestError <- pure RdKafkaRespErrNoError")
    wrong_topic = replace_once(source, "[Right name] | TopicName (T.pack name) == topic -> Right topic", "[Right _] -> Right topic")
    return [
        ("configuration-removed", no_config, "/singleton/", "singleton configuration reaches the broker"),
        ("old-acquisition-lifetime", old_acquisition, "/setter rejection releases allocated/", "setter rejection releases allocated request before submission"),
        ("queue-finalization-removed", no_queue, "/setter rejection releases allocated/", "setter rejection releases allocated request before submission"),
        ("options-finalization-removed", no_options, "/setter rejection releases allocated/", "setter rejection releases allocated request before submission"),
        ("delete-request-finalization-removed", no_delete, "/missing topic deletion/", "missing topic deletion preserves its error and releases the request"),
        ("event-finalization-removed", no_event, "/broker-rejected values/", "broker-rejected values release requests and response events repeatedly"),
        ("native-request-error-ignored", no_error, "/unresponsive broker preserves/", "unresponsive broker preserves the native timeout error"),
        ("topic-identity-ignored", wrong_topic, "/wrong topic response/", "wrong topic response cannot report success"),
    ]


def main():
    candidate = fingerprint()
    revision = run("git", "rev-parse", "HEAD").strip()
    base = ROOT / "dist-topic-mutations"
    base.mkdir(exist_ok=True)
    output = Path(tempfile.mkdtemp(prefix=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ-"), dir=base))
    subprocess.run(["cabal", "sdist", "--output-directory=" + str(output)], cwd=ROOT, check=True)
    archives = list(output.glob("*.tar.gz"))
    if len(archives) != 1:
        raise ValueError("expected one candidate source archive")
    with tarfile.open(archives[0]) as archive:
        archive.extractall(output / "source", filter="data")
    package, = (output / "source").iterdir()
    # The copied package must never discover the parent checkout's project.
    (package / "cabal.project").write_text("packages: .\n")
    target = package / "src/Kafka/Topic.hs"
    source = target.read_text()
    variants = [("unchanged-control", source, "/topic-configuration/", None), *mutations(source)]
    results = []
    for name, text, pattern, expected_failure in variants:
        if expected_failure and text == source:
            raise ValueError("mutation did not change source: " + name)
        target.write_text(text)
        build_log = output / (name + ".build.log")
        with build_log.open("w") as log:
            subprocess.run(["cabal", "build", "rdkafka-hs-integration", "-f", "integration"], cwd=package, stdout=log, stderr=subprocess.STDOUT, check=True)
        binary = subprocess.check_output(["cabal", "list-bin", "rdkafka-hs-integration", "-f", "integration", "-v0"], cwd=package, text=True).strip()
        command = [binary, "-p", pattern, "+RTS", "-N2", "-A256m", "-RTS"]
        completed = subprocess.run(command, cwd=package, capture_output=True, text=True, timeout=120)
        test_log = output / (name + ".test.log")
        test_log.write_text(completed.stdout + completed.stderr)
        if expected_failure:
            if completed.returncode != 1 or not re.search(re.escape(expected_failure) + r":\s+FAIL", completed.stdout):
                raise ValueError("mutation did not fail its intended test: " + name + "; see " + str(test_log))
        elif completed.returncode != 0:
            raise ValueError("unchanged control failed: " + str(test_log))
        results.append(dict(mutation=name, compiled=True, exit=completed.returncode, pattern=pattern, expected_failure=expected_failure,
                            command=command, build_log=str(build_log.relative_to(ROOT)), test_log=str(test_log.relative_to(ROOT))))
        print(name + ": " + ("control passed" if expected_failure is None else "intended regression failed"), flush=True)
    target.write_text(source)
    evidence = dict(revision=revision, fingerprint=candidate, archive_sha256=hashlib.sha256(archives[0].read_bytes()).hexdigest(),
                    source_unchanged=fingerprint() == candidate, native=run("pkg-config", "--modversion", "rdkafka").strip(), results=results)
    if not evidence["source_unchanged"]:
        raise ValueError("candidate changed during mutation execution")
    (output / "results.json").write_text(json.dumps(evidence, indent=2) + "\n")
    print("Mutation evidence: " + str(output / "results.json"))


if __name__ == "__main__":
    main()
