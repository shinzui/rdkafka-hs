import copy
import hashlib
import re
import importlib.util
import json
import sys
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parent
# Install into scripts/test-coverage-tools.py before use.
sys.path.insert(0, str(ROOT))
import coverage_common as common


def load(name, file):
    spec = importlib.util.spec_from_file_location(name, ROOT / file)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


checker = load("checker", "check-coverage.py")
preprocessor = load("preprocessor", "coverage-preprocess.py")


class GateControls(unittest.TestCase):
    def setUp(self):
        self.matrix = json.loads((checker.ROOT / "docs/testing/coverage-matrix.json").read_text())
        self.exports = self.matrix["exports"]
        self.evidence = dict(fingerprint="candidate", tests=["suite.case"], suites=["rdkafka-hs-test", "rdkafka-hs-integration"],
                             artifacts={"README.md": hashlib.sha256((checker.ROOT / "README.md").read_bytes()).hexdigest()},
                             measured_native_wrapper_ticks=1, misses=[])
        self.identity = patch.object(checker, "fingerprint", return_value="candidate")
        self.identity.start()
        self.addCleanup(self.identity.stop)

    def errors(self, allow=True):
        return checker.validate(self.matrix, self.exports, self.evidence, allow)

    def test_owned_open_inventory_is_valid_for_development(self):
        self.assertEqual(self.errors(), [])

    def test_final_gate_rejects_open_rows(self):
        self.assertTrue(any("open (EP-" in e for e in self.errors(False)))

    def test_temporary_unassigned_export_is_rejected(self):
        self.exports = copy.deepcopy(self.exports)
        self.exports["Kafka.Version"].append(dict(symbol="Kafka.Version.untestedExport", members=[]))
        errors = self.errors()
        self.assertTrue(any("public exports changed" in e for e in errors))
        self.assertTrue(any("unassigned public symbol Kafka.Version.untestedExport" in e for e in errors))

    def test_new_constructor_cannot_hide_inside_existing_type(self):
        self.exports = copy.deepcopy(self.exports)
        self.exports["Kafka.Version"][0]["members"].append("Kafka.Version.NewConstructor")
        self.assertTrue(any("public exports changed" in e for e in self.errors()))

    def test_missing_test_is_rejected(self):
        self.matrix["contracts"][0]["tests"] = ["removed.test"]
        self.assertTrue(any("unregistered test removed.test" in e for e in self.errors()))

    def test_stale_source_is_rejected(self):
        self.evidence["fingerprint"] = "previous candidate"
        self.assertIn("measured evidence is stale: run just coverage", self.errors())

    def test_missing_tix_artifact_is_rejected(self):
        self.evidence["artifacts"] = {"dist-coverage/missing-control.tix": "missing"}
        self.assertTrue(any("missing coverage artifact" in e for e in self.errors()))

    def test_modified_artifact_is_rejected(self):
        self.evidence["artifacts"]["README.md"] = "wrong"
        self.assertTrue(any("modified coverage artifact" in e for e in self.errors()))

    def test_zero_native_wrapper_coverage_is_rejected(self):
        self.evidence["measured_native_wrapper_ticks"] = 0
        self.assertIn("missing hand-written .chs coverage", self.errors())

    def test_missing_suite_is_rejected(self):
        self.evidence["suites"] = ["rdkafka-hs-test"]
        self.assertIn("coverage requires both suites", self.errors())

    def test_unexplained_source_miss_is_rejected(self):
        self.evidence["misses"] = [dict(module="Kafka.Version", line=1, label="ExpBox False", generated=False, contract="OWN-source-coverage", owner=14)]
        self.assertTrue(any("unexplained hand-written coverage miss" in e for e in self.errors(False)))

    def test_normal_haskell_is_unchanged(self):
        source = "module Example where\nexample = 1\n"
        actual, mapping = preprocessor.transform("Example.hs", source)
        self.assertEqual(actual, '{-# LINE 1 "Example.hs" #-}\n' + source)
        self.assertIsNone(mapping)

    def test_c2hs_spans_map_both_generated_and_handwritten_code(self):
        source = (checker.ROOT / "src/Kafka/Internal/RdKafka.chs").read_text().splitlines()
        generated = next(i for i, s in enumerate(source, 1) if s.startswith("{#"))
        handwritten = next(i for i, s in enumerate(source, 1) if s.startswith("newRdKafkaConfT ::"))
        text = f'{{-# LINE {generated} "src/Kafka/Internal/RdKafka.chs" #-}}\ngenerated = 1\n{{-# LINE {handwritten} "src/Kafka/Internal/RdKafka.chs" #-}}\nwrapper = 2\n'
        actual, mapping = preprocessor.transform("Generated.hs", text)
        self.assertTrue(mapping[1]["generated"])
        self.assertFalse(mapping[3]["generated"])
        self.assertEqual(mapping[3]["line"], handwritten)
        self.assertNotIn('"src/Kafka/Internal/RdKafka.chs"', actual)
        self.assertIn('{-# LINE 1 "Generated.hs" #-}', actual)

    def test_compiler_derived_temporary_export_is_rejected(self):
        with tempfile.TemporaryDirectory(prefix="rdkafka-export-control-") as directory:
            root = Path(directory)
            source = root / "Version.hs"
            source.write_text("module Kafka.Version (untestedExport) where\nuntestedExport :: Int\nuntestedExport = 1\n")
            subprocess.run(["ghc", "-c", str(source), "-odir", str(root / "build"), "-hidir", str(root / "build")], check=True, capture_output=True)
            with patch.object(common, "exposed_modules", return_value=["Kafka.Version"]), patch.object(common, "components", return_value={"lib": root}):
                exports = common.compiler_exports("unused")
            self.exports = copy.deepcopy(self.exports)
            self.exports["Kafka.Version"] = exports["Kafka.Version"]
            self.assertIn("unassigned public symbol Kafka.Version.untestedExport", self.errors())

    def test_semantic_fixture_mutations_fail_after_successful_compilation(self):
        with tempfile.TemporaryDirectory(prefix="rdkafka-fixture-controls-") as directory:
            root = Path(directory)
            for name in ("FixtureSupport", "FixtureSupportTest", "CallbackTestSupport", "FaultTestSupport"):
                target = root / "Kafka" / (name + ".hs")
                target.parent.mkdir(exist_ok=True)
                target.write_text((checker.ROOT / "test-integration/Kafka" / (name + ".hs")).read_text())
            (root / "Main.hs").write_text("module Main (main) where\nimport Kafka.FixtureSupportTest (tests)\nimport Test.Tasty (defaultMain)\nmain :: IO ()\nmain = defaultMain tests\n")
            source_path = root / "Kafka/FixtureSupport.hs"
            source = source_path.read_text()
            variants = [
                ("control", source, 0, None),
                ("fabricated enqueue success", source.replace("Left (ImmediateError err) -> pure (Left err)", "Left (ImmediateError _) -> pure (Right ())"), 1, "immediate enqueue rejection propagates"),
                ("swallowed poll error", source.replace("| otherwise -> pure (Left err)", "| otherwise -> go acc"), 1, "unexpected poll error propagates")]
            for name, text, expected_exit, failed_case in variants:
                with self.subTest(mutation=name):
                    if failed_case:
                        self.assertNotEqual(source, text)
                    source_path.write_text(text)
                    command = ["cabal", "exec", "--", "ghc", "--make", "-O1", "-threaded", "-XGHC2024", "-XOverloadedStrings", "-i" + str(root), "-package", "rdkafka-hs", str(root / "Main.hs"), "-o", str(root / "controls")]
                    compiled = subprocess.run(command, cwd=checker.ROOT, capture_output=True, text=True)
                    self.assertEqual(compiled.returncode, 0, compiled.stdout + compiled.stderr)
                    result = subprocess.run([str(root / "controls")], capture_output=True, text=True)
                    self.assertEqual(result.returncode, expected_exit, result.stdout + result.stderr)
                    if failed_case:
                        self.assertRegex(result.stdout, re.escape(failed_case) + r":\s+FAIL")


if __name__ == "__main__":
    unittest.main()
