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
    def test_only_compiler_deriving_declarations_are_generated(self):
        for line in ("  deriving stock (Eq, Show)", "deriving newtype (Num)", "deriving stock instance Eq Native"):
            self.assertTrue(common.compiler_generated_declaration(line))
        for line in ("instance Show Native where", "show native = nativeName native", "  nativeField :: Int", "-- deriving is mentioned in this comment"):
            self.assertFalse(common.compiler_generated_declaration(line))
        with self.assertRaisesRegex(ValueError, "mixed deriving/code"):
            common.compiler_generated_declaration("deriving stock Eq; manual = undefined")

    def test_shared_property_source_changes_candidate_identity(self):
        with tempfile.TemporaryDirectory(prefix="rdkafka-shared-source-control-") as directory:
            root = Path(directory)
            support = root / "test-support" / "Kafka"
            support.mkdir(parents=True)
            source = support / "PropertySupport.hs"
            source.write_text("module Kafka.PropertySupport where\nvalue = 1\n")
            with patch.object(common, "ROOT", root):
                first = common.fingerprint()
                source.write_text("module Kafka.PropertySupport where\nvalue = 2\n")
                self.assertNotEqual(common.fingerprint(), first)

    def test_test_local_module_cannot_inflate_library_ticks(self):
        with tempfile.TemporaryDirectory(prefix="rdkafka-package-coverage-control-") as directory:
            root = Path(directory)
            source, output = root / "source.tix", root / "library.tix"
            source.write_text('Tix [TixModule "library/Kafka.Sample" 1 1 [0],TixModule "main/Kafka.Sample" 2 1 [99]]\n')
            includes = common.library_hpc_includes([root / "library" / "Kafka.Sample.mix"])
            subprocess.run(["hpc", "sum", "--union", *includes, "--output=" + str(output), str(source)], check=True)
            measured = output.read_text()
            self.assertIn('"library/Kafka.Sample"', measured)
            self.assertIn('[0]', measured)
            self.assertNotIn('"main/Kafka.Sample"', measured)
            self.assertNotIn('[99]', measured)

    def setUp(self):
        self.matrix = json.loads((checker.ROOT / "docs/testing/coverage-matrix.json").read_text())
        self.exports = self.matrix["exports"]
        # These are schema/gate controls, independent of live candidate rows.
        # A real accepted row may legitimately be stale during implementation.
        for row in self.matrix["contracts"]:
            row["status"] = "unimplemented"
            row["tests"] = []
            row.pop("fingerprint", None)
        self.evidence = dict(fingerprint="candidate", tests=["suite.case", "suite.runtime-contract.case"], suites=["rdkafka-hs-test", "rdkafka-hs-integration"],
                             artifacts={"README.md": hashlib.sha256((checker.ROOT / "README.md").read_bytes()).hexdigest()},
                             supplemental_runs=[dict(kind="nonthreaded-runtime", rts_way="rts_v", tests=["suite.runtime-contract.case"], passed=1, tix="README.md")],
                             measured_native_wrapper_ticks=1, misses=[])
        self.identity = patch.object(checker, "fingerprint", return_value="candidate")
        self.identity.start()
        self.addCleanup(self.identity.stop)

    def errors(self, allow=True):
        return checker.validate(self.matrix, self.exports, self.evidence, allow)

    def test_qualification_context_requires_exact_current_registration_and_complete_artifacts(self):
        with tempfile.TemporaryDirectory(prefix="rdkafka-context-control-") as directory:
            root = Path(directory)
            listing, log = root / "listing.txt", root / "run.log"
            listing.write_text("broker.case\n")
            log.write_text("All 1 tests passed (0.1s)\n")
            context = dict(kind="qualification-context", mode="--broker-qualification", fingerprint="candidate", tests=["broker.case"], passed=1,
                           command=["binary", "--broker-qualification"], environment={"broker": "isolated"}, source={"archive": "controlled", "candidate_fingerprint": "candidate"},
                           listing=str(listing), log=str(log), artifacts={str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in (listing, log)})
            self.assertEqual(common.qualification_context_errors(context, "candidate", ["broker.case"]), [])
            self.assertIn("stale qualification context", common.qualification_context_errors(context, "new-candidate"))
            self.assertIn("qualification context differs from current registered tests", common.qualification_context_errors(context, "candidate", ["broker.case", "missing.case"]))
            for broken in ("All 0 tests passed\n", "broker.case: FAIL\nAll 1 tests passed\n", "interrupted\n"):
                log.write_text(broken)
                context["artifacts"][str(log)] = hashlib.sha256(log.read_bytes()).hexdigest()
                self.assertIn("qualification log does not prove every test passed", common.qualification_context_errors(context, "candidate"))
            log.write_text("All 1 tests passed\n")
            self.assertTrue(any("modified qualification artifact" in e for e in common.qualification_context_errors(context, "candidate")))

    def test_owned_open_inventory_is_valid_for_development(self):
        self.assertEqual(self.errors(), [])

    def test_transferred_rows_keep_the_original_owner(self):
        row = self.matrix["contracts"][0]
        row.update(owner=16, previous_owner=14)
        self.assertEqual(self.errors(), [])
        self.assertTrue(any(row["id"] + ": open (EP-16)" == error for error in self.errors(False)))
        row["previous_owner"] = 99
        self.assertIn(row["id"] + ": invalid ownership transfer", self.errors())

    def test_unknown_owner_is_rejected_for_rows_and_misses(self):
        row = self.matrix["contracts"][0]
        row["owner"] = 99
        self.evidence["misses"] = [dict(module="Kafka.Version", line=1, label="ExpBox False", generated=False, contract="OWN-source-coverage", owner=99)]
        errors = self.errors()
        self.assertIn(row["id"] + ": invalid child owner", errors)
        self.assertTrue(any("unclassified coverage miss" in error for error in errors))

    def test_runtime_supplement_is_required(self):
        self.evidence.pop("supplemental_runs")
        self.assertIn("missing nonthreaded runtime coverage", self.errors())

    def test_threaded_run_cannot_substitute_for_nonthreaded_coverage(self):
        self.evidence["supplemental_runs"][0]["rts_way"] = "rts_thr"
        self.assertIn("runtime supplement is not nonthreaded", self.errors())

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

    def test_current_tested_row_is_accepted(self):
        row = self.matrix["contracts"][0]
        row.update(status="tested", tests=["suite.case"], fingerprint="candidate")
        self.assertEqual(self.errors(), [])

    def test_stale_tested_row_is_rejected_with_current_measurements(self):
        row = self.matrix["contracts"][0]
        row.update(status="tested", tests=["suite.case"], fingerprint="previous candidate")
        self.assertTrue(any(row["id"] + ": stale source evidence" in error for error in self.errors()))

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

    def test_source_classification_requires_matching_file_bytes(self):
        key = "Kafka.Version:1:ExpBox False"
        self.evidence["misses"] = [dict(module="Kafka.Version", line=1, label="ExpBox False", generated=False,
                                       source="src/Kafka/Version.hs", contract="OWN-source-coverage", owner=16)]
        classification = dict(status="inapplicable", rationale="Synthetic control", source="src/Kafka/Version.hs",
                              source_sha256=hashlib.sha256((checker.ROOT / "src/Kafka/Version.hs").read_bytes()).hexdigest())
        self.matrix["miss_classifications"][key] = classification
        self.assertEqual(self.errors(), [])
        classification["source_sha256"] = "previous source"
        self.assertIn("stale source classification: " + key, self.errors())
        classification.pop("source_sha256")
        self.assertIn("stale source classification: " + key, self.errors())

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

    def test_generated_enum_internal_blank_does_not_advance_into_manual_source(self):
        source = (checker.ROOT / "src/Kafka/Internal/RdKafka.chs").read_text().splitlines()
        hook = next(i for i, line in enumerate(source, 1) if line.startswith("{#enum rd_kafka_conf_res_t"))
        text = f'{{-# LINE {hook} "src/Kafka/Internal/RdKafka.chs" #-}}\ndata Generated = A\ninstance Enum Generated where\n  fromEnum A = 0\n\n  toEnum 0 = A\n{{-# LINE {hook} "src/Kafka/Internal/RdKafka.chs" #-}}\n\nmanual = 1\n'
        _, mapping = preprocessor.transform("Generated.hs", text)
        self.assertTrue(all(mapping[index]["generated"] for index in range(1, 6)))
        self.assertFalse(mapping[8]["generated"])
        self.assertEqual(mapping[8]["line"], hook + 1)

    def test_spaced_c2hs_hook_is_generated(self):
        source = (checker.ROOT / "src/Kafka/Internal/RdKafka.chs").read_text().splitlines()
        hook = next(i for i, line in enumerate(source, 1) if line.startswith("{# fun"))
        end = next(i for i in range(hook, len(source) + 1) if "#}" in source[i - 1])
        text = f'{{-# LINE {hook} "src/Kafka/Internal/RdKafka.chs" #-}}\ngenerated = 1\n\ngeneratedMore = 2\n{{-# LINE {end} "src/Kafka/Internal/RdKafka.chs" #-}}\n\nmanual = 3\n'
        _, mapping = preprocessor.transform("Generated.hs", text)
        self.assertTrue(all(mapping[index]["generated"] for index in range(1, 4)))
        self.assertFalse(mapping[6]["generated"])
        self.assertEqual(mapping[6]["line"], end + 1)

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
        prepared = subprocess.run(["cabal", "build", "rdkafka-hs-integration", "-f", "integration"], cwd=checker.ROOT, capture_output=True, text=True)
        self.assertEqual(prepared.returncode, 0, prepared.stdout + prepared.stderr)
        plan = json.loads((checker.ROOT / "dist-newstyle/cache/plan.json").read_text())
        component = next(c for c in plan["install-plan"] if c.get("pkg-name") == "rdkafka-hs" and c.get("component-name") == "test:rdkafka-hs-integration")
        # Expose the exact selected component dependencies. Broad -package NAME
        # flags can expose several installed variants; cabal exec's default
        # library environment omits test-only packages such as network.
        packages = [arg for unit in component["depends"] for arg in ("-package-id", unit)]
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
                    command = ["cabal", "exec", "--", "ghc", "--make", "-O1", "-threaded", "-XGHC2024", "-XOverloadedStrings", "-i" + str(root), "-hide-all-packages", *packages, str(root / "Main.hs"), "-o", str(root / "controls")]
                    compiled = subprocess.run(command, cwd=checker.ROOT, capture_output=True, text=True)
                    self.assertEqual(compiled.returncode, 0, compiled.stdout + compiled.stderr)
                    result = subprocess.run([str(root / "controls")], capture_output=True, text=True)
                    self.assertEqual(result.returncode, expected_exit, result.stdout + result.stderr)
                    if failed_case:
                        self.assertRegex(result.stdout, re.escape(failed_case) + r":\s+FAIL")


class CompatibilityControls(unittest.TestCase):
    def setUp(self):
        import tarfile
        self.directory = tempfile.TemporaryDirectory(prefix='rdkafka-compatibility-controls-')
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        for module in (checker, common):
            patcher = patch.object(module, 'ROOT', self.root)
            patcher.start()
            self.addCleanup(patcher.stop)
        identity = patch.object(checker, 'fingerprint', return_value='candidate')
        identity.start()
        self.addCleanup(identity.stop)
        (self.root / 'src').mkdir()
        (self.root / 'scripts').mkdir()
        (self.root / 'src/Sample.hs').write_text('module Sample where\n')
        (self.root / 'scripts/check.py').write_text('print("check")\n')
        (self.root / 'rdkafka-hs.cabal').write_text('name: sample\n')
        (self.root / 'cabal.project').write_text('packages: .\n')
        archive = self.root / 'package.tar.gz'
        with tarfile.open(archive, 'w:gz') as package:
            for name in ['src/Sample.hs', 'scripts/check.py', 'rdkafka-hs.cabal']:
                package.add(self.root / name, arcname='sample/' + name)
        files = common.runtime_sources()
        files['scripts/check.py'] = hashlib.sha256((self.root / 'scripts/check.py').read_bytes()).hexdigest()
        self.artifact = lambda p: dict(path=p.name, sha256=hashlib.sha256(p.read_bytes()).hexdigest())
        proof = dict(fingerprint='candidate', tested_fingerprint='candidate', archive=self.artifact(archive), files=files)
        (self.root / 'proof.json').write_text(json.dumps(proof))
        primary = ['rdkafka-hs.unit', 'rdkafka-hs-integration.producer-regressions.case', 'rdkafka-hs-integration.public-contract.runtime-contract.case', 'rdkafka-hs-integration.topic-configuration.unsupported configuration follows policy']
        self.coverage = dict(primary_tests=primary, qualification_contexts=[dict(mode='--broker-qualification', tests=['broker.case']), dict(mode='--authentication-qualification', tests=['security.case'])])
        self.matrix = dict(qualification=dict(status='passed', candidate_fingerprint='candidate', tuples=[]))
        for rid, (ghc, native, scenarios) in checker.REQUIRED_TUPLES.items():
            rows = []
            for sid in sorted(scenarios):
                tests = checker.scenario_tests(sid, self.coverage)
                counts = [1, 3] if sid == 'complete-normal-and-mapped' else [len(tests)]
                log = self.root / (sid + '.log')
                log.write_text(''.join('All %d tests passed\n' % count for count in counts))
                rows.append(dict(id=sid, required=True, status='passed', command=['test'], tests=tests, passed=len(tests), log_counts=counts, log=self.artifact(log), artifacts={'log': self.artifact(log)}))
            for scenario in rows:
                if scenario['id'] == 'native-ownership-diagnostics':
                    memory = self.root / 'memory.valgrind'
                    memory.write_text('ERROR SUMMARY: 0 errors\ndefinitely lost: 0 bytes\nindirectly lost: 0 bytes\n')
                    scenario['diagnostic_reports'] = [self.artifact(memory)]
            configuration = self.root / (rid + '-configuration.json')
            configuration.write_text(json.dumps({'fixture': 'owned'}))
            self.matrix['qualification']['tuples'].append(dict(id=rid, required=True, status='passed', source=dict(candidate_fingerprint='candidate'), environment=dict(ghc=ghc, native_version=native, loaded_native_identity=dict(version=native), os='os', kernel='Darwin' if rid.startswith('darwin') else 'Linux', architecture='aarch64', cabal='3.16', broker_version='broker', broker_topology='owned'), scenarios=rows, test_configuration={'fixture': 'owned'}, configuration_artifact=self.artifact(configuration)))
        controls = {}
        for name, (code, signature) in checker.REQUIRED_CONTROLS.items():
            log = self.root / (name + '.log')
            log.write_text(signature)
            controls[name] = dict(status='detected', exit=code, log=self.artifact(log))
        log = self.root / 'package.log'
        log.write_text('All 1 tests passed\nAll 3 tests passed\n')
        example = self.root / 'example.log'
        example.write_text('EXAMPLE VERIFIED: four exact records, ordered offsets, fancy header and committed offset 4')
        self.report = dict(schema_version=1, status='passed', fingerprint='candidate', controls=controls, package=dict(archive=self.artifact(archive), source=dict(candidate_fingerprint='candidate', scope_proof='proof.json'), tests=primary, passed=4, log_counts=[1, 3], log=self.artifact(log), built_examples=True, example=dict(records=4, committed_offset=4, log=self.artifact(example))), artifacts={'proof': self.artifact(self.root / 'proof.json')})

    def errors(self):
        return checker.validate_compatibility(self.matrix, self.report, self.coverage)

    def test_complete_manifest_passes(self):
        self.assertEqual(self.errors(), [])

    def test_missing_required_tuple_fails(self):
        self.matrix['qualification']['tuples'].pop()
        self.assertIn('missing or duplicate required tuple', self.errors())

    def test_unavailable_required_tuple_fails(self):
        self.matrix['qualification']['tuples'][0]['status'] = 'unavailable'
        self.assertTrue(any('unavailable' in e for e in self.errors()))

    def test_stale_source_fails(self):
        self.matrix['qualification']['tuples'][0]['source']['candidate_fingerprint'] = 'old'
        self.assertTrue(any('source-scope proof' in e for e in self.errors()))

    def test_configuration_change_fails(self):
        self.matrix['qualification']['tuples'][0]['test_configuration']['fixture'] = 'other'
        self.assertTrue(any('stale test configuration' in e for e in self.errors()))

    def test_corrupted_artifact_fails(self):
        self.report['package']['log']['sha256'] = 'wrong'
        self.assertTrue(any('modified compatibility artifact' in e for e in self.errors()))

    def test_zero_tests_fail(self):
        self.matrix['qualification']['tuples'][0]['scenarios'][0].update(tests=[], passed=0)
        self.assertTrue(any('zero, duplicate or incomplete' in e for e in self.errors()))

    def test_wrong_loaded_version_fails(self):
        self.matrix['qualification']['tuples'][0]['environment']['loaded_native_identity']['version'] = 'wrong'
        self.assertTrue(any('wrong actual compiler/loaded native version' in e for e in self.errors()))

    def test_missing_ownership_control_fails(self):
        self.report['controls'].pop('linux-header-retention')
        self.assertIn('missing required ownership diagnostic control', self.errors())

    def test_omitted_scenario_case_fails(self):
        scenario = next(s for r in self.matrix['qualification']['tuples'] for s in r['scenarios'] if s['id'] == 'complete-normal-and-mapped')
        scenario['tests'] = scenario['tests'][:-1]
        scenario['passed'] -= 1
        self.assertTrue(any('complete registered scenario' in e for e in self.errors()))

    def test_native_memory_finding_fails_even_with_updated_checksum(self):
        scenario = next(s for r in self.matrix['qualification']['tuples'] for s in r['scenarios'] if s['id'] == 'native-ownership-diagnostics')
        memory = self.root / 'memory.valgrind'
        memory.write_text('ERROR SUMMARY: 1 errors\ndefinitely lost: 48 bytes\nrd_kafka_headers_new\n')
        scenario['diagnostic_reports'] = [self.artifact(memory)]
        self.assertTrue(any('native memory finding' in e for e in self.errors()))

    def test_package_source_change_fails_even_with_matching_fingerprint(self):
        (self.root / 'src/Sample.hs').write_text('module Sample where\nchanged = True\n')
        self.assertIn('source-scope runtime/build/test files changed', self.errors())

    def test_tooling_change_invalidates_package_proof(self):
        (self.root / 'scripts/check.py').write_text('print("changed")\n')
        self.assertIn('source-scope runtime/build/test files changed', self.errors())

    def test_unchanged_runtime_archive_allows_tooling_only_behavior_reuse(self):
        proof = json.loads((self.root / 'proof.json').read_text())
        proof['files'].pop('scripts/check.py')
        proof['tested_fingerprint'] = 'tested'
        (self.root / 'behavior-proof.json').write_text(json.dumps(proof))
        (self.root / 'scripts/check.py').write_text('print("changed")\n')
        self.assertEqual(common.source_scope_errors(dict(candidate_fingerprint='tested', scope_proof='behavior-proof.json'), 'candidate'), [])
        (self.root / 'src/Sample.hs').write_text('changed')
        self.assertIn('source-scope runtime/build/test files changed', common.source_scope_errors(dict(candidate_fingerprint='tested', scope_proof='behavior-proof.json'), 'candidate'))


if __name__ == "__main__":
    unittest.main()
