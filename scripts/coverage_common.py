"""Candidate identity and compiler-derived public exports for coverage tools."""

import hashlib
import json
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def run(*args):
    return subprocess.check_output(args, cwd=ROOT, text=True)


def library_hpc_includes(mixfiles):
    return ["--include=" + p.parent.name + ":" + p.stem for p in mixfiles]


def compiler_generated_declaration(line):
    """Ticks on deriving declarations come from GHC, not handwritten methods."""
    if not re.match(r"^\s*deriving\b", line):
        return False
    if ";" in line:
        raise ValueError("mixed deriving/code line requires explicit coverage classification")
    return True


def fingerprint():
    # Evidence-only documentation is deliberately excluded. Include untracked
    # test/source additions; git diff alone cannot identify a dirty candidate.
    files = set()
    for directory in ("src", "test", "test-integration", "test-support", "scripts", "nix"):
        files.update(p for p in (ROOT / directory).rglob("*") if p.is_file() and "__pycache__" not in p.parts)
    files.update(ROOT / name for name in ("rdkafka-hs.cabal", "cabal.project", "cabal.project.local", "cabal.project.freeze", ".envrc", ".envrc.local", "flake.nix", "flake.lock", "flake.module.nix", "justfile"))
    digest = hashlib.sha256()
    for path in sorted(files):
        if path.exists():
            digest.update(str(path.relative_to(ROOT)).encode() + b"\0" + path.read_bytes() + b"\0")
    return digest.hexdigest()


def exposed_modules():
    cabal = (ROOT / "rdkafka-hs.cabal").read_text()
    # Scope deliberately limited to this package's flat exposed-modules stanza.
    # Reject unrecognised syntax instead of silently losing conditional exports.
    sections = re.findall(r"^  exposed-modules:\s*\n((?:    [^\n]+\n)+)", cabal, re.M)
    if len(sections) != 1:
        raise ValueError("expected exactly one flat exposed-modules stanza")
    modules = sections[0].split()
    if not all(re.fullmatch(r"[A-Z][\w]*(?:\.[A-Z][\w]*)*", m) for m in modules):
        raise ValueError("unsupported exposed-modules syntax")
    return modules


def components(builddir):
    plan = json.loads((ROOT / builddir / "cache/plan.json").read_text())
    return {c["component-name"]: Path(c["dist-dir"]) for c in plan["install-plan"] if c.get("pkg-name") == "rdkafka-hs"}


def compiler_exports(builddir):
    library = components(builddir)["lib"] / "build"
    result = {}
    for module in exposed_modules():
        interface = library / (module.replace(".", "/") + ".hi")
        # GHC resolves multiline exports, module re-exports, constructors,
        # record selectors and opaque types. This parser consumes its explicit
        # export section, never a source-code approximation of Haskell exports.
        output = run("ghc", "--show-iface", str(interface))
        match = re.search(r"^exports:\n((?:  [^\n]+\n)+)", output, re.M)
        if not match:
            raise ValueError(f"missing compiler exports: {module}")
        entries = []
        for line in match[1].splitlines():
            names = re.findall(r"[A-Za-z_][A-Za-z_0-9'.]*", line)
            if not names:
                raise ValueError(f"unsupported export syntax: {line}")
            names = [name if "." in name else module + "." + name for name in names]
            entries.append({"symbol": names[0], "members": sorted(set(names[1:]))})
        result[module] = sorted(entries, key=lambda x: x["symbol"])
    return result


QUALIFICATION_MODES = frozenset(("--broker-qualification", "--authentication-qualification"))


def qualification_context_errors(context, candidate, registered=None):
    """Supplement behavioral registration only; never manufacture HPC ticks."""
    errors = []
    if context.get("kind") != "qualification-context" or context.get("mode") not in QUALIFICATION_MODES:
        errors.append("unsupported qualification context")
    if context.get("fingerprint") != candidate:
        errors.append("stale qualification context")
    names = context.get("tests", [])
    if not isinstance(names, list) or not names or len(names) != len(set(names)):
        errors.append("empty or duplicate qualification tests")
    elif registered is not None and set(names) != set(registered):
        errors.append("qualification context differs from current registered tests")
    if context.get("passed") != len(names):
        errors.append("incomplete qualification context")
    if not context.get("environment") or not context.get("source") or context.get("mode") not in context.get("command", []):
        errors.append("missing qualification source/environment/command")
    artifacts = context.get("artifacts", {})
    if not isinstance(artifacts, dict) or not artifacts:
        errors.append("missing qualification artifacts")
        artifacts = {}
    for name, checksum in artifacts.items():
        path = ROOT / name
        if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != checksum:
            errors.append("missing or modified qualification artifact: " + name)
    for field in ("listing", "log"):
        name = context.get(field)
        if name not in artifacts:
            errors.append("missing qualification " + field)
            continue
        path = ROOT / name
        if not path.is_file():
            continue
        content = path.read_text()
        if field == "listing" and content.splitlines() != names:
            errors.append("qualification listing differs from recorded tests")
        if field == "log" and (re.findall(r"All (\d+) tests passed", content) != [str(len(names))] or re.search(r": FAIL(?:\s|$)", content)):
            errors.append("qualification log does not prove every test passed")
    return errors
