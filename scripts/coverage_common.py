"""Candidate identity and compiler-derived public exports for coverage tools."""

import hashlib
import json
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def run(*args):
    return subprocess.check_output(args, cwd=ROOT, text=True)


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
