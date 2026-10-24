#!/usr/bin/env python3
"""GHC -F adapter: retain c2hs code but make its spans measurable by HPC.

GHC 9.12 ignores ticks whose LINE filename differs from the module's .hs
filename. Replace only c2hs LINE pragmas with blank lines, retaining physical
coordinates and a sidecar mapping back to the .chs source. Ordinary .hs inputs
retain their original source filename and contents. The adapter never changes production build inputs.
"""

import json
import re
import sys
from pathlib import Path


def transform(original, text):
    if '"src/Kafka/Internal/RdKafka.chs"' not in text:
        return '{-# LINE 1 ' + json.dumps(original) + ' #-}\n' + text, None
    source = Path("src/Kafka/Internal/RdKafka.chs").read_text().splitlines()
    generated = set()
    directive = False
    for number, line in enumerate(source, 1):
        if re.match(r"^\s*\{#(?:fun|enum|pointer|typedef)\b", line):
            directive = True
        if directive:
            generated.add(number)
        if "#}" in line:
            directive = False
    result = []
    mapping = []
    origin = None
    line_number = 0
    for physical, line in enumerate(text.splitlines(keepends=True), 1):
        match = re.fullmatch(r'{-# LINE (\d+) "([^"]+)" #-}\s*', line)
        if match:
            line_number = int(match[1])
            origin = match[2]
            result.append("\n")
            mapping.append(None)
        else:
            result.append(line)
            is_generated = origin != "src/Kafka/Internal/RdKafka.chs" or line_number in generated
            mapping.append({"file": origin, "line": line_number, "generated": is_generated})
            if not is_generated:
                line_number += 1
            elif 1 <= line_number <= len(source) and "#}" in source[line_number - 1] and not line.strip():
                # c2hs resets to the closing hook line, emitting its suffix
                # (usually blank) before continuing verbatim hand-written code.
                line_number += 1
    # Every explicitly typed hand-written declaration must map back to its
    # exact declaration line. This catches mapping drift around inline hooks.
    signatures = {match[1]: i for i, line in enumerate(source, 1)
                  if (match := re.match(r"^([a-zA-Z_][\w']*)\s*::", line))}
    for physical, line in enumerate(text.splitlines(), 1):
        match = re.match(r"^([a-zA-Z_][\w']*)\s*::", line)
        if match and match[1] in signatures:
            entry = mapping[physical - 1]
            if entry is None or entry["generated"] or entry["line"] != signatures[match[1]]:
                raise ValueError("unmapped hand-written declaration: " + match[1])
    # GHC supplies the original source location before invoking -F. State it
    # explicitly because c2hs's first LINE is removed from the input.
    prefix = '{-# LINE 1 ' + json.dumps(original) + ' #-}\n'
    return prefix + "".join(result), mapping


def main():
    original, input_name, output_name = sys.argv[-3:]
    text, mapping = transform(original, Path(input_name).read_text())
    Path(output_name).write_text(text)
    if mapping is not None:
        Path(original + ".coverage-map.json").write_text(json.dumps(mapping))


if __name__ == "__main__":
    main()
