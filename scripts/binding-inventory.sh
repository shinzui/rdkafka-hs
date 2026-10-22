#!/bin/sh
# Regenerate docs/bindings.md: every function librdkafka's public header
# exports, grouped by the header's own @name sections, marked bound or unbound
# by src/Kafka/Internal/RdKafka.chs.
#
# Usage: scripts/binding-inventory.sh [OUTPUT]   (default: docs/bindings.md)
#
# RDKAFKA_SOURCE_HEADER must name src/rdkafka.h in a librdkafka release
# checkout outside /nix/store whose version matches the library pkg-config
# finds; `just librdkafka-source` prepares one. The script exits non-zero when
# the module binds a function the header does not export.
set -eu

out=${1:-docs/bindings.md}
module=src/Kafka/Internal/RdKafka.chs

die() {
  echo "binding-inventory: $*" >&2
  exit 1
}

[ -n "${RDKAFKA_SOURCE_HEADER:-}" ] ||
  die "set RDKAFKA_SOURCE_HEADER to src/rdkafka.h of a librdkafka release checkout (see 'just librdkafka-source')"
header=$(realpath "$RDKAFKA_SOURCE_HEADER") || die "cannot resolve $RDKAFKA_SOURCE_HEADER"
case "$header" in
  /nix/store/*) die "refusing to read a header in /nix/store; use a release checkout" ;;
esac
[ -f "$header" ] || die "$header is not a file"
[ -f "$module" ] || die "run from the repository root ($module not found)"

linked=$(pkg-config --modversion rdkafka) || die "pkg-config cannot find rdkafka"
hexversion=$(awk '$1 == "#define" && $2 == "RD_KAFKA_VERSION" { print $3; exit }' "$header")
[ -n "$hexversion" ] || die "$header does not define RD_KAFKA_VERSION"
headerversion=$(printf '%d.%d.%d' \
  $((hexversion >> 24 & 255)) $((hexversion >> 16 & 255)) $((hexversion >> 8 & 255)))
[ "$headerversion" = "$linked" ] ||
  die "header is librdkafka $headerversion but pkg-config finds $linked"

if command -v sha256sum >/dev/null 2>&1; then
  digest=$(sha256sum "$header" | cut -d ' ' -f 1)
else
  digest=$(shasum -a 256 "$header" | cut -d ' ' -f 1)
fi

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

# Exported functions as "group<TAB>name", in header order. Group headings are
# read from comments before comments are stripped; declarations are joined
# across lines up to their first parenthesis.
awk '
  BEGIN { group = "Before the first group"; incomment = 0; collecting = 0 }
  {
    line = $0
    if (match(line, /@name[ \t]+/)) {
      heading = substr(line, RSTART + RLENGTH)
      sub(/[ \t]*(\*\/)?[ \t]*$/, "", heading)
      group = heading
    }
    code = ""
    while (length(line) > 0) {
      if (incomment) {
        i = index(line, "*/")
        if (i == 0) line = ""
        else { line = substr(line, i + 2); incomment = 0 }
      } else {
        i = index(line, "/*"); j = index(line, "//")
        if (j > 0 && (i == 0 || j < i)) { code = code substr(line, 1, j - 1); line = "" }
        else if (i == 0) { code = code line; line = "" }
        else { code = code substr(line, 1, i - 1); line = substr(line, i + 2); incomment = 1 }
      }
    }
    if (code ~ /^[ \t]*#/) next
    if (!collecting && code ~ /(^|[^A-Za-z0-9_])RD_EXPORT([^A-Za-z0-9_]|$)/) {
      collecting = 1; decl = ""; declgroup = group
    }
    if (collecting) {
      decl = decl " " code
      p = index(decl, "(")
      if (p > 0) {
        before = substr(decl, 1, p - 1)
        if (match(before, /[A-Za-z_][A-Za-z0-9_]*[ \t]*$/)) {
          name = substr(before, RSTART, RLENGTH)
          gsub(/[ \t]/, "", name)
          if (name ~ /^rd_kafka_/ && !(name in seen)) {
            seen[name] = 1
            print declgroup "\t" name
          }
        }
        collecting = 0
      } else if (index(decl, ";") > 0) {
        collecting = 0
      }
    }
  }
' "$header" >"$tmp/exports"

# Bindings as "name<TAB>haskell name<TAB>kind".
awk '
  function camel(s,    n, parts, i, r) {
    n = split(s, parts, "_")
    r = parts[1]
    for (i = 2; i <= n; i++)
      r = r toupper(substr(parts[i], 1, 1)) substr(parts[i], 2)
    return r
  }
  pending != "" && NF > 0 {
    hs = $1
    sub(/::.*/, "", hs)
    print pending "\t" hs "\t" pendingkind
    pending = ""
  }
  {
    line = $0
    while (match(line, /\{#[ \t]*fun[ \t]+((pure|unsafe|safe)[ \t]+)*rd_kafka_[A-Za-z0-9_]+([ \t]+as[ \t]+[^ \t{]+)?/)) {
      d = substr(line, RSTART, RLENGTH)
      line = substr(line, RSTART + RLENGTH)
      match(d, /rd_kafka_[A-Za-z0-9_]+/)
      name = substr(d, RSTART, RLENGTH)
      hs = name
      if (match(d, /[ \t]as[ \t]+[^ \t{]+$/)) {
        hs = substr(d, RSTART, RLENGTH)
        sub(/^[ \t]as[ \t]+/, "", hs)
        if (hs == "^") hs = camel(name)
      }
      print name "\t" hs "\tc2hs fun"
    }
    line = $0
    while (match(line, /\{#[ \t]*call[ \t]+((pure|unsafe|safe)[ \t]+)*rd_kafka_[A-Za-z0-9_]+/)) {
      d = substr(line, RSTART, RLENGTH)
      line = substr(line, RSTART + RLENGTH)
      match(d, /rd_kafka_[A-Za-z0-9_]+/)
      print substr(d, RSTART, RLENGTH) "\t\tc2hs call"
    }
    if ($0 ~ /^[ \t]*foreign[ \t]+import[ \t]/ && match($0, /"[^"]*rd_kafka_[A-Za-z0-9_]+"/)) {
      s = substr($0, RSTART, RLENGTH)
      kind = (s ~ /&rd_kafka_/) ? "finalizer pointer" : "foreign import"
      match(s, /rd_kafka_[A-Za-z0-9_]+/)
      pending = substr(s, RSTART, RLENGTH)
      pendingkind = kind
      rest = $0
      sub(/^.*"[ \t]*/, "", rest)
      if (rest ~ /::/) {
        sub(/[ \t]*::.*/, "", rest)
        print pending "\t" rest "\t" pendingkind
        pending = ""
      }
    }
  }
' "$module" >"$tmp/bound"

awk -F '\t' -v version="$linked" -v digest="$digest" -v module="$module" '
  FNR == NR {
    label = ($2 == "") ? "inline `{#call}`" : "`" $2 "`"
    if ($3 == "finalizer pointer") label = label " (finalizer pointer)"
    else if ($3 == "foreign import") label = label " (foreign import)"
    if (!(($1, label) in boundpair)) {
      boundpair[$1, label] = 1
      bound[$1] = ($1 in bound) ? bound[$1] ", " label : label
    }
    next
  }
  {
    if (!($1 in groupseen)) { groupseen[$1] = 1; groups[++ngroups] = $1 }
    members[$1] = members[$1] "\n" $2
    exported[$2] = 1
    total++
    if ($2 in bound) nbound++
  }
  END {
    print "# librdkafka binding inventory"
    print ""
    print "Generated by `scripts/binding-inventory.sh` from librdkafka'"'"'s public header and"
    print "`" module "`. Do not edit by hand: run `just bindings-inventory` after"
    print "changing the binding module, and `just bindings-check` to verify."
    print ""
    print "- librdkafka: " version
    print "- Header: `src/rdkafka.h`, sha256 `" digest "`"
    print "- Exported functions: " total
    print "- Bound: " nbound + 0
    print "- Unbound: " total - nbound
    for (g = 1; g <= ngroups; g++) {
      name = groups[g]
      n = split(substr(members[name], 2), fns, "\n")
      nb = 0
      for (i = 1; i <= n; i++) if (fns[i] in bound) nb++
      print ""
      print "## " name
      print ""
      print nb " of " n " bound."
      if (nb > 0) {
        print ""
        print "| C function | Haskell binding |"
        print "| --- | --- |"
        for (i = 1; i <= n; i++)
          if (fns[i] in bound) print "| `" fns[i] "` | " bound[fns[i]] " |"
      }
      if (nb < n) {
        print ""
        print "<details><summary>Unbound (" n - nb ")</summary>"
        print ""
        for (i = 1; i <= n; i++)
          if (!(fns[i] in bound)) print "- `" fns[i] "`"
        print ""
        print "</details>"
      }
    }
    nstale = 0
    for (f in bound) if (!(f in exported)) stale[++nstale] = f
    if (nstale > 0) {
      print ""
      print "## Bound but not exported by the header"
      print ""
      for (i = 1; i <= nstale; i++) print "- `" stale[i] "`: " bound[stale[i]]
      exit 3
    }
  }
' "$tmp/bound" "$tmp/exports" >"$tmp/out" || status=$?

mkdir -p "$(dirname "$out")"
cp "$tmp/out" "$out"
if [ "${status:-0}" -eq 3 ]; then
  die "the binding module binds functions the header does not export; see $out"
elif [ "${status:-0}" -ne 0 ]; then
  die "inventory generation failed"
fi
