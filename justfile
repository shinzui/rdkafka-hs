# Justfile for rdkafka-hs

default:
    @just --list


# --- Private Redpanda (Apple Container) ---

# Start the project-local broker at 127.0.0.1:39092 (idempotent).
[group("redpanda")]
redpanda-up:
    redpanda-local-up

# Stop the project-local broker, keeping its data.
[group("redpanda")]
redpanda-down:
    redpanda-local-down

# Exit non-zero unless the project-local broker is serving.
[group("redpanda")]
redpanda-status:
    redpanda-local-status

# Tail the project-local broker's logs.
[group("redpanda")]
redpanda-logs:
    redpanda-local-logs

# Delete the project-local broker's containers, network and volume.
[group("redpanda")]
redpanda-purge:
    redpanda-local-purge


# --- Build and test ---

# Build all enabled components.
[group("build")]
build:
    cabal build all

# Run the unit test suite.
[group("build")]
test:
    cabal test rdkafka-hs-test

# Run the integration suite against the project-local broker. Extra arguments
# go to cabal, e.g. just test-it '--test-options=-p /fatal/'.
[group("build")]
[positional-arguments]
test-it *ARGS:
    redpanda-local-up
    cabal test rdkafka-hs-integration -f integration --test-show-details=direct "$@"

# Build the example executable.
[group("build")]
build-examples:
    cabal build -f examples all

# Format code via treefmt-nix.
[group("build")]
fmt:
    nix fmt

# Build and unit-test the package in the Nix sandbox, run the flake checks
# (formatting, pre-commit), and validate the knowledge bundles and the binding
# inventory.
[group("build")]
check: docs-check bindings-check
    nix build
    nix flake check


# --- Bindings ---

# Print the path of src/rdkafka.h in a cached clone of the librdkafka release
# that pkg-config finds, cloning it on first use.
[group("bindings")]
librdkafka-source:
    #!/bin/sh
    set -eu
    version=$(pkg-config --modversion rdkafka)
    dir="${XDG_CACHE_HOME:-$HOME/.cache}/rdkafka-hs/librdkafka-v$version"
    if [ ! -f "$dir/src/rdkafka.h" ]; then
      git clone --quiet --depth 1 --branch "v$version" https://github.com/confluentinc/librdkafka.git "$dir" >&2
    fi
    echo "$dir/src/rdkafka.h"

# Regenerate docs/bindings.md (RDKAFKA_SOURCE_HEADER overrides the header).
[group("bindings")]
bindings-inventory:
    #!/bin/sh
    set -eu
    RDKAFKA_SOURCE_HEADER="${RDKAFKA_SOURCE_HEADER:-$(just librdkafka-source)}" scripts/binding-inventory.sh

# Fail when docs/bindings.md does not match the binding module and header.
[group("bindings")]
bindings-check:
    #!/bin/sh
    set -eu
    fresh=$(mktemp)
    trap 'rm -f "$fresh"' EXIT
    RDKAFKA_SOURCE_HEADER="${RDKAFKA_SOURCE_HEADER:-$(just librdkafka-source)}" scripts/binding-inventory.sh "$fresh"
    if ! diff -u docs/bindings.md "$fresh"; then
      echo "docs/bindings.md is stale; run 'just bindings-inventory'" >&2
      exit 1
    fi


# --- Documentation ---

# Validate the ADR and improvement-request bundles against their profiles.
[group("docs")]
docs-check:
    okf validate docs/adr --strict --profile docs/adr/profile.dhall --profile-enforce --log-enforce
    okf validate docs/improvement-requests --strict --profile docs/improvement-requests/profile.dhall --profile-enforce --log-enforce
    mori validate

# Measure both suites, including hand-written c2hs wrappers, on one candidate.
[group("build")]
coverage:
    scripts/test-coverage.sh

# Reject open contracts, stale evidence and unexplained coverage misses.
[group("build")]
coverage-check:
    cabal build lib:rdkafka-hs
    python3 scripts/check-coverage.py
