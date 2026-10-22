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

# Run the integration suite against the project-local broker.
[group("build")]
test-it *ARGS:
    redpanda-local-up
    cabal test rdkafka-hs-integration -f integration --test-show-details=direct {{ARGS}}

# Build the example executable.
[group("build")]
build-examples:
    cabal build -f examples all

# Format code via treefmt-nix.
[group("build")]
fmt:
    nix fmt

# Build and unit-test the package in the Nix sandbox, run the flake checks
# (formatting, pre-commit) and validate the knowledge bundles.
[group("build")]
check: docs-check
    nix build
    nix flake check


# --- Documentation ---

# Validate the ADR and improvement-request bundles against their profiles.
[group("docs")]
docs-check:
    okf validate docs/adr --strict --profile docs/adr/profile.dhall --profile-enforce --log-enforce
    okf validate docs/improvement-requests --strict --profile docs/improvement-requests/profile.dhall --profile-enforce --log-enforce
    mori validate
