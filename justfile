# Justfile for rdkafka-hs

default:
    @just --list


# --- Build and test ---

# Build all enabled components.
[group("build")]
build:
    cabal build all

# Run the unit test suite.
[group("build")]
test:
    cabal test rdkafka-hs-test

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
