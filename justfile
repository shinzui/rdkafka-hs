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

# Build and test the package in the Nix sandbox and run the flake checks.
[group("build")]
check:
    nix flake check
