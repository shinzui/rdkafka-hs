# Project-specific flake-parts customizations for rdkafka-hs. seihou never
# regenerates this file; see flake.module.nix.example for the available options.
{ ... }:
{
  perSystem = { pkgs, ... }: {
    # c2hs preprocesses src/Kafka/Internal/RdKafka.chs against rdkafka.h.
    haskellProject.extraDevPackages = [ pkgs.haskellPackages.c2hs ];

    # rdkafka.pc lists librdkafka's own libraries under Requires.private, and
    # `pkg-config --cflags rdkafka` (run by Cabal for pkgconfig-depends) needs
    # their .pc files too. The Nix package build already has them.
    haskellProject.extraBuildInputs = map pkgs.lib.getDev pkgs.rdkafka.buildInputs;

    # Private test broker for the integration suite. The port is
    # redpanda.kafka-port in .seihou/manifest.json; keep it in sync with
    # test-integration/Kafka/TestEnv.hs and README.md.
    haskellProject.shellHook = ''
      export RDKAFKA_TEST_BROKER="''${RDKAFKA_TEST_BROKER:-127.0.0.1:39092}"
    '';

    # seihou owns these files and rewrites them on every `seihou run`; formatting
    # them would turn each regeneration into a conflict, and the generated
    # nix/haskell.nix is not nixpkgs-fmt clean.
    treefmt.settings.global.excludes = [
      "flake.nix"
      "nix/*.nix"
    ];
  };
}
