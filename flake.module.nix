# Project-specific flake-parts customizations for rdkafka-hs. seihou never
# regenerates this file; see flake.module.nix.example for the available options.
{ ... }:
{
  perSystem = { pkgs, ... }: {
    # c2hs preprocesses src/Kafka/Internal/RdKafka.chs against rdkafka.h.
    haskellProject.extraDevPackages = [ pkgs.haskellPackages.c2hs ];

    # seihou owns these files and rewrites them on every `seihou run`; formatting
    # them would turn each regeneration into a conflict, and the generated
    # nix/haskell.nix is not nixpkgs-fmt clean.
    treefmt.settings.global.excludes = [
      "flake.nix"
      "nix/*.nix"
    ];
  };
}
