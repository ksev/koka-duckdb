{
  description = "A very basic flake";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs?ref=nixos-unstable";
  };

  outputs = {
    nixpkgs,
    ...
  }: let
    pkgs = nixpkgs.legacyPackages.x86_64-linux;
  in {
    devShells.x86_64-linux.default = pkgs.mkShell {
      buildInputs = [
        pkgs.duckdb
        pkgs.clang-tools
        pkgs.koka
      ];

      shellHook = ''
        export CPATH=$CPATH:${pkgs.koka}/share/koka/v${pkgs.koka.version}/kklib/include
      '';
    };
  };
}
