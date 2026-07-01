{
  description = "Reusable Stellar flowctl processors and sinks";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

  outputs = { self, nixpkgs }:
    let
      systems = [ "x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin" ];
      forAllSystems = f: nixpkgs.lib.genAttrs systems (system: f nixpkgs.legacyPackages.${system});
    in
    {
      devShells = forAllSystems (pkgs: {
        default =
          let
            duckdb154 = pkgs.stdenvNoCC.mkDerivation {
              pname = "duckdb";
              version = "1.5.4";
              src =
                if pkgs.stdenv.hostPlatform.system == "x86_64-linux"
                then pkgs.fetchurl {
                  url = "https://github.com/duckdb/duckdb/releases/download/v1.5.4/duckdb_cli-linux-amd64.zip";
                  hash = "sha256-Hy+nJPsFSz2+Gpy9E95bdpl9hQ5wh+x2K6iNsE4BgM8=";
                }
                else throw "duckdb 1.5.4 CLI is pinned only for x86_64-linux";
              nativeBuildInputs = [ pkgs.unzip ];
              unpackPhase = "unzip $src";
              installPhase = ''
                install -Dm755 duckdb $out/bin/duckdb
              '';
            };
          in
          pkgs.mkShell {
            packages = [
              pkgs.go
              pkgs.gopls
              pkgs.gcc
              pkgs.gnumake
              pkgs.protobuf
              pkgs.protoc-gen-go
              duckdb154
              pkgs.git
            ];

            shellHook = ''
              export PS1="(obsrvr-stellar-components) $PS1"
            '';
          };
      });

      packages = forAllSystems (pkgs:
        let
          version = "0.1.0";
          buildComponent = name: pkgs.buildGoModule {
            pname = name;
            inherit version;
            src = ./.;
            subPackages = [ "components/${name}/cmd/component" ];
            nativeBuildInputs = [ pkgs.gcc ];
            env.CGO_ENABLED = "1";
            vendorHash = null;
          };
        in
        rec {
          stellar-ledger-processor = buildComponent "stellar-ledger-processor";
          jsonl-sink = buildComponent "jsonl-sink";
          postgres-sink = buildComponent "postgres-sink";
          ducklake-sink = buildComponent "ducklake-sink";
          default = stellar-ledger-processor;
        });
    };
}
