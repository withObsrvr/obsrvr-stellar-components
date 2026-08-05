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
            duckdb155 = pkgs.stdenvNoCC.mkDerivation {
              pname = "duckdb";
              version = "1.5.5";
              src =
                if pkgs.stdenv.hostPlatform.system == "x86_64-linux"
                then pkgs.fetchurl {
                  url = "https://github.com/duckdb/duckdb/releases/download/v1.5.5/duckdb_cli-linux-amd64.zip";
                  hash = "sha256-CMDKEXER/O3hQjnQCTeSNSvv3BdCGMNE0jLBMnlkPQU=";
                }
                else throw "duckdb 1.5.5 CLI is pinned only for x86_64-linux";
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
              duckdb155
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
            doCheck = false;
          };
        in
        rec {
          stellar-ledger-processor = buildComponent "stellar-ledger-processor";
          jsonl-sink = buildComponent "jsonl-sink";
          postgres-sink = buildComponent "postgres-sink";
          ducklake-sink = buildComponent "ducklake-sink";
          quack-ducklake-server = buildComponent "quack-ducklake-server";
          index-materializer = buildComponent "index-materializer";
          ducklake-gatekeeper = buildComponent "ducklake-gatekeeper";
          default = stellar-ledger-processor;
        });
    };
}
