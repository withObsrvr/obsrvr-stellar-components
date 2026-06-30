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
        default = pkgs.mkShell {
          packages = [
            pkgs.go
            pkgs.gopls
            pkgs.gnumake
            pkgs.protobuf
            pkgs.protoc-gen-go
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
