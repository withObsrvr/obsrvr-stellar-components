{
  description = "obsrvr-stellar-components - Apache Arrow-native Stellar data processing components";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    gomod2nix = {
      url = "github:nix-community/gomod2nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, flake-utils, gomod2nix }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
        
        # Go version
        go = pkgs.go_1_23;
        
        # Apache Arrow C++ library
        arrow-cpp = pkgs.arrow-cpp.override {
          enableFlight = true;
          enableS3 = true;
          enableGcs = true;
        };
        
        # Common build inputs
        buildInputs = with pkgs; [
          # Core development tools
          go
          gomod2nix.legacyPackages.${system}.gomod2nix
          
          # Apache Arrow dependencies
          arrow-cpp
          
          # Build tools
          gcc
          pkg-config
          cmake
          
          # SSL/TLS
          openssl
          openssl.dev
          
          # Compression libraries
          zlib
          lz4
          zstd
          snappy
          brotli
          
          # Additional libraries for Stellar Go SDK
          libsodium
          
          # Development utilities
          yq-go
          jq
          curl
          git
          
          # Container tools
          docker
          docker-compose
          
          # Monitoring tools
          prometheus
          grafana
          
          # Database tools (for development)
          redis
          postgresql
        ];
        
        # Development shell packages
        devShellPackages = buildInputs ++ (with pkgs; [
          # Additional development tools
          golangci-lint
          gosec
          gotests
          gotools
          gopls
          delve # Go debugger
          
          # Kubernetes tools (for deployment)
          kubectl
          kubernetes-helm
          
          # Cloud CLI tools
          awscli2
          google-cloud-sdk
          
          # Performance tools
          hyperfine
          wrk
          
          # Documentation tools
          pandoc
          
          # Shell utilities
          direnv
          pre-commit
          
          # Nix tools
          nixpkgs-fmt
          nil # Nix language server
        ]);
        
        # Component build function
        buildComponent = { name, src ? ./components/${name}/src }:
          pkgs.buildGoModule rec {
            pname = "obsrvr-${name}";
            version = "1.0.0";
            
            inherit src;
            
            vendorHash = null; # Will be set per component
            
            nativeBuildInputs = with pkgs; [
              pkg-config
              go
            ];
            
            buildInputs = with pkgs; [
              arrow-cpp
              openssl
              libsodium
            ];
            
            # CGO configuration
            CGO_ENABLED = "1";
            
            # Build flags
            ldflags = [
              "-s"
              "-w" 
              "-X main.version=${version}"
              "-X main.buildTime=1970-01-01T00:00:00Z" # Reproducible builds
            ];
            
            # Environment variables for Arrow
            preBuild = ''
              export PKG_CONFIG_PATH="${arrow-cpp}/lib/pkgconfig:$PKG_CONFIG_PATH"
              export CGO_CFLAGS="-I${arrow-cpp}/include"
              export CGO_LDFLAGS="-L${arrow-cpp}/lib -larrow -larrow_flight"
            '';
            
            meta = with pkgs.lib; {
              description = "Apache Arrow-native Stellar data processing component: ${name}";
              homepage = "https://github.com/withobsrvr/obsrvr-stellar-components";
              license = licenses.asl20;
              maintainers = [ "Obsrvr Team" ];
            };
          };
        
        # Schema tools build
        schemaTools = pkgs.buildGoModule rec {
          pname = "obsrvr-schema-tools";
          version = "1.0.0";
          
          src = ./schemas;
          
          vendorHash = null; # TODO: Generate with gomod2nix
          
          nativeBuildInputs = with pkgs; [
            pkg-config
            go
          ];
          
          buildInputs = [
            arrow-cpp
          ];
          
          CGO_ENABLED = "1";
          
          preBuild = ''
            export PKG_CONFIG_PATH="${arrow-cpp}/lib/pkgconfig:$PKG_CONFIG_PATH"
            export CGO_CFLAGS="-I${arrow-cpp}/include"
            export CGO_LDFLAGS="-L${arrow-cpp}/lib -larrow"
          '';
        };
        
        # Docker images
        dockerImages = {
          stellar-arrow-source = pkgs.dockerTools.buildLayeredImage {
            name = "obsrvr/stellar-arrow-source";
            tag = "latest";
            
            contents = with pkgs; [ 
              cacert 
              tzdata
              (buildComponent { name = "stellar-arrow-source"; })
            ];
            
            config = {
              Entrypoint = [ "/bin/obsrvr-stellar-arrow-source" ];
              ExposedPorts = {
                "8815/tcp" = {}; # Arrow Flight
                "8088/tcp" = {}; # Health check
              };
              Env = [
                "SSL_CERT_FILE=${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt"
              ];
            };
          };
          
          ttp-arrow-processor = pkgs.dockerTools.buildLayeredImage {
            name = "obsrvr/ttp-arrow-processor";
            tag = "latest";
            
            contents = with pkgs; [
              cacert
              tzdata
              (buildComponent { name = "ttp-arrow-processor"; })
            ];
            
            config = {
              Entrypoint = [ "/bin/obsrvr-ttp-arrow-processor" ];
              ExposedPorts = {
                "8816/tcp" = {}; # Arrow Flight
                "8088/tcp" = {}; # Health check
              };
              Env = [
                "SSL_CERT_FILE=${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt"
              ];
            };
          };
          
          arrow-analytics-sink = pkgs.dockerTools.buildLayeredImage {
            name = "obsrvr/arrow-analytics-sink";
            tag = "latest";
            
            contents = with pkgs; [
              cacert
              tzdata
              (buildComponent { name = "arrow-analytics-sink"; })
            ];
            
            config = {
              Entrypoint = [ "/bin/obsrvr-arrow-analytics-sink" ];
              ExposedPorts = {
                "8817/tcp" = {}; # Arrow Flight
                "8080/tcp" = {}; # WebSocket
                "8081/tcp" = {}; # REST API  
                "8088/tcp" = {}; # Health check
              };
              Env = [
                "SSL_CERT_FILE=${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt"
              ];
            };
          };
        };
        
        # Development scripts
        devScripts = {
          # Build all components
          build-all = pkgs.writeShellScriptBin "build-all" ''
            echo "Building all components with Nix..."
            nix build .#stellar-arrow-source
            nix build .#ttp-arrow-processor
            nix build .#arrow-analytics-sink
            nix build .#schema-tools
            echo "All components built successfully!"
          '';
          
          # Run development environment
          dev-env = pkgs.writeShellScriptBin "dev-env" ''
            echo "Starting development environment..."
            
            # Create necessary directories
            mkdir -p data logs
            
            # Set environment variables
            export STELLAR_NETWORK=testnet
            export STELLAR_RPC_URL=https://soroban-testnet.stellar.org
            export NETWORK_PASSPHRASE="Test SDF Network ; September 2015"
            export DATA_DIR="$PWD/data"
            export LOG_LEVEL=info
            
            echo "Environment ready!"
            echo "Use 'nix develop' to enter the development shell"
          '';
          
          # Run tests
          test-all = pkgs.writeShellScriptBin "test-all" ''
            echo "Running all tests with Nix..."
            
            # Test schemas
            cd schemas && go test -v ./...
            
            # Test components
            for component in stellar-arrow-source ttp-arrow-processor arrow-analytics-sink; do
              echo "Testing $component..."
              cd "components/$component/src" && go test -v ./...
              cd - > /dev/null
            done
            
            echo "All tests completed!"
          '';
          
          # Generate vendor files for Nix builds
          generate-vendor = pkgs.writeShellScriptBin "generate-vendor" ''
            echo "Generating vendor files for Nix builds..."
            
            # Generate for schemas
            cd schemas && gomod2nix generate
            
            # Generate for components
            for component in stellar-arrow-source ttp-arrow-processor arrow-analytics-sink; do
              echo "Generating vendor for $component..."
              cd "components/$component/src" && gomod2nix generate
              cd - > /dev/null
            done
            
            echo "Vendor files generated!"
          '';
          
          # Docker build with Nix
          docker-build = pkgs.writeShellScriptBin "docker-build" ''
            echo "Building Docker images with Nix..."
            
            nix build .#dockerImages.stellar-arrow-source
            nix build .#dockerImages.ttp-arrow-processor
            nix build .#dockerImages.arrow-analytics-sink
            
            # Load images into Docker
            docker load < result-stellar-arrow-source
            docker load < result-ttp-arrow-processor  
            docker load < result-arrow-analytics-sink
            
            echo "Docker images built and loaded!"
          '';
        };
        
      in
      {
        # Development shell
        devShells.default = pkgs.mkShell {
          buildInputs = devShellPackages ++ [
            devScripts.build-all
            devScripts.dev-env  
            devScripts.test-all
            devScripts.generate-vendor
            devScripts.docker-build
          ];
          
          shellHook = ''
            echo "🚀 obsrvr-stellar-components development environment"
            echo ""
            echo "Available commands:"
            echo "  build-all         - Build all components"
            echo "  dev-env           - Set up development environment"
            echo "  test-all          - Run all tests"
            echo "  generate-vendor   - Generate vendor files for Nix"
            echo "  docker-build      - Build Docker images with Nix"
            echo ""
            echo "Environment:"
            echo "  Go version: $(go version)"
            echo "  Arrow C++:  ${arrow-cpp.version}"
            echo ""
            
            # Set up environment variables
            export CGO_ENABLED=1
            export PKG_CONFIG_PATH="${arrow-cpp}/lib/pkgconfig:$PKG_CONFIG_PATH"
            export CGO_CFLAGS="-I${arrow-cpp}/include"
            export CGO_LDFLAGS="-L${arrow-cpp}/lib -larrow -larrow_flight"
            
            # Create necessary directories
            mkdir -p data logs build dist
            
            # Set up Git hooks if pre-commit is available
            if command -v pre-commit >/dev/null 2>&1; then
              pre-commit install 2>/dev/null || true
            fi
          '';
        };
        
        # Package outputs
        packages = {
          # Individual components
          stellar-arrow-source = buildComponent { name = "stellar-arrow-source"; };
          ttp-arrow-processor = buildComponent { name = "ttp-arrow-processor"; };  
          arrow-analytics-sink = buildComponent { name = "arrow-analytics-sink"; };
          
          # Schema tools
          schema-tools = schemaTools;
          
          # Docker images
          inherit dockerImages;
          
          # Default package
          default = self.packages.${system}.stellar-arrow-source;
        };
        
        # Application outputs (for `nix run`)
        apps = {
          stellar-arrow-source = flake-utils.lib.mkApp {
            drv = self.packages.${system}.stellar-arrow-source;
          };
          
          ttp-arrow-processor = flake-utils.lib.mkApp {
            drv = self.packages.${system}.ttp-arrow-processor;
          };
          
          arrow-analytics-sink = flake-utils.lib.mkApp {
            drv = self.packages.${system}.arrow-analytics-sink;
          };
          
          schema-tools = flake-utils.lib.mkApp {
            drv = self.packages.${system}.schema-tools;
          };
          
          # Default app
          default = self.apps.${system}.stellar-arrow-source;
        };
        
        # Check outputs (for `nix flake check`)
        checks = {
          # Build checks
          build-stellar-arrow-source = self.packages.${system}.stellar-arrow-source;
          build-ttp-arrow-processor = self.packages.${system}.ttp-arrow-processor;
          build-arrow-analytics-sink = self.packages.${system}.arrow-analytics-sink;
          build-schema-tools = self.packages.${system}.schema-tools;
          
          # Format check
          format-check = pkgs.runCommand "format-check" { buildInputs = [ pkgs.nixpkgs-fmt ]; } ''
            nixpkgs-fmt --check ${./.} && touch $out
          '';
          
          # Bundle validation
          validate-bundle = pkgs.runCommand "validate-bundle" {
            buildInputs = [ pkgs.yq-go ];
          } ''
            cd ${./.}
            ./scripts/validate-bundle.sh bundle.yaml && touch $out
          '';
          
          # Pipeline validation
          validate-pipelines = pkgs.runCommand "validate-pipelines" {
            buildInputs = [ pkgs.yq-go ];
          } ''
            cd ${./.}
            for template in templates/*.yaml; do
              ./scripts/validate-pipeline.sh "$template"
            done
            touch $out
          '';
        };
        
        # Formatter (for `nix fmt`)
        formatter = pkgs.nixpkgs-fmt;
      });
}