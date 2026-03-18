{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
  };

  outputs = {nixpkgs, ...}: {
    formatter.x86_64-linux = nixpkgs.legacyPackages.x86_64-linux.alejandra;
    devShells.x86_64-linux = let
      pkgs = import nixpkgs {
        system = "x86_64-linux";
      };
      lib = pkgs.lib;
      python = pkgs.python311;
    in {
      default = pkgs.mkShell {
        buildInputs = [
          pkgs.stdenv.cc.cc.lib
          pkgs.gcc-unwrapped.lib
          pkgs.glibc
        ];
        packages = with pkgs; [
          stdenv.cc
          uv
          python
          minikube
          kubectl
          git-cliff
        ];
        LD_LIBRARY_PATH = lib.makeLibraryPath [
          pkgs.stdenv.cc.cc.lib
          pkgs.gcc-unwrapped.lib
          pkgs.glibc
          pkgs.glib
          pkgs.python311
        ];
        # UV_PYTHON = "${python}/bin/python";
        shellHook = ''
          uv python pin 3.11
        '';
      };
    };
  };
}
