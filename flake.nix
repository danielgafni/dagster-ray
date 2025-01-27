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
      python = pkgs.python39;
    in {
      default = pkgs.mkShell {
        buildInputs = [pkgs.stdenv.cc.cc.lib];
        packages = with pkgs; [
          stdenv.cc
          uv
          python
        ];
        LD_LIBRARY_PATH = lib.makeLibraryPath [pkgs.stdenv.cc.cc.lib pkgs.glib pkgs.python39];
        UV_PYTHON = "${python}/bin/python";
        shellHook = ''
          uv venv --allow-existing
          uv sync --frozen --all-extras
        '';
      };
    };
  };
}
