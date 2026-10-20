{
  pkgs,
  lib,
  config,
  ...
}: let
  # Python versions we support. Each one gets a `python-<version>` profile,
  # activated with `devenv shell -P python-<version>`.
  pythonVersions = ["3.10" "3.11" "3.12" "3.13" "3.14"];
  defaultPythonVersion = "3.11";

  caBundle = "${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt";

  # Dagger 1.0 betas are not packaged in nixpkgs or github:dagger/nix yet.
  dagger = let
    version = "1.0.0-beta.20";
    platform = "${
      if pkgs.stdenv.hostPlatform.isDarwin
      then "darwin"
      else "linux"
    }_${
      if pkgs.stdenv.hostPlatform.isAarch64
      then "arm64"
      else "amd64"
    }";
    # from https://dl.dagger.io/dagger/releases/${version}/checksums.txt
    hashes = {
      darwin_amd64 = "4707d7573a95d2db314b19abe8dc28620b30eadd7a369b57a14312942690071d";
      darwin_arm64 = "19e623d264b217570c0f8b84d85c8321c2e63f164fcdd4426f3e49245ec642cf";
      linux_amd64 = "5982b67992b55cc1cc8636509a0f672a401368a16b865bbe1d257195e7b516ae";
      linux_arm64 = "7b9a9989092832006f6188d92b60f92375a784bf2c92217b083999be6c6e2874";
    };
  in
    pkgs.stdenvNoCC.mkDerivation {
      pname = "dagger";
      inherit version;
      src = pkgs.fetchurl {
        url = "https://dl.dagger.io/dagger/releases/${version}/dagger_v${version}_${platform}.tar.gz";
        sha256 = hashes.${platform};
      };
      sourceRoot = ".";
      installPhase = "install -Dm755 dagger $out/bin/dagger";
    };
in {
  packages = [
    pkgs.cacert
    pkgs.stdenv.cc
    pkgs.kubectl
    pkgs.git-cliff
    dagger
  ];

  env = {
    NIX_SSL_CERT_FILE = caBundle;
    REQUESTS_CA_BUNDLE = caBundle;

    # Without this uv obeys .python-version and rebuilds the venv with its own
    # interpreter, which undoes the profile's Python version and the manylinux setup.
    UV_PYTHON = "${config.languages.python.package}/bin/python";
  };

  # OpenSSL only looks at /etc/ssl/cert.pem by default, which does not exist on NixOS, so
  # HTTPS from Python fails with CERTIFICATE_VERIFY_FAILED (e.g. mkdocstrings fetching
  # https://docs.python.org/3/objects.inv). Nixpkgs' OpenSSL is patched to read
  # NIX_SSL_CERT_FILE, but interpreters from elsewhere are not, so set the standard variable
  # too. It has to happen here rather than in `env` because the cacert setup hook moves
  # SSL_CERT_FILE into NIX_SSL_CERT_FILE and unsets it.
  enterShell = ''
    export SSL_CERT_FILE="${caBundle}"
  '';

  languages.python = {
    enable = true;
    version = lib.mkDefault defaultPythonVersion;
    manylinux.enable = pkgs.stdenv.isLinux;
    venv.enable = true;
    uv = {
      enable = true;
      sync = {
        enable = true;
        allExtras = true;
        allGroups = true;
      };
    };
  };

  profiles = lib.genAttrs (map (version: "python-${version}") pythonVersions) (name: {
    module.languages.python.version = lib.removePrefix "python-" name;
  });
}
