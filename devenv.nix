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
    version = "1.0.0-beta.21";
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
      darwin_amd64 = "fdbe191c965d240ee9cc90719fa9dca1586805cbb89b0a0087314a84a0937354";
      darwin_arm64 = "781f4862c022a19b9a4c498b59e1d9cfda1ee3cdc8ce210e4545bd8842a7416d";
      linux_amd64 = "c5ac777a5c99089869660943381136e22177b59a46412e310c2fef594760a041";
      linux_arm64 = "ba9d55638636918a3404313511bceba5cc0af22f487c1c1b6a5ba60ce70a61bb";
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
