{
  pkgs,
  lib,
  config,
  inputs,
  ...
}: let
  # Python versions we support. Each one gets a `python-<version>` profile,
  # activated with `devenv shell -P python-<version>`.
  pythonVersions = ["3.10" "3.11" "3.12" "3.13"];
  defaultPythonVersion = "3.11";

  caBundle = "${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt";
in {
  packages = [
    pkgs.cacert
    pkgs.stdenv.cc
    pkgs.minikube
    pkgs.kubectl
    pkgs.git-cliff
    inputs.dagger.packages.${pkgs.stdenv.system}.dagger
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
