{
  pkgs,
  lib,
  ...
}: let
  pythonVersion = builtins.getEnv "PYTHON_VERSION";
  version =
    if pythonVersion != ""
    then pythonVersion
    else "3.11";
  pythonPkg = builtins.getAttr "python${builtins.replaceStrings ["."] [""] version}" pkgs;
in {
  packages = [
    pkgs.stdenv.cc
    pkgs.uv
    pkgs.minikube
    pkgs.kubectl
    pkgs.git-cliff
  ];

  languages.python = {
    enable = true;
    package = pythonPkg;
  };

  env.LD_LIBRARY_PATH = lib.makeLibraryPath [
    pkgs.stdenv.cc.cc.lib
    pkgs.gcc-unwrapped.lib
    pkgs.glibc
    pkgs.glib
    pythonPkg
  ];

  enterShell = ''
    uv python pin ${version}
  '';
}
