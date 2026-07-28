{
  pkgs,
  lib,
  inputs,
  ...
}: {
  packages = [
    pkgs.stdenv.cc
    pkgs.minikube
    pkgs.kubectl
    pkgs.git-cliff
    inputs.dagger.packages.${pkgs.stdenv.system}.dagger
  ];

  languages.python = {
    enable = true;
    version = "3.11";
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
}
