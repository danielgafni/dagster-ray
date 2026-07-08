{pkgs, lib, ...}: {
  packages = [
    pkgs.stdenv.cc
    pkgs.minikube
    pkgs.kubectl
    pkgs.git-cliff
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
