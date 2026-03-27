{pkgs, lib, ...}: {
  packages = [
    pkgs.stdenv.cc
    pkgs.uv
    pkgs.minikube
    pkgs.kubectl
    pkgs.git-cliff
  ];

  languages.python = {
    enable = true;
    version = "3.11";
  };

  env.LD_LIBRARY_PATH = lib.makeLibraryPath [
    pkgs.stdenv.cc.cc.lib
    pkgs.gcc-unwrapped.lib
    pkgs.glibc
    pkgs.glib
    pkgs.python311
  ];

  enterShell = ''
    uv python pin 3.11
  '';
}
