{
  pkgs ? import <nixpkgs> {}
}:
let allpkgs = pkgs // {
  # TODO: need an easy way to switch between runtimes.
  stdenv = pkgs.clangStdenv;
};
in
rec {
  zookeeper_client_c = pkgs.callPackage ./nix/zookeeper/zookeeper-client-c.nix {};
  bitmask = pkgs.callPackage ./nix/bitmask/bitmask.nix {};
  fmt = pkgs.callPackage ./nix/fmt/fmt.nix {};
  cpp_base64 = pkgs.callPackage ./nix/cpp-base64/cpp-base64.nix {};
  cxxopts = pkgs.callPackage ./nix/cxxopts/cxxopts.nix {};
  deathhandler = pkgs.callPackage ./nix/deathhandler/deathhandler.nix {};
  treadmill_runtime = pkgs.callPackage ./treadmill-runtime.nix (allpkgs // {
    inherit zookeeper_client_c;
    inherit bitmask;
    inherit fmt;
    inherit cpp_base64;
    inherit cxxopts;
    inherit deathhandler;
  });
}
