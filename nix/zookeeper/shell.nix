{ pkgs ? import <nixpkgs> {} }:
with pkgs;
let cppunit_dir = builtins.toString cppunit; in
let openssl_dir = builtins.toString openssl.dev; in
let sasl_dir = builtins.toString cyrus_sasl.dev; in
# autoconf
# automake
# libtool
# coreutils
mkShell {
  buildInputs = [
    stdenv
    jdk11_headless
    cppunit
    openssl.dev
    krb5
    cyrus_sasl
    cmake
    maven
    coreutils
  ];
  shellHook = ''
    export PKG_CONFIG_PATH=${cppunit_dir}/lib/pkgconfig:${openssl_dir}/lib/pkgconfig:${sasl_dir}/lib/pkgconfig
  '';
}
