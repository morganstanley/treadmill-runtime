{ stdenv
, krb5
, cyrus_sasl
, openssl
, cmake
, nix-gitignore
, spdlog
, libuv
, rapidjson
, libyaml
, catch2
, zlib
, zookeeper_client_c
, bitmask
, fmt
, cpp_base64
, cxxopts
, deathhandler
, ...
}:
stdenv.mkDerivation {
    name = "treadmill-runtime";
    buildInputs = [
      krb5
      cyrus_sasl
      openssl.dev
      spdlog
      libuv
      rapidjson
      libyaml
      catch2
      zlib
      zookeeper_client_c
      bitmask
      fmt
      cpp_base64
      cxxopts
      deathhandler
    ] ;
    nativeBuildInputs = [cmake];
    src = nix-gitignore.gitignoreSource [] ./.;
}
