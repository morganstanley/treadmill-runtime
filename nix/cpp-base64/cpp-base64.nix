{ stdenv
, fetchFromGitHub
}:
stdenv.mkDerivation rec {
  pname = "cpp-base64";
  version = "V2.rc.07";
  src = fetchFromGitHub {
    owner = "ReneNyffenegger";
    repo = "cpp-base64";
    rev = version;
    sha256 = "1832ffb928vd6qwv2bm08xpzickf71pmidb52p9pfznp7bs2jfb3";
  };
  phases = [ "unpackPhase" "patchPhase" "installPhase" ];
  patches = [ ./Makefile.patch ];
  installPhase = ''
    make install PREFIX=$out
  '';
}
