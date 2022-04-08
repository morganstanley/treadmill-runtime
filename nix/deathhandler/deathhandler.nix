{ stdenv
, fetchFromGitHub
}:
stdenv.mkDerivation {
  pname = "deathhandler";
  version = "1.0";
  src = fetchFromGitHub {
    owner = "vmarkovtsev";
    repo = "DeathHandler";
    rev = "6b8599b81d54734e42f09c146cbde1049f1e8b69";
    sha256 = "09spsbfn2dwk73w8imvwnmdydxy2q7a2m401i0h3ml4ck9v7ih8i";
  };
  buildInputs = [];
  phases = [ "unpackPhase" "buildPhase" "installPhase" ];
  buildPhase = ''
    g++ -c death_handler.cc
    ar -rc libdeath_handler.a death_handler.o
    ranlib libdeath_handler.a
  '';
  installPhase = ''
    install -D death_handler.h $out/include/death_handler.h
    install -D libdeath_handler.a $out/lib/libdeath_handler.a
  '';
}
