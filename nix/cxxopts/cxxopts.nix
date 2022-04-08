{ stdenv
, cmake
, fetchFromGitHub
}:
stdenv.mkDerivation rec {
  pname = "cxxopts";
  version = "v2.2.1";

  src = fetchFromGitHub {
    owner = "jarro2783";
    repo = "cxxopts";
    rev = "302302b30839505703d37fb82f536c53cf9172fa";
    sha256 = "0d3y747lsh1wkalc39nxd088rbypxigm991lk3j91zpn56whrpha";
  };

  nativeBuildInputs = [
    cmake
  ];

  buildInputs = [];
}
