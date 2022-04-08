{ stdenv
, cmake
, fetchFromGitHub
}:
stdenv.mkDerivation {
  pname = "bitmask";
  version = "1.1.2";
  src = fetchFromGitHub {
    owner = "oliora";
    repo = "bitmask";
    rev = "1.1.2";
    sha256 = "1v4ph20z2ync3d1cqcifsx87hn7avd8y9zfj2mbybynsxksbxmgd";
  };

  nativeBuildInputs = [
    cmake
  ];

  buildInputs = [];
}
