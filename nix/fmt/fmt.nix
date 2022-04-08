{ stdenv
, cmake
, fetchFromGitHub
}:
stdenv.mkDerivation rec {
  pname = "fmt";
  version = "7.1.2";
  src = fetchFromGitHub {
    owner = "fmtlib";
    repo = "fmt";
    rev = version;
    sha256 = "1ixwkqdbm01rcmqni15z4jj2rzhb3w7nlhkw2w4dzc4m101vr6yz";
  };

  nativeBuildInputs = [
    cmake
  ];

  buildInputs = [];
}
