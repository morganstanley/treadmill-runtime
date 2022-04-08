{ stdenv
, fetchFromGitHub
, maven
, openjdk_headless
, cmake
, openssl
, krb5
, cyrus_sasl
}:
let
  # Using fetchFromGitHub as we don't need history for the build.
  src = fetchFromGitHub {
    owner = "apache";
    repo = "zookeeper";
    rev = "12b4e6821997534e1ff58e2e29b9df0beab817d3";
    sha256 = "1ryad4k1bqs0cz6b9l12qmazngpws5kb7iyk9yx4lryz73dd84kn";
  };

  # As explained in:
  # https://fzakaria.com/2020/07/20/packaging-a-maven-application-with-nix.html
  #
  # This creates a Maven repository capturing the dependencies
  # required for transforming the Jute definitions.
  mvnDeps = stdenv.mkDerivation {
    name = "zookeeper-jute-mvn";
    nativeBuildInputs = [
      maven
      openjdk_headless
    ];

    inherit src;

    phases = [ "unpackPhase" "patchPhase" "buildPhase" ];

    # We don't want to depend on the 'hostname' utility, and even less
    # to have a result which depends on the name of the build host.
    patchPhase = ''
      sed -i 's@executable="hostname"@executable="true"@' pom.xml
    '';

    # The 'find ... -delete' is important if we want the hash to be
    # stable, as Maven writes "arbitrary" timestamps to these files.
    buildPhase = ''
      mvn -Dmaven.repo.local=$out --batch-mode \
          --projects zookeeper-jute \
          generate-sources
      find $out -type f -regex '.+\(\.lastUpdated\|resolver-status\.properties\|_remote\.repositories\)' -delete
    '';

    # Make this a fixed-output derivation.
    outputHashAlgo = "sha256";
    outputHashMode = "recursive";
    outputHash = "150400ix0gxzmq9rnhw0gliq69q6h0mmxgrifi5v0xc3ah78b9cm";
  };
in
stdenv.mkDerivation {
  pname = "zookeeper-client-c";
  version = "3.7.0pre";
  nativeBuildInputs = [
    cmake
    maven
    openjdk_headless
  ];

  buildInputs = [
    openssl.dev
    krb5
    cyrus_sasl
  ];

  inherit src;

  patches = [
    ./CMakeLists.txt.patch
  ];
  prePatch = ''
    sed -i 's@executable="hostname"@executable="true"@' pom.xml
  '';

  # Generate C files from Jute before configuring the C client.
  preConfigure = ''
    mvn -Dmaven.repo.local=${mvnDeps} --batch-mode --offline \
          --projects zookeeper-jute \
          generate-sources
  '';

  # Path from build to CMakeLists.txt
  cmakeDir = "../zookeeper-client/zookeeper-client-c";
  cmakeFlags = [
    "-DCMAKE_VERBOSE_MAKEFILE=YES"
    "-DWANT_CPPUNIT=NO"
  ];
}
