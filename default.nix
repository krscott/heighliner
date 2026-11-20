{
  lib,
  stdenv,
}:

stdenv.mkDerivation {
  name = "heighliner";

  src = lib.cleanSource ./.;

  nativeBuildInputs = [ ];
  buildInputs = [ ];

  installPhase = ''
    runHook preInstall

    mkdir -p "$out/bin"
    printf '%s\n' \
      '#!/usr/bin/env bash' \
      'set -euo pipefail' \
      'echo "Hello from heighliner"' \
      >"$out/bin/heighliner"
    chmod +x "$out/bin/heighliner"

    runHook postInstall
  '';

  meta.mainProgram = "heighliner";
}
