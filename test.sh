#!/usr/bin/env bash
# Compiles main + test sources and runs the test suite.
set -euo pipefail
cd "$(dirname "$0")"
rm -rf build/test-classes
mkdir -p build/test-classes
javac --release 21 -d build/test-classes $(find src/main/java src/test/java -name '*.java')
java -cp build/test-classes io.alerting.testing.TestMain "$@"
