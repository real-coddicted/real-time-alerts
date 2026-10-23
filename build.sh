#!/usr/bin/env bash
# Builds build/alerting-engine.jar with plain javac (no external dependencies). Requires JDK 21+.
set -euo pipefail
cd "$(dirname "$0")"
rm -rf build/classes
mkdir -p build/classes
javac --release 21 -Xlint:all,-serial -d build/classes $(find src/main/java -name '*.java')
jar --create --file build/alerting-engine.jar --main-class io.alerting.Main -C build/classes .
echo "built build/alerting-engine.jar"
