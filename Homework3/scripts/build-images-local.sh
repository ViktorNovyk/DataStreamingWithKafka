#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is not installed or not available in PATH"
  exit 1
fi

if ! docker info >/dev/null 2>&1; then
  echo "docker daemon is not available. Start Docker and retry."
  exit 1
fi

cd "${PROJECT_ROOT}"

./mvnw -pl csv-producer,domain-statistics-stream -am compile jib:dockerBuild

echo "Local Docker images are ready:"
echo "  - csv-producer:latest"
echo "  - domain-statistics-stream:latest"
