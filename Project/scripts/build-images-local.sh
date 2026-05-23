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

mvn -pl csv-producer,language-detection-service,sentiment-detection-service,keyword-service,statistics-service -am compile jib:dockerBuild

echo "Local Docker images are ready:"
echo "  - project-csv-producer:latest"
echo "  - project-language-detection-service:latest"
echo "  - project-sentiment-detection-service:latest"
echo "  - project-keyword-service:latest"
echo "  - project-statistics-service:latest"
