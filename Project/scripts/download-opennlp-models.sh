#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
MODELS_ROOT="${PROJECT_ROOT}/models/opennlp"

UD_MODELS_BASE_URL="${UD_MODELS_BASE_URL:-https://downloads.apache.org/opennlp/models/ud-models-1.3}"
LANGDETECT_BASE_URL="${LANGDETECT_BASE_URL:-https://downloads.apache.org/opennlp/models/langdetect/1.8.3}"
MODEL_VERSION="${OPENNLP_UD_MODEL_VERSION:-1.3-2.5.4}"

# Supported by language-detection-service and keyword-service.
SUPPORTED_MODELS=(
  "en ud-ewt"
  "de ud-gsd"
  "cs ud-pdtc"
  "it ud-vit"
  "et ud-edt"
  "sl ud-ssj"
  "el ud-gdt"
  "lv ud-lvtb"
  "uk ud-iu"
  "sv ud-talbanken"
  "is ud-icepahc"
)

download() {
  local url="$1"
  local target="$2"
  local tmp="${target}.tmp"

  mkdir -p "$(dirname -- "${target}")"
  echo "Downloading ${url}"

  if command -v curl >/dev/null 2>&1; then
    curl --fail --location --retry 3 --output "${tmp}" "${url}"
  elif command -v wget >/dev/null 2>&1; then
    wget --tries=3 --output-document="${tmp}" "${url}"
  else
    echo "Neither curl nor wget is installed."
    exit 1
  fi

  mv -f "${tmp}" "${target}"
  echo "Saved ${target}"
}

folder_for_model_type() {
  case "$1" in
    tokens) echo "tokens" ;;
    pos) echo "pos" ;;
    lemmas) echo "lemmas" ;;
    *)
      echo "Unsupported model type: $1" >&2
      exit 1
      ;;
  esac
}

download_ud_model() {
  local language="$1"
  local treebank="$2"
  local model_type="$3"
  local folder
  local filename

  folder="$(folder_for_model_type "${model_type}")"
  filename="opennlp-${language}-${treebank}-${model_type}-${MODEL_VERSION}.bin"
  download "${UD_MODELS_BASE_URL}/${filename}" "${MODELS_ROOT}/${folder}/${filename}"
}

download "${LANGDETECT_BASE_URL}/langdetect-183.bin" "${MODELS_ROOT}/langdetect-183.bin"

for entry in "${SUPPORTED_MODELS[@]}"; do
  read -r language treebank <<< "${entry}"
  download_ud_model "${language}" "${treebank}" "tokens"
  download_ud_model "${language}" "${treebank}" "pos"
  download_ud_model "${language}" "${treebank}" "lemmas"
done

echo "OpenNLP models are ready under ${MODELS_ROOT}"
