#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"

VENV_DIR="${SENTIMENT_EXPORT_VENV:-${PROJECT_ROOT}/.venv}"
TARGET_DIR="${1:-${PROJECT_ROOT}/models/sentiment-distilbert}"
MODEL_ID="${SENTIMENT_DISTILBERT_MODEL_ID:-Yuu-Xie/distilbert-base-multilingual-cased-sentiment}"

if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 is not installed or not available in PATH"
  exit 1
fi

python3 -m venv "${VENV_DIR}"
# shellcheck disable=SC1091
source "${VENV_DIR}/bin/activate"

python3 -m pip install --upgrade pip
python3 -m pip install \
  "optimum[onnxruntime]" \
  "transformers" \
  "tokenizers" \
  "torch"

mkdir -p "${TARGET_DIR}"
rm -f \
  "${TARGET_DIR}/model.onnx" \
  "${TARGET_DIR}/model-int8.onnx" \
  "${TARGET_DIR}/config.json" \
  "${TARGET_DIR}/special_tokens_map.json" \
  "${TARGET_DIR}/tokenizer.json" \
  "${TARGET_DIR}/tokenizer_config.json" \
  "${TARGET_DIR}/vocab.txt"

python3 - <<PY
from pathlib import Path
from optimum.exporters.onnx import main_export
from transformers import AutoTokenizer

model_id = "${MODEL_ID}"
target_dir = Path("${TARGET_DIR}")

main_export(
    model_name_or_path=model_id,
    output=target_dir,
    task="text-classification",
    monolith=True,
)

tokenizer = AutoTokenizer.from_pretrained(model_id, use_fast=True)
tokenizer.save_pretrained(target_dir)

print(f"Saved {model_id} ONNX export and tokenizer files to {target_dir}")
PY

echo "Export complete. Run ./scripts/quantize-sentiment-model.sh to create model-int8.onnx."
