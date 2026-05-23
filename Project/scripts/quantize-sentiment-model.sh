#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"

SOURCE_MODEL="${1:-${PROJECT_ROOT}/models/sentiment-distilbert/model.onnx}"
TARGET_MODEL="${2:-${PROJECT_ROOT}/models/sentiment-distilbert/model-int8.onnx}"
VENV_DIR="${SENTIMENT_EXPORT_VENV:-${PROJECT_ROOT}/.venv-sentiment-export}"

PYTHON_BIN="python3"
if [[ -x "${VENV_DIR}/bin/python" ]]; then
  PYTHON_BIN="${VENV_DIR}/bin/python"
fi

if [[ ! -f "${SOURCE_MODEL}" ]]; then
  echo "Source ONNX model does not exist: ${SOURCE_MODEL}"
  exit 1
fi

"${PYTHON_BIN}" - <<PY
from onnxruntime.quantization import QuantType, quantize_dynamic

source_model = "${SOURCE_MODEL}"
target_model = "${TARGET_MODEL}"

quantize_dynamic(
    source_model,
    target_model,
    weight_type=QuantType.QInt8
)

print(f"Saved quantized model to {target_model}")
PY
