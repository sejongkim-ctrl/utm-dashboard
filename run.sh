#!/bin/bash
# UTM Performance Dashboard 실행 스크립트

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
VENV_DIR="$SCRIPT_DIR/.venv"

# venv 생성 (최초 1회)
if [ ! -d "$VENV_DIR" ]; then
    echo "가상환경 생성 중..."
    python3 -m venv "$VENV_DIR"
    "$VENV_DIR/bin/pip" install --upgrade pip -q
    "$VENV_DIR/bin/pip" install -r "$SCRIPT_DIR/requirements.txt" -q
    echo "설치 완료"
fi

# .env 로드 (있으면)
if [ -f "$SCRIPT_DIR/.env" ]; then
    set -a
    source "$SCRIPT_DIR/.env"
    set +a
fi

# 대시보드 실행
echo "UTM Dashboard 시작: http://localhost:8501"
"$VENV_DIR/bin/streamlit" run "$SCRIPT_DIR/app.py" \
    --server.port 8501 \
    --server.headless true
