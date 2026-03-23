#!/bin/bash
# EC2 instance setup script for the spiders PDF processing pipeline.
# Run once on a fresh instance after SSH-ing in.
#
# Usage:
#   bash setup_ec2.sh <s3-bucket> <input-prefix> [output-prefix]
#
# Example:
#   bash setup_ec2.sh spiders-pardosa-artie-sh pardosa pardosa_processed
#
# The script will:
#   1. Install system dependencies (Python venv, Tesseract + language packs)
#   2. Pull the pipeline script and requirements from S3
#   3. Create a Python venv and install dependencies
#   4. Pull input PDFs from S3
#   5. Print the command to run the pipeline

set -euo pipefail

BUCKET="${1:?Usage: bash setup_ec2.sh <s3-bucket> <input-prefix> [output-prefix]}"
INPUT_PREFIX="${2:?Usage: bash setup_ec2.sh <s3-bucket> <input-prefix> [output-prefix]}"
OUTPUT_PREFIX="${3:-${INPUT_PREFIX}_processed}"

WORKDIR="$HOME/spiders"
INPUT_DIR="$WORKDIR/$INPUT_PREFIX"
OUTPUT_DIR="$WORKDIR/$OUTPUT_PREFIX"

echo "========================================"
echo "  Spiders EC2 setup"
echo "  Bucket : s3://$BUCKET"
echo "  Input  : $INPUT_DIR"
echo "  Output : $OUTPUT_DIR"
echo "========================================"

# ---------------------------------------------------------------------------
# 1. System dependencies
# ---------------------------------------------------------------------------
echo ""
echo "--- Installing system dependencies ---"
sudo apt-get update -q
sudo apt install -y \
    python3.12-venv \
    tesseract-ocr \
    tesseract-ocr-rus \
    tesseract-ocr-ukr \
    tesseract-ocr-deu \
    tesseract-ocr-fra \
    tesseract-ocr-ita \
    tesseract-ocr-spa \
    tesseract-ocr-lat

# ---------------------------------------------------------------------------
# 2. Pull scripts from S3
# ---------------------------------------------------------------------------
echo ""
echo "--- Pulling scripts from S3 ---"
mkdir -p "$WORKDIR/scripts"
aws s3 cp "s3://$BUCKET/repo/requirements.txt" "$WORKDIR/requirements.txt"
aws s3 cp "s3://$BUCKET/repo/scripts/pdf_processor.py" "$WORKDIR/scripts/pdf_processor.py"

# ---------------------------------------------------------------------------
# 3. Python venv
# ---------------------------------------------------------------------------
echo ""
echo "--- Setting up Python environment ---"
python3 -m venv "$WORKDIR/.venv"
source "$WORKDIR/.venv/bin/activate"
pip install --quiet --upgrade pip
pip install --quiet -r "$WORKDIR/requirements.txt"

# ---------------------------------------------------------------------------
# 4. Pull input PDFs from S3
# ---------------------------------------------------------------------------
echo ""
echo "--- Pulling input PDFs from S3 ---"
mkdir -p "$INPUT_DIR"
aws s3 sync "s3://$BUCKET/$INPUT_PREFIX" "$INPUT_DIR"

# ---------------------------------------------------------------------------
# 5. Patch paths in the script
# ---------------------------------------------------------------------------
sed -i "s|INPUT_DIR = Path(\".*\")|INPUT_DIR = Path(\"$INPUT_DIR\")|" "$WORKDIR/scripts/pdf_processor.py"
sed -i "s|OUT_BASE  = Path(\".*\")|OUT_BASE  = Path(\"$OUTPUT_DIR\")|" "$WORKDIR/scripts/pdf_processor.py"

echo ""
echo "========================================"
echo "  Setup complete."
echo ""
echo "  To run the pipeline:"
echo "    tmux new -s batch"
echo "    source $WORKDIR/.venv/bin/activate"
echo "    WORKERS=4 HF_HUB_OFFLINE=0 python3 $WORKDIR/scripts/pdf_processor.py"
echo ""
echo "  After the run, push results to S3:"
echo "    aws s3 sync $OUTPUT_DIR s3://$BUCKET/$OUTPUT_PREFIX"
echo "========================================"
