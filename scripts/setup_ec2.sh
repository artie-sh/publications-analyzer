#!/bin/bash
# Run this on a fresh Ubuntu 22.04 EC2 instance in eu-west-1.
# The instance needs an IAM role (or credentials) with s3:GetObject/PutObject/ListBucket
# on spiders-pardosa-artie-sh.
#
# Usage:  bash setup_ec2.sh 2>&1 | tee setup.log

set -euo pipefail
BUCKET="spiders-pardosa-artie-sh"
REGION="eu-west-1"
REPO="$HOME/spiders"

# ---------------------------------------------------------------------------
# 1. System dependencies
# ---------------------------------------------------------------------------
echo "=== Installing system packages ==="
sudo apt-get update -qq
sudo apt-get install -y -qq \
    python3 python3-pip python3-venv \
    tesseract-ocr tesseract-ocr-eng tesseract-ocr-rus tesseract-ocr-ukr \
    tesseract-ocr-deu tesseract-ocr-spa tesseract-ocr-fra tesseract-ocr-ita \
    poppler-utils libgl1 awscli

# ---------------------------------------------------------------------------
# 2. Repo skeleton
# ---------------------------------------------------------------------------
echo "=== Creating directory structure ==="
mkdir -p "$REPO/scripts" "$REPO/finalization" "$REPO/finalization_short" \
         "$REPO/finalization_long" "$REPO/pardosa_processed"

# ---------------------------------------------------------------------------
# 3. Download from S3
# ---------------------------------------------------------------------------
echo "=== Downloading scripts ==="
aws s3 sync s3://$BUCKET/scripts/   "$REPO/scripts/"        --region $REGION
aws s3 cp   s3://$BUCKET/requirements.txt "$REPO/requirements.txt" --region $REGION

echo "=== Downloading PDFs ==="
aws s3 sync s3://$BUCKET/finalization/ "$REPO/finalization/" --region $REGION

echo "=== Downloading HuggingFace model cache ==="
mkdir -p "$HOME/.cache/huggingface"
aws s3 sync s3://$BUCKET/hf_cache/ "$HOME/.cache/huggingface/" --region $REGION

# ---------------------------------------------------------------------------
# 4. Python environment
# ---------------------------------------------------------------------------
echo "=== Setting up Python venv ==="
python3 -m venv "$REPO/.venv"
source "$REPO/.venv/bin/activate"
pip install --upgrade pip -q
pip install -r "$REPO/requirements.txt" -q

# ---------------------------------------------------------------------------
# 5. Split PDFs into short (<50 pages) and long (>=50 pages)
# ---------------------------------------------------------------------------
echo "=== Splitting PDFs by page count ==="
python3 - <<'PYEOF'
import fitz, shutil
from pathlib import Path

HOME   = Path.home()
FIN    = HOME / "spiders/finalization"
SHORT  = HOME / "spiders/finalization_short"
LONG   = HOME / "spiders/finalization_long"

for pdf in sorted(FIN.glob("*.pdf")):
    doc = fitz.open(str(pdf))
    n   = len(doc)
    doc.close()
    dest = SHORT if n < 50 else LONG
    shutil.copy2(pdf, dest / pdf.name)
    print(f"  {'SHORT' if n < 50 else 'LONG ':5}  {n:4d}p  {pdf.name[:70]}")
PYEOF

# ---------------------------------------------------------------------------
# 6. Process short PDFs
# ---------------------------------------------------------------------------
echo "=== Processing short PDFs (pdf_processor.py) ==="
PDF_INPUT_DIR="$REPO/finalization_short" \
PDF_OUT_BASE="$REPO/pardosa_processed" \
WORKERS=4 \
python3 "$REPO/scripts/pdf_processor.py"

# ---------------------------------------------------------------------------
# 7. Split long PDFs into 50-page parts
# ---------------------------------------------------------------------------
echo "=== Splitting long PDFs into parts ==="
python3 "$REPO/scripts/split_pdfs.py" "$REPO/finalization_long" 50

# ---------------------------------------------------------------------------
# 8. Process long PDFs (parts -> merge into pardosa_processed)
# ---------------------------------------------------------------------------
echo "=== Processing long PDFs (finalize_large_pdfs.py) ==="
FINALIZATION_DIR="$REPO/finalization_long" \
PROCESSED_DIR="$REPO/pardosa_processed" \
SPECIES_MAP="$REPO/scripts/species_map.json" \
python3 "$REPO/scripts/finalize_large_pdfs.py"

# ---------------------------------------------------------------------------
# 9. Upload results to S3
# ---------------------------------------------------------------------------
echo "=== Uploading results to S3 ==="
aws s3 sync "$REPO/pardosa_processed/" s3://$BUCKET/pardosa_processed_new/ --region $REGION

echo ""
echo "=== All done. Shutting down in 60 seconds (Ctrl-C to cancel) ==="
sleep 60
sudo shutdown -h now
