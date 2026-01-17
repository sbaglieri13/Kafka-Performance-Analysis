#!/usr/bin/env bash
set -euo pipefail

# Vai alla directory del progetto
cd "$(dirname "$0")"

echo "Setup environment started..."

# Step 1: Crea venv
echo ">> Step 1/4: Creating Python virtual environment..."
if [ -d "venv" ]; then
    echo "   X Virtual environment already exists, skipping..."
else
    python -m venv venv
    echo "   ✓ Virtual environment created"
fi
echo ""

# Step 2: Attiva virtual environment
echo ">> Step 2/4: Activating venv..."
source venv/bin/activate
echo "   ✓ Virtual environment activated"
echo ""

# Step 3: Installa requirements.txt
echo ">> Step 3/4: Installing Python dependencies..."
if [ -f "requirements.txt" ]; then
    pip install -r requirements.txt > /dev/null 2>&1
    echo "   ✓ Dependencies installed"
else
    echo "   X Warning: requirements.txt not found"
fi
echo ""

# Step 4: Crea directory necessarie
echo ">> Step 4/4: Creating project directories..."

directories=(
    "data"
    "data/stats"
)

for dir in "${directories[@]}"; do
    if [ -d "$dir" ]; then
        echo "   - $dir (already exists)"
    else
        mkdir -p "$dir"
        echo "   ✓ $dir created"
    fi
done

echo ""
echo "Setup completed successfully!"