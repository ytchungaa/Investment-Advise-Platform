#!/bin/bash
set -e

echo "Checking Python installation..."

if ! command -v python3 &> /dev/null; then
    echo "Python3 not found. Install it first."
    exit 1
fi

echo "Creating virtual environment..."

if [[ ! -d "venv" ]]; then
    python3 -m venv venv
else
    echo "Virtual environment already exists, skipping."
fi

echo "Installing dependencies..."

venv/bin/python -m pip install --upgrade pip

if [[ -f "requirements.txt" ]]; then
    venv/bin/python -m pip install -r requirements.txt
else
    echo "No requirements.txt found, skipping."
fi

echo ""
echo "Setup complete."
echo "Activate with:"
echo "source venv/bin/activate"
