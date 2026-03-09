#!/bin/bash

echo "===== Starting Data Pipeline ====="

echo "Step 1: Running fetch_data.py"
python fetch_data.py

echo "Step 2: Running clean_data.py"
python clean_data.py

echo "Step 3: Executing analytic notebook"

jupyter nbconvert \
    --execute visualization.ipynb \
    --to notebook \
    --inplace

echo "Step 4: Starting Jupyter Notebook Server"

jupyter notebook \
    --ip=0.0.0.0 \
    --port=8888 \
    --no-browser \
    --allow-root