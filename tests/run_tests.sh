#!/bin/bash
set -e

echo "=== Running Unit Tests ==="
pytest tests/ -v -m "not integration" --tb=short

echo ""
echo "=== Running Integration Tests ==="
pytest tests/ -v -m integration --tb=short -x
