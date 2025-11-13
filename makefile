# --- Environment / Shell ---
.DEFAULT_GOAL := help
SHELL := /bin/bash
.PHONY: init install clean help

# Set the default shell to bash for consistency
SHELL := /bin/bash

init:
	@echo "--- 🏗️  uv: initializing project..."
	@uv init --app --package
	@echo "--- ✅ uv: project initialized."

install: setup
	@echo "--- 📦 Installing dependencies..."
	@uv add bcrypt fastapi isort markdown pandas pydrive pytest python-dotenv requests ruff sqlalchemy thefuzz types-requests
	@echo "--- ✅ Dependencies installed."

clean:
	@echo "Cleaning Python cache files and build artifacts..."
	# Find and remove all .pyc files
	find . -type f -name "*.pyc" -delete

	# Find and remove all __pycache__ directories
	# Using -exec rm -rf instead of -delete for non-empty directories
	find . -type d -name "__pycache__" -exec rm -rf {} +

	# Find and remove pytest cache
	find . -type d -name ".pytest_cache" -exec rm -rf {} +

	# Find and remove tox directories
	find . -type d -name ".tox" -exec rm -rf {} +

	# Find and remove .egg-info directories
	find . -type d -name "*.egg-info" -exec rm -rf {} +

	# Remove build and dist directories
	rm -rf build/
	rm -rf dist/

	@echo "Cleaning complete."

# init install run dev test lint clean help
# --- Commands ---
help:
	@echo "Available commands:"
	@echo "  init             initialize project"
	@echo "  install          Install dependencies using uv"
	@echo ""
	@echo "Cleaning repo:"
	@echo "  clean    - Remove Python cache files and build artifacts"
