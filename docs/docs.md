Here is a document on how to use the Hermes software based on the provided project files.

## How to Use the Hermes Software

### 1. Overview

**Hermes** is a Python-based data platform designed to download, process, and analyze supermarket prices from Argentina, developed for the Observatorio de Economía at the Universidad Nacional de la Patagonia San Juan Bosco.

The system is built around a data pipeline that scrapes pricing data (specifically from the "Precios Claros" program), processes it, stores it in a **SQLite database**, and provides tools for analysis, reporting, and a web-based dashboard.

### 2. Core Features

Based on the project's `README.md`, its core functionalities are:

* **Automated Data Extraction**: Scrapes price data from online sources, capturing product names, SKUs, prices, promotions, and availability.
* **Robust Data Storage**: Collected data is stored in an SQLite database with a defined schema for data integrity and efficient retrieval.
* **Advanced Querying Engine**: The platform includes a REST API (built with **FastAPI**) and a web interface for running custom queries, comparing prices, and tracking trends.
* **Flexible Report Generation**: The system can generate reports on competitive pricing, historical data, and market insights.

### 3. Setup and Installation

To get the project running, you will need Python and the `uv` package manager.

1.  **Prerequisites**:
    * **Python 3.14** or newer (as specified in `pyproject.toml` and `.python-version`).
    * **uv** Python package manager.

2.  **Install Dependencies**:
    * Navigate to the project's root directory (`hermes/`).
    * Run `uv sync` to install all required dependencies specified in the `pyproject.toml` and `uv.lock` files.

### 4. How to Use: The Data Pipeline

The main workflow involves running a series of scripts to fetch, store, and clean the data. These scripts are located in the `hermes/scripts/` directory.

#### Step 1: Initialize the System (First-Time-Only)

Before you can scrape, you must initialize the system parameters. This step creates the configuration files needed for the scrapers.

* **Command**: `uv run hermes/scripts/hermes_scrape_precios_claros_start.py`
* **What it does**: This script executes the `PreciosClarosStart` action, which creates the initial parameter files (like the states and cities selector) required for the scraper.

#### Step 2: Scrape New Data

This command runs the scraper to fetch fresh data from the "Precios Claros" source.

* **Command**: `uv run hermes/scripts/hermes_scrape_precios_claros_update.py`
* **What it does**: This runs the `PreciosClarosUpdate` action. It uses the `SampleBuilder` to orchestrate the `Scraper` and `DataProcessor`. New data is saved as `.jsonl` files in a timestamped "store" directory, managed by the `TreeStore`.

#### Step 3: Load Scraped Data into the Database

After scraping, you must move the data from the temporary `.jsonl` files into the main SQLite database.

* **Command**: `uv run hermes/scripts/hermes_scrape_precios_claros_to_database.py`
* **What it does**: This script (mentioned in the `README.md`) executes the `PreciosClarosToDatabase` action. It iterates through all unprocessed "stores" (created in Step 2) and uses the `DatabaseRepository` to load the data into the SQLite database, matching the SQLAlchemy models.

#### Step 4: Clean and Tag Data

To make the data easier to analyze, run the cleaning script to tag articles based on their descriptions.

* **Command**: `uv run hermes/scripts/hermes_scrape_precios_claros_clean_descriptions.py`
* **What it does**: This runs the `CleanArticleDescriptions` action. It analyzes article descriptions to automatically generate and apply tags (e.g., "Coca-Cola Classic" -> "Classic"). This populates the `article_tags` table in the database.

### 5. How to Use: The Reporting Dashboard

The project includes a **FastAPI** web server that provides both a REST API and a simple HTML/JavaScript frontend for viewing reports.

* **Command**: `uv run hermes/scripts/api_dashboard.py`
* **What it does**:
    * Starts a web server (likely on `http://0.0.0.0:8080` as per `base.yaml`).
    * Serves the main dashboard from `hermes/src/hermes/reporting/frontend/index.html`.
    * Provides API endpoints (like `/api/reports/by-tag` and `/api/reports/by-brand`) that query the database using functions from `hermes/src/hermes/reporting/reports.py`.
* **Using the Dashboard**:
    1.  Run the `api_dashboard.py` script.
    2.  Open your web browser and navigate to the server address (e.g., `http://127.0.0.1:8080`).
    3.  Use the dropdown menu to select a report type ("View by Product Tag", "View by Brand", "Brand Competition Analysis") and click "Generate Report".
    4.  You can save the generated report as a Markdown file.

### 6. Other Tools & Scripts

The project includes other useful scripts for inspection and analysis:

* **Inspect Scraped Data**:
    * **Command**: `uv run hermes/scripts/hermes_scrape_precios_claros_inspect.py`
    * **Action**: Runs `PreciosClarosInspect`, which generates Markdown reports (`.md`) detailing statistics about the raw data "stores" (e.g., how many articles and points of sale were scraped).
* **Query Article Tags**:
    * **Command**: `uv run hermes/scripts/hermes_scrape_precios_claros_query_descriptions.py`
    * **Action**: Runs `QueryArticleTagsAndArticleCards` to generate two Markdown reports: one listing all tagged articles grouped by tag, and another listing all articles that are still untagged.

### 7. Configuration

* The application's configuration is managed by `hermes/src/hermes/core/config.py`.
* Default settings are loaded from `hermes/src/hermes/config/base.yaml`.
* These settings are overridden by default with `hermes/src/hermes/config/development.yaml`.
* You can specify a different configuration file (e.g., `production.yaml`) by setting the `HERMES_CONFIG_PATH` environment variable.
* Key settings you may want to review include `project.instance`, `database.name`, and `logging.level`.
