# Hermes

Software developed for Observatorio de Economía, Facultad de Ciencias Económicas, Universidad Nacional de la Patagonia San Juan Bosco.

## Core Functionalities

Hermes is a comprehensive data platform designed to provide deep insights into the retail pricing landscape. Its core functionalities follow a robust data processing pipeline:

### Automated Data Extraction: 

Hermes scrapes price data from diverse online sources, including major supermarket e-commerce portals and local retailer websites. 
The extraction engine is configurable to capture key data points such as 
product name, Stock Keeping Unit (SKU), current price, promotional discounts, and stock availability.

### Robust Data Storage: 

Collected information is stored in a database. 
Hermes uses the database sqlite, ensuring data integrity through well-defined schemas and efficient retrieval via optimized indexing.

### Advanced Querying Engine: 

The platform enables complex data analysis through a querying engine. 
Developers can leverage a REST API for programmatic access, while end-users can utilize a web interface to execute custom queries, 
perform price comparisons, track historical fluctuations, and identify significant market trends.

### Flexible Report Generation: 

Hermes generates comprehensive reports in multiple standard formats, including CSV, JSON, and PDF. 
The system can produce scheduled or on-demand reports detailing competitive pricing analyses, historical data visualizations, and customizable market insight dashboards.


## Operation

### Get buscavida database

uv run scripts/hermes_scrape_precios_claros_to_database.py 
uv run scripts/hermes_scrape_precios_claros_clean_descriptions.py

