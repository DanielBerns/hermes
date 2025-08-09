from datetime import datetime

# Initialize the CRUD system
db_crud = DatabaseCRUD("sqlite:///fuel_prices.db")

# Example of weekly point of sale updates (100 records)
points_of_sale_data = [
    {
        'code_and_flag': 'SHELL123',
        'state_code': 'CA',
        'state_name': 'California',
        'city_name': 'Los Angeles',
        'flag_name': 'Shell',
        'business_name': 'Shell Oil Company',
        'branch_name': 'Downtown LA',
        'addresses': ['123 Main St', '456 Oak Ave']
    },
    # ... more point of sale records
]

# Process points of sale updates
new_points_of_sale = db_crud.bulk_create_points_of_sale(points_of_sale_data)
print(f"Processed {len(new_points_of_sale)} points of sale")

# Example of weekly price updates (20,000 records)
prices_data = [
    {
        'timestamp': datetime.now(),
        'article_code': 'REG87',
        'pos_code': 'SHELL123',
        'amount': 350  # $3.50
    },
    # ... more price records
]

# Process price updates in batches
new_prices = db_crud.bulk_create_prices(prices_data)
print(f"Processed {len(new_prices)} price records")

# Example of querying price statistics
article_code = 'REG87'
article = db_crud.get_article_code_by_value(article_code)
if article:
    stats = db_crud.get_price_statistics_by_article(article.id)
    print(f"Price statistics for {article_code}: {stats}")

    # Get price trends for the last 30 days
    trends = db_crud.get_price_trends_by_article(article.id, days=30)
    print(f"Price trends for the last 30 days: {len(trends)} data points")
