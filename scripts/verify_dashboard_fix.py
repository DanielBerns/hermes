import sys
import os
from sqlalchemy.orm import Session

# Add src to path
sys.path.append(os.path.join(os.getcwd(), "src"))

from hermes.dashboard.dependencies import get_db_uri
from hermes.domain.session import get_session
from hermes.domain.models import ArticleBrand, ArticleTag, ArticleCard, ArticleDescription, ArticleCode, ArticlePackage

import requests

BASE_URL = "http://127.0.0.1:8001"

def populate_db():
    print("Populating database with sample data...")
    db_uri = get_db_uri()
    print(f"DB URI: {db_uri}")
    
    with get_session(db_uri) as session:
        # Check if data exists
        if session.query(ArticleBrand).first():
            print("Data already exists. Skipping population.")
            return

        # Create Brands
        samsung = ArticleBrand(brand="Samsung")
        apple = ArticleBrand(brand="Apple")
        sony = ArticleBrand(brand="Sony")
        session.add_all([samsung, apple, sony])
        session.flush()

        # Create Tags
        electronics = ArticleTag(tag="electronics")
        mobile = ArticleTag(tag="mobile")
        home = ArticleTag(tag="home")
        session.add_all([electronics, mobile, home])
        session.flush()

        # Create Descriptions (Required by models)
        desc1 = ArticleDescription(description="Samsung Galaxy S23")
        desc2 = ArticleDescription(description="iPhone 15")
        desc3 = ArticleDescription(description="Sony TV")
        session.add_all([desc1, desc2, desc3])
        session.flush()
        
        # Create Codes (Required)
        code1 = ArticleCode(code="SAMS23")
        code2 = ArticleCode(code="IPH15")
        code3 = ArticleCode(code="SONYTV")
        session.add_all([code1, code2, code3])
        session.flush()
        
        # Create Packages (Required)
        pkg = ArticlePackage(package="Box")
        session.add(pkg)
        session.flush()

        # Create Cards
        card1 = ArticleCard(
            brand_id=samsung.id, 
            description_id=desc1.id, 
            package_id=pkg.id, 
            code_id=code1.id
        )
        card1.tags.append(electronics)
        card1.tags.append(mobile)

        card2 = ArticleCard(
            brand_id=apple.id, 
            description_id=desc2.id, 
            package_id=pkg.id, 
            code_id=code2.id
        )
        card2.tags.append(electronics)
        card2.tags.append(mobile)
        
        card3 = ArticleCard(
            brand_id=sony.id, 
            description_id=desc3.id, 
            package_id=pkg.id, 
            code_id=code3.id
        )
        card3.tags.append(electronics)
        card3.tags.append(home)

        session.add_all([card1, card2, card3])
        session.commit()
        print("Database populated successfully.")

def test_tags_endpoint():
    print("Testing /api/tags...")
    try:
        response = requests.get(f"{BASE_URL}/api/tags")
        response.raise_for_status()
        tags = response.json()
        print(f"SUCCESS: Retrieved {len(tags)} tags. Sample: {tags[:3]}")
        return tags
    except Exception as e:
        print(f"FAILURE: {e}")
        return []

def test_brands_endpoint():
    print("Testing /api/brands...")
    try:
        response = requests.get(f"{BASE_URL}/api/brands")
        response.raise_for_status()
        brands = response.json()
        print(f"SUCCESS: Retrieved {len(brands)} brands. Sample: {brands[:3]}")
        return brands
    except Exception as e:
        print(f"FAILURE: {e}")
        return []

def test_report_by_tag(tags):
    if not tags:
        print("SKIPPING: No tags available to test report.")
        return

    tag = "electronics"
    if tag not in tags:
        tag = tags[0]

    print(f"Testing /api/reports/by-tag with tag='{tag}'...")
    try:
        response = requests.get(f"{BASE_URL}/api/reports/by-tag", params={"tag": tag})
        response.raise_for_status()
        report = response.json()
        print(f"SUCCESS: Retrieved report for tag '{tag}'. Data keys (Brands found): {list(report[tag].keys())}")
        
    except Exception as e:
        print(f"FAILURE: {e}")

def run_verification():
    try:
        populate_db()
    except Exception as e:
        print(f"Population failed: {e}")
        # Continue to tests anyway in case data existed
    
    tags = test_tags_endpoint()
    test_brands_endpoint()
    test_report_by_tag(tags)

if __name__ == "__main__":
    run_verification()
