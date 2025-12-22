import logging
import importlib.resources
from pathlib import Path

from fastapi import FastAPI, Depends, HTTPException, Query
from typing import List, Optional
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy.orm import Session

from hermes.dashboard.dependencies import get_db
from hermes.reporting.reports import (
    get_report_by_tag,
    get_report_by_brand,
    get_report_brand_competition,
    get_all_tags,
    get_all_brands
)

logger = logging.getLogger(__name__)

def create_app() -> FastAPI:
    """
    Factory function to create the FastAPI app.
    """
    app = FastAPI(title="Hermes Dashboard")

    # --- Static Frontend Serving ---
    # Robustly find the frontend directory using importlib
    # Assuming frontend is in 'hermes.reporting.frontend'
    try:
        # For Python 3.9+
        frontend_dir = importlib.resources.files("hermes.reporting") / "frontend"
    except (ImportError, AttributeError):
        # Fallback for older python or if structure differs (though user is on Linux/recent env usually)
        # This assumes the file is in src/hermes/dashboard/app.py and frontend is in src/hermes/reporting/frontend
        frontend_dir = Path(__file__).parents[2] / "reporting" / "frontend"

    if not frontend_dir.exists():
        logger.warning(f"Frontend directory not found at {frontend_dir}")
    else:
        logger.info(f"Serving frontend from {frontend_dir}")
        app.mount("/static", StaticFiles(directory=str(frontend_dir)), name="static")

    @app.get("/api/reports/by-tag")
    async def report_by_tag(tag: Optional[str] = Query(None), db: Session = Depends(get_db)):
        """API endpoint for the Tag-Centric View report."""
        return get_report_by_tag(db, tag_filter=tag)

    @app.get("/api/reports/by-brand")
    async def report_by_brand(brand: Optional[str] = Query(None), db: Session = Depends(get_db)):
        """API endpoint for the Brand-Centric View report."""
        return get_report_by_brand(db, brand_filter=brand)

    @app.get("/api/reports/brand-competition/{target_brand_name}")
    async def report_brand_competition(target_brand_name: str, db: Session = Depends(get_db)):
        """API endpoint for the Brand Competition Analysis report."""
        if not target_brand_name:
            raise HTTPException(status_code=400, detail="Target brand name cannot be empty.")
        return get_report_brand_competition(db, target_brand_name)

    @app.get("/api/tags", response_model=List[str])
    async def get_tags(db: Session = Depends(get_db)):
        """API endpoint to get all available tags."""
        return get_all_tags(db)

    @app.get("/api/brands", response_model=List[str])
    async def get_brands(db: Session = Depends(get_db)):
        """API endpoint to get all available brands."""
        return get_all_brands(db)

    @app.get("/")
    async def read_index():
        """Serves the main index.html file."""
        if not frontend_dir.exists():
             raise HTTPException(status_code=404, detail="Frontend not found")
        index_path = frontend_dir / "index.html"
        return FileResponse(index_path)

    return app

# Expose the app object for uvicorn to import
app = create_app()
