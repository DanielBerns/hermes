from pathlib import Path
import logging

import os

from fastapi import FastAPI, Depends, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# from hermes.message_board.agent import initialize_agent, SendPublicMessage
from hermes.core.helpers import get_directory, get_resource
from hermes.core.infra import Infra
from hermes.domain.sample import Sample
from hermes.domain.session import get_session

# Assuming your reports module is discoverable
from hermes.reporting.reports import (
    get_report_by_tag,
    get_report_by_brand,
    get_report_brand_competition,
)

# --- Database Setup ---

filename = Path(__file__)
script, project_identifier = filename.stem, filename.parents[1].stem
infra = Infra(script, project_identifier)

logger = logging.getLogger(__name__)
logger.info(f"Starting {script}")

mecon_container = infra.info_storage.container(Sample.MECON)
db_container = infra.info_storage.container(Sample.DATABASE, base=mecon_container)
db_uri = str(get_resource(db_container, infra.database_name, ".db"))

logger.info(f"DB_URI: {db_uri}")

# --- FastAPI App Initialization ---
app = FastAPI()

def get_db():
    with get_session(db_uri) as session:
        yield session

# --- API Endpoints ---
@app.get("/api/reports/by-tag")
async def report_by_tag(db: Session = Depends(get_db)):
    """API endpoint for the Tag-Centric View report."""
    return get_report_by_tag(db)

@app.get("/api/reports/by-brand")
async def report_by_brand(db: Session = Depends(get_db)):
    """API endpoint for the Brand-Centric View report."""
    return get_report_by_brand(db)

@app.get("/api/reports/brand-competition/{target_brand_name}")
async def report_brand_competition(target_brand_name: str, db: Session = Depends(get_db)):
    """API endpoint for the Brand Competition Analysis report."""
    if not target_brand_name:
        raise HTTPException(status_code=400, detail="Target brand name cannot be empty.")
    return get_report_brand_competition(db, target_brand_name)

# --- Static Frontend Serving ---
# Create a 'frontend' directory for your HTML/JS/CSS files
frontend_dir = Path(Path(__file__).parents[1] / "src" / "hermes" / "reporting" / "frontend")

# Mount the static directory
app.mount("/static", StaticFiles(directory=frontend_dir), name="static")

@app.get("/")
async def read_index():
    """Serves the main index.html file."""
    index_path = os.path.join(frontend_dir, "index.html")
    return FileResponse(index_path)
