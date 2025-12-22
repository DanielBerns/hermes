
import uvicorn
import logging
from pathlib import Path
from hermes.dashboard.app import app

if __name__ == "__main__":
    # Configure logging for the runner
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("dashboard_runner")
    logger.info("Starting Hermes Dashboard...")
    
    # Run server
    uvicorn.run(app, host="127.0.0.1", port=8000)
