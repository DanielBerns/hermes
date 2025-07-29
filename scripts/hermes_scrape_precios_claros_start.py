from pathlib import Path

from hermes.core.action import execute
from hermes.scrape_precios_claros.scrape_precios_claros import ScrapePreciosClarosStart

def main() -> None:
    script = Path(__file__).stem
    project_identifier = "unpsjb_fce_obsecon"
    action = DownloadDataStart()
    execute(script, project_identifier, action)

if __name__ == "__main__":
    main()
