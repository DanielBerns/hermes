from pathlib import Path

from hermes.scrape.carrefour.extract import CarrefourExtract
from hermes.core.helpers import get_timestamp

DEFAULT_SEARCHES_TXT =
def main() -> None:
    webdeprecios_home = Path.home() /  "Info" / "webdeprecios"
    searches_txt = webdeprecios_home / "searches.txt"
    if searches_txt.exists():
        pass
    else:
        default_searches_txt = Path(__file__).parents[0] / "assets" / "searches.txt"
        shutil.copy(default_searches_txt, searches_txt)
    timestamp = get_timestamp()
    extractor = CarrefourExtract(webdeprecios_home, timestamp, searches_txt)

    path_to_driver = str(Path.home() / 'Software' / 'geckodriver')
    path_to_browser = str(Path('/', 'usr', 'bin','firefox'))
    extractor.execute(path_to_driver, path_to_browser)

if __name__ == "__main__":
    main()
