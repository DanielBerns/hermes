from pathlib import Path

from hermes.scrape.carrefour import CarrefourExtract, get_timestamp

def main() -> None:
    searches_txt = Path('.', 'searches').with_suffix('.txt').absolute()
    target = Path('~', 'Info', 'webdeprecios').expanduser()
    timestamp = get_timestamp()
    agent = Carrefour(searches_txt, target, timestamp)

    path_to_driver = str(Path('~', 'Software', 'geckodriver').expanduser())
    path_to_browser = str(Path('/', 'usr', 'bin','firefox'))
    agent.execute(path_to_driver, path_to_browser)

if __name__ == "__main__":
    main()
