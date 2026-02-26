from random import shuffle
from pathlib import Path
from typing import List

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from hermes.scrape.helpers import random_sleep

class CarrefourExtract:
    def __init__(
        self,
        target: Path,
        timestamp: str,
        searches_txt: str
    ) -> None:
        self._online_shop = "https://www.carrefour.com.ar/"
        with open(searches_txt, "r") as f:
             searches: List[str] = [line[:-1] for line in f]
             shuffle(searches) # simulate human interaction
        self._searches: List[str] = searches
        store = target / timestamp / "carrefour"
        store.mkdir(mode=0o700, parents=True, exist_ok=True)
        self._store: Path = store
        self._number: int = 0

    @property
    def online_shop(self) -> str:
        return self._online_shop

    @property
    def searches(self) -> List[str]:
        return self._searches

    @property
    def store(self) -> Path:
        return self._store

    def target_html(self, index: int) -> Path:
        identifier = f"{index:>04d}"
        result = Path(self.store, identifier).with_suffix(".html")
        return result

    def execute(self, path_to_driver: str, path_to_browser: str, headless: bool = False) -> None:
        print(f"path_to_driver {path_to_driver:s}")
        print(f"path_to_browser {path_to_browser:s}")
        options = webdriver.FirefoxOptions()
        if headless:
            options.add_argument("-headless")
        options.binary_location = path_to_browser
        service = webdriver.FirefoxService(executable_path=path_to_driver)
        driver = webdriver.Firefox(service=service, options=options)
        driver.get(self.online_shop)
        print("title:", "Carrefour" in driver.title)

        # Get the main window handle
        original_window = driver.current_window_handle
        # Handle the popup (e.g., click a button)
        # onetrust-accept-btn-handler
        # //*[@id="onetrust-accept-btn-handler"]
        # https://www.youtube.com/watch?v=KIYXxY_86ng
        random_sleep() # Let the user actually see something!
        try:
            accept_all_cookies = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, '//*[@id="onetrust-accept-btn-handler"]'))
                )
            accept_all_cookies.click()
            print("clicked accept all cookies")
        except Exception as message:
            print(str(message))

        number = 0
        for cursor, this_search in enumerate(self.searches):
            attempts = 5
            ok = False
            while attempts > 0:
                try:
                    search_id = f"downshift-{number:d}-input"
                    elem = driver.find_element(By.ID, search_id)
                    attempts = 0
                    ok = True
                    print(f"search {this_search:s} - {search_id:s} - attempts {attempts:d} number {number:d}")
                except Exception as error:
                    attempts -= 1
                    number += 1
            if not ok:
                print(f"attempts {attempts:d} - number {number:d} - ok False")
                break
            elem.clear()
            elem.send_keys(this_search)
            elem.send_keys(Keys.RETURN)
            random_sleep(20, 10, 40) # Allow the browser to fully download the page
            page_source = driver.page_source
            with open(self.target_html(cursor), "w") as target:
                target.write(page_source)
            random_sleep(30, 1, 60) # Random wait to simulate human interaction

        driver.quit()
