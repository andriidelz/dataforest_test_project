import os
import multiprocessing as mp
import logging
from playwright.sync_api import sync_playwright, Playwright
from abc import ABC, abstractmethod
from task2.process_manager import ProcessManager

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class BaseBookScraper(ABC):
    BASE_URL = 'https://books.toscrape.com'

    @abstractmethod
    def scrape_category(self, category_url, browser):
        pass

    @abstractmethod
    def collect_data(self, page, category):
        pass

class BookScraper(BaseBookScraper):
    def __init__(self, process_count=3, use_cdp=False):
        self.process_count = int(os.getenv('PROCESS_COUNT', process_count))
        self.use_cdp = use_cdp or bool(os.getenv('CDP_ENDPOINT'))
        self.cdp_endpoint = os.getenv('CDP_ENDPOINT')
        self.categories = []
        self.results = None  # Initialized in run()

    def run(self):
        self.results = mp.Manager().list()
        self.populate_categories()
        manager = ProcessManager(self.process_count, self.scrape_worker_wrapper, self.categories, self.results, self.use_cdp, self.cdp_endpoint)
        manager.start_processes()
        manager.monitor()
        logging.info(f"Collected {len(self.results)} books.")
        with open('books.json', 'w') as f:
            import json
            json.dump(list(self.results), f)

    def populate_categories(self):
        with sync_playwright() as p:
            browser = self.launch_browser(p)
            page = browser.new_page()
            try:
                page.goto(self.BASE_URL, wait_until="domcontentloaded")
                category_links = page.locator("div.side_categories ul li ul li a").all()
                self.categories = [self.BASE_URL + '/' + link.get_attribute('href') for link in category_links]
                logging.info(f"Found {len(self.categories)} categories")
            except Exception as e:
                logging.error(f"Error populating categories: {e}")
            finally:
                page.close()
                browser.close()

    def launch_browser(self, playwright: Playwright):
        if self.use_cdp and self.cdp_endpoint:
            return playwright.chromium.connect_over_cdp(self.cdp_endpoint)
        return playwright.chromium.launch(headless=True)

    def scrape_worker_wrapper(self, categories_subset, results, use_cdp, cdp_endpoint):
        with sync_playwright() as p:
            browser = self.launch_browser(p)
            try:
                for cat_url in categories_subset:
                    self.scrape_category(cat_url, browser)
            except Exception as e:
                logging.error(f"Error in worker for categories {categories_subset}: {e}")
            finally:
                browser.close()

    def scrape_category(self, category_url, browser):
        page = browser.new_page()
        try:
            page.goto(category_url, wait_until="domcontentloaded")
            category = page.locator("div.page-header h1").text_content().strip()
            logging.info(f"Scraping category: {category} ({category_url})")
            while True:
                book_links = page.locator("article.product_pod h3 a").all()
                for link in book_links:
                    book_url = self.BASE_URL + '/catalogue/' + link.get_attribute('href').replace('../../../', '')
                    detail_page = browser.new_page()
                    try:
                        detail_page.goto(book_url, wait_until="domcontentloaded")
                        logging.info(f"Scraping book: {book_url}")
                        data = self.collect_data(detail_page, category)
                        if data:
                            self.results.append(data)
                    except Exception as e:
                        logging.error(f"Failed to scrape book {book_url}: {e}")
                    finally:
                        detail_page.close()
                next_button = page.locator("li.next a")
                if next_button.count() == 0:
                    break
                next_url = category_url.rsplit('/', 1)[0] + '/' + next_button.get_attribute('href')
                page.goto(next_url, wait_until="domcontentloaded")
        except Exception as e:
            logging.error(f"Error scraping category {category_url}: {e}")
        finally:
            page.close()

    def collect_data(self, page, category):
        try:
            # Use more specific locators for the detail page
            title = page.locator("div.product_main h1").first.text_content().strip()
            price = page.locator("div.product_main p.price_color").first.text_content().strip()
            rating = page.locator("div.product_main p.star-rating").first.get_attribute('class').split()[-1]
            stock = page.locator("div.product_main p.availability").first.text_content().strip().replace('\n', '').strip()
            image_url = self.BASE_URL + page.locator("div#product_gallery img").first.get_attribute('src').replace('../../', '/')
            description = page.locator("#product_description ~ p").first.text_content().strip() if page.locator("#product_description").count() > 0 else 'N/A'
            product_info = {}
            info_rows = page.locator("table.table-striped tr").all()
            for row in info_rows:
                th = row.locator("th").text_content().strip()
                td = row.locator("td").text_content().strip()
                product_info[th] = td
            return {
                'title': title,
                'category': category,
                'price': price,
                'rating': rating,
                'stock_availability': stock,
                'image_url': image_url,
                'description': description,
                'product_information': product_info
            }
        except Exception as e:
            logging.error(f"Error collecting data: {e}")
            return None

if __name__ == '__main__':
    from utils.config import load_config
    config = load_config()
    scraper = BookScraper(process_count=config.get('PROCESS_COUNT', 3), use_cdp=bool(config.get('CDP_ENDPOINT')))
    scraper.run()