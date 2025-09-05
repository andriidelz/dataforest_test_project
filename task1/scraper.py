import os
import threading
import queue
import requests
from lxml import html
from abc import ABC, abstractmethod
from utils.db import DatabaseConnection
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BaseScraper(ABC):
    def __init__(self, categories): 
        self.categories = categories
        self.task_queue = queue.Queue()
        self.data_queue = queue.Queue()
        self.session = requests.Session()

    @abstractmethod
    def fetch_category_products(self, category):
        pass

    @abstractmethod
    def parse_product(self, product_html, category):
        pass

    def run(self, thread_count=5):
        logger.info(f"Starting scraper with {thread_count} threads")
        for category in self.categories:
            products = self.fetch_category_products(category)
            logger.info(f"Found {len(products)} products for category {category}")
            for product in products:
                self.task_queue.put((product, category))
        threads = []
        for _ in range(thread_count):
            t = threading.Thread(target=self.scrape_worker)
            t.start()
            threads.append(t)
        db_writer = threading.Thread(target=self.db_writer_worker)
        db_writer.start()
        self.task_queue.join()
        self.data_queue.put(None)
        db_writer.join()
        logger.info("Scraping completed")

    def scrape_worker(self):
        while True:
            try:
                item = self.task_queue.get(timeout=1)
            except queue.Empty:
                break
            product, category = item
            data = self.parse_product(product, category)
            if data:
                self.data_queue.put(data)
            self.task_queue.task_done()

    def db_writer_worker(self):
        db = DatabaseConnection()
        conn = db.connect()
        cur = conn.cursor()
        while True:
            data = self.data_queue.get()
            if data is None:
                break
            self.insert_to_db(cur, data)
            conn.commit()
        cur.close()
        conn.close()

    def insert_to_db(self, cur, data):
        insert_query = """
            INSERT INTO products (name, category, price_range, description)
            VALUES (%s, %s, %s, %s)
        """
        cur.execute(insert_query, (data['name'], data['category'], data['price_range'], data['description']))

class VendrScraper(BaseScraper):
    BASE_URL = 'https://www.vendr.com'

    def fetch_category_products(self, category):
        """Fetch products from category page."""
        url = f"{self.BASE_URL}/categories/{category.lower().replace(' ', '-')}"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'
        }
        try:
            logger.info(f"Fetching {url}")
            response = self.session.get(url, headers=headers, timeout=10)
            if response.status_code != 200:
                logger.error(f"Failed to fetch {url}: Status {response.status_code}")
                return []
            tree = html.fromstring(response.content)
            # Updated XPath: try multiple possible product containers
            product_blocks = tree.xpath(
                "//div[contains(@class, 'vendor-card') or contains(@class, 'vendor-item') or contains(@class, 'product-card')]"
                " | //li[contains(@class, 'vendor-card') or contains(@class, 'vendor-item') or contains(@class, 'product-card')]"
            )
            logger.info(f"Raw HTML for {url}: {html.tostring(tree, pretty_print=True).decode()[:500]}...")  # Log snippet of HTML
            return product_blocks
        except requests.RequestException as e:
            logger.error(f"Error fetching {url}: {e}")
            return []

    def parse_product(self, product_html, category):
        """Parse product data, fetching detail page if needed."""
        try:
            # Try multiple selectors for name
            name_elements = product_html.xpath(
                ".//h2[contains(@class, 'vendor-title') or contains(@class, 'title')]/text()"
                " | .//a[contains(@class, 'vendor-name') or contains(@class, 'title')]/text()"
                " | .//span[contains(@class, 'title')]/text()"
            )
            name = name_elements[0].strip() if name_elements else None
            if not name:
                logger.warning("No product name found")
                return None

            # Get detail page URL
            detail_path_elements = product_html.xpath(".//a[contains(@href, '/marketplace/') or contains(@href, '/vendor/')]/@href")
            detail_path = detail_path_elements[0] if detail_path_elements else None
            if not detail_path:
                logger.warning(f"No detail page link found for product in {category}")
                return None
            detail_url = self.BASE_URL + detail_path if not detail_path.startswith('http') else detail_path

            # Try to get description from list page
            description_elements = product_html.xpath(
                ".//p[contains(@class, 'description') or contains(@class, 'vendor-description')]/text()"
                " | .//div[contains(@class, 'description') or contains(@class, 'vendor-description')]/text()"
            )
            description = description_elements[0].strip() if description_elements else 'N/A'

            # Fetch detail page for price range and possibly description
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'
            }
            logger.info(f"Fetching detail page {detail_url}")
            response = self.session.get(detail_url, headers=headers, timeout=10)
            if response.status_code != 200:
                logger.error(f"Failed to fetch {detail_url}: Status {response.status_code}")
                return None
            detail_tree = html.fromstring(response.content)
            price_range_elements = detail_tree.xpath(
                ".//span[contains(@class, 'price-range') or contains(@class, 'price')]/text()"
                " | .//p[contains(text(), 'Price') or contains(@class, 'price')]/text()"
            )
            price_range = price_range_elements[0].strip() if price_range_elements else 'N/A'

            # Try to get description from detail page if not found on list page
            if description == 'N/A':
                description_elements = detail_tree.xpath(
                    ".//p[contains(@class, 'description') or contains(@class, 'vendor-description')]/text()"
                    " | .//div[contains(@class, 'description') or contains(@class, 'vendor-description')]/text()"
                )
                description = description_elements[0].strip() if description_elements else 'N/A'

            data = {
                'name': name,
                'category': category,
                'price_range': price_range,
                'description': description
            }
            logger.info(f"Parsed product: {data['name']}")
            return data
        except (IndexError, requests.RequestException) as e:
            logger.error(f"Error parsing product or fetching detail page: {e}")
            return None

if __name__ == '__main__':
    from utils.config import load_config
    config = load_config()
    categories = ['DevOps', 'IT Infrastructure', 'Data Analytics and Management']
    scraper = VendrScraper(categories)
    thread_count = int(config.get('THREAD_COUNT', 5))
    db = DatabaseConnection()
    conn = db.connect()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS products (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255),
            category VARCHAR(255),
            price_range VARCHAR(100),
            description TEXT
        )
    """)
    conn.commit()
    cur.close()
    conn.close()
    scraper.run(thread_count)