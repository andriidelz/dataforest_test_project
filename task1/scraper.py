import os
import threading
import queue
import requests
from lxml import html
from abc import ABC, abstractmethod
from utils.db import DatabaseConnection
import logging
import json

# Logging configuration
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
    def parse_product(self, product_data, category):
        pass

    def run(self, thread_count=5):
        logger.info(f"Launching scraper from {thread_count} threads")
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
        try:
            cur.execute(insert_query, (data['name'], data['category'], data['price_range'], data['description']))
            logger.info(f"Added product: {data['name']}")
        except Exception as e:
            logger.error(f"Error when adding product {data.get('name', 'Unknown')}: {e}")

class VendrScraper(BaseScraper):
    BASE_URL = 'https://www.vendr.com'
    API_KEY = os.getenv('VENDR_API_KEY')  # Add API-key from .env

    def fetch_category_products(self, category):
        """Try to get products via API, then via HTML."""
        products = self.fetch_api_products(category)
        if products:
            return products
        logger.warning(f"API didn't give results for {category}. Attempt HTML-scraping...")
        url = f"{self.BASE_URL}/categories/{category.lower().replace(' ', '-')}"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'
        }
        try:
            logger.info(f"Loading HTML {url}")
            response = self.session.get(url, headers=headers, timeout=10)
            if response.status_code != 200:
                logger.error(f"Failed to download {url}: Status {response.status_code}")
                return []
            tree = html.fromstring(response.content)
            logger.info(f"HTML for {url}: {html.tostring(tree, pretty_print=True).decode()[:500]}...")
            product_blocks = tree.xpath(
                "//div[contains(@class, 'software-item') or contains(@class, 'vendor-card') or contains(@class, 'product-item')]"
                " | //li[contains(@class, 'software-item') or contains(@class, 'vendor-card') or contains(@class, 'product-item')]"
            )
            return product_blocks
        except requests.RequestException as e:
            logger.error(f"Error to download HTML {url}: {e}")
            return []

    def fetch_api_products(self, category):
        """Getting products via API with using API-key."""
        api_urls = [
            f"{self.BASE_URL}/api/v1/catalog?category={category.lower().replace(' ', '-')}",
            f"{self.BASE_URL}/api/v1/marketplace?category={category.lower().replace(' ', '-')}",
            f"{self.BASE_URL}/api/v1/software?category={category.lower().replace(' ', '-')}"
        ]
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
            'Accept': 'application/json',
            'X-API-Key': self.API_KEY or ''  # Add API-key, if it exists
        }
        for api_url in api_urls:
            try:
                logger.info(f"Loading API {api_url}")
                response = self.session.get(api_url, headers=headers, timeout=10)
                if response.status_code != 200:
                    logger.error(f"Failed to download API {api_url}: Status {response.status_code}")
                    continue
                data = response.json()
                products = data.get('products', []) or data.get('data', []) or data.get('software', [])
                logger.info(f"Found {len(products)} products via API for {category} from {api_url}")
                return [
                    {'data': product, 'source': 'api'} for product in products
                ]
            except (requests.RequestException, json.JSONDecodeError) as e:
                logger.error(f"Error to download API {api_url}: {e}")
        return []

    def parse_product(self, product_data, category):
        """Parsing product's data with API or HTML."""
        if isinstance(product_data, dict) and product_data.get('source') == 'api':
            try:
                product = product_data['data']
                name = product.get('name', '') or product.get('title', '')
                description = product.get('description', 'N/A')
                price_range = product.get('price_range', 'N/A') or product.get('price', 'N/A')
                if not name:
                    logger.warning("Not found the name of product from API")
                    return None
                data = {
                    'name': name,
                    'category': category,
                    'price_range': price_range,
                    'description': description
                }
                logger.info(f"Parsed product from API: {data['name']}")
                return data
            except Exception as e:
                logger.error(f"Error when parsing API-datas: {e}")
                return None
        else:
            try:
                product_html = product_data
                name_elements = product_html.xpath(
                    ".//h2[contains(@class, 'title') or contains(@class, 'software-title')]/text()"
                    " | .//h3[contains(@class, 'title') or contains(@class, 'software-title')]/text()"
                    " | .//a[contains(@class, 'name') or contains(@class, 'software-name')]/text()"
                )
                name = name_elements[0].strip() if name_elements else None
                if not name:
                    logger.warning("Not found the name of product in HTML")
                    return None

                detail_path_elements = product_html.xpath(
                    ".//a[contains(@href, '/marketplace/') or contains(@href, '/software/')]/@href"
                )
                detail_path = detail_path_elements[0] if detail_path_elements else None
                if not detail_path:
                    logger.warning(f"Not found the reference on the page of details for product in {category}")
                    return None
                detail_url = self.BASE_URL + detail_path if not detail_path.startswith('http') else detail_path

                description_elements = product_html.xpath(
                    ".//p[contains(@class, 'description') or contains(@class, 'software-description')]/text()"
                    " | .//div[contains(@class, 'description') or contains(@class, 'software-description')]/text()"
                )
                description = description_elements[0].strip() if description_elements else 'N/A'

                headers = {
                    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
                    'X-API-Key': self.API_KEY or ''
                }
                logger.info(f"Loading the page of details {detail_url}")
                response = self.session.get(detail_url, headers=headers, timeout=10)
                if response.status_code != 200:
                    logger.error(f"Failed to download {detail_url}: Status {response.status_code}")
                    return None
                detail_tree = html.fromstring(response.content)
                price_range_elements = detail_tree.xpath(
                    ".//span[contains(@class, 'price') or contains(@class, 'price-range')]/text()"
                    " | .//p[contains(text(), 'Price') or contains(@class, 'price')]/text()"
                )
                price_range = price_range_elements[0].strip() if price_range_elements else 'N/A'

                if description == 'N/A':
                    description_elements = detail_tree.xpath(
                        ".//p[contains(@class, 'description') or contains(@class, 'software-description')]/text()"
                        " | .//div[contains(@class, 'description') or contains(@class, 'software-description')]/text()"
                    )
                    description = description_elements[0].strip() if description_elements else 'N/A'

                data = {
                    'name': name,
                    'category': category,
                    'price_range': price_range,
                    'description': description
                }
                logger.info(f"Parsed product from HTML: {data['name']}")
                return data
            except (IndexError, requests.RequestException) as e:
                logger.error(f"Error when HTML parsing or downloading page of details: {e}")
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