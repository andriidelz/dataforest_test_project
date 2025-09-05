import unittest
from lxml import html
from task1.scraper import VendrScraper

class TestVendrScraper(unittest.TestCase):
    def setUp(self):
        self.scraper = VendrScraper(['DevOps'])

    def test_parse_product(self):
        sample_html = """
        <div class='vendor-item'>
            <h2>Sample Product</h2>
            <span class='price-range'>$100-$500</span>
            <p class='description'>A test product.</p>
        </div>
        """
        product_html = html.fromstring(sample_html)
        result = self.scraper.parse_product(product_html, 'DevOps')
        expected = {
            'name': 'Sample Product',
            'category': 'DevOps',
            'price_range': '$100-$500',
            'description': 'A test product.'
        }
        self.assertEqual(result, expected)

if __name__ == '__main__':
    unittest.main()