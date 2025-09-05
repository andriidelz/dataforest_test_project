import unittest
from unittest.mock import Mock
from task2.scraper import BookScraper

class TestBookScraper(unittest.TestCase):
    def setUp(self):
        self.scraper = BookScraper(process_count=1)

    def test_collect_data(self):
        mock_page = Mock()
        mock_page.locator.side_effect = lambda selector: Mock(
            text_content=lambda: {
                'div.product_main h1': 'Sample Book',
                'p.price_color': '£19.99',
                'p.availability': 'In stock',
                '#product_description ~ p': 'A sample description.'
            }.get(selector, ''),
            get_attribute=lambda attr: {'class': 'star-rating Five', 'src': '../../sample.jpg'}.get(attr, ''),
            count=lambda: 1 if selector == '#product_description' else 0
        )
        mock_page.locator("table.table-striped tr").all.return_value = [
            Mock(locator=lambda x: Mock(text_content=lambda: 'UPC' if x == 'th' else '12345'))
        ]
        result = self.scraper.collect_data(mock_page, 'Fiction')
        expected = {
            'title': 'Sample Book',
            'category': 'Fiction',
            'price': '£19.99',
            'rating': 'Five',
            'stock_availability': 'In stock',
            'image_url': 'https://books.toscrape.com/sample.jpg',
            'description': 'A sample description.',
            'product_information': {'UPC': '12345'}
        }
        self.assertEqual(result, expected)

if __name__ == '__main__':
    unittest.main()