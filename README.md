Dataforest Test Project

Setup

Clone the repository.

Install dependencies: pip install -r requirements.txt

Install Playwright browsers: playwright install

Create a .env file with:

DB_HOST=localhost
DB_PORT=5432
DB_NAME=vendr_db
DB_USER=your_user
DB_PASSWORD=your_password
THREAD_COUNT=5
PROCESS_COUNT=3
CDP_ENDPOINT=ws://localhost:9222/devtools/browser/... (optional)

Set up PostgreSQL database and ensure it's running.

Running

Task 1 (Vendr.com scraper, branch task1):

python -m task1.scraper

Task 2 (Books.toScrape.com scraper, branch task2):

python -m task2.scraper

Testing

Run unit tests:

python -m unittest discover tests

Notes

Task 1: Adjust XPath selectors in task1/scraper.py based on Vendr.com's HTML structure.

Task 2: Output is saved to books.json. Add DB storage if required.