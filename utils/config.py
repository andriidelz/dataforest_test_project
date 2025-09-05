from dotenv import load_dotenv
import os

def load_config():
    """Load configuration from .env file."""
    load_dotenv()
    return {
        'DB_HOST': os.getenv('DB_HOST', 'localhost'),
        'DB_PORT': os.getenv('DB_PORT', '5432'),
        'DB_NAME': os.getenv('DB_NAME', 'vendr_db'),
        'DB_USER': os.getenv('DB_USER', 'user'),
        'DB_PASSWORD': os.getenv('DB_PASSWORD', 'pass'),
        'THREAD_COUNT': os.getenv('THREAD_COUNT', '5'),
        'PROCESS_COUNT': os.getenv('PROCESS_COUNT', '3'),
        'CDP_ENDPOINT': os.getenv('CDP_ENDPOINT')
    }