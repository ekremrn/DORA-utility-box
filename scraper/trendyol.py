import os

from tqdm import tqdm
from dotenv import load_dotenv
from argparse import ArgumentParser

from pymongo.server_api import ServerApi
from pymongo.mongo_client import MongoClient

from dora_scraper.utils.browser import get_soup
from dora_scraper.platforms.trendyol import (
    extract_category_links,
    generate_pagination_urls,
    extract_products_from_category_page,
)

load_dotenv()  

URI = str(os.getenv("DORA_MONGO_DATABASE_URI"))
NAME = str(os.getenv("DORA_MONGO_DATABASE_NAME"))
COLLECTION_NAME = str(os.getenv("DORA_MONGO_DATABASE_PRODUCTS_COLLECTION_NAME"))
CLIENT = MongoClient(URI, server_api=ServerApi("1"))
DB = CLIENT[NAME]
COLLECTION = DB[COLLECTION_NAME]


parser = ArgumentParser()
parser.add_argument("-c", "--categories", nargs="+", default=["kadın"])
opt = parser.parse_args()

soup = get_soup("https://www.trendyol.com/")
category_links = extract_category_links(soup, opt.categories)

# Category Link Collect Process
page_urls = list()
for category_link in category_links:
    page_urls.extend(generate_pagination_urls(category_link))


progress = tqdm(page_urls, desc="trendyol", ncols=100, colour="green")
for url in progress:
    soup = get_soup(url)
    products = extract_products_from_category_page(soup)

    for product in products:
        id = product.get("_id")
        if not COLLECTION.find_one({"_id": id}):
            COLLECTION.insert_one(product)

