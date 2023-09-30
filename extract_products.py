import os
import json

from tqdm import tqdm
from dotenv import load_dotenv

from argparse import ArgumentParser
from pymongo.server_api import ServerApi
from pymongo.mongo_client import MongoClient


load_dotenv()

DATABASE_URI = str(os.getenv("DATABASE_URI"))
DATABASE_NAME = str(os.getenv("DATABASE_NAME"))
DATABASE_PRODUCTS_COLLECTION = str(os.getenv("DATABASE_PRODUCTS_COLLECTION"))

CLIENT = MongoClient(DATABASE_URI, server_api=ServerApi("1"))
DB = CLIENT[DATABASE_NAME]
PRODUCTS_COLLECTION = DB[DATABASE_PRODUCTS_COLLECTION]

parser = ArgumentParser()
parser.add_argument("-p", "--jsonpath", type=str, required=True)
opt = parser.parse_args()

DATA = json.load(open(opt.jsonpath))

process = tqdm(
    DATA, desc="Extracting...", ncols=100, colour="green"
)
for row in process:
    # if not PRODUCTS_COLLECTION.find_one({"_id": row.get("_id")}):
    PRODUCTS_COLLECTION.insert_one(row)
    

CLIENT.close()
