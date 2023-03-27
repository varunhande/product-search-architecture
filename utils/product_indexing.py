import pymongo
import requests
from datetime import date, datetime
from google.cloud import storage
from helper import *
from user_definition import *

# Function to write data to a Google Cloud Storage bucket
def write(bucket_name, blob_name, text):
    storage_client = storage.Client.from_service_account_json(service_account_key_file)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_string(data=json.dumps(text), content_type='application/json')

# Function to fetch products from ASOS RapidAPI
def get_products(url, querystring, headers, calls=5):
    res = []
    total_pages = 0
    offset = int(querystring["offset"])
    complete = False

    for call in range(1, calls + 1):
        querystring["offset"] = str(offset)
        response = requests.request("GET", url, headers=headers, params=querystring)
        try:
            data = json.loads(response.text)
            n = len(data["products"])
            if n > 0:
                for product in data["products"]:
                    product["categoryId"] = querystring["categoryId"]
                    product["categoryName"] = data["categoryName"]
                    res.append(product)
                offset += n
                if n < 48:
                    print(f"No more products after page {call}!")
                    complete = True
                    break
            else:
                print(f"No more products after page {call}!")
                complete = True
                break
        except:
            print(f"Error encountered at page no {call}")
            print(f"Status code: {response.status_code}")
            print(f"Error message: {json.loads(response.text)}")
            break

        total_pages += 1

    return res, offset, total_pages, complete

# Function to fetch products and write to Google Cloud Storage
def fetch_products():
    # Read configuration details
    config = configparser.ConfigParser()
    dirname = os.path.abspath(os.path.dirname(__file__))
    config.read(dirname + "/config.ini")
    url = config["API"]["PRODUCT_URL"]

    # Set up MongoDB connection
    connection_url = f"mongodb+srv://{mongo_username}:{mongo_password}@{mongo_ip_address}"
    client = pymongo.MongoClient(connection_url)
    db = client[database_name]

    # Check if the categories collection exists in the database
    exists = False
    if categories_collection in db.list_collection_names():
        print(f"{categories_collection} exists!")
        exists = True

    if not exists:
        print(f"{categories_collection} does not exist!")
        categories_indexing()

    # Fetch categories data
    collection = db[categories_collection]
    category_data = list(collection.find({"complete": False}).limit(5))

    # Set up headers for RapidAPI
    headers = {
        "X-RapidAPI-Key": config["API"]["RAPIDAPI_KEY"],
       
        "X-RapidAPI-Host": config["API"]["RAPIDAPI_HOST"],
    }

    # Initialize the products list
    products = []

    # Fetch products for each category
    for category in category_data:
        querystring = {
            "store": "US",
            "offset": category.get("offset", 0),
            "categoryId": category["_id"],
            "limit": "48",
            "country": "US",
            "currency": "USD",
            "sizeSchema": "US",
            "lang": "en-US",
        }
        res, offset, pages, complete = get_products(url, querystring, headers)
        products.extend(res)

        # Update the category data in the database
        try:
            collection.update_one({"_id": category["_id"]}, {"$set": {"offset": offset, "complete": complete}})
        except Exception as e:
            print(e)

    # Write the fetched products data to Google Cloud Storage
    bucket = os.environ.get('GS_BUCKET_NAME')
    product_filename = f"products_{datetime.now().strftime('%H%m%S')}.json"
    write(bucket, str(date.today()) + "/" + product_filename, products)
