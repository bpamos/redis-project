import redis
import json

# Redis connection details
REDIS_HOST = 'redis-11094.c325.us-east-1-4.ec2.cloud.redislabs.com'
REDIS_PORT = 11094
REDIS_PASSWORD = 'YourPasswordHere'  # Replace with your actual password, if needed

# Connect to Redis
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD)

# Flush the Redis database
r.flushdb()

# Create index with JSON schema
index_schema = """\
FT.CREATE idx:product ON JSON \
PREFIX 1 product: \
SCHEMA \
$.actual_price AS actual_price NUMERIC \
$.average_rating AS average_rating NUMERIC \
$.brand AS brand TEXT \
$.category AS category TAG \
$.crawled_at AS crawled_at TEXT \
$.description AS description TEXT \
$.discount AS discount TEXT \
$.out_of_stock AS out_of_stock TAG \
$.pid AS pid TEXT \
$.seller AS seller TEXT \
$.selling_price AS selling_price NUMERIC \
$.sub_category AS sub_category TAG \
$.title AS title TEXT \
$.url AS url TEXT \
"""

# Execute the index creation command
r.execute_command(index_schema)

print("Index 'productIdx' created successfully.")

# Function to convert JSON data into cleaned format
def convert_to_cleaned_json(data, key_prefix):
    cleaned_data = []
    for i, item in enumerate(data):
        cleaned_item = {
            "key": f"{key_prefix}:{i}",
            **item
        }
        for field in ['actual_price', 'average_rating', 'selling_price']:
            value = cleaned_item.get(field, "")
            if value.strip():  # Check if the value is not empty
                cleaned_item[field] = float(value.replace(',', '')) if ',' in value else float(value)
            else:
                cleaned_item[field] = 0.0  # Assign a default value
        cleaned_data.append(cleaned_item)
    return cleaned_data

# Load data from JSON file
with open('flipkart_fashion_products_dataset.json', 'r') as file:
    data = json.load(file)

# Convert JSON data to cleaned format
cleaned_data = convert_to_cleaned_json(data, "product")

# Define batch size
batch_size = 200

# Run JSON.SET on each item with pipelining for every 100 items
batch_size = 200
for i in range(0, len(cleaned_data), batch_size):
    with r.pipeline() as pipe:
        for j, item in enumerate(cleaned_data[i:i+batch_size], start=i):
            key = f"product:{j}"
            value = json.dumps(item)
            pipe.execute_command('JSON.SET', key, '.', value)
        pipe.execute()


print("Data loaded into Redis successfully.")


